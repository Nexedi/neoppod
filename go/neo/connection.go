// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package neo
// Connection management

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/someonegg/gocontainer/rbuf"

	"lab.nexedi.com/kirr/go123/xbytes"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
)

// NodeLink is a node-node link in NEO
//
// A node-node link represents bidirectional symmetrical communication
// channel in between 2 NEO nodes. The link provides service for packets
// exchange and for multiplexing several communication connections on
// top of the node-node link.
//
// New connection can be created with .NewConn() . Once connection is
// created and data is sent over it, on peer's side another corresponding
// new connection can be accepted via .Accept(), and all further communication
// send/receive exchange will be happening in between those 2 connections.
//
// A NodeLink has to be explicitly closed, once it is no longer needed.
//
// It is safe to use NodeLink from multiple goroutines simultaneously.
type NodeLink struct {
	peerLink net.Conn // raw conn to peer

	connMu     sync.Mutex
	connTab    map[uint32]*Conn // connId -> Conn associated with connId
	nextConnId uint32           // next connId to use for Conn initiated by us

	serveWg sync.WaitGroup	// for serve{Send,Recv}
	acceptq chan *Conn	// queue of incoming connections for Accept
	txq	chan txReq	// tx requests from Conns go via here
				// (rx packets are routed to Conn.rxq)

	axdown  chan struct{}	// ready when accept is marked as no longer operational
	axdown1 sync.Once	// CloseAccept may be called severall times

	down     chan struct{}  // ready when NodeLink is marked as no longer operational
	downOnce sync.Once      // shutdown may be due to both Close and IO error
	downWg   sync.WaitGroup // for activities at shutdown
	errClose error          // error got from peerLink.Close

	errMu    sync.Mutex
	errRecv	 error		// error got from recvPkt on shutdown

	axclosed atomic32	// whether CloseAccept was called
	closed   atomic32	// whether Close was called

	rxbuf    rbuf.RingBuf	// buffer for reading from peerLink
}

// Conn is a connection established over NodeLink
//
// Data can be sent and received over it.
// Once connection is no longer needed it has to be closed.
//
// It is safe to use Conn from multiple goroutines simultaneously.
type Conn struct {
	link      *NodeLink
	connId    uint32

	rxq	   chan *PktBuf	 // received packets for this Conn go here
	rxqActive  atomic32	 // 1 while serveRecv is doing `rxq <- ...`
	rxdownFlag atomic32	 // 1 when RX is marked no longer operational

	rxerrOnce sync.Once     // rx error is reported only once - then it is link down or closed XXX !light?
//	errMsg	  *Error	// error message for peer if rx is down	XXX try to do without it

	// there are two modes a Conn could be used:
	// - full mode - where full Conn functionality is working, and
	// - light mode - where only subset functionality is working
	//
	// the light mode is used to implement Recv1 & friends - there any
	// connection is used max to send and/or receive only 1 packet and then
	// has to be reused for efficiency ideally without reallocating anything.
	//
	// everything below is used during !light mode only.

	rxdown     chan struct{} // ready when RX is marked no longer operational
	rxdownOnce sync.Once	 // ----//----	XXX review
	rxclosed   atomic32	 // whether CloseRecv was called

	txerr     chan error	 // transmit results for this Conn go back here

	txdown     chan struct{} // ready when Conn TX is marked as no longer operational
	txdownOnce sync.Once	 // tx shutdown may be called by both Close and nodelink.shutdown
	txclosed   atomic32	 // whether CloseSend was called


	// closing Conn is shutdown + some cleanup work to remove it from
	// link.connTab including arming timers etc. Let this work be spawned only once.
	// (for Conn.Close to be valid called several times)
	closeOnce sync.Once
}

var ErrLinkClosed   = errors.New("node link is closed")	// operations on closed NodeLink
var ErrLinkDown     = errors.New("node link is down")	// e.g. due to IO error
var ErrLinkNoListen = errors.New("node link is not listening for incoming connections")
var ErrLinkManyConn = errors.New("too many opened connections")
var ErrClosedConn   = errors.New("connection is closed")

// LinkError is returned by NodeLink operations
type LinkError struct {
	Link *NodeLink
	Op   string
	Err  error
}

// ConnError is returned by Conn operations
type ConnError struct {
	Link   *NodeLink
	ConnId uint32	 // NOTE Conn's are reused - cannot use *Conn here
	Op     string
	Err    error
}

// LinkRole is a role an end of NodeLink is intended to play
type LinkRole int
const (
	LinkServer LinkRole = iota // link created as server
	LinkClient                 // link created as client

	// for testing:
	linkNoRecvSend LinkRole = 1 << 16 // do not spawn serveRecv & serveSend
	linkFlagsMask  LinkRole = (1<<32 - 1) << 16
)

// newNodeLink makes a new NodeLink from already established net.Conn
//
// Role specifies how to treat our role on the link - either as client or
// server. The difference in between client and server roles is in:
//
//    how connection ids are allocated for connections initiated at our side:
//    there is no conflict in identifiers if one side always allocates them as
//    even (server) and its peer as odd (client).
//
// Usually server role should be used for connections created via
// net.Listen/net.Accept and client role for connections created via net.Dial.
//
// Though it is possible to wrap just-established raw connection into NodeLink,
// users should always use Handshake which performs protocol handshaking first.
func newNodeLink(conn net.Conn, role LinkRole) *NodeLink {
	var nextConnId uint32
	switch role &^ linkFlagsMask {
	case LinkServer:
		nextConnId = 0 // all initiated by us connId will be even
	case LinkClient:
		nextConnId = 1 // ----//---- odd
	default:
		panic("invalid conn role")
	}

	nl := &NodeLink{
		peerLink:   conn,
		connTab:    map[uint32]*Conn{},
		nextConnId: nextConnId,
		acceptq:    make(chan *Conn),	// XXX +buf
		txq:        make(chan txReq),
		axdown:     make(chan struct{}),
		down:       make(chan struct{}),
	}
	if role&linkNoRecvSend == 0 {
		nl.serveWg.Add(2)
		go nl.serveRecv()
		go nl.serveSend()
	}
	return nl
}

// connPool is freelist for Conn
// XXX make it per-link?
var connPool = sync.Pool{New: func() interface{} {
	return &Conn{
		rxq:    make(chan *PktBuf, 1),	// NOTE non-blocking - see serveRecv XXX +buf
		txerr:  make(chan error, 1),	// NOTE non-blocking - see Conn.Send
		txdown: make(chan struct{}),
		rxdown: make(chan struct{}),
	}
}}

// connAlloc allocates Conn from freelist
func (link *NodeLink) connAlloc(connId uint32) *Conn {
	c := connPool.Get().(*Conn)
	c.reinit()
	c.link = link
	c.connId = connId
	return c
}

// release releases connection to freelist
func (c *Conn) release() {
	c.reinit()	// XXX just in case
	connPool.Put(c)
}

// reinit reinitializes connection after reallocating it from freelist
func (c *Conn) reinit() {
	c.link = nil
	c.connId = 0
	// .rxq		- set initially; does not change
	c.rxqActive.Set(0)	// XXX store relaxed?
	c.rxdownFlag.Set(0)	// ----//----

	c.rxerrOnce = sync.Once{}	// XXX ok?


	// XXX vvv not strictly needed for light mode?
	ensureOpen(&c.rxdown)
	c.rxdownOnce = sync.Once{}	// XXX ok?
	c.rxclosed.Set(0)

	// .txerr	- never closed

	ensureOpen(&c.txdown)
	c.txdownOnce = sync.Once{}	// XXX ok?
	c.txclosed.Set(0)

	c.closeOnce = sync.Once{}	// XXX ok?
}

// ensureOpen make sure *ch stays non-closed chan struct{} for signalling.
// if it is already closed, the channel is remade.
func ensureOpen(ch *chan struct{}) {
	select {
	case <-*ch:
		*ch = make(chan struct{})
	default:
		// not closed - nothing to do
	}
}

// newConn creates new Conn with id=connId and registers it into connTab.
// Must be called with connMu held.
func (nl *NodeLink) newConn(connId uint32) *Conn {
	c := nl.connAlloc(connId)
	nl.connTab[connId] = c
	return c
}

// NewConn creates new connection on top of node-node link.
func (nl *NodeLink) NewConn() (*Conn, error) {
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	if nl.connTab == nil {
		if nl.closed.Get() != 0 {
			return nil, nl.err("newconn", ErrLinkClosed)
		}
		return nil, nl.err("newconn", ErrLinkDown)
	}

	// nextConnId could wrap around uint32 limits - find first free slot to
	// not blindly replace existing connection
	for i := uint32(0) ;; i++ {
		_, exists := nl.connTab[nl.nextConnId]
		if !exists {
			break
		}
		nl.nextConnId += 2

		if i > math.MaxUint32 / 2 {
			return nil, nl.err("newconn", ErrLinkManyConn)
		}
	}

	c := nl.newConn(nl.nextConnId)
	nl.nextConnId += 2

	return c, nil
}

// shutdownAX marks acceptq as no longer operational and interrupts Accept.
func (link *NodeLink) shutdownAX() {
	link.axdown1.Do(func() {
		close(link.axdown)

		// dequeue all connections already queued in link.acceptq
		// (once serveRecv sees link.axdown it won't try to put new connections into
		//  link.acceptq, but something finite could be already there)
loop:
		for {
			select {
			case conn := <-link.acceptq:
				// serveRecv already put at least 1 packet into conn.rxq before putting
				// conn into .acceptq - shutting it down will send the error to peer.
				conn.shutdownRX(errConnRefused)

				link.connMu.Lock()
				delete(link.connTab, conn.connId)
				link.connMu.Unlock()

			default:
				break loop
			}
		}
	})
}

// shutdown closes raw link to peer and marks NodeLink as no longer operational.
// it also shutdowns all opened connections over this node link.
func (nl *NodeLink) shutdown() {
	nl.shutdownAX()
	nl.downOnce.Do(func() {
		close(nl.down)

		// close actual link to peer. this will wakeup {send,recv}Pkt
		// NOTE we need it here so that e.g. aborting on error in serveSend wakes up serveRecv
		nl.errClose = nl.peerLink.Close()

		nl.downWg.Add(1)
		go func() {
			defer nl.downWg.Done()

			// wait for serve{Send,Recv} to complete before shutting connections down
			//
			// we have to do it so that e.g. serveSend has chance
			// to return last error from sendPkt to requester.
			nl.serveWg.Wait()

			// clear + mark down .connTab + shutdown all connections
			nl.connMu.Lock()
			connTab := nl.connTab
			nl.connTab = nil
			nl.connMu.Unlock()

			// conn.shutdown() outside of link.connMu lock
			for _, conn := range connTab {
				conn.shutdown()
			}
		}()
	})
}

// CloseAccept instructs node link to not accept incoming connections anymore.
//
// Any blocked Accept() will be unblocked and return error.
// The peer will receive "connection refused" if it tries to connect after.
//
// It is safe to call CloseAccept several times.
func (link *NodeLink) CloseAccept() {
	link.axclosed.Set(1)
	link.shutdownAX()
}

// Close closes node-node link.
//
// All blocking operations - Accept and IO on associated connections
// established over node link - are automatically interrupted with an error.
// Underlying raw connection is closed.
// It is safe to call Close several times.
func (nl *NodeLink) Close() error {
	nl.axclosed.Set(1)
	nl.closed.Set(1)
	nl.shutdown()
	nl.downWg.Wait()
	return nl.err("close", nl.errClose)
}

// shutdown marks connection as no longer operational and interrupts Send and Recv.
func (c *Conn) shutdown() {
	c.shutdownTX()
	c.shutdownRX(errConnClosed)
}

// shutdownTX marks TX as no longer operational (?) and interrupts Send.
func (c *Conn) shutdownTX() {
	c.txdownOnce.Do(func() {
		close(c.txdown)
	})
}

// shutdownRX marks .rxq as no loner operational and interrupts Recv.
func (c *Conn) shutdownRX(errMsg *Error) {
	c.rxdownOnce.Do(func() {
		close(c.rxdown)	// wakeup Conn.Recv
		c.downRX(errMsg)
	})
}

// downRX marks .rxq as no longer operational.
//
// used in shutdownRX and separately to mark RX down for light Conns.
func (c *Conn) downRX(errMsg *Error) {
	// let serveRecv know RX is down for this connection
	c.rxdownFlag.Set(1) // XXX cmpxchg and return if already down?

	// drain all packets from .rxq:
	// - something could be already buffered there
	// - serveRecv could start writing rxq at the same time we set rxdownFlag; we derace it.
	i := 0
	for {
		// we set .rxdownFlag=1 above.
		// now if serveRecv is outside `.rxq <- ...` critical section we know it is either:
		// - before it	-> it will eventually see .rxdownFlag=1 and wont send pkt ro rxq.
		// - after it	-> it already sent pkt to rxq and won't touch
		//		   rxq until next packet (where it will hit "before it").
		//
		// when serveRecv stopped sending we know we are done draining when rxq is empty.
		if c.rxqActive.Get() == 0 && len(c.rxq) == 0 {
			break
		}

		select {
		case <-c.rxq:
			i++

		default:
			// ok - continue spinning
		}
	}

	// if something was queued already there - reply "connection closed"
	if i != 0 {
		go c.link.replyNoConn(c.connId, errMsg)
	}
}


// time to keep record of a closed connection so that we can properly reply
// "connection closed" if a packet comes in with same connID.
var connKeepClosed = 1*time.Minute

// CloseRecv closes reading end of connection.
//
// Any blocked Recv*() will be unblocked and return error.
// The peer will receive "connection closed" if it tries to send anything after.
//
// It is safe to call CloseRecv several times.
func (c *Conn) CloseRecv() {
	c.rxclosed.Set(1)
	c.shutdownRX(errConnClosed)
}

// Close closes connection.
//
// Any blocked Send*() or Recv*() will be unblocked and return error.
//
// NOTE for Send() - once transmission was started - it will complete in the
// background on the wire not to break node-node link framing.
//
// It is safe to call Close several times.
func (c *Conn) Close() error {
	nl := c.link
	c.closeOnce.Do(func() {
		c.rxclosed.Set(1)
		c.txclosed.Set(1)
		c.shutdown()

		// adjust link.connTab
		var tmpclosed *Conn
		nl.connMu.Lock()
		if nl.connTab != nil {
			// connection was initiated by us - simply delete - we always
			// know if a packet comes to such connection - it is closed.
			//
			// XXX checking vvv should be possible without connMu lock
			if c.connId == nl.nextConnId % 2 {
				delete(nl.connTab, c.connId)

			// connection was initiated by peer which we accepted - put special
			// "closed" connection into connTab entry for some time to reply
			// "connection closed" if another packet comes to it.
			//
			// ( we cannot reuse same connection since after it is marked as
			//   closed Send refuses to work )
			} else {
				// delete(nl.connTab, c.connId)
				// XXX vvv was temp. disabled - costs a lot in 1req=1conn model

				// c implicitly goes away from connTab
				tmpclosed = nl.newConn(c.connId)
			}
		}
		nl.connMu.Unlock()

		if tmpclosed != nil {
			tmpclosed.shutdownRX(errConnClosed)

			time.AfterFunc(connKeepClosed, func() {
				nl.connMu.Lock()
				delete(nl.connTab, c.connId)
				nl.connMu.Unlock()
			})
		}
	})

	return nil
}

// ---- receive ----

// errAcceptShutdownAX returns appropriate error when link.axdown is found ready in Accept
func (link *NodeLink) errAcceptShutdownAX() error {
	switch {
	case link.closed.Get() != 0:
		return ErrLinkClosed

	case link.axclosed.Get() != 0:
		return ErrLinkNoListen

	default:
		// XXX ok? - recheck
		return ErrLinkDown
	}
}

// Accept waits for and accepts incoming connection on top of node-node link.
func (nl *NodeLink) Accept(/*ctx context.Context*/) (*Conn, error) {
	select {
	case <-nl.axdown:
		return nil, nl.err("accept", nl.errAcceptShutdownAX())

	case c := <-nl.acceptq:
		return c, nil

// XXX for long-lived links - better to propagate ctx cancel to link.Close to
// lower cases that are run at every select.
//
// XXX see xio.CloseWhenDone() for helper for this.
/*
	// XXX ctx cancel tests
	case <-ctx.Done():
		return nil, ctx.Err()
*/
	}
}

// errRecvShutdown returns appropriate error when c.rxdown is found ready in recvPkt
func (c *Conn) errRecvShutdown() error {
	switch {
	case c.rxclosed.Get() != 0:
		return ErrClosedConn

	case c.link.closed.Get() != 0:
		return ErrLinkClosed

	default:
		// we have to check what was particular RX error on nodelink shutdown
		// only do that once - after reporting RX error the first time
		// tell client the node link is no longer operational.
		var err error
		c.rxerrOnce.Do(func() {
			c.link.errMu.Lock()
			err = c.link.errRecv
			c.link.errMu.Unlock()
		})
		if err == nil {
			err = ErrLinkDown
		}
		return err
	}
}

// recvPkt receives raw packet from connection
func (c *Conn) recvPkt() (*PktBuf, error) {
	select {
	case <-c.rxdown:
		return nil, c.err("recv", c.errRecvShutdown())

	case pkt := <-c.rxq:
		return pkt, nil
	}
}

// serveRecv handles incoming packets routing them to either appropriate
// already-established connection or, if node link is accepting incoming
// connections, to new connection put to accept queue.
func (nl *NodeLink) serveRecv() {
	defer nl.serveWg.Done()
	for {
		// receive 1 packet
		// XXX if nl.peerLink was just closed by tx->shutdown we'll get ErrNetClosing
		pkt, err := nl.recvPkt()
		//fmt.Printf("\n%p recvPkt -> %v, %v\n", nl, pkt, err)
		if err != nil {
			// on IO error framing over peerLink becomes broken
			// so we shut down node link and all connections over it.

			nl.errMu.Lock()
			nl.errRecv = err
			nl.errMu.Unlock()

			nl.shutdown()
			return
		}

		// pkt.ConnId -> Conn
		connId := ntoh32(pkt.Header().ConnId)
		accept := false

		nl.connMu.Lock()

		// connTab is never nil here - because shutdown before
		// resetting it waits for us to finish.
		conn := nl.connTab[connId]

		if conn == nil {
			// message with connid for a stream initiated by peer
			// it will be considered to be accepted (not if .axdown)
			if connId % 2 != nl.nextConnId % 2 {
				accept = true
				conn = nl.newConn(connId)
			}

			// else it is message with connid that should be initiated by us
			// leave conn=nil - we'll reply errConnClosed
		}

		nl.connMu.Unlock()

		//fmt.Printf("%p\tconn: %v\n", nl, conn)
		if conn == nil {
			// see ^^^ "message with connid that should be initiated by us"
			go nl.replyNoConn(connId, errConnClosed)
			continue
		}

		// route packet to serving goroutine handler
		//
		// TODO backpressure when Recv is not keeping up with Send on peer side?
		//      (not to let whole link starve because of one connection)
		//
		// NOTE rxq must be buffered with at least 1 element so that
		// queuing pkt succeeds for incoming connection that is not yet
		// there in acceptq.
		conn.rxqActive.Set(1)
		rxdown := conn.rxdownFlag.Get() != 0
		if !rxdown {
			conn.rxq <- pkt
		}
		conn.rxqActive.Set(0)

		//fmt.Printf("%p\tconn.rxdown: %v\taccept: %v\n", nl, rxdown, accept)


		// conn exists but rx is down - "connection closed"
		// (this cannot happen for newly accepted connection)
		if rxdown {
			go nl.replyNoConn(connId, errConnClosed)
			continue
		}

		// this packet established new connection - try to accept it
		if accept {
			// don't even try `link.acceptq <- ...` if link.axdown is ready
			// ( else since select is picking random ready variant Accept/serveRecv
			//   could receive something on axdown Link sometimes )
			axdown := false
			select {
			case <-nl.axdown:
				axdown = true

			default:
				// ok
			}

			// put conn to .acceptq
			if !axdown {
				// XXX can avoid select here if shutdownAX cares to drain acceptq (?)
				select {
				case <-nl.axdown:
					axdown = true

				case nl.acceptq <- conn:
					//fmt.Printf("%p\t.acceptq <- conn  ok\n", nl)
					// ok
				}
			}

			// we are not accepting the connection
			if axdown {
				conn.shutdownRX(errConnRefused)
				nl.connMu.Lock()
				delete(nl.connTab, conn.connId)
				nl.connMu.Unlock()
			}
		}

		//fmt.Printf("%p\tafter accept\n", nl)
/*
		// XXX goes away in favour of .rxdownFlag; reasons
		// - no need to reallocate rxdown for light conn
		// - no select

		// don't even try `conn.rxq <- ...` if conn.rxdown is ready
		// ( else since select is picking random ready variant Recv/serveRecv
		//   could receive something on rxdown Conn sometimes )
		rxdown := false
		select {
		case <-conn.rxdown:
			rxdown = true
		default:
			// ok
		}

		// route packet to serving goroutine handler
		//
		// TODO backpressure when Recv is not keeping up with Send on peer side?
		//      (not to let whole link starve because of one connection)
		//
		// NOTE rxq must be buffered with at least 1 element so that
		// queuing pkt succeeds for incoming connection that is not yet
		// there in acceptq.
		if !rxdown {
			// XXX can avoid select here: if conn closer cares to drain rxq (?)
			select {
			case <-conn.rxdown:
				rxdown = true

			case conn.rxq <- pkt:
				// ok
			}
		}
*/
	}
}

// ---- network replies for closed / refused connections ----

var errConnClosed  = &Error{PROTOCOL_ERROR, "connection closed"}
var errConnRefused = &Error{PROTOCOL_ERROR, "connection refused"}

// replyNoConn sends error message to peer when a packet was sent to closed / nonexistent connection
func (link *NodeLink) replyNoConn(connId uint32, errMsg Msg) {
	//fmt.Printf("%s .%d: -> replyNoConn %v\n", link, connId, errMsg)
	link.sendMsg(connId, errMsg) // ignore errors
	//fmt.Printf("%s .%d: replyNoConn(%v) -> %v\n", link, connId, errMsg, err)
}

// ---- transmit ----

// txReq is request to transmit a packet. Result error goes back to errch
type txReq struct {
	pkt   *PktBuf
	errch chan error
}

// errSendShutdown returns appropriate error when c.txdown is found ready in Send
func (c *Conn) errSendShutdown() error {
	switch {
	case c.txclosed.Get() != 0:
		return ErrClosedConn

	// the only other error possible besides Conn being .Close()'ed is that
	// NodeLink was closed/shutdowned itself - on actual IO problems corresponding
	// error is delivered to particular Send that caused it.

	case c.link.closed.Get() != 0:
		return ErrLinkClosed

	default:
		return ErrLinkDown
	}
}

// sendPkt sends raw packet via connection.
//
// on success pkt is freed.
func (c *Conn) sendPkt(pkt *PktBuf) error {
	err := c.sendPkt2(pkt)
	return c.err("send", err)
}

func (c *Conn) sendPkt2(pkt *PktBuf) error {
	// connId must be set to one associated with this connection
	if pkt.Header().ConnId != hton32(c.connId) {
		panic("Conn.sendPkt: connId wrong")
	}

	var err error

	select {
	case <-c.txdown:
		return c.errSendShutdown()

	case c.link.txq <- txReq{pkt, c.txerr}:
		select {
		// tx request was sent to serveSend and is being transmitted on the wire.
		// the transmission may block for indefinitely long though and
		// we cannot interrupt it as the only way to interrupt is
		// .link.Close() which will close all other Conns.
		//
		// That's why we are also checking for c.txdown while waiting
		// for reply from serveSend (and leave pkt to finish transmitting).
		//
		// NOTE after we return straight here serveSend won't be later
		// blocked on c.txerr<- because that backchannel is a non-blocking one.
		case <-c.txdown:

			// also poll c.txerr here because: when there is TX error,
			// serveSend sends to c.txerr _and_ closes c.txdown .
			// We still want to return actual transmission error to caller.
			select {
			case err = <-c.txerr:
				return err
			default:
				return c.errSendShutdown()
			}

		case err = <-c.txerr:
			return err
		}
	}
}

// serveSend handles requests to transmit packets from client connections and
// serially executes them over associated node link.
func (nl *NodeLink) serveSend() {
	defer nl.serveWg.Done()
	for {
		select {
		case <-nl.down:
			return

		case txreq := <-nl.txq:
			// XXX if n.peerLink was just closed by rx->shutdown we'll get ErrNetClosing
			err := nl.sendPkt(txreq.pkt)
			//fmt.Printf("sendPkt -> %v\n", err)

			// FIXME if several goroutines call conn.Send
			// simultaneously - c.txerr even if buffered(1) will be
			// overflown and thus deadlock here.
			txreq.errch <- err

			// on IO error framing over peerLink becomes broken
			// so we shut down node link and all connections over it.
			//
			// XXX dup wrt sendPktDirect
			// XXX move to link.sendPkt?
			if err != nil {
				nl.shutdown()
				return
			}
		}
	}
}

// ---- transmit direct mode ----

// serveSend is not strictly needed - net.Conn.Write already can be used by multiple
// goroutines simultaneously and works atomically; (same for Conn.Read etc - see pool.FD)
// https://github.com/golang/go/blob/go1.9-3-g099336988b/src/net/net.go#L109
// https://github.com/golang/go/blob/go1.9-3-g099336988b/src/internal/poll/fd_unix.go#L14
//
// thus the only reason we use serveSend is so that Conn.Close can "interrupt" Conn.Send via .txdown .
// however this adds overhead and is not needed in light mode.

// sendPktDirect sends raw packet with appropriate connection ID directly via link.
func (c *Conn) sendPktDirect(pkt *PktBuf) error {
	// set pkt connId associated with this connection
	pkt.Header().ConnId = hton32(c.connId)

	// XXX if n.peerLink was just closed by rx->shutdown we'll get ErrNetClosing
	err := c.link.sendPkt(pkt)
	//fmt.Printf("sendPkt -> %v\n", err)

	// on IO error framing over peerLink becomes broken
	// so we shut down node link and all connections over it.
	//
	// XXX dup wrt serveSend
	// XXX move to link.sendPkt?
	if err != nil {
		c.link.shutdown()
	}

	return err
}


// ---- raw IO ----

const dumpio = false

// sendPkt sends raw packet to peer.
//
// tx error, if any, is returned as is and is analyzed in serveSend.
//
// XXX pkt should be freed always or only on error?
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	if dumpio {
		// XXX -> log
		fmt.Printf("%v > %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
		//defer fmt.Printf("\t-> sendPkt err: %v\n", err)
	}

	// NOTE Write writes data in full, or it is error
	_, err := nl.peerLink.Write(pkt.Data)
	pkt.Free()
	return err
}

var ErrPktTooBig = errors.New("packet too big")

// recvPkt receives raw packet from peer.
//
// rx error, if any, is returned as is and is analyzed in serveRecv
func (nl *NodeLink) recvPkt() (*PktBuf, error) {
	pkt := pktAlloc(4096)
	// len=4K but cap can be more since pkt is from pool - use all space to buffer reads
	// XXX vvv -> pktAlloc() ?
	data := pkt.Data[:cap(pkt.Data)]

	n := 0 // number of pkt bytes obtained so far

	// next packet could be already prefetched in part by previous read
	if nl.rxbuf.Len() > 0 {
		δn, _ := nl.rxbuf.Read(data[:pktHeaderLen])
		n += δn
	}

	// first read to read pkt header and hopefully rest of packet in 1 syscall
	if n < pktHeaderLen {
		δn, err := io.ReadAtLeast(nl.peerLink, data[n:], pktHeaderLen - n)
		if err != nil {
			return nil, err
		}
		n += δn
	}

	pkth := pkt.Header()

	pktLen := int(pktHeaderLen + ntoh32(pkth.MsgLen)) // whole packet length
	if pktLen > pktMaxSize {
		return nil, ErrPktTooBig
	}

	// resize data if we don't have enough room in it
	data = xbytes.Resize(data, pktLen)
	data = data[:cap(data)]

	// we might have more data already prefetched in rxbuf
	if nl.rxbuf.Len() > 0 {
		δn, _ := nl.rxbuf.Read(data[n:pktLen])
		n += δn
	}

	// read rest of pkt data, if we need to
	if n < pktLen {
		δn, err := io.ReadAtLeast(nl.peerLink, data[n:], pktLen - n)
		if err != nil {
			return nil, err
		}
		n += δn
	}

	// put overread data into rxbuf for next reader
	if n > pktLen {
		nl.rxbuf.Write(data[pktLen:n])
	}

	// fixup data/pkt
	data = data[:n]
	pkt.Data = data

	if /* XXX temp show only tx */ true && dumpio {
		// XXX -> log
		fmt.Printf("%v < %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
	}

	return pkt, nil
}


// ---- Handshake ----

// Handshake performs NEO protocol handshake just after raw connection between 2 nodes was established.
// On success raw connection is returned wrapped into NodeLink.
// On error raw connection is closed.
func Handshake(ctx context.Context, conn net.Conn, role LinkRole) (nl *NodeLink, err error) {
	err = handshake(ctx, conn, ProtocolVersion)
	if err != nil {
		return nil, err
	}

	// handshake ok -> NodeLink
	return newNodeLink(conn, role), nil
}

// HandshakeError is returned when there is an error while performing handshake
type HandshakeError struct {
	// XXX just keep .Conn? (but .Conn can be closed)
	LocalAddr  net.Addr
	RemoteAddr net.Addr
	Err        error
}

func (e *HandshakeError) Error() string {
	return fmt.Sprintf("%s - %s: handshake: %s", e.LocalAddr, e.RemoteAddr, e.Err.Error())
}

func handshake(ctx context.Context, conn net.Conn, version uint32) (err error) {
	errch := make(chan error, 2)

	// tx handshake word
	txWg := sync.WaitGroup{}
	txWg.Add(1)
	go func() {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], version) // XXX -> hton32 ?
		_, err := conn.Write(b[:])
		// XXX EOF -> ErrUnexpectedEOF ?
		errch <- err
		txWg.Done()
	}()


	// rx handshake word
	go func() {
		var b [4]byte
		_, err := io.ReadFull(conn, b[:])
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // can be returned with n = 0
		}
		if err == nil {
			peerVersion := binary.BigEndian.Uint32(b[:]) // XXX -> ntoh32 ?
			if peerVersion != version {
				err = fmt.Errorf("protocol version mismatch: peer = %08x  ; our side = %08x", peerVersion, version)
			}
		}
		errch <- err
	}()

	connClosed := false
	defer func() {
		// make sure our version is always sent on the wire, if possible,
		// so that peer does not see just closed connection when on rx we see version mismatch
		//
		// NOTE if cancelled tx goroutine will wake up without delay
		txWg.Wait()

		// don't forget to close conn if returning with error + add handshake err context
		if err != nil {
			err = &HandshakeError{conn.LocalAddr(), conn.RemoteAddr(), err}
			if !connClosed {
				conn.Close()
			}
		}
	}()


	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			conn.Close() // interrupt IO
			connClosed = true
			return ctx.Err()

		case err = <-errch:
			if err != nil {
				return err
			}
		}
	}

	// handshaked ok
	return nil
}


// ---- Dial & Listen at raw NodeLink level ----

// DialLink connects to address on given network, handshakes and wraps the connection as NodeLink
func DialLink(ctx context.Context, net xnet.Networker, addr string) (nl *NodeLink, err error) {
	peerConn, err := net.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	return Handshake(ctx, peerConn, LinkClient)
}

// ListenLink starts listening on laddr for incoming connections and wraps them as NodeLink.
//
// The listener accepts only those connections that pass handshake.
func ListenLink(net xnet.Networker, laddr string) (LinkListener, error) {
	rawl, err := net.Listen(laddr)
	if err != nil {
		return nil, err
	}

	l := &linkListener{
		l:        rawl,
		acceptq:  make(chan linkAccepted),
		closed:   make(chan struct{}),
	}
	go l.run()

	return l, nil
}

// LinkListener is net.Listener adapted to return handshaked NodeLink on Accept.
type LinkListener interface {
	// from net.Listener:
	Close() error
	Addr() net.Addr

	// Accept returns new incoming connection wrapped into NodeLink.
	// It accepts only those connections which pass handshake.
	Accept() (*NodeLink, error)
}

type linkListener struct {
	l       net.Listener
	acceptq chan linkAccepted
	closed  chan struct {}
}

type linkAccepted struct {
	link  *NodeLink
	err   error
}

func (l *linkListener) Close() error {
	err := l.l.Close()
	close(l.closed)
	return err
}

func (l *linkListener) run() {
	// context that cancels when listener stops
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	for {
		// stop on close
		select {
		case <-l.closed:
			return
		default:
		}

		// XXX add backpressure on too much incoming connections without client .Accept ?
		conn, err := l.l.Accept()
		go l.accept(runCtx, conn, err)
	}
}

func (l *linkListener) accept(ctx context.Context, conn net.Conn, err error) {
	link, err := l.accept1(ctx, conn, err)

	select {
	case l.acceptq <- linkAccepted{link, err}:
		// ok

	case <-l.closed:
		// shutdown
		if link != nil {
			link.Close()
		}
	}
}

func (l *linkListener) accept1(ctx context.Context, conn net.Conn, err error) (*NodeLink, error) {
	// XXX err ctx?

	if err != nil {
		return nil, err
	}

	// NOTE Handshake closes conn in case of failure
	link, err := Handshake(ctx, conn, LinkServer)
	if err != nil {
		return nil, err
	}

	return link, nil
}

func (l *linkListener) Accept() (*NodeLink, error) {
	select{
	case <-l.closed:
		// we know raw listener is already closed - return proper error about it
		_, err := l.l.Accept()
		return nil, err

	case a := <-l.acceptq:
		return a.link, a.err
	}
}

func (l *linkListener) Addr() net.Addr {
	return l.l.Addr()
}


// ---- for convenience: Conn -> NodeLink & local/remote link addresses  ----

// LocalAddr returns local address of the underlying link to peer.
func (nl *NodeLink) LocalAddr() net.Addr {
	return nl.peerLink.LocalAddr()
}

// RemoteAddr returns remote address of the underlying link to peer.
func (nl *NodeLink) RemoteAddr() net.Addr {
	return nl.peerLink.RemoteAddr()
}

// Link returns underlying NodeLink of this connection.
func (c *Conn) Link() *NodeLink {
	return c.link
}

// ConnID returns connection identifier used for the connection.
func (c *Conn) ConnID() uint32 {
	return c.connId
}


// ---- for convenience: String / Error / Cause ----
func (nl *NodeLink) String() string {
	s := fmt.Sprintf("%s - %s", nl.LocalAddr(), nl.RemoteAddr())
	return s	// XXX add "(closed)" if nl is closed ?
			// XXX other flags e.g. (down) ?
}

func (c *Conn) String() string {
	s := fmt.Sprintf("%s .%d", c.link, c.connId)
	return s	// XXX add "(closed)" if c is closed ?
}

func (e *LinkError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Link, e.Op, e.Err)
}

func (e *ConnError) Error() string {
	return fmt.Sprintf("%s .%d: %s: %s", e.Link, e.ConnId, e.Op, e.Err)
}

func (e *LinkError) Cause() error { return e.Err }
func (e *ConnError) Cause() error { return e.Err }

func (nl *NodeLink) err(op string, e error) error {
	if e == nil {
		return nil
	}
	return &LinkError{Link: nl, Op: op, Err: e}
}

func (c *Conn) err(op string, e error) error {
	if e == nil {
		return nil
	}
	return &ConnError{Link: c.link, ConnId: c.connId, Op: op, Err: e}
}


// ---- exchange of messages ----

//trace:event traceMsgRecv(c *Conn, msg Msg)
//trace:event traceMsgSendPre(l *NodeLink, connId uint32, msg Msg)
// XXX do we also need traceConnSend?

// msgPack allocates PktBuf and encodes msg into it.
func msgPack(connId uint32, msg Msg) *PktBuf {
	l := msg.neoMsgEncodedLen()
	buf := pktAlloc(pktHeaderLen+l)

	h := buf.Header()
	h.ConnId = hton32(connId)
	h.MsgCode = hton16(msg.neoMsgCode())
	h.MsgLen = hton32(uint32(l)) // XXX casting: think again

	msg.neoMsgEncode(buf.Payload())
	return buf
}

// TODO msgUnpack

// Recv receives message
// it receives packet and decodes message from it
func (c *Conn) Recv() (Msg, error) {
	pkt, err := c.recvPkt()
	if err != nil {
		return nil, err
	}
	defer pkt.Free()

	// decode packet
	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)
	msgType := msgTypeRegistry[msgCode]
	if msgType == nil {
		err = fmt.Errorf("invalid msgCode (%d)", msgCode)
		// XXX "decode" -> "recv: decode"?
		return nil, c.err("decode", err)
	}

	// TODO use free-list for decoded messages + when possible decode in-place
	msg := reflect.New(msgType).Interface().(Msg)
//	msg := reflect.NewAt(msgType, bufAlloc(msgType.Size())


	_, err = msg.neoMsgDecode(pkt.Payload())
	if err != nil {
		return nil, c.err("decode", err) // XXX "decode:" is already in ErrDecodeOverflow
	}

	traceMsgRecv(c, msg)
	return msg, nil
}

// sendMsg sends message with specified connection ID.
//
// it encodes message int packet, sets header appropriately and sends it.
//
// it is ok to call sendMsg in parallel with serveSend. XXX link to sendPktDirect for rationale?
func (link *NodeLink) sendMsg(connId uint32, msg Msg) error {
	traceMsgSendPre(link, connId, msg)

	buf := msgPack(connId, msg)
	return link.sendPkt(buf)	// XXX more context in err? (msg type)
	// FIXME ^^^ shutdown whole link on error
}

// Send sends message.
//
// it encodes message into packet and sends it.
func (c *Conn) Send(msg Msg) error {
	traceMsgSendPre(c.link, c.connId, msg)

	buf := msgPack(c.connId, msg)
	return c.sendPkt(buf)		// XXX more context in err? (msg type)
}

func (c *Conn) sendMsgDirect(msg Msg) error {
	return c.link.sendMsg(c.connId, msg)
}


// Expect receives message and checks it is one of expected types
//
// if verification is successful the message is decoded inplace and returned
// which indicates index of received message.
//
// on error (-1, err) is returned
func (c *Conn) Expect(msgv ...Msg) (which int, err error) {
	// XXX a bit dup wrt Recv
	pkt, err := c.recvPkt()
	if err != nil {
		return -1, err
	}
	defer pkt.Free()

	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)

	for i, msg := range msgv {
		if msg.neoMsgCode() == msgCode {
			_, err = msg.neoMsgDecode(pkt.Payload())
			if err != nil {
				return -1, c.err("decode", err)
			}
			return i, nil
		}
	}

	// unexpected message
	msgType := msgTypeRegistry[msgCode]
	if msgType == nil {
		return -1, c.err("decode", fmt.Errorf("invalid msgCode (%d)", msgCode))
	}

	// XXX also add which messages were expected ?
	return -1, c.err("recv", fmt.Errorf("unexpected message: %v", msgType))
}

// Ask sends request and receives response.
//
// It expects response to be exactly of resp type and errors otherwise
// XXX clarify error semantic (when Error is decoded)
// XXX do the same as Expect wrt respv ?
func (c *Conn) Ask(req Msg, resp Msg) error {
	err := c.Send(req)
	if err != nil {
		return err
	}

	nerr := &Error{}
	which, err := c.Expect(resp, nerr)
	switch which {
	case 0:
		return nil
	case 1:
		return ErrDecode(nerr)
	}

	return err
}

// ---- exchange of 1-1 request-reply ----
// (impedance matcher for current neo/py imlementation)

// lightClose closes light connection.
//
// No Send or Recv must be in flight.
// The caller must not use c after call to close - the connection is returned to freelist.
//
// XXX must be called only once.
func (c *Conn) lightClose() {
	nl := c.link
	nl.connMu.Lock()
	if nl.connTab != nil {
		// XXX find way to keep initiated by us conn as closed for some time (see Conn.Close)
		// but timer costs too much...
		delete(nl.connTab, c.connId)
	}
	nl.connMu.Unlock()

	c.release()
}

// Request is a message received from the link + connection handle to make a reply.
//
// Request represents 1 request - 0|1 reply interaction model XXX
type Request struct {
	Msg  Msg
	conn *Conn
}

// Recv1 receives message from link and wraps it into Request.
//
// XXX doc
func (link *NodeLink) Recv1() (Request, error) {
	conn, err := link.Accept(/*context.TODO()*/)	// XXX remove context?
	if err != nil {
		return Request{}, err	// XXX or return *Request? (want to avoid alloc)
	}

	// NOTE serveRecv guaranty that when a conn is accepted, there is 1 message in conn.rxq
	msg, err := conn.Recv()		// XXX directly from <-rxq
	if err != nil {
		conn.Close() // XXX -> conn.lightClose()
		return Request{}, err
	}

	// noone will be reading from conn anymore - mark rx down so that if
	// peer sends any another packet with same .ConnID serveRecv does not
	// deadlock trying to put it to conn.rxq.
	conn.downRX(errConnClosed)

	return Request{Msg: msg, conn: conn}, nil
}

// Reply sends response to request.
//
// XXX doc
func (req *Request) Reply(resp Msg) error {
	return req.conn.sendMsgDirect(resp)
	//err1 := req.conn.Send(resp)
	//err2 := req.conn.Close()	// XXX no - only Send here?
	//return xerr.First(err1, err2)
}

// Close must be called to free request resources.
//
// XXX doc
func (req *Request) Close() {	// XXX +error?
	//return req.conn.Close()
	// XXX req.Msg.Release() ?
	req.Msg = nil
	req.conn.lightClose()
	req.conn = nil // just in case
}


// Send1 sends one message over link.
//
// XXX doc
func (link *NodeLink) Send1(msg Msg) error {
	conn, err := link.NewConn()
	if err != nil {
		return err
	}

	conn.downRX(errConnClosed) // XXX just new conn this way

	err = conn.sendMsgDirect(msg)
	conn.lightClose()
	return err
}


// Ask1 sends request and receives response.	XXX in 1-1 model
//
// See Conn.Ask for semantic details.
// XXX doc
func (link *NodeLink) Ask1(req Msg, resp Msg) (err error) {
	conn, err := link.NewConn()
	if err != nil {
		return err
	}

	defer conn.lightClose()

	err = conn.sendMsgDirect(req)
	if err != nil {
		return err
	}

	nerr := &Error{}
	which, err := conn.Expect(resp, nerr)
	switch which {
	case 0:
		return nil
	case 1:
		return ErrDecode(nerr)
	}

	return err
}

func (req *Request) Link() *NodeLink {
	return req.conn.Link()
}

// ---- misc ----

// syntax sugar for atomic load/store to raise signal/noise in logic
type atomic32 struct {
	v int32	// struct member so `var a atomic32; if a == 0 ...` does not work
}

func (a *atomic32) Get() int32 {
	return atomic.LoadInt32(&a.v)
}

func (a *atomic32) Set(v int32) {
	atomic.StoreInt32(&a.v, v)
}
