// Copyright (C) 2016-2018  Nexedi SA and Contributors.
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

// Package neonet provides service to establish links and exchange messages in
// a NEO network.
//
// A NEO node - node link can be established with DialLink and ListenLink
// similarly to how it is done in standard package net. Once established, a
// link (NodeLink) provides service for multiplexing several communication
// connections on top of it. Connections (Conn) in turn provide service to
// exchange NEO protocol messages.
//
// New connections can be created with link.NewConn(). Once connection is
// created and a message is sent over it, on peer's side another corresponding
// new connection can be accepted via link.Accept(), and all further communication
// send/receive exchange will be happening in between those 2 connections.
//
// Use conn.Send and conn.Recv to actually exchange messages. See Conn
// documentation for other message-exchange utilities like Ask and Expect.
//
// See also package lab.nexedi.com/kirr/neo/go/neo/proto for definition of NEO
// messages.
//
//
// Lightweight mode
//
// XXX document
package neonet

// XXX neonet compatibility with NEO/py depends on the following small NEO/py patch:
//
//	https://lab.nexedi.com/kirr/neo/commit/dd3bb8b4
//
// which adjusts message ID a bit so it behaves like stream_id in HTTP/2:
//
//	- always even for server initiated streams
//	- always odd  for client initiated streams
//
// and is incremented by += 2, instead of += 1 to maintain above invariant.
//
// See http://navytux.spb.ru/~kirr/neo.html#development-overview (starting from
// "Then comes the link layer which provides service to exchange messages over
// network...") for the rationale.
//
// Unfortunately current NEO/py maintainer is very much against merging that patch.


//go:generate gotrace gen .

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	//"runtime"
	"sync"
	"time"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/internal/packed"

	"github.com/someonegg/gocontainer/rbuf"

	"lab.nexedi.com/kirr/go123/xbytes"
)

// NodeLink is a node-node link in NEO.
//
// A node-node link represents bidirectional symmetrical communication
// channel in between 2 NEO nodes. The link provides service for multiplexing
// several communication connections on top of the node-node link.
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
	txq	chan txReq	// tx requests from Conns go via here
				// (rx packets are routed to Conn.rxq)

	acceptq  chan *Conn	// queue of incoming connections for Accept
	axqWrite atomic32	//  1 while serveRecv is doing `acceptq <- ...`
	axqRead  atomic32	// +1 while Accept is doing `... <- acceptq`
	axdownFlag atomic32	//  1 when AX is marked no longer operational

//	axdown  chan struct{}	// ready when accept is marked as no longer operational
	axdown1 sync.Once	// CloseAccept may be called several times

	down     chan struct{}  // ready when NodeLink is marked as no longer operational
	downOnce sync.Once      // shutdown may be due to both Close and IO error
	downWg   sync.WaitGroup // for activities at shutdown
	errClose error          // error got from peerLink.Close

	errMu    sync.Mutex
	errRecv	 error		// error got from recvPkt on shutdown

	axclosed atomic32	// whether CloseAccept was called
	closed   atomic32	// whether Close was called

	rxbuf    rbuf.RingBuf	// buffer for reading from peerLink

	// scheduling optimization: whenever serveRecv sends to Conn.rxq
	// receiving side must ack here to receive G handoff.
	// See comments in serveRecv for details.
	rxghandoff chan struct{}
}

// XXX rx handoff make latency better for serial request-reply scenario but
// does a lot of harm for case when there are several parallel requests -
// serveRecv after handing off is put to tail of current cpu runqueue - not
// receiving next requests and not spawning handlers for them, thus essential
// creating Head-of-line (HOL) blocking problem.
//
// XXX ^^^ problem reproducible on deco but not on z6001
const rxghandoff = true	// XXX whether to do rxghandoff trick

// Conn is a connection established over NodeLink.
//
// Messages can be sent and received over it.
// Once connection is no longer needed it has to be closed.
//
// It is safe to use Conn from multiple goroutines simultaneously.
type Conn struct {
	link      *NodeLink
	connId    uint32

	rxq	   chan *pktBuf	 // received packets for this Conn go here
	rxqWrite   atomic32	 //  1 while serveRecv is doing `rxq <- ...`
	rxqRead    atomic32      // +1 while Conn.Recv is doing `... <- rxq`
	rxdownFlag atomic32	 //  1 when RX is marked no longer operational
	// XXX ^^^ split to different cache lines?

	rxerrOnce sync.Once     // rx error is reported only once - then it is link down or closed XXX !light?

	// there are two modes a Conn could be used:
	// - full mode - where full Conn functionality is working, and
	// - light mode - where only subset functionality is working
	//
	// the light mode is used to implement Recv1 & friends - there any
	// connection is used max to send and/or receive only 1 packet and then
	// has to be reused for efficiency ideally without reallocating anything.
	//
	// everything below is used during !light mode only.

//	rxdown     chan struct{} // ready when RX is marked no longer operational
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

// LinkError is returned by NodeLink operations.
type LinkError struct {
	Link *NodeLink
	Op   string
	Err  error
}

// ConnError is returned by Conn operations.
type ConnError struct {
	Link   *NodeLink
	ConnId uint32	 // NOTE Conn's are reused - cannot use *Conn here
	Op     string
	Err    error
}

// _LinkRole is a role an end of NodeLink is intended to play
//
// XXX _LinkRole will need to become public again if _Handshake does.
type _LinkRole int
const (
	_LinkServer _LinkRole = iota // link created as server
	_LinkClient                  // link created as client

	// for testing:
	linkNoRecvSend _LinkRole = 1 << 16 // do not spawn serveRecv & serveSend
	linkFlagsMask  _LinkRole = (1<<32 - 1) << 16
)

// newNodeLink makes a new NodeLink from already established net.Conn .
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
func newNodeLink(conn net.Conn, role _LinkRole) *NodeLink {
	var nextConnId uint32
	switch role &^ linkFlagsMask {
	case _LinkServer:
		nextConnId = 0 // all initiated by us connId will be even
	case _LinkClient:
		nextConnId = 1 // ----//---- odd
	default:
		panic("invalid conn role")
	}

	nl := &NodeLink{
		peerLink:   conn,
		connTab:    map[uint32]*Conn{},
		nextConnId: nextConnId,
		acceptq:    make(chan *Conn),	// XXX +buf ?
		txq:        make(chan txReq),
		rxghandoff: make(chan struct{}),
//		axdown:     make(chan struct{}),
		down:       make(chan struct{}),
	}
	if role&linkNoRecvSend == 0 {
		nl.serveWg.Add(2)
		go nl.serveRecv()
		go nl.serveSend()
	}
	return nl
}

// connPool is freelist for Conn.
// XXX make it per-link?
var connPool = sync.Pool{New: func() interface{} {
	return &Conn{
		rxq:    make(chan *pktBuf, 1),	// NOTE non-blocking - see serveRecv XXX +buf ?
		txerr:  make(chan error, 1),	// NOTE non-blocking - see Conn.Send
		txdown: make(chan struct{}),
//		rxdown: make(chan struct{}),
	}
}}

// connAlloc allocates Conn from freelist.
func (link *NodeLink) connAlloc(connId uint32) *Conn {
	c := connPool.Get().(*Conn)
	c.reinit()
	c.link = link
	c.connId = connId
	return c
}

// release releases connection to freelist.
func (c *Conn) release() {
	c.reinit()	// XXX just in case
	connPool.Put(c)
}

// reinit reinitializes connection after reallocating it from freelist.
func (c *Conn) reinit() {
	c.link = nil
	c.connId = 0
	// .rxq		- set initially; does not change
	c.rxqWrite.Set(0)	// XXX store relaxed?
	c.rxqRead.Set(0)	// ----//----
	c.rxdownFlag.Set(0)	// ----//----

	c.rxerrOnce = sync.Once{}	// XXX ok?


	// XXX vvv not strictly needed for light mode?
//	ensureOpen(&c.rxdown)
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
// must be called with connMu held.
func (link *NodeLink) newConn(connId uint32) *Conn {
	c := link.connAlloc(connId)
	link.connTab[connId] = c
	return c
}

// NewConn creates new connection on top of node-node link.
func (link *NodeLink) NewConn() (*Conn, error) {
	link.connMu.Lock()
	//defer link.connMu.Unlock()
	c, err := link._NewConn()
	link.connMu.Unlock()
	return c, err
}

func (link *NodeLink) _NewConn() (*Conn, error) {
	if link.connTab == nil {
		if link.closed.Get() != 0 {
			return nil, link.err("newconn", ErrLinkClosed)
		}
		return nil, link.err("newconn", ErrLinkDown)
	}

	// nextConnId could wrap around uint32 limits - find first free slot to
	// not blindly replace existing connection
	for i := uint32(0) ;; i++ {
		_, exists := link.connTab[link.nextConnId]
		if !exists {
			break
		}
		link.nextConnId += 2

		if i > math.MaxUint32 / 2 {
			return nil, link.err("newconn", ErrLinkManyConn)
		}
	}

	c := link.newConn(link.nextConnId)
	link.nextConnId += 2

	return c, nil
}

// shutdownAX marks acceptq as no longer operational and interrupts Accept.
func (link *NodeLink) shutdownAX() {
	link.axdown1.Do(func() {
//		close(link.axdown)

		link.axdownFlag.Set(1)	// XXX cmpxchg and return if already down?

		// drain all connections from .acceptq:
		// - something could be already buffered there
		// - serveRecv could start writing acceptq at the same time we set axdownFlag; we derace it
		for {
			// if serveRecv is outside `.acceptq <- ...` critical
			// region and fully drained - we are done.
			// see description of the logic in shutdownRX
			if link.axqWrite.Get() == 0 && len(link.acceptq) == 0 {
				break
			}

			select {
			case conn := <-link.acceptq:
				// serveRecv already put at least 1 packet into conn.rxq before putting
				// conn into .acceptq - shutting it down will send the error to peer.
				conn.shutdownRX(errConnRefused)

				link.connMu.Lock()
				delete(link.connTab, conn.connId)
				link.connMu.Unlock()

			default:
				// ok - continue spinning
			}
		}

		// wakeup Accepts
		for {
			// similarly to above: .axdownFlag vs .axqRead
			// see logic description in shutdownRX
			if link.axqRead.Get() == 0 {
				break
			}

			select {
			case link.acceptq <- nil:
				// ok - woken up

			default:
				// ok - continue spinning
			}
		}
	})
}

// shutdown closes raw link to peer and marks NodeLink as no longer operational.
//
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
// The peer will receive "connection refused" if it tries to connect after and
// for already-queued connection requests.
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
func (link *NodeLink) Close() error {
	link.axclosed.Set(1)
	link.closed.Set(1)
	link.shutdown()
	link.downWg.Wait()
	return link.err("close", link.errClose)
}

// shutdown marks connection as no longer operational and interrupts Send and Recv.
func (c *Conn) shutdown() {
	c.shutdownTX()
	c.shutdownRX(errConnClosed)
}

// shutdownTX marks TX as no longer operational and interrupts Send.
func (c *Conn) shutdownTX() {
	c.txdownOnce.Do(func() {
		close(c.txdown)
	})
}

// shutdownRX marks .rxq as no longer operational and interrupts Recv.
func (c *Conn) shutdownRX(errMsg *proto.Error) {
	c.rxdownOnce.Do(func() {
//		close(c.rxdown)	// wakeup Conn.Recv
		c.downRX(errMsg)
	})
}

// downRX marks .rxq as no longer operational.
//
// used in shutdownRX and separately to mark RX down for light Conns.
func (c *Conn) downRX(errMsg *proto.Error) {
	// let serveRecv know RX is down for this connection
	c.rxdownFlag.Set(1) // XXX cmpxchg and return if already down?

	// drain all packets from .rxq:
	// - something could be already buffered there
	// - serveRecv could start writing rxq at the same time we set rxdownFlag; we derace it.
	i := 0
	for {
		// we set .rxdownFlag=1 above.
		// now if serveRecv is outside `.rxq <- ...` critical section we know it is either:
		// - before it	-> it will eventually see .rxdownFlag=1 and won't send pkt to rxq.
		// - after it	-> it already sent pkt to rxq and won't touch
		//		   rxq until next packet (where it will hit "before it").
		//
		// when serveRecv stopped sending we know we are done draining when rxq is empty.
		if c.rxqWrite.Get() == 0 && len(c.rxq) == 0 {
			break
		}

		select {
		case <-c.rxq:
			c.rxack()
			i++

		default:
			// ok - continue spinning
		}
	}

	// if something was queued already there - reply "connection closed"
	if i != 0 {
		go c.link.replyNoConn(c.connId, errMsg)
	}

	// wakeup recvPkt(s)
	for {
		// similarly to above:
		// we set .rxdownFlag=1
		// now if recvPkt is outside `... <- .rxq` critical section we know that it is either:
		// - before it	-> it will eventually see .rxdownFlag=1 and won't try to read rxq.
		// - after it	-> it already read pktfrom rxq and won't touch
		//                 rxq until next recvPkt (where it will hit "before it").
		if c.rxqRead.Get() == 0 {
			break
		}

		select {
		case c.rxq <- nil:
			// ok - woken up

		default:
			// ok - continue spinning
		}
	}
}


// time to keep record of a closed connection so that we can properly reply
// "connection closed" if a packet comes in with same connID.
var connKeepClosed = 1*time.Minute

// CloseRecv closes reading end of connection.
//
// Any blocked Recv*() will be unblocked and return error.
// The peer will receive "connection closed" if it tries to send anything after
// and for messages already in local rx queue.
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
	link := c.link
	c.closeOnce.Do(func() {
		c.rxclosed.Set(1)
		c.txclosed.Set(1)
		c.shutdown()

		// adjust link.connTab
		var tmpclosed *Conn
		link.connMu.Lock()
		if link.connTab != nil {
			// connection was initiated by us - simply delete - we always
			// know if a packet comes to such connection - it is closed.
			//
			// XXX checking vvv should be possible without connMu lock
			if c.connId == link.nextConnId % 2 {
				delete(link.connTab, c.connId)

			// connection was initiated by peer which we accepted - put special
			// "closed" connection into connTab entry for some time to reply
			// "connection closed" if another packet comes to it.
			//
			// ( we cannot reuse same connection since after it is marked as
			//   closed Send refuses to work )
			} else {
				// delete(link.connTab, c.connId)
				// XXX vvv was temp. disabled - costs a lot in 1req=1conn model

				// c implicitly goes away from connTab
				tmpclosed = link.newConn(c.connId)
			}
		}
		link.connMu.Unlock()

		if tmpclosed != nil {
			tmpclosed.shutdownRX(errConnClosed)

			time.AfterFunc(connKeepClosed, func() {
				link.connMu.Lock()
				delete(link.connTab, c.connId)
				link.connMu.Unlock()
			})
		}
	})

	return nil
}

// ---- receive ----

// errAcceptShutdownAX returns appropriate error when link.axdown is found ready in Accept.
func (link *NodeLink) errAcceptShutdownAX() error {
	switch {
	case link.closed.Get() != 0:
		return ErrLinkClosed

	case link.axclosed.Get() != 0:
		return ErrLinkNoListen

	default:
		// XXX do the same as in errRecvShutdown (check link.errRecv)
		return ErrLinkDown
	}
}

// Accept waits for and accepts incoming connection on top of node-node link.
func (link *NodeLink) Accept() (*Conn, error) {
	// semantically equivalent to the following:
	// ( this is hot path for py compatibility mode because new connection
	//   is established in every message and select hurts performance )
	//
	// select {
	// case <-link.axdown:
	// 	return nil, link.err("accept", link.errAcceptShutdownAX())
	//
	// case c := <-link.acceptq:
	// 	return c, nil
	// }

	var conn *Conn
	var err  error

	link.axqRead.Add(1)
	axdown := link.axdownFlag.Get() != 0
	if !axdown {
		conn = <-link.acceptq
	}
	link.axqRead.Add(-1)

	// in contrast to recvPkt we can decide about error after releasing axqRead
	// reason: link is not going to be released to a free pool.
	if axdown || conn == nil {
		err = link.err("accept", link.errAcceptShutdownAX())
	}

	return conn, err
}

// errRecvShutdown returns appropriate error when c.rxdown is found ready in recvPkt.
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
func (c *Conn) recvPkt() (*pktBuf, error) {
	// semantically equivalent to the following:
	// (this is hot path and select is not used for performance reason)
	//
	// select {
	// case <-c.rxdown:
	// 	return nil, c.err("recv", c.errRecvShutdown())
	//
	// case pkt := <-c.rxq:
	// 	return pkt, nil
	// }

	var pkt *pktBuf
	var err error

	c.rxqRead.Add(1)
	rxdown := c.rxdownFlag.Get() != 0
	if !rxdown {
		pkt = <-c.rxq
	}

	// decide about error while under rxqRead (if after - the Conn can go away to be released)
	if rxdown || pkt == nil {
		err = c.err("recv", c.errRecvShutdown())
	}

	c.rxqRead.Add(-1)
	if err == nil {
		c.rxack()
	}
	return pkt, err
}

// rxack unblocks serveRecv after it handed G to us.
// see comments about rxghandoff in serveRecv.
func (c *Conn) rxack() {
	if !rxghandoff {
		return
	}
	//fmt.Printf("conn: rxack <- ...\n")
	c.link.rxghandoff <- struct{}{}
	//fmt.Printf("\tconn: rxack <- ... ok\n")
}

// serveRecv handles incoming packets routing them to either appropriate
// already-established connection or, if node link is accepting incoming
// connections, to new connection put to accept queue.
func (nl *NodeLink) serveRecv() {
	defer nl.serveWg.Done()
	for {
		// receive 1 packet
		// NOTE if nl.peerLink was just closed by tx->shutdown we'll get ErrNetClosing
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
		connId := packed.Ntoh32(pkt.Header().ConnId)
		accept := false

		nl.connMu.Lock()

		// connTab is never nil here - because shutdown, before
		// resetting it, waits for us to finish.
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
		conn.rxqWrite.Set(1)
		rxdown := conn.rxdownFlag.Get() != 0
		if !rxdown {
			conn.rxq <- pkt
		}
		conn.rxqWrite.Set(0)

		//fmt.Printf("%p\tconn.rxdown: %v\taccept: %v\n", nl, rxdown, accept)


		// conn exists, but rx is down - "connection closed"
		// (this cannot happen for newly accepted connection)
		if rxdown {
			go nl.replyNoConn(connId, errConnClosed)
			continue
		}

		// this packet established new connection - try to accept it
		if accept {
			nl.axqWrite.Set(1)
			axdown := nl.axdownFlag.Get() != 0
			if !axdown {
				nl.acceptq <- conn
			}
			nl.axqWrite.Set(0)

			// we are not accepting the connection
			if axdown {
				go nl.replyNoConn(connId, errConnRefused)
				nl.connMu.Lock()
				delete(nl.connTab, conn.connId)
				nl.connMu.Unlock()

				continue
			}
		}

		//fmt.Printf("%p\tafter accept\n", nl)

		// Normally serveRecv G will continue to run with G waking up
		// on rxq/acceptq only being put into the runqueue of current proc.
		// By default proc runq will execute only when sendRecv blocks
		// again next time deep in nl.recvPkt(), but let's force the switch
		// now without additional waiting to reduce latency.

		// XXX bad - puts serveRecv to global runq thus with high p to switch M
		//runtime.Gosched()

		// handoff execution to receiving goroutine via channel.
		// rest of serveRecv is put to current P local runq.
		//
		// details:
		// - https://github.com/golang/go/issues/20168
		// - https://github.com/golang/go/issues/15110
		//
		// see BenchmarkTCPlo* - for serveRecv style RX handoff there
		// cuts RTT from 12.5μs to 6.6μs (RTT without serveRecv style G is 4.8μs)
		//
		// TODO report upstream
		if rxghandoff {
			//fmt.Printf("serveRecv: <-rxghandoff\n")
			<-nl.rxghandoff
			//fmt.Printf("\tserveRecv: <-rxghandoff ok\n")
		}

/*
		// XXX goes away in favour of .rxdownFlag; reasons
		// - no need to reallocate rxdown for light conn
		// - no select
		//
		// XXX review synchronization via flags for correctness (e.g.
		// if both G were on the same runqueue, spinning in G1 will
		// prevent G2 progress)
		//
		// XXX maybe we'll need select if we add ctx into Send/Recv.

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

		...

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
*/
	}
}

// ---- network replies for closed / refused connections ----

var errConnClosed  = &proto.Error{proto.PROTOCOL_ERROR, "connection closed"}
var errConnRefused = &proto.Error{proto.PROTOCOL_ERROR, "connection refused"}

// replyNoConn sends error message to peer when a packet was sent to closed / nonexistent connection
func (link *NodeLink) replyNoConn(connId uint32, errMsg proto.Msg) {
	//fmt.Printf("%s .%d: -> replyNoConn %v\n", link, connId, errMsg)
	link.sendMsg(connId, errMsg) // ignore errors
	//fmt.Printf("%s .%d: replyNoConn(%v) -> %v\n", link, connId, errMsg, err)
}

// ---- transmit ----

// txReq is request to transmit a packet. Result error goes back to errch.
type txReq struct {
	pkt   *pktBuf
	errch chan error
}

// errSendShutdown returns appropriate error when c.txdown is found ready in Send.
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
func (c *Conn) sendPkt(pkt *pktBuf) error {
	err := c.sendPkt2(pkt)
	return c.err("send", err)
}

func (c *Conn) sendPkt2(pkt *pktBuf) error {
	// connId must be set to one associated with this connection
	if pkt.Header().ConnId != packed.Hton32(c.connId) {
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
			//
			// -> require "Conn.Send must not be used concurrently"?
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
func (c *Conn) sendPktDirect(pkt *pktBuf) error {
	// set pkt connId associated with this connection
	pkt.Header().ConnId = packed.Hton32(c.connId)

	// NOTE if n.peerLink was just closed by rx->shutdown we'll get ErrNetClosing
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
func (nl *NodeLink) sendPkt(pkt *pktBuf) error {
	if dumpio {
		// XXX -> log
		fmt.Printf("%v > %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
		//defer fmt.Printf("\t-> sendPkt err: %v\n", err)
	}

	// NOTE Write writes data in full, or it is error
	_, err := nl.peerLink.Write(pkt.data)
	pkt.Free()
	return err
}

var ErrPktTooBig = errors.New("packet too big")

// recvPkt receives raw packet from peer.
//
// rx error, if any, is returned as is and is analyzed in serveRecv
//
// XXX dup in ZEO.
func (nl *NodeLink) recvPkt() (*pktBuf, error) {
	// FIXME if rxbuf is non-empty - first look there for header and then if
	// we know size -> allocate pkt with that size.
	pkt := pktAlloc(4096)
	// len=4K but cap can be more since pkt is from pool - use all space to buffer reads
	// XXX vvv -> pktAlloc() ?
	data := pkt.data[:cap(pkt.data)]

	n := 0 // number of pkt bytes obtained so far

	// next packet could be already prefetched in part by previous read
	if nl.rxbuf.Len() > 0 {
		δn, _ := nl.rxbuf.Read(data[:proto.PktHeaderLen])
		n += δn
	}

	// first read to read pkt header and hopefully rest of packet in 1 syscall
	if n < proto.PktHeaderLen {
		δn, err := io.ReadAtLeast(nl.peerLink, data[n:], proto.PktHeaderLen - n)
		if err != nil {
			return nil, err
		}
		n += δn
	}

	pkth := pkt.Header()

	msgLen := packed.Ntoh32(pkth.MsgLen)
	if msgLen > proto.PktMaxSize - proto.PktHeaderLen {
		return nil, ErrPktTooBig
	}
	pktLen := int(proto.PktHeaderLen + msgLen) // whole packet length

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
	pkt.data = data

	if dumpio {
		// XXX -> log
		fmt.Printf("%v < %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
	}

	return pkt, nil
}


// ---- for convenience: Conn -> NodeLink & local/remote link addresses  ----

// LocalAddr returns local address of the underlying link to peer.
func (link *NodeLink) LocalAddr() net.Addr {
	return link.peerLink.LocalAddr()
}

// RemoteAddr returns remote address of the underlying link to peer.
func (link *NodeLink) RemoteAddr() net.Addr {
	return link.peerLink.RemoteAddr()
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

func (link *NodeLink) String() string {
	s := fmt.Sprintf("%s - %s", link.LocalAddr(), link.RemoteAddr())
	return s	// XXX add "(closed)" if link is closed ?
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

//trace:event traceMsgRecv(c *Conn, msg proto.Msg)
//trace:event traceMsgSendPre(l *NodeLink, connId uint32, msg proto.Msg)
// XXX do we also need traceConnSend?

// msgPack allocates pktBuf and encodes msg into it.
func msgPack(connId uint32, msg proto.Msg) *pktBuf {
	l := msg.NEOMsgEncodedLen()
	buf := pktAlloc(proto.PktHeaderLen+l)

	h := buf.Header()
	h.ConnId = packed.Hton32(connId)
	h.MsgCode = packed.Hton16(msg.NEOMsgCode())
	h.MsgLen = packed.Hton32(uint32(l)) // XXX casting: think again

	msg.NEOMsgEncode(buf.Payload())
	return buf
}

// TODO msgUnpack

// Recv receives message from the connection.
func (c *Conn) Recv() (proto.Msg, error) {
	pkt, err := c.recvPkt()
	if err != nil {
		return nil, err
	}
	//defer pkt.Free()
	msg, err := c._Recv(pkt)
	pkt.Free()
	return msg, err
}

func (c *Conn) _Recv(pkt *pktBuf) (proto.Msg, error) {
	// decode packet
	pkth := pkt.Header()
	msgCode := packed.Ntoh16(pkth.MsgCode)
	msgType := proto.MsgType(msgCode)
	if msgType == nil {
		err := fmt.Errorf("invalid msgCode (%d)", msgCode)
		// XXX "decode" -> "recv: decode"?
		return nil, c.err("decode", err)
	}

	// TODO use free-list for decoded messages + when possible decode in-place
	msg := reflect.New(msgType).Interface().(proto.Msg)
//	msg := reflect.NewAt(msgType, bufAlloc(msgType.Size())


	_, err := msg.NEOMsgDecode(pkt.Payload())
	if err != nil {
		return nil, c.err("decode", err) // XXX "decode:" is already in ErrDecodeOverflow
	}

	traceMsgRecv(c, msg)
	return msg, nil
}

// sendMsg sends message with specified connection ID.
//
// it encodes message into packet, sets header appropriately and sends it.
//
// it is ok to call sendMsg in parallel with serveSend. XXX link to sendPktDirect for rationale?
func (link *NodeLink) sendMsg(connId uint32, msg proto.Msg) error {
	traceMsgSendPre(link, connId, msg)

	buf := msgPack(connId, msg)
	return link.sendPkt(buf)	// XXX more context in err? (msg type)
	// FIXME ^^^ shutdown whole link on error
}

// Send sends message over the connection.
func (c *Conn) Send(msg proto.Msg) error {
	traceMsgSendPre(c.link, c.connId, msg)

	buf := msgPack(c.connId, msg)
	return c.sendPkt(buf)		// XXX more context in err? (msg type)
}

func (c *Conn) sendMsgDirect(msg proto.Msg) error {
	return c.link.sendMsg(c.connId, msg)
}


// Expect receives message and checks it is one of expected types.
//
// If verification is successful the message is decoded inplace and returned
// which indicates index of received message.
//
// On error (-1, err) is returned.
func (c *Conn) Expect(msgv ...proto.Msg) (which int, err error) {
	// XXX a bit dup wrt Recv
	pkt, err := c.recvPkt()
	if err != nil {
		return -1, err
	}
	//defer pkt.Free()
	which, err = c._Expect(pkt, msgv...)
	pkt.Free()
	return which, err
}

func (c *Conn) _Expect(pkt *pktBuf, msgv ...proto.Msg) (int, error) {
	pkth := pkt.Header()
	msgCode := packed.Ntoh16(pkth.MsgCode)

	for i, msg := range msgv {
		if msg.NEOMsgCode() == msgCode {
			_, err := msg.NEOMsgDecode(pkt.Payload())
			if err != nil {
				return -1, c.err("decode", err)
			}
			return i, nil
		}
	}

	// unexpected message
	msgType := proto.MsgType(msgCode)
	if msgType == nil {
		return -1, c.err("decode", fmt.Errorf("invalid msgCode (%d)", msgCode))
	}

	// XXX also add which messages were expected ?
	return -1, c.err("recv", fmt.Errorf("unexpected message: %v", msgType))
}

// Ask sends request and receives response.
//
// It expects response to be exactly of resp type and errors otherwise.
//
// XXX clarify error semantic (when Error is decoded)
// XXX do the same as Expect wrt respv ?
func (c *Conn) Ask(req proto.Msg, resp proto.Msg) error {
	err := c.Send(req)
	if err != nil {
		return err
	}

	nerr := &proto.Error{}
	which, err := c.Expect(resp, nerr)
	switch which {
	case 0:
		return nil
	case 1:
		return proto.ErrDecode(nerr)
	}

	return err
}

// ---- exchange of 1-1 request-reply ----
// (impedance matcher for current neo/py implementation)

// lightClose closes light connection.
//
// No Send or Recv must be in flight.
// The caller must not use c after call to close - the connection is returned to freelist.
//
// XXX must be called only once.
func (c *Conn) lightClose() {
	nl := c.link
	releaseok := false
	nl.connMu.Lock()
	if nl.connTab != nil {
		// XXX find way to keep initiated by us conn as closed for some time (see Conn.Close)
		// but timer costs too much...
		delete(nl.connTab, c.connId)
		releaseok = true
	}
	nl.connMu.Unlock()

	// we can release c only if we removed it from connTab.
	//
	// if not - the scenario could be: nl.shutdown sets nl.connTab=nil and
	// then iterates connTab snapshot taken under nl.connMu lock. If so
	// this activity (which calls e.g. Conn.shutdown) could be running with
	// us in parallel.
	if releaseok {
		c.release()
	}
}

// Request is a message received from the link + connection handle to make a reply.
//
// Request represents 1 request - 0|1 reply interaction model XXX
type Request struct {
	Msg  proto.Msg
	conn *Conn
}

// Recv1 receives message from link and wraps it into Request.
//
// XXX doc
func (link *NodeLink) Recv1() (Request, error) {
	conn, err := link.Accept()
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
func (req *Request) Reply(resp proto.Msg) error {
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
func (link *NodeLink) Send1(msg proto.Msg) error {
	conn, err := link.NewConn()
	if err != nil {
		return err
	}

	conn.downRX(errConnClosed) // XXX just initially create conn this way

	err = conn.sendMsgDirect(msg)
	conn.lightClose()
	return err
}


// Ask1 sends request and receives response.	XXX in 1-1 model
//
// See Conn.Ask for semantic details.
// XXX doc
func (link *NodeLink) Ask1(req proto.Msg, resp proto.Msg) (err error) {
	conn, err := link.NewConn()
	if err != nil {
		return err
	}

	//defer conn.lightClose()
	err = conn._Ask1(req, resp)
	conn.lightClose()
	return err
}

func (conn *Conn) _Ask1(req proto.Msg, resp proto.Msg) error {
	err := conn.sendMsgDirect(req)
	if err != nil {
		return err
	}

	nerr := &proto.Error{}
	which, err := conn.Expect(resp, nerr)
	switch which {
	case 0:
		return nil
	case 1:
		return proto.ErrDecode(nerr)
	}

	return err
}

func (req *Request) Link() *NodeLink {
	return req.conn.Link()
}
