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
// For a node to be able to accept new incoming connection it has to have
// "server" role - see newNodeLink() for details. XXX might change to everyone is able to accept.
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
				// = nil if NodeLink is not accepting connections
	txq	chan txReq	// tx requests from Conns go via here
				// (rx packets are routed to Conn.rxq)

	down     chan struct{}  // ready when NodeLink is marked as no longer operational
	downOnce sync.Once      // shutdown may be due to both Close and IO error
	downWg   sync.WaitGroup // for activities at shutdown
	errClose error          // error got from peerLink.Close

	errMu    sync.Mutex
	errRecv	 error		// error got from recvPkt on shutdown

	closed   uint32		// whether Close was called
}

// Conn is a connection established over NodeLink
//
// Data can be sent and received over it.
// Once connection is no longer needed it has to be closed.
//
// It is safe to use Conn from multiple goroutines simultaneously.
type Conn struct {
	nodeLink  *NodeLink
	connId    uint32
	rxq	  chan *PktBuf	// received packets for this Conn go here
	txerr     chan error	// transmit results for this Conn go back here

	down      chan struct{} // ready when Conn is marked as no longer operational
	downOnce  sync.Once	// shutdown may be called by both Close and nodelink.shutdown

	rxerrOnce sync.Once     // rx error is reported only once - then it is link down or closed
	closed    int32         // 1 if Close was called or "connection closed" entry
				// incremented during every replyNoConn() in progress

	errMsg	  *Error	// error message for replyNoConn
}


var ErrLinkClosed   = errors.New("node link is closed")	// operations on closed NodeLink
var ErrLinkDown     = errors.New("node link is down")	// e.g. due to IO error
var ErrLinkNoListen = errors.New("node link is not listening for incoming connections")
var ErrLinkManyConn = errors.New("too many opened connections")
var ErrClosedConn   = errors.New("connection is closed")

// XXX unify LinkError & ConnError -> NetError?
// (think from point of view how user should be handling errors)
// XXX or it is good to be able to distinguish between only conn error vs whole-link error?

// LinkError is usually returned by NodeLink operations
type LinkError struct {
	Link *NodeLink
	Op   string
	Err  error
}

// ConnError is usually returned by Conn operations
type ConnError struct {
	Conn *Conn
	Op   string
	Err  error
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

/*
// LinkFlags allow to customize NodeLink behaviour
type LinkFlags int
const (
	// LinkListen tells link to accept incoming connections.
	//
	// NOTE it is valid use-case even for link originating through DialLink
	// to accept incoming connections over established channel.
	//
	// NOTE listen put to flags - not e.g. link.Listen() call - because
	// otherwise e.g. for client originated links if after DialLink client
	// calls link.Listen() there is a race window: before Listen is called
	// in which peer could start connecting to our side.
	LinkListen LinkFlags = 1 << iota

	// for testing:
	linkNoRecvSend LinkFlags = 1 << 16 // do not spawn serveRecv & serveSend
)
*/

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
	var acceptq chan *Conn
	switch role &^ linkFlagsMask {
	case LinkServer:
		nextConnId = 0             // all initiated by us connId will be even
		acceptq = make(chan *Conn) // accept queue; TODO use backlog?
	case LinkClient:
		nextConnId = 1 // ----//---- odd
		acceptq = nil  // not accepting incoming connections
	default:
		panic("invalid conn role")
	}

	nl := &NodeLink{
		peerLink:   conn,
		connTab:    map[uint32]*Conn{},
		nextConnId: nextConnId,
		acceptq:    acceptq,	// XXX reenable make(chan *Conn), // accepting initially
		txq:        make(chan txReq),
		down:       make(chan struct{}),
	}
	if role&linkNoRecvSend == 0 {
		nl.serveWg.Add(2)
		go nl.serveRecv()
		go nl.serveSend()
	}
	return nl
}

// newConn creates new Conn with id=connId and registers it into connTab.
// Must be called with connMu held.
func (nl *NodeLink) newConn(connId uint32) *Conn {
	c := &Conn{nodeLink: nl,
		connId: connId,
		rxq:    make(chan *PktBuf, 1), // NOTE non-blocking - see serveRecv
		txerr:  make(chan error, 1),   // NOTE non-blocking - see Conn.Send
		down:   make(chan struct{}),
	}
	nl.connTab[connId] = c
	return c
}

// NewConn creates new connection on top of node-node link
func (nl *NodeLink) NewConn() (*Conn, error) {
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	if nl.connTab == nil {
		if atomic.LoadUint32(&nl.closed) != 0 {
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

// shutdown closes raw link to peer and marks NodeLink as no longer operational.
// it also shutdowns all opened connections over this node link.
func (nl *NodeLink) shutdown() {
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

			nl.connMu.Lock()
			for _, conn := range nl.connTab {
				// NOTE anything waking up on Conn.down must not lock
				// connMu - else it will deadlock.
				conn.shutdown()
			}
			nl.connTab = nil // clear + mark down
			nl.connMu.Unlock()
		}()
	})
}

// Close closes node-node link.
// All blocking operations - Accept and IO on associated connections
// established over node link - are automatically interrupted with an error.
// Underlying raw connection is closed.
// It is safe to call Close several times
func (nl *NodeLink) Close() error {
	atomic.StoreUint32(&nl.closed, 1)
	nl.shutdown()
	nl.downWg.Wait()
	return nl.err("close", nl.errClose)
}

// shutdown marks connection as no longer operational
func (c *Conn) shutdown() {
	c.downOnce.Do(func() {
		close(c.down)
	})
}


var connKeepClosed = 1*time.Minute

// Close closes connection.
// Any blocked Send*() or Recv*() will be unblocked and return error
//
// NOTE for Send() - once transmission was started - it will complete in the
// background on the wire not to break node-node link framing.
//
// It is safe to call Close several times.
func (c *Conn) Close() error {
	nl := c.nodeLink

	// adjust nodeLink.connTab
	nl.connMu.Lock()
	if nl.connTab != nil {
		// connection was initiated by us - simply delete - we always
		// know if a packet comes to such connection it is closed.
		if c.connId == nl.nextConnId % 2 {
			delete(nl.connTab, c.connId)

		// connection was initiated by peer which we accepted - put special
		// "closed" connection into connTab entry for some time to reply
		// "connection closed" if another packet comes to it.
		} else {
			cc := nl.newConn(c.connId)
			// cc.closed=1 so that cc is not freed by replyNoConn
			// NOTE cc.down stays not closed so Send could work
			atomic.StoreInt32(&cc.closed, 1)
			cc.errMsg = errConnClosed
			time.AfterFunc(connKeepClosed, func() {
				nl.connMu.Lock()
				delete(nl.connTab, cc.connId)
				nl.connMu.Unlock()

				cc.shutdown()
			})
		}

	}
	nl.connMu.Unlock()

	atomic.StoreInt32(&c.closed, 1)
	c.shutdown()
	return nil
}

// ---- receive ----

// Accept waits for and accepts incoming connection on top of node-node link
func (nl *NodeLink) Accept() (c *Conn, err error) {
	defer func() {
		if err != nil {
			err = nl.err("accept", err)
		}
	}()

	// this node link is not accepting connections
	if nl.acceptq == nil {
		return nil, ErrLinkNoListen
	}

	select {
	case <-nl.down:
		if atomic.LoadUint32(&nl.closed) != 0 {
			return nil, ErrLinkClosed
		}
		return nil, ErrLinkDown

	case c := <-nl.acceptq:
		return c, nil
	}
}

// errRecvShutdown returns appropriate error when c.down is found ready in recvPkt
func (c *Conn) errRecvShutdown() error {
	switch {
	case atomic.LoadInt32(&c.closed) != 0:
		return ErrClosedConn

	case atomic.LoadUint32(&c.nodeLink.closed) != 0:
		return ErrLinkClosed

	default:
		// we have to check what was particular RX error on nodelink shutdown
		// only do that once - after reporting RX error the first time
		// tell client the node link is no longer operational.
		var err error
		c.rxerrOnce.Do(func() {
			c.nodeLink.errMu.Lock()
			err = c.nodeLink.errRecv
			c.nodeLink.errMu.Unlock()
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
	case <-c.down:
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
		//fmt.Printf("recvPkt -> %v, %v\n", pkt, err)
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

		//fmt.Printf("RX .%d -> %v\n", connId, conn)
		if conn == nil {
			// "new" connection will be needed in all cases - e.g.
			// temporarily to reply "connection refused"
			conn = nl.newConn(connId)

			//fmt.Printf("connId: %d (%d)\n", connId, connId % 2)
			//fmt.Printf("nextConnId: %d (%d)\n", nl.nextConnId, nl.nextConnId % 2)

			// message with connid that should be initiated by us
			if connId % 2 == nl.nextConnId % 2 {
				conn.errMsg = errConnClosed

			// message with connid for a stream initiated by peer
			} else {
				if nl.acceptq == nil {
					conn.errMsg = errConnRefused
				} else {
					// we are accepting new incoming connection
					accept = true
				}

				//fmt.Println("ZZZ", conn.errMsg, accept)
			}
		}

		// we are not accepting packet in any way
		if conn.errMsg != nil {
			//fmt.Printf(".%d EMSG: %v\n", connId, conn.errMsg)
			atomic.AddInt32(&conn.closed, 1)
			nl.connMu.Unlock()
			go conn.replyNoConn()
			continue
		}

		// route packet to serving goroutine handler
		//
		// TODO backpressure when Recv is not keeping up with Send on peer side?
		//      (not to let whole nodelink starve because of one connection)
		//
		// NOTE rxq must be buffered with at least 1 element so that
		// queuing pkt succeeds for incoming connection that is not yet
		// there in acceptq.
		conn.rxq <- pkt

		// keep connMu locked until here: so that ^^^ `conn.rxq <- pkt` can be
		// sure conn stays not down e.g. closed by Conn.Close or NodeLink.shutdown
		//
		// XXX try to release connMu earlier - before `rxq <- pkt`
		nl.connMu.Unlock()

		if accept {
			select {
			case <-nl.down:
				// Accept and loop calling it can exit if shutdown was requested
				// if so we are also exiting

				// make sure not to leave rx error as nil
				nl.errMu.Lock()
				nl.errRecv = ErrLinkDown
				nl.errMu.Unlock()

				return

			case nl.acceptq <- conn:
				// ok
			}
		}
	}
}

// ---- network replies for closed / refused connections ----

var errConnClosed  = &Error{PROTOCOL_ERROR, "connection closed"}
var errConnRefused = &Error{PROTOCOL_ERROR, "connection refused"}

// replyNoConn sends error message to peer when a packet was sent to closed / nonexistent connection
// and removes connection from nodeLink connTab if ekeep==false.
//func (c *Conn) replyNoConn(e Msg, ekeep bool) {
func (c *Conn) replyNoConn() {
	c.Send(c.errMsg) // ignore errors
	//fmt.Println("errsend:", err)

	// remove connTab entry - if all users of this temporary conn created
	// only to send the error are now gone.
	c.nodeLink.connMu.Lock()
	if atomic.AddInt32(&c.closed, -1) == 0 {
		delete(c.nodeLink.connTab, c.connId)
	}
	c.nodeLink.connMu.Unlock()
}

// ---- transmit ----

// txReq is request to transmit a packet. Result error goes back to errch
type txReq struct {
	pkt   *PktBuf
	errch chan error
}

// errSendShutdown returns appropriate error when c.down is found ready in Send
func (c *Conn) errSendShutdown() error {
	switch {
	case atomic.LoadInt32(&c.closed) != 0:
		return ErrClosedConn

	// the only other error possible besides Conn being .Close()'ed is that
	// NodeLink was closed/shutdowned itself - on actual IO problems corresponding
	// error is delivered to particular Send that caused it.

	case atomic.LoadUint32(&c.nodeLink.closed) != 0:
		return ErrLinkClosed

	default:
		return ErrLinkDown
	}
}

// sendPkt sends raw packet via connection
func (c *Conn) sendPkt(pkt *PktBuf) error {
	err := c.sendPkt2(pkt)
	return c.err("send", err)
}

func (c *Conn) sendPkt2(pkt *PktBuf) error {
	// set pkt connId associated with this connection
	pkt.Header().ConnId = hton32(c.connId)
	var err error

	select {
	case <-c.down:
		return c.errSendShutdown()

	case c.nodeLink.txq <- txReq{pkt, c.txerr}:
		select {
		// tx request was sent to serveSend and is being transmitted on the wire.
		// the transmission may block for indefinitely long though and
		// we cannot interrupt it as the only way to interrupt is
		// .nodeLink.Close() which will close all other Conns.
		//
		// That's why we are also checking for c.down while waiting
		// for reply from serveSend (and leave pkt to finish transmitting).
		//
		// NOTE after we return straight here serveSend won't be later
		// blocked on c.txerr<- because that backchannel is a non-blocking one.
		case <-c.down:

			// also poll c.txerr here because: when there is TX error,
			// serveSend sends to c.txerr _and_ closes c.down .
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

			txreq.errch <- err

			// on IO error framing over peerLink becomes broken
			// so we shut down node link and all connections over it.
			if err != nil {
				nl.shutdown()
				return
			}
		}
	}
}


// ---- raw IO ----

const dumpio = true

// sendPkt sends raw packet to peer
// tx error, if any, is returned as is and is analyzed in serveSend
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	if dumpio {
		// XXX -> log
		fmt.Printf("%v > %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt.Dump())
		//defer fmt.Printf("\t-> sendPkt err: %v\n", err)
	}

	// NOTE Write writes data in full, or it is error
	_, err := nl.peerLink.Write(pkt.Data)
	return err
}

var ErrPktTooBig = errors.New("packet too big")

// recvPkt receives raw packet from peer
// rx error, if any, is returned as is and is analyzed in serveRecv
func (nl *NodeLink) recvPkt() (*PktBuf, error) {
	// TODO organize rx buffers management (freelist etc)

	// first read to read pkt header and hopefully up to page of data in 1 syscall
	pkt := &PktBuf{make([]byte, 4096)}
	// TODO reenable, but NOTE next packet can be also prefetched here -> use buffering ?
	//n, err := io.ReadAtLeast(nl.peerLink, pkt.Data, PktHeadLen)
	n, err := io.ReadFull(nl.peerLink, pkt.Data[:PktHeadLen])
	if err != nil {
		return nil, err
	}

	pkth := pkt.Header()

	// XXX -> better PktHeader.Decode() ?
	pktLen := PktHeadLen + ntoh32(pkth.MsgLen) // .MsgLen is payload-only length without header
	if pktLen > MAX_PACKET_SIZE {
		return nil, ErrPktTooBig
	}

	// XXX -> pkt.Data = xbytes.Resize32(pkt.Data[:n], pktLen)
	if pktLen > uint32(cap(pkt.Data)) {
		// grow rxbuf
		rxbuf2 := make([]byte, pktLen)
		copy(rxbuf2, pkt.Data[:n])
		pkt.Data = rxbuf2
	}
	// cut .Data len to length of packet
	pkt.Data = pkt.Data[:pktLen]

	// read rest of pkt data, if we need to
	if n < len(pkt.Data) {
		_, err = io.ReadFull(nl.peerLink, pkt.Data[n:])
		if err != nil {
			return nil, err
		}
	}

	if dumpio {
		// XXX -> log
		fmt.Printf("%v < %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt.Dump())
	}

	return pkt, nil
}


// ---- Handshake ----

// Handshake performs NEO protocol handshake just after raw connection between 2 nodes was established.
// On success raw connection is returned wrapped into NodeLink.
// On error raw connection is closed.
func Handshake(ctx context.Context, conn net.Conn, role LinkRole) (nl *NodeLink, err error) {
	err = handshake(ctx, conn, PROTOCOL_VERSION)
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

/*
XXX do if this is needed in a second place besides talkMaster1
// ---- Listen for single Conn over NodeLink use-cases ----

// XXX
func ListenSingleConn(link *NodeLink) ConnListener {
	l := &listen1conn{link}
	// XXX go ...
	return l
}

// ConnListener XXX ...
type ConnListener interface {
	// XXX +Close, Addr ?

	// Accept returns new connection multiplexed over NodeLink
	Accept() (*Conn, error)
}

type listen1conn struct {
	link *NodeLink
}

func ...
*/


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
	return c.nodeLink
}

// ConnID returns connection identifier used for the connection.
func (c *Conn) ConnID() uint32 {
	return c.connId
}


// ---- for convenience: String / Error ----
func (nl *NodeLink) String() string {
	s := fmt.Sprintf("%s - %s", nl.LocalAddr(), nl.RemoteAddr())
	return s	// XXX add "(closed)" if nl is closed ?
			// XXX other flags e.g. (down) ?
}

func (c *Conn) String() string {
	s := fmt.Sprintf("%s .%d", c.nodeLink, c.connId)
	return s	// XXX add "(closed)" if c is closed ?
}

func (e *LinkError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Link, e.Op, e.Err)
}

func (e *ConnError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Conn, e.Op, e.Err)
}

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
	return &ConnError{Conn: c, Op: op, Err: e}
}


// ---- exchange of messages ----

//trace:event traceConnRecv(c *Conn, msg Msg)
//trace:event traceConnSendPre(c *Conn, msg Msg)
// XXX do we also need traceConnSend?

// Recv receives message
// it receives packet and decodes message from it
func (c *Conn) Recv() (Msg, error) {
	// TODO use freelist for PktBuf
	pkt, err := c.recvPkt()
	if err != nil {
		return nil, err
	}

	// decode packet
	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)
	msgType := msgTypeRegistry[msgCode]
	if msgType == nil {
		err = fmt.Errorf("invalid msgCode (%d)", msgCode)
		// XXX "decode" -> "recv: decode"?
		return nil, &ConnError{Conn: c, Op: "decode", Err: err}
	}

	// TODO use free-list for decoded messages + when possible decode in-place
	msg := reflect.New(msgType).Interface().(Msg)
	_, err = msg.neoMsgDecode(pkt.Payload())
	if err != nil {
		return nil, &ConnError{Conn: c, Op: "decode", Err: err}
	}

	traceConnRecv(c, msg)
	return msg, nil
}

// Send sends message
// it encodes message into packet and sends it
func (c *Conn) Send(msg Msg) error {
	traceConnSendPre(c, msg)

	l := msg.neoMsgEncodedLen()
	buf := PktBuf{make([]byte, PktHeadLen+l)} // TODO -> freelist

	h := buf.Header()
	// h.ConnId will be set by conn.Send
	h.MsgCode = hton16(msg.neoMsgCode())
	h.MsgLen = hton32(uint32(l)) // XXX casting: think again

	msg.neoMsgEncode(buf.Payload())

	// XXX why pointer?
	// XXX more context in err? (msg type)
	return c.sendPkt(&buf)
}


// Expect receives message and checks it is one of expected types
//
// if verification is successful the message is decoded inplace and returned
// which indicates index of received message.
//
// on error (-1, err) is returned
func (c *Conn) Expect(msgv ...Msg) (which int, err error) {
	// XXX a bit dup wrt Recv
	// TODO use freelist for PktBuf
	pkt, err := c.recvPkt()
	if err != nil {
		return -1, err
	}

	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)

	for i, msg := range msgv {
		if msg.neoMsgCode() == msgCode {
			_, err = msg.neoMsgDecode(pkt.Payload())
			if err != nil {
				return -1, &ConnError{Conn: c, Op: "decode", Err: err}
			}
			return i, nil
		}
	}

	// unexpected message
	msgType := msgTypeRegistry[msgCode]
	if msgType == nil {
		return -1, &ConnError{c, "decode", fmt.Errorf("invalid msgCode (%d)", msgCode)}
	}

	// XXX also add which messages were expected ?
	return -1, &ConnError{c, "recv", fmt.Errorf("unexpected message: %v", msgType)}
}

// Ask sends request and receives response.
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
