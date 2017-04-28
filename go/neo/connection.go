// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

package neo
// Connection management

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"fmt"
	//"lab.nexedi.com/kirr/go123/xruntime/debug"
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
// "server" role - see NewNodeLink() for details.
//
// A NodeLink has to be explicitly closed, once it is no longer needed.
//
// It is safe to use NodeLink from multiple goroutines simultaneously.
type NodeLink struct {
	peerLink net.Conn		// raw conn to peer

	connMu     sync.Mutex	// TODO -> RW ?
	connTab    map[uint32]*Conn	// connId -> Conn associated with connId
	nextConnId uint32		// next connId to use for Conn initiated by us

	serveWg sync.WaitGroup		// for serve{Send,Recv}
	acceptq chan *Conn		// queue of incoming connections for Accept
					// = nil if NodeLink is not accepting connections

	txq	chan txReq		// tx requests from Conns go via here

	errMu    sync.Mutex
//	errSend  error			// error got from sendPkt, if any
	errRecv	 error			// error got from recvPkt, if any

	// once because: NodeLink has to be explicitly closed by user; it can also
	// be "closed" by IO errors on peerLink
	closeOnce sync.Once

	closed      chan struct{}	// XXX text
	closeCalled uint32		// whether Close was called; ^^^ can be from IO error

	closeWg  sync.WaitGroup		// XXX for close waiter
	errClose error			// error got from peerLink.Close

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
	txerr     chan error	// transmit errors for this Conn go back here

	closed        chan struct{} // whether Conn is marked as no longer operational
	closeCalled   uint32        // whether Close was called; ^^^ can be from IO error on node link
	rxerrOnce     sync.Once     // XXX whether actual RX error was already reported to caller

	// once because: Conn has to be explicitly closed by user; it can also
	// be closed by NodeLink.Close .
	closeOnce sync.Once
}

// ErrLinkClosed is the error indicated for operations on closed NodeLink
var ErrLinkClosed   = errors.New("node link is closed")	// XXX -> read/write  but also Accept ?
var ErrLinkStopped  = errors.New("node link was stopped")	// XXX due to IO errors?
var ErrLinkNoListen = errors.New("node link is not listening for incoming connections")
var ErrClosedConn   = errors.New("read/write on closed connection")


// LinkRole is a role an end of NodeLink is intended to play
type LinkRole int
const (
	LinkServer LinkRole = iota	// link created as server
	LinkClient			// link created as client

	// for testing:
	linkNoRecvSend LinkRole = 1<<16	// do not spawn serveRecv & serveSend
	linkFlagsMask  LinkRole = (1<<32 - 1) << 16
)

// NewNodeLink makes a new NodeLink from already established net.Conn
//
// Role specifies how to treat our role on the link - either as client or
// server. The difference in between client and server roles are in:
//
// 1. how connection ids are allocated for connections initiated at our side:
//    there is no conflict in identifiers if one side always allocates them as
//    even (server) and its peer as odd (client).
//
// 2. NodeLink.Accept() works only on server side.
//
// Usually server role should be used for connections created via
// net.Listen/net.Accept and client role for connections created via net.Dial.
func NewNodeLink(conn net.Conn, role LinkRole) *NodeLink {
	var nextConnId uint32
	var acceptq chan *Conn
	switch role&^linkFlagsMask {
	case LinkServer:
		nextConnId = 0			// all initiated by us connId will be even
		acceptq = make(chan *Conn)	// accept queue; TODO use backlog
	case LinkClient:
		nextConnId = 1	// ----//---- odd
		acceptq = nil	// not accepting incoming connections
	default:
		panic("invalid conn role")
	}

	nl := &NodeLink{
		peerLink:   conn,
		connTab:    map[uint32]*Conn{},
		nextConnId: nextConnId,
		acceptq:    acceptq,
		txq:        make(chan txReq),
		closed:     make(chan struct{}),
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
		rxq: make(chan *PktBuf),    // TODO buffering
		txerr: make(chan error, 1), // NOTE non-blocking - see Conn.Send
		closed: make(chan struct{}),
	}
	nl.connTab[connId] = c
	return c
}

// NewConn creates new connection on top of node-node link
func (nl *NodeLink) NewConn() *Conn {
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	if nl.connTab == nil {
		// XXX -> error (because NodeLink can become "closed" due to IO errors ?
		panic("NewConn() on closed node-link")
	}
	c := nl.newConn(nl.nextConnId)
	nl.nextConnId += 2
	return c
}

// close is worker for Close & friends.
// It marks NodeLink and all active Conns as closed.
func (nl *NodeLink) close() {
	nl.closeOnce.Do(func() {
		close(nl.closed)

		// close actual link to peer. this will wakeup serve{Send,Recv}
		// NOTE we need it here so that e.g. aborting on error serveSend wakes up serveRecv
		nl.errClose = nl.peerLink.Close()

		nl.closeWg.Add(1)
		go func() {
			defer nl.closeWg.Done()

			// wait for serve{Send,Recv} to complete before signalling to Conns
			nl.serveWg.Wait()

			nl.connMu.Lock()
			for _, conn := range nl.connTab {
				// NOTE anything waking up on Conn.closed must not lock
				// connMu - else it will deadlock.
				conn.close()
			}
			nl.connTab = nil	// clear + mark closed
			nl.connMu.Unlock()
		}()
	})
}

// Close closes node-node link.
// IO on connections established over it is automatically interrupted with an error.
func (nl *NodeLink) Close() error {
	atomic.StoreUint32(&nl.closeCalled, 1)
	nl.close()
	nl.closeWg.Wait() // wait for close to complete
	return nl.errClose
}

// worker for Close() & co
func (c *Conn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
}

// Close closes connection
// Any blocked Send() or Recv() will be unblocked and return error
//
// NOTE for Send() - once transmission was started - it will complete in the
// background on the wire not to break framing.
func (c *Conn) Close() error {
	// adjust nodeLink.connTab
	c.nodeLink.connMu.Lock()
	delete(c.nodeLink.connTab, c.connId)
	c.nodeLink.connMu.Unlock()

	atomic.StoreUint32(&c.closeCalled, 1)
	c.close()
	return nil
}

// Accept waits for and accepts incoming connection on top of node-node link
func (nl *NodeLink) Accept() (*Conn, error) {
	// this node link is not accepting connections
	if nl.acceptq == nil {
		return nil, ErrLinkNoListen
	}

	select {
	case <-nl.closed:
		return nil, ErrLinkClosed // XXX + op = Accept ?

	case c := <-nl.acceptq:	// XXX -> only c, ok := <-nl.acceptq ?
		return c, nil
	}
}

// errRecvClosed returns appropriate error when c.closed is found ready in Recv
func (c *Conn) errRecvClosed() error {
	switch {
	case atomic.LoadUint32(&c.closeCalled) != 0:
		return ErrClosedConn

	case atomic.LoadUint32(&c.nodeLink.closeCalled) != 0:
		return ErrLinkClosed

	default:
		// we have to check what was particular RX error on nodelink
		// only do that once - after reportin RX error the first time
		// tell client the node link is no longer operational.
		var err error
		c.rxerrOnce.Do(func() {
			c.nodeLink.errMu.Lock()
			err = c.nodeLink.errRecv
			c.nodeLink.errMu.Unlock()
		})
		if err == nil {
			err = ErrLinkStopped
		}
		return err
	}
}

// Recv receives packet from connection
func (c *Conn) Recv() (*PktBuf, error) {
	select {
	case <-c.closed:
		return nil, c.errRecvClosed()

	case pkt := <-c.rxq:	// XXX try to leave only pkt, ok := <-c.rxq
		return pkt, nil	// XXX error = ?
	}
}

// serveRecv handles incoming packets routing them to either appropriate
// already-established connection or, if node link is accepting incoming
// connections, to new connection put to accept queue.
func (nl *NodeLink) serveRecv() {
	defer nl.serveWg.Done()
	for {
		// receive 1 packet
		pkt, err := nl.recvPkt()
		fmt.Printf("recvPkt -> %v, %v\n", pkt, err)
		if err != nil {
			// on IO error framing over peerLink becomes broken
			// so we mark node link and all connections as closed and stop service

			/*
			select {
			case <-nl.closed:
				// error was due to closing NodeLink
				err = ErrLinkClosed
			default:
			}
			*/

			nl.errMu.Lock()
			nl.errRecv = err
			nl.errMu.Unlock()

			// wake-up all conns & mark node link as closed
			// NOTE this also wakeups serveSend/sendPkt
			nl.close()

			return
		}

		// pkt.ConnId -> Conn
		connId := ntoh32(pkt.Header().ConnId)

		nl.connMu.Lock()
		conn := nl.connTab[connId]
		if conn == nil {
			if nl.acceptq != nil {
				// we are accepting new incoming connection
				conn = nl.newConn(connId)
				// XXX what if Accept exited because of just recently close(nl.closed)?
				//     -> check nl.closed here too ?
				nl.acceptq <- conn
			}
		}

		// we have not accepted incoming connection - ignore packet
		if conn == nil {
			// XXX also log / increment counter?
			nl.connMu.Unlock()
			continue
		}

		// route packet to serving goroutine handler
		// XXX what if Conn.Recv exited because of just recently close(nl.closed) ?
		//     -> check nl.closed here too ?
		conn.rxq <- pkt

		// keep connMu locked until here: so that ^^^ `conn.rxq <- pkt` can be
		// sure conn stays not closed e.g. by Conn.Close
		nl.connMu.Unlock()
	}
}


// txReq is request to transmit a packet. Result error goes back to errch
type txReq struct {
	pkt   *PktBuf
	errch chan error
}

// errSendClosed returns approproate error when c.closed is found ready in Send
func (c *Conn) errSendClosed() error {
	switch {
	case atomic.LoadUint32(&c.closeCalled) != 0:
		return ErrClosedConn

	// the only other error possible besides Conn being .Close()'ed is that
	// NodeLink was closed/stopped itself - on actual IO problems corresponding
	// error is delivered to particular Send that caused it.

	case atomic.LoadUint32(&c.nodeLink.closeCalled) != 0:
		return ErrLinkClosed

	default:
		return ErrLinkStopped
	}
}

// Send sends packet via connection
func (c *Conn) Send(pkt *PktBuf) error {
	// set pkt connId associated with this connection
	pkt.Header().ConnId = hton32(c.connId)
	var err error

	select {
	case <-c.closed:
		return c.errSendClosed()

	case c.nodeLink.txq <- txReq{pkt, c.txerr}:
		select {
		// tx request was sent to serveSend and is being transmitted on the wire.
		// the transmission may block for indefinitely long though and
		// we cannot interrupt it as the only way to interrupt is
		// .nodeLink.Close() which will close all other Conns.
		//
		// That's why we are also checking for c.closed while waiting
		// for reply from serveSend (and leave pkt to finish transmitting).
		//
		// NOTE after we return straight here serveSend won't be later
		// blocked on c.txerr<- because that backchannel is a non-blocking one.
		case <-c.closed:

			// also poll c.txerr here because: when there is TX error,
			// serveSend sends to c.txerr _and_ closes c.closed .
			// We still want to return actual transmission error to caller.
			select {
			case err = <-c.txerr:
				return err	// XXX if nil ?
			default:
				return c.errSendClosed()
			}

		case err = <-c.txerr:
			//fmt.Printf("%v <- c.txerr\n", err)
			return err	// XXX if nil ?
		}
	}

//	return err
}

// serveSend handles requests to transmit packets from client connections and
// serially executes them over associated node link.
func (nl *NodeLink) serveSend() {
	defer nl.serveWg.Done()
	for {
		select {
		case <-nl.closed:
			return

		case txreq := <-nl.txq:
			err := nl.sendPkt(txreq.pkt)
			fmt.Printf("sendPkt -> %v\n", err)

			// on IO error framing over peerLink becomes broken
			// so mark node link and all connections as closed and stop service

			/*
			if err != nil {

				select {
				case <-nl.closed:
					// error was due to closing NodeLink
					err = ErrLinkClosed
				default:
				}
			}
			*/

			txreq.errch <- err	// XXX recheck wakeup logic for err case

			// XXX we need to first wait till _both_ serveRecv & serveSend complete
			// and only then close all Conns. Reason: e.g. when remote shutdowns
			// both sendPkt and recvPkt get error. If recvPkt was
			// first serveRecv will be first to mark connections as
			// closed and even though sendPkt will return proper IO
			// error it won't be delivered as Conn.Send waiting for
			// it already waked up on c.closed without seeing
			// txreq.errch being ready.

			if err != nil {
				// XXX use errMu to lock vvv if needed
//				nl.sendErr = err

				// wake-up all conns & mark node link as closed
				// NOTE this also wakeups serveRecv/recvPkt
				nl.close()

				return
			}
		}
	}
}


// XXX used ?
func errClosedConn(err error) error {
	if err != nil {
		return err
	}
	return ErrClosedConn
}

// ---- raw IO ----

// sendPkt sends raw packet to peer
// tx error, if any, is returned as is and is analyzed in serveSend
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	if false {
		// XXX -> log
		fmt.Printf("%v > %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
		//defer fmt.Printf("\t-> sendPkt err: %v\n", err)
	}

	// NOTE Write writes data in full, or it is error
	_, err := nl.peerLink.Write(pkt.Data)
	return err
}

var ErrPktTooSmall = errors.New("packet too small")
var ErrPktTooBig   = errors.New("packet too big")

// recvPkt receives raw packet from peer
// rx error, if any, is returned as is and is analyzed in serveRecv
func (nl *NodeLink) recvPkt() (*PktBuf, error) {
	// TODO organize rx buffers management (freelist etc)
	// TODO cleanup lots of ntoh32(...)

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
	if ntoh32(pkth.Len) < PktHeadLen {
		return nil, ErrPktTooSmall	// length is a whole packet len with header
	}
	if ntoh32(pkth.Len) > MAX_PACKET_SIZE {
		return nil, ErrPktTooBig
	}

	// XXX -> pkt.Data = xbytes.Resize32(pkt.Data[:n], ntoh32(pkth.Len))
	if ntoh32(pkth.Len) > uint32(cap(pkt.Data)) {
		// grow rxbuf
		rxbuf2 := make([]byte, ntoh32(pkth.Len))
		copy(rxbuf2, pkt.Data[:n])
		pkt.Data = rxbuf2
	}
	// cut .Data len to length of packet
	pkt.Data = pkt.Data[:ntoh32(pkth.Len)]

	// read rest of pkt data, if we need to
	if n < len(pkt.Data) {
		_, err = io.ReadFull(nl.peerLink, pkt.Data[n:])
		if err != nil {
			return nil, err
		}
	}

	if false {
		// XXX -> log
		fmt.Printf("%v < %v: %v\n", nl.peerLink.LocalAddr(), nl.peerLink.RemoteAddr(), pkt)
	}

	return pkt, nil
}



// ---- for convenience: Dial/Listen ----

// Dial connects to address on named network and wrap the connection as NodeLink
// TODO +tls.Config
func Dial(ctx context.Context, network, address string) (*NodeLink, error) {
	d := net.Dialer{}
	peerConn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewNodeLink(peerConn, LinkClient), nil
}

// like net.Listener but Accept returns net.Conn wrapped in NodeLink
type Listener struct {
	net.Listener
}

func (l *Listener) Accept() (*NodeLink, error) {
	peerConn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewNodeLink(peerConn, LinkServer), nil
}

// TODO +tls.Config
// TODO +ctx		-> no as .Close() will interrupt all .Accept()
func Listen(network, laddr string) (*Listener, error) {
	l, err := net.Listen(network, laddr)
	if err != nil {
		return nil, err
	}
	return &Listener{l}, nil
}




// ----------------------------------------



// XXX ^^^ original description about notify/ask/answer
// All packets are classified to be of one of the following kind:
// - notify:	a packet is sent without expecting any reply
// - ask:	a packet is sent and reply is expected
// - answer:	a packet replying to previous ask
//
// At any time there can be several Asks packets issued by both nodes.
// For an Ask packet a single Answer reply is expected		XXX vs protocol where there is one request and list of replies ?
//
// XXX -> multiple subconnection explicitly closed with ability to chat
// multiple packets without spawning goroutines? And only single answer
// expected implemented that after only ask-send / answer-receive the
// (sub-)connection is explicitly closed ?
//
// XXX it is maybe better to try to avoid multiplexing by hand and let the OS do it?
//
// A reply to particular Ask packet, once received, will be delivered to
// corresponding goroutine which originally issued Ask	XXX this can be put into interface
