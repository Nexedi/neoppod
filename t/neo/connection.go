// Copyright (C) 2016  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// NEO | Connection management

package neo

import (
	"errors"
	"io"
	"net"
	"sync"
	"unsafe"

	"fmt"
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
// new connection will be created - accepting first packet "request" - and all
// further communication send/receive exchange will be happenning in between
// those 2 connections.
//
// For a node to be able to accept new incoming connection it has to register
// corresponding handler with .HandleNewConn() . Without such handler
// registered the node will be able to only initiate new connections, not
// accept new ones from its peer.
//
// A NodeLink has to be explicitly closed, once it is no longer needed.
//
// It is safe to use NodeLink from multiple goroutines simultaneously.
type NodeLink struct {
	peerLink net.Conn		// raw conn to peer

	connMu   sync.Mutex	// TODO -> RW ?
	connTab  map[uint32]*Conn	// connId -> Conn associated with connId
	nextConnId uint32		// next connId to use for Conn initiated by us

	serveWg sync.WaitGroup

	handleNewConn func(conn *Conn)	// handler for new connections

	txreq	chan txReq		// tx requests from Conns go via here
	closed  chan struct{}
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

	// Conn has to be explicitly closed by user; it can also be closed by NodeLink.Close
	closeOnce sync.Once
	closed    chan struct{}
}

// Buffer with packet data
// XXX move me out of here
type PktBuf struct {
	Data	[]byte	// whole packet data including all headers	XXX -> Buf ?
}

// Get pointer to packet header
func (pkt *PktBuf) Header() *PktHead {
	// XXX check len(Data) < PktHead ? -> no, Data has to be allocated with cap >= PktHeadLen
	return (*PktHead)(unsafe.Pointer(&pkt.Data[0]))
}

// Get packet payload
func (pkt *PktBuf) Payload() []byte {
	return pkt.Data[PktHeadLen:]
}


type ConnRole int
const (
	ConnServer ConnRole = iota	// connection created as server
	ConnClient			// connection created as client

	// for testing:
	connNoRecvSend ConnRole = 1<<16	// do not spawn serveRecv & serveSend
	connFlagsMask  ConnRole = (1<<32 - 1) << 16
)

// Make a new NodeLink from already established net.Conn
//
// role specifies how to treat conn - either as server or client one.
// The differrence in between client and server roles are in connid % 2		XXX text
//
// Usually server role should be used for connections created via
// net.Listen/net.Accept and client role for connections created via net.Dial.
func NewNodeLink(conn net.Conn, role ConnRole) *NodeLink {
	var nextConnId uint32
	switch role&^connFlagsMask {
	case ConnServer:
		nextConnId = 0	// all initiated by us connId will be even
	case ConnClient:
		nextConnId = 1	// ----//---- odd
	default:
		panic("invalid conn role")
	}

	nl := NodeLink{
		peerLink:   conn,
		connTab:    map[uint32]*Conn{},
		nextConnId: nextConnId,
		txreq:      make(chan txReq),
		closed:     make(chan struct{}),
	}
	if role&connNoRecvSend == 0 {
		nl.serveWg.Add(2)
		go nl.serveRecv()
		go nl.serveSend()
	}
	return &nl
}

// Close node-node link.
// IO on connections established over it is automatically interrupted with an error.
func (nl *NodeLink) Close() error {
	close(nl.closed)
	err := nl.peerLink.Close()

	// wait for serve{Send,Recv} to complete
	fmt.Printf("%p serveWg.Wait ...\n", nl)
	nl.serveWg.Wait()
	fmt.Printf("%p\t (wait) -> woken up\n", nl)

	// close active Conns
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	for _, conn := range nl.connTab {
		conn.close()
	}
	nl.connTab = nil	// clear + mark closed
	return err
}

// send raw packet to peer
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	_, err := nl.peerLink.Write(pkt.Data)	// FIXME write Data in full
	if err != nil {
		// XXX do we need to retry if err is temporary?
		// TODO data could be written partially and thus the message stream is now broken
		// -> close connection / whole NodeLink ?
	}
	return err
}

// receive raw packet from peer
func (nl *NodeLink) recvPkt() (*PktBuf, error) {
	// TODO organize rx buffers management (freelist etc)
	// TODO cleanup lots of ntoh32(...)
	// XXX do we need to retry if err is temporary?
	// TODO on error framing is broken -> close connection / whole NodeLink ?

	// first read to read pkt header and hopefully up to page of data in 1 syscall
	pkt := &PktBuf{make([]byte, 4096)}
	n, err := io.ReadAtLeast(nl.peerLink, pkt.Data, PktHeadLen)
	if err != nil {
		return nil, err	// XXX err adjust ?
	}

	pkth := pkt.Header()

	// XXX -> better PktHeader.Decode() ?
	if ntoh32(pkth.Len) < PktHeadLen {
		panic("TODO pkt.Len < PktHeadLen")	// XXX err	(length is a whole packet len with header)
	}
	if ntoh32(pkth.Len) > MAX_PACKET_SIZE {
		panic("TODO message too big")	// XXX err
	}

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
			return nil, err	// XXX err adjust ?
		}
	}

	return pkt, nil
}


// worker for NewConn() & friends. Must be called with connMu held.
func (nl *NodeLink) newConn(connId uint32) *Conn {
	c := &Conn{nodeLink: nl,
		connId: connId,
		rxq: make(chan *PktBuf),
		txerr: make(chan error),
		closed: make(chan struct{}),
	}
	nl.connTab[connId] = c
	return c
}

// Create a connection on top of node-node link
func (nl *NodeLink) NewConn() *Conn {
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	if nl.connTab == nil {
		panic("NewConn() on closed node-link")
	}
	c := nl.newConn(nl.nextConnId)
	nl.nextConnId += 2
	return c
}


// serveRecv handles incoming packets routing them to either appropriate
// already-established connection or to new serving goroutine.
func (nl *NodeLink) serveRecv() {
	defer nl.serveWg.Done()
	for {
		// receive 1 packet
		println(nl, "serveRecv -> recv...")
		pkt, err := nl.recvPkt()
		fmt.Printf("%p\t (recv) -> %v\n", nl, err)
		if err != nil {
			// this might be just error on close - simply stop in such case
			select {
			case <-nl.closed:
				// XXX check err actually what is on interrupt?
				return
			}
			panic(err)	// XXX err
		}

		// pkt.ConnId -> Conn
		connId := ntoh32(pkt.Header().ConnId)
		var handleNewConn func(conn *Conn)

		nl.connMu.Lock()
		conn := nl.connTab[connId]
		if conn == nil {
			handleNewConn = nl.handleNewConn
			if handleNewConn != nil {
				conn = nl.newConn(connId)
			}
		}
		nl.connMu.Unlock()

		// we have not accepted incoming connection - ignore packet
		if conn == nil {
			// XXX also log?
			continue
		}

		// we are accepting new incoming connection - spawn
		// connection-serving goroutine
		if handleNewConn != nil {
			// TODO avoid spawning goroutine for each new Ask request -
			//	- by keeping pool of read inactive goroutine / conn pool ?
			go handleNewConn(conn)
		}

		// route packet to serving goroutine handler
		conn.rxq <- pkt
	}
}


// request to transmit a packet. Result error goes back to errch
type txReq struct {
	pkt *PktBuf
	errch chan error
}

// serveSend handles requests to transmit packets from client connections and
// serially executes them over associated node link.
func (nl *NodeLink) serveSend() {
	defer nl.serveWg.Done()
runloop:
	for {
		fmt.Printf("%p serveSend -> select ...\n", nl)
		select {
		case <-nl.closed:
			fmt.Printf("%p\t (send) -> closed\n", nl)
			break runloop

		case txreq := <-nl.txreq:
			fmt.Printf("%p\t (send) -> txreq\n", nl)
			err := nl.sendPkt(txreq.pkt)
			fmt.Printf("%p\t (send) -> err: %v\n", nl, err)
			if err != nil {
				// XXX also close whole nodeLink since tx framing now can be broken?
			}
			txreq.errch <- err
		}
	}

	fmt.Printf("%p\t (send) -> exit\n", nl)
}

// XXX move to NodeLink ctor ?
// Set handler for new incoming connections
func (nl *NodeLink) HandleNewConn(h func(*Conn)) {
	nl.connMu.Lock()
	defer nl.connMu.Unlock()
	nl.handleNewConn = h	// NOTE can change handler at runtime XXX do we need this?
}


// ErrClosedConn is the error indicated for read/write operations on closed Conn
var ErrClosedConn = errors.New("read/write on closed connection")

// Send packet via connection
func (c *Conn) Send(pkt *PktBuf) error {
	// set pkt connId associated with this connection
	pkt.Header().ConnId = hton32(c.connId)

	select {
	case <-c.closed:
		return ErrClosedConn

	case c.nodeLink.txreq <- txReq{pkt, c.txerr}:
		err := <-c.txerr
		return err	// XXX adjust err with c?
	}
}

// Receive packet from connection
func (c *Conn) Recv() (*PktBuf, error) {
	select {
	case <-c.closed:
		// XXX closed c.rxq might be just indicator for this
		return nil, ErrClosedConn

	case pkt, ok := <-c.rxq:
		if !ok {			// see ^^^
			return nil, io.EOF	// XXX check erroring & other errors?
		}
		return pkt, nil
	}
}

// worker for Close() & co
func (c *Conn) close() {
	c.closeOnce.Do(func() {
		fmt.Printf("%p Conn.close\n", c)
		close(c.closed)	// XXX better just close c.rxq + ??? for tx
	})
}

// Close connection
// Any blocked Send() or Recv() will be unblocked and return error
// XXX Send() - if started - will first complete (not to break framing)
func (c *Conn) Close() error {	// XXX do we need error here?
	// adjust nodeLink.connTab
	c.nodeLink.connMu.Lock()
	delete(c.nodeLink.connTab, c.connId)
	c.nodeLink.connMu.Unlock()
	c.close()
	return nil
}



// TODO
//func Dial(ctx context.Context, network, address string) (*NodeLink, error)	// + tls.Config
//func Listen(network, laddr string) (net.Listener, error)	// + tls.Config
//	ln.Accept -> will return net.Conn wrapped in NodeLink



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



// // Send notify packet to peer
// func (c *NodeLink) Notify(pkt XXX) error {
// 	// TODO
// }
//
// // Send packet and wait for replied answer packet
// func (c *NodeLink) Ask(pkt XXX) (answer Pkt, err error) {
// 	// TODO
// }
