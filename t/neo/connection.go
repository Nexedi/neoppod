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
	"unsafe"
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

	// TODO locking
	connTab  map[uint32]*Conn	// msgid -> connection associated with msgid
	handleNewConn func(conn *Conn)	// handler for new connections	XXX -> ConnHandler (a-la Handler in net/http) ?

	// TODO peerLink .LocalAddr() vs .RemoteAddr() -> msgid even/odd ? (XXX vs NAT ?)
	txreq	chan txReq		// tx requests from Conns go here

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
	rxq	  chan *PktBuf
	txerr     chan error	// transmit errors go back here
	closed    chan struct{}
}

// Buffer with packet data
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


// Make a new NodeLink from already established net.Conn
func NewNodeLink(c net.Conn) *NodeLink {
	nl := NodeLink{
		peerLink: c,
		connTab:  map[uint32]*Conn{},
		txreq:    make(chan txReq),
		closed:   make(chan struct{}),
	}
	// TODO go nl.serveRecv()
	// TODO go nl.serveSend()
	return &nl
}

// Close node-node link.
// IO on connections established over it is automatically interrupted with an error.
func (nl *NodeLink) Close() error {
	close(nl.closed)
	err := nl.peerLink.Close()
	// TODO close active Conns
	// XXX wait for serve{Send,Recv} to complete
	nl.wg.Wait()

}

// send raw packet to peer
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	_, err := nl.peerLink.Write(pkt.Data)
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

// Make a connection on top of node-node link
func (nl *NodeLink) NewConn() *Conn {
	c := &Conn{nodeLink: nl,
		rxq: make(chan *PktBuf),
		txerr: make(chan error),
		closed: make(chan struct{}),
	}
	// XXX locking
	nl.connTab[0] = c	// FIXME 0 -> msgid; XXX also check not a duplicate
	return c
}


// serveRecv handles incoming packets routing them to either appropriate
// already-established connection or to new serving goroutine.
func (nl *NodeLink) serveRecv() {
	for {
		// receive 1 packet
		pkt, err := nl.recvPkt()
		if err != nil {
			// this might be just error on close - simply stop in such case
			select {
			case <-nl.closed:
				// XXX check err actually what is on interrupt?
				return
			}
			panic(err)	// XXX err
		}

		// if we don't yet have connection established for pkt.MsgId -
		// spawn connection-serving goroutine
		// XXX connTab locking
		conn := nl.connTab[ntoh32(pkt.Header().MsgId)]
		if conn == nil {
			if nl.handleNewConn == nil {
				// we are not accepting incoming connections - ignore packet
				// XXX also log?
				continue
			}

			conn = nl.NewConn()
			// TODO avoid spawning goroutine for each new Ask request -
			//	- by keeping pool of read inactive goroutine / conn pool
			go nl.handleNewConn(conn)
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
	for {
		select {
		case <-nl.closed:
			break

		case txreq := <-nl.txreq:
			pkt.Header().MsgId = hton32(0)	// TODO next msgid, or using same msgid as received
			err := nl.sendPkt(txreq.pkt)
			if err != nil {
				// XXX also close whole nodeLink since tx framing now can be broken?
			}
			txreq.errch <- err
		}
	}
}

// XXX move to NodeLink ctor
// Set handler for new incoming connections
func (nl *NodeLink) HandleNewConn(h func(*Conn)) {
	// XXX locking
	nl.handleNewConn = h	// NOTE can change handler at runtime XXX do we need this?
}


// ErrClosedConn is the error indicated for read/write operations on closed Conn
var ErrClosedConn = errors.New("read/write on closed connection")

// Send packet via connection
func (c *Conn) Send(pkt *PktBuf) error {
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

// Close connection
// Any blocked Send() or Recv() will be unblocked and return error
// XXX Send() - if started - will first complete (not to break framing)
func (c *Conn) Close() error {	// XXX do we need error here?
	// TODO adjust c.nodeLink.connTab + more ?
	// XXX check for double close?
	close(c.closed)	// XXX better just close c.rxq + ??? for tx
	return nil
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



// // Send notify packet to peer
// func (c *NodeLink) Notify(pkt XXX) error {
// 	// TODO
// }
//
// // Send packet and wait for replied answer packet
// func (c *NodeLink) Ask(pkt XXX) (answer Pkt, err error) {
// 	// TODO
// }
