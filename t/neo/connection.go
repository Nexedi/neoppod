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
	"encoding/binary"
	"io"
	"net"
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
}

// Buffer with packet data
type PktBuf struct {
	//PktHead
	Data	[]byte	// whole packet data including all headers
}

// Get pointer to packet header
func (pkt *PktBuf) Head() *PktHead {
	// XXX check len(Data) < PktHead ?
	return (*PktHead)(unsafe.Pointer(&pkt.Data[0]))
}


// Make a new NodeLink from already established net.Conn
func NewNodeLink(c net.Conn) *NodeLink {
	nl := NodeLink{
		peerLink: c,
		connTab:  map[uint32]*Conn{},
	}
	// XXX run serveRecv() in a goroutine here?
	return &nl
}

// send raw packet to peer
func (nl *NodeLink) sendPkt(pkt *PktBuf) error {
	n, err := nl.peerLink.Write(pkt.WholeBuffer())	// XXX WholeBuffer
	if err != nil {
		// XXX do we need to retry if err is temporary?
		// TODO data could be written partially and thus the message stream is now broken
		// -> close connection / whole NodeLink ?
	}
	return err
}

// receive raw packet from peer
func (nl *NodeLink) recvPkt() (pkt *PktBuf, err error) {
	// TODO organize rx buffers management (freelist etc)

	// first read to read pkt header and hopefully up to page of data in 1 syscall
	rxbuf := make([]byte, 4096)
	n, err := io.ReadAtLeast(nl.peerLink, rxbuf, PktHeadLen)
	if err != nil {
		panic(err)	// XXX err
	}

	pkt.Id   = binary.BigEndian.Uint32(rxbuf[0:])	// XXX -> PktHeader.Decode() ?
	pkt.Code = binary.BigEndian.Uint16(rxbuf[4:])
	pkt.Len  = binary.BigEndian.Uint32(rxbuf[6:])

	if pkt.Len < PktHeadLen {
		panic("TODO pkt.Len < PktHeadLen")	// XXX err	(length is a whole packet len with header)
	}
	if pkt.Len > MAX_PACKET_SIZE {
		panic("TODO message too big")	// XXX err
	}

	if pkt.Len > uint32(len(rxbuf)) {
		// grow rxbuf
		rxbuf2 := make([]byte, pkt.Len)
		copy(rxbuf2, rxbuf[:n])
		rxbuf = rxbuf2
	}

	// read rest of pkt data, if we need to
	_, err = io.ReadFull(nl.peerLink, rxbuf[n:pkt.Len])
	if err != nil {
		panic(err)	// XXX err
	}

	return pkt, nil
}


// Make a connection on top of node-node link
func (nl *NodeLink) NewConn() *Conn {
	c := &Conn{nodeLink: nl, rxq: make(chan *PktBuf)}
	// XXX locking
	nl.connTab[0] = c	// FIXME 0 -> msgid; XXX also check not a duplicate
	return c
}


// ServeRecv handles incoming packets routing them to either appropriate
// already-established connection or to new serving goroutine.
// TODO vs cancel
// XXX someone has to run serveRecv in a goroutine - XXX user or we internally ?
func (nl *NodeLink) serveRecv() error {
	for {
		// receive 1 packet
		pkt, err := nl.recvPkt()
		if err != nil {
			panic(err)	// XXX err
		}

		// if we don't yet have connection established for pkt.Id spawn connection-serving goroutine
		// XXX connTab locking
		conn := nl.connTab[pkt.Id]
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

// Set handler for new incoming connections
func (nl *NodeLink) HandleNewConn(h func(*Conn)) {
	// XXX locking
	nl.handleNewConn = h	// NOTE can change handler at runtime XXX do we need this?
}

// Close node-node link.
// IO on connections established over it is automatically interrupted with an error.
// XXX ^^^ recheck
func (nl *NodeLink) Close() error {
	// TODO
	return nil
}



// Send packet via connection
// XXX vs cancel
func (c *Conn) Send(pkt *PktBuf) error {
	pkt.Id = 0	// TODO next msgid, or using same msgid as received
	err := c.nodeLink.sendPkt(pkt)
	return err	// XXX do we need to adjust err ?
}

// Receive packet from connection
// XXX vs cancel
func (c *Conn) Recv() (*PktBuf, error) {
	pkt, ok := <-c.rxq
	if !ok {
		return nil, io.EOF	// XXX check erroring & other errors?
	}
	return pkt, nil
}

// Close connection
// Any blocked Send() or Recv() will be unblocked and return error
// XXX vs cancel
func (c *Conn) Close() error {	// XXX do we need error here?
	// TODO adjust c.nodeLink.connTab + more ?
	// TODO interrupt Send/Recv
	panic("TODO Conn.Close")
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
