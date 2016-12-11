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

// NEO | Connection management. Tests

package neo

import (
	"net"
	"testing"
)

func xsend(c *Conn, pkt *PktBuf) {
	err := c.Send(pkt)
	if err != nil {
		t.Fatal(err)	// XXX make sure this happens in main goroutine
	}
}

func xrecv(c *Conn) *PktBuf {
	pkt, err := c.Recv()
	if err != nil {
		t.Fatal(err)	// XXX make sure this happens in main goroutine
	}
	return pkt
}

func xsendPkt(nl *NodeLink, pkt *PktBuf) {
	err := nl.sendPkt(pkt)
	if err != nil {
		panic("TODO")
	}
	return err
}

func xrecvPkt(nl *NodeLink) *PktBuf {
	pkt, err := nl.recvPkt()
	if err != nil {
		panic("TODO")
	}
	return pkt
}

func xspawn(funcv ...func()) {
	for f := range funcv {
		go func() {
			defer func() {
				e := recover()
				if e == nil {
					return
				}
			}
		}
	}
}

func TestNodeLink(t *testing.T) {
	// TODO verify NodeLink via net.Pipe
	node1, node2 := net.Pipe()
	nl1 := NewNodeLink(node1)
	nl2 := NewNodeLink(node2)

	// TODO setup context
	// TODO on context.cancel -> nl{1,2} -> Close
	// TODO every func: run with exception catcher (including t.Fatal)
	//	if caught:
	//		* ctx.cancel
	//		* wait all for finish
	//		* rethrough in main

	// first check raw exchange works
	go func() {
		pkt = ...
		err := nl1.sendPkt(pkt)
		if err != nil {
			t.Fatal(...)	// XXX bad in goroutine
		}
		pkt, err = nl1.recvPkt()
		if err != nil {
			t.Fatal(...)
		}
		// TODO check pkt == what was sent back
	}()
	go func() {
		pkt, err := nl2.recvPkt()
		if err != nil {
			t.Fatal(...)
		}
		// TODO check pkt == what was sent

		// TODO change pkt a bit
		// send pkt back
		err = nl2.sendPkt(pkt)
		if err != nil {
			t.Fatal(...)	// XXX bad in goroutine
		}
	}

/*
	// test 1 channels on top of nodelink
	c1 := nl1.NewConn()
	nl2.HandleNewConn(func(c *Conn) {
		pkt := xrecv(c)	// XXX t.Fatal() must be called from main goroutine -> context.Cancel ?
		// change pkt a bit (TODO) and send it back
		err = c.Send(pkt)	// XXX err
		c.Close()		// XXX err
	})
	c1.Send(pkt)	// XXX err
	pkt2 := c1.Recv()	// XXX err
	// TODO check pkt2 is pkt1 + small modification

	// test 2 channels with replies comming in reversed time order
	*/
}
