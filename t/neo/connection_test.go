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

func xsend(c *Conn, pkt PktBuf) {
	err := c.Send(pkt)
	if err != nil {
		t.Fatal(err)
	}
}

func xrecv(c *Conn) PktBuf {
	pkt, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

func TestNodeLink(t *testing.T) {
	// TODO verify NodeLink via net.Pipe
	node1, node2 := net.Pipe()
	nl1 := NewNodeLink(node1)
	nl2 := NewNodeLink(node2)

	// first check raw exchange works
	go func() {
		err := nl1.sendPkt(...)
		if err != nil {
			t.Fatal(...)	// XXX bad in goroutine
		}
	}()
	pkt, err := nl2.recvPkt(...)
	if err != nil {
		t.Fatal(...)
	}
	// TODO check pkt == what was sent
	// TODO also check ^^^ in opposite direction

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
