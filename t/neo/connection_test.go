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
	//"fmt"

	"bytes"
	"context"
	"io"
	"fmt"
	"net"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func xsend(c *Conn, pkt *PktBuf) {
	err := c.Send(pkt)
	if err != nil {
		//t.Fatal(err)	// XXX make sure this happens in main goroutine
		panic("TODO")
	}
}

func xrecv(c *Conn) *PktBuf {
	pkt, err := c.Recv()
	if err != nil {
		//t.Fatal(err)	// XXX make sure this happens in main goroutine
		panic("TODO")
	}
	return pkt
}

func xsendPkt(nl *NodeLink, pkt *PktBuf) {
	err := nl.sendPkt(pkt)
	if err != nil {
		panic("TODO")
	}
}

func xrecvPkt(nl *NodeLink) *PktBuf {
	pkt, err := nl.recvPkt()
	if err != nil {
		panic("TODO")
	}
	return pkt
}

/*
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
*/


// // run f in a goroutine; check its return error; if != nil -> t.Fatal in main goroutine
// func xgo(t *testing.T, f func() error) {
// 	var err error
// 	done := make(chan struct{})
// 	go func() {
// 		err = f()
// 		close(done)
// 		if err != nil {
// 			panic(err)	// XXX temp - see vvv
// 		}
// 	}()
// 	/* FIXME below just blocks main goroutine waiting for f() to complete
// 	<-done
// 	if err != nil {
// 		t.Fatal(err)	// TODO adjust lineno (report calling location, not here)
// 	}
// 	*/
// }

func xwait(t *testing.T, w interface { Wait() error }) {
	err := w.Wait()
	if err != nil {
		t.Fatal(err)	// TODO include caller location
	}
}

// Prepare PktBuf with content
func mkpkt(msgid uint32, msgcode uint16, payload []byte) *PktBuf {
	pkt := &PktBuf{make([]byte, PktHeadLen + len(payload))}
	pkth := pkt.Header()
	pkth.MsgId = hton32(msgid)
	pkth.MsgCode = hton16(msgcode)
	pkth.Len = hton32(PktHeadLen + 4)
	copy(pkt.Payload(), payload)
	return pkt
}

// delay a bit
// needed e.g. to test Close interaction with waiting read or write
// (we cannot easily sync and make sure e.g. read is started and became asleep)
func tdelay() {
	time.Sleep(1*time.Millisecond)
}

// create NodeLinks connected via net.Pipe
func nodeLinkPipe() (nl1, nl2 *NodeLink) {
	node1, node2 := net.Pipe()
	nl1 = NewNodeLink(node1)
	nl2 = NewNodeLink(node2)
	return nl1, nl2
}


func TestNodeLink(t *testing.T) {
	nl1, nl2 := nodeLinkPipe()

	// Close vs recvPkt
	g := &errgroup.Group{}
	g.Go(func() error {
		tdelay()
		return nl1.Close()
	})
	pkt, err := nl1.recvPkt()
	if !(pkt == nil && err == io.ErrClosedPipe) {
		t.Fatalf("NodeLink.recvPkt() after close: pkt = %v  err = %v", pkt, err)
	}
	xwait(t, g)

	// Close vs sendPkt
	g = &errgroup.Group{}
	g.Go(func() error {
		tdelay()
		return nl2.Close()
	})
	pkt = &PktBuf{[]byte("data")}
	err = nl2.sendPkt(pkt)
	if err != io.ErrClosedPipe {
		t.Fatalf("NodeLink.sendPkt() after close: err = %v", err)
	}
	xwait(t, g)

	// TODO (?) every func: run with exception catcher (including t.Fatal)
	//	if caught:
	//		* ctx.cancel
	//		* wait all for finish
	//		* rethrough in main
	nl1, nl2 = nodeLinkPipe()

	// check raw exchange works
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		// send ping; wait for pong
		pkt := mkpkt(1, 2, []byte("ping"))
		err := nl1.sendPkt(pkt)
		if err != nil {
			t.Errorf("nl1.sendPkt: %v", err)
			return err
		}
		pkt, err = nl1.recvPkt()
		if err != nil {
			t.Errorf("nl1.recvPkt: %v", err)
			return err
		}
		// TODO vvv also check msgid, msgcode
		if !bytes.Equal(pkt.Payload(), []byte("pong")) {
			// XXX vvv -> util ?
			e := fmt.Errorf("nl1 received: %v  ; want \"pong\"", pkt.Data)
			t.Error(e)
			return e
		}
		return nil
	})
	g.Go(func() error {
		// wait for ping; send pong
		pkt, err := nl2.recvPkt()
		if err != nil {
			t.Errorf("nl2.recvPkt: %v", err)
			return err
		}
		// TODO vvv also check msgid, msgcode
		if !bytes.Equal(pkt.Payload(), []byte("ping")) {
			// XXX vvv -> util ?
			e := fmt.Errorf("nl2 received: %v  ; want \"ping\"", pkt.Data)
			t.Error(e)
			return e
		}
		pkt = mkpkt(3, 4, []byte("pong"))
		err = nl2.sendPkt(pkt)
		if err != nil {
			t.Errorf("nl2.sendPkt: %v", err)
			return err
		}
		return nil
	})

	g2 := &errgroup.Group{}
	g2.Go(func() error {
		<-ctx.Done()
		ev := xerror.Errorv{}
		ev.addif( nl1.Close() )
		ev.addif( nl2.Close() )
		return ev.Reduce()	// nil if len==0; [0] if len==1; [...] otherwise

		/*
		for c := range []io.Closer{nl1, nl2} {
			err := c.Close()
			if err != nil {
				ev = append(ev, err)
			}
		}
		*/
		/*
		err := nl1.Close()
		err2 := nl2.Close()
		if err == nil {
			err = err2
		}
		*/
	})

	xwait(g)
	xwait(g2)

	// close nodelinks either when checks are done, or upon first error
	closed := make(chan struct{})
	go func() {
		<-ctx.Done()
		nl1.Close()	// XXX err
		nl2.Close()	// XXX err
		close(closed)
	}()

	xwait(t, g)
	<-closed


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
