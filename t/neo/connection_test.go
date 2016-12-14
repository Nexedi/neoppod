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
	//"fmt"
	"net"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/myname"
	"lab.nexedi.com/kirr/go123/xerr"
)

// run function which raises exception, and return exception as error, if any
// XXX -> exc.Runx ?
func runx(xf func()) (err error) {
	//here := my.FuncName()
	here := myname.Func()
	defer exc.Catch(func(e *exc.Error) {
		err = exc.Addcallingcontext(here, e)
	})

	xf()
	return
}

// XXX move me out of here ?
type workGroup struct {
	*errgroup.Group
}

// like errgroup.Go but translates exceptions to errors
func (wg *workGroup) Gox(xf func ()) {
	wg.Go(func() error {
		return runx(xf)
	})
}

func WorkGroup() *workGroup {
	return &workGroup{&errgroup.Group{}}
}

func WorkGroupCtx(ctx context.Context) (*workGroup, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	return &workGroup{g}, ctx
}

////////////////////////////////////////

func xclose(c io.Closer) {
	err := c.Close()
	exc.Raiseif(err)
}

func xsend(c *Conn, pkt *PktBuf) {
	err := c.Send(pkt)
	exc.Raiseif(err)
}

func xrecv(c *Conn) *PktBuf {
	pkt, err := c.Recv()
	exc.Raiseif(err)
	return pkt
}

func xsendPkt(nl *NodeLink, pkt *PktBuf) {
	err := nl.sendPkt(pkt)
	exc.Raiseif(err)
}

func xrecvPkt(nl *NodeLink) *PktBuf {
	pkt, err := nl.recvPkt()
	exc.Raiseif(err)
	return pkt
}

func xwait(t *testing.T, w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
/*
	if err != nil {		// XXX -> exc.Raise ?
		t.Fatal(err)	// TODO include caller location
	}
*/
}

// Prepare PktBuf with content
func mkpkt(msgid uint32, msgcode uint16, payload []byte) *PktBuf {
	pkt := &PktBuf{make([]byte, PktHeadLen + len(payload))}
	h := pkt.Header()
	h.MsgId = hton32(msgid)
	h.MsgCode = hton16(msgcode)
	h.Len = hton32(PktHeadLen + 4)
	copy(pkt.Payload(), payload)
	return pkt
}

// Verify PktBuf is as expected
func xverifyPkt(pkt *PktBuf, msgid uint32, msgcode uint16, payload []byte) {
	errv := xerr.Errorv{}
	h := pkt.Header()
	// TODO include caller location
	if ntoh32(h.MsgId) != msgid {
		errv.Appendf("header: unexpected msgid %v  (want %v)", ntoh32(h.MsgId), msgid)
	}
	if ntoh16(h.MsgCode) != msgcode {
		errv.Appendf("header: unexpected msgcode %v  (want %v)", ntoh16(h.MsgCode), msgcode)
	}
	if ntoh32(h.Len) != uint32(PktHeadLen + len(payload)) {
		errv.Appendf("header: unexpected length %v  (want %v)", ntoh32(h.Len), PktHeadLen + len(payload))
	}
	if !bytes.Equal(pkt.Payload(), payload) {
		errv.Appendf("payload differ")	// XXX also print payload ?
	}

	exc.Raiseif( errv.Err() )
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
	// TODO catch exception -> add proper location from it -> t.Fatal (see git-backup)

	nl1, nl2 := nodeLinkPipe()

	// Close vs recvPkt
	wg := WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl1)
	})
	pkt, err := nl1.recvPkt()
	if !(pkt == nil && err == io.ErrClosedPipe) {
		t.Fatalf("NodeLink.recvPkt() after close: pkt = %v  err = %v", pkt, err)
	}
	xwait(t, wg)

	// Close vs sendPkt
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl2)
	})
	pkt = &PktBuf{[]byte("data")}
	err = nl2.sendPkt(pkt)
	if err != io.ErrClosedPipe {
		t.Fatalf("NodeLink.sendPkt() after close: err = %v", err)
	}
	xwait(t, wg)

	// TODO (?) every func: run with exception catcher (including t.Fatal)
	//	if caught:
	//		* ctx.cancel
	//		* wait all for finish
	//		* rethrough in main
	nl1, nl2 = nodeLinkPipe()

	// check raw exchange works
	wg, ctx := WorkGroupCtx(context.Background())
	wg.Gox(func() {
		// send ping; wait for pong
		pkt := mkpkt(1, 2, []byte("ping"))
		xsendPkt(nl1, pkt)
		pkt = xrecvPkt(nl1)
		xverifyPkt(pkt, 3, 4, []byte("pong"))
		/*
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
		err = verifyPkt("nl1 received", t, pkt, 3, 4, []byte("pong"))
		if err != nil {
			return err
		}
		return nil
		*/
	})
	wg.Gox(func() {
		// wait for ping; send pong
		pkt = xrecvPkt(nl2)
		xverifyPkt(pkt, 1, 2, []byte("ping"))
		pkt = mkpkt(3, 4, []byte("pong"))
		xsendPkt(nl2, pkt)
		/*
		pkt, err := nl2.recvPkt()
		if err != nil {
			t.Errorf("nl2.recvPkt: %v", err)
			return err
		}
		err = verifyPkt("nl2 received", t, pkt, 1, 2, []byte("ping"))
		if err != nil {
			return err
		}
		pkt = mkpkt(3, 4, []byte("pong"))
		err = nl2.sendPkt(pkt)
		if err != nil {
			t.Errorf("nl2.sendPkt: %v", err)
			return err
		}
		return nil
		*/
	})

	// close nodelinks either when checks are done, or upon first error
	wgclose := WorkGroup()
	wgclose.Gox(func() {
		<-ctx.Done()
		xclose(nl1)
		xclose(nl2)
	})

	xwait(t, wg)
	xwait(t, wgclose)



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
