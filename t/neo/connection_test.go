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
	//"lab.nexedi.com/kirr/go123/myname"
	"lab.nexedi.com/kirr/go123/xerr"
)

// XXX move me out of here ?
type workGroup struct {
	*errgroup.Group
}

// like errgroup.Go but translates exceptions to errors
func (wg *workGroup) Gox(xf func ()) {
	wg.Go(func() error {
		return exc.Runx(xf)
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

func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

// Prepare PktBuf with content
func _mkpkt(connid uint32, msgcode uint16, payload []byte) *PktBuf {
	pkt := &PktBuf{make([]byte, PktHeadLen + len(payload))}
	h := pkt.Header()
	h.ConnId = hton32(connid)
	h.MsgCode = hton16(msgcode)
	h.Len = hton32(PktHeadLen + 4)
	copy(pkt.Payload(), payload)
	return pkt
}

func mkpkt(msgcode uint16, payload []byte) *PktBuf {
	// in Conn exchange connid is automatically set by Conn.Send
	return _mkpkt(0, msgcode, payload)
}

// Verify PktBuf is as expected
func xverifyPkt(pkt *PktBuf, connid uint32, msgcode uint16, payload []byte) {
	errv := xerr.Errorv{}
	h := pkt.Header()
	// TODO include caller location
	if ntoh32(h.ConnId) != connid {
		errv.Appendf("header: unexpected connid %v  (want %v)", ntoh32(h.ConnId), connid)
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

func _nodeLinkPipe(flags1, flags2 ConnRole) (nl1, nl2 *NodeLink) {
	node1, node2 := net.Pipe()
	nl1 = NewNodeLink(node1, ConnClient | flags1)
	nl2 = NewNodeLink(node2, ConnServer | flags2)
	return nl1, nl2
}
// create NodeLinks connected via net.Pipe
func nodeLinkPipe() (nl1, nl2 *NodeLink) {
	return _nodeLinkPipe(0, 0)
}

func TestNodeLink(t *testing.T) {
	// TODO catch exception -> add proper location from it -> t.Fatal (see git-backup)

	// Close vs recvPkt
	nl1, nl2 := _nodeLinkPipe(connNoRecvSend, connNoRecvSend)
	wg := WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl1)
	})
	pkt, err := nl1.recvPkt()
	if !(pkt == nil && err == io.ErrClosedPipe) {
		t.Fatalf("NodeLink.recvPkt() after close: pkt = %v  err = %v", pkt, err)
	}
	xwait(wg)
	xclose(nl2)

	// Close vs sendPkt
	nl1, nl2 = _nodeLinkPipe(connNoRecvSend, connNoRecvSend)
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl1)
	})
	pkt = &PktBuf{[]byte("data")}
	err = nl1.sendPkt(pkt)
	if err != io.ErrClosedPipe {
		t.Fatalf("NodeLink.sendPkt() after close: err = %v", err)
	}
	xwait(wg)
	xclose(nl2)

	// check raw exchange works
	nl1, nl2 = _nodeLinkPipe(connNoRecvSend, connNoRecvSend)

	wg, ctx := WorkGroupCtx(context.Background())
	wg.Gox(func() {
		// send ping; wait for pong
		pkt := _mkpkt(1, 2, []byte("ping"))
		xsendPkt(nl1, pkt)
		pkt = xrecvPkt(nl1)
		xverifyPkt(pkt, 3, 4, []byte("pong"))
	})
	wg.Gox(func() {
		// wait for ping; send pong
		pkt = xrecvPkt(nl2)
		xverifyPkt(pkt, 1, 2, []byte("ping"))
		pkt = _mkpkt(3, 4, []byte("pong"))
		xsendPkt(nl2, pkt)
	})

	// close nodelinks either when checks are done, or upon first error
	wgclose := WorkGroup()
	wgclose.Gox(func() {
		<-ctx.Done()
		xclose(nl1)
		xclose(nl2)
	})

	xwait(wg)
	xwait(wgclose)


	// Test connections on top of nodelink

	// Close vs Recv
	nl1, nl2 = _nodeLinkPipe(0, connNoRecvSend)
	c := nl1.NewConn()
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(c)
	})
	pkt, err = c.Recv()
	if !(pkt == nil && err == ErrClosedConn) {
		t.Fatalf("Conn.Recv() after close: pkt = %v  err = %v", pkt, err)
	}
	xwait(wg)
	xclose(nl1)
	xclose(nl2)

	// Close vs Send
	nl1, nl2 = _nodeLinkPipe(0, connNoRecvSend)
	c = nl1.NewConn()
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(c)
	})
	pkt = &PktBuf{[]byte("data")}
	err = c.Send(pkt)
	if err != ErrClosedConn {
		t.Fatalf("Conn.Send() after close: err = %v", err)
	}
	xwait(wg)

	// NodeLink.Close vs Conn.Send/Recv
	c11 := nl1.NewConn()
	c12 := nl1.NewConn()
	wg = WorkGroup()
	wg.Gox(func() {
		pkt, err := c11.Recv()
		if !(pkt == nil && err == ErrClosedConn) {
			exc.Raisef("Conn.Recv() after NodeLink.close: pkt = %v  err = %v", pkt, err)
		}
	})
	wg.Gox(func() {
		pkt := &PktBuf{[]byte("data")}
		err := c12.Send(pkt)
		if err != ErrClosedConn {
			exc.Raisef("Conn.Send() after close: err = %v", err)
		}
	})
	tdelay()
	xclose(nl1)
	xwait(wg)
	xclose(c11)
	xclose(c12)
	xclose(nl2)	// for completeness

	// Conn accept + exchange
	nl1, nl2 = nodeLinkPipe()
	nl2.HandleNewConn(func(c *Conn) {
		// TODO raised err -> errch
		pkt := xrecv(c)
		xverifyPkt(pkt, c.connId, 33, []byte("ping"))

		// change pkt a bit and send it back
		xsend(c, mkpkt(34, []byte("pong")))
		xclose(c)
	})
	c1 := nl1.NewConn()
	pkt = mkpkt(33, []byte("ping"))
	xsend(c1, pkt)
	pkt2 := xrecv(c1)
	xverifyPkt(pkt2, c1.connId, 34, []byte("pong"))

	xclose(c1)
	xclose(nl1)
	xclose(nl2)

	// test 2 channels with replies comming in reversed time order
}
