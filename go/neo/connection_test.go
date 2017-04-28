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
// Connection management. Tests

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
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

func xnewconn(nl *NodeLink) *Conn {
	c, err := nl.NewConn()
	exc.Raiseif(err)
	return c
}

func xaccept(nl *NodeLink) *Conn {
	c, err := nl.Accept()
	exc.Raiseif(err)
	return c
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
	h.Len = hton32(PktHeadLen + uint32(len(payload)))
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
		errv.Appendf("payload differ")
	}

	exc.Raiseif( errv.Err() )
}

// delay a bit
// needed e.g. to test Close interaction with waiting read or write
// (we cannot easily sync and make sure e.g. read is started and became asleep)
//
// XXX JM suggested to really wait till syscall starts this way:
// - via polling get traceback for thread that is going to call syscall and eventuall block
// - if from that traceback we can see that blocking syscall is already called
//   -> this way we can know that it is already blocking and thus sleep-hack can be avoided
// this can be done via runtime/pprof -> "goroutine" predefined profile
func tdelay() {
	time.Sleep(1*time.Millisecond)
}

// create NodeLinks connected via net.Pipe
func _nodeLinkPipe(flags1, flags2 LinkRole) (nl1, nl2 *NodeLink) {
	node1, node2 := net.Pipe()
	nl1 = NewNodeLink(node1, LinkClient | flags1)
	nl2 = NewNodeLink(node2, LinkServer | flags2)
	return nl1, nl2
}

func nodeLinkPipe() (nl1, nl2 *NodeLink) {
	return _nodeLinkPipe(0, 0)
}

func TestNodeLink(t *testing.T) {
	// TODO catch exception -> add proper location from it -> t.Fatal (see git-backup)

	// Close vs recvPkt
	nl1, nl2 := _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
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
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
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

	// Close vs Accept
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl2)
	})
	c, err := nl2.Accept()
	if !(c == nil && err == ErrLinkClosed) {
		t.Fatalf("NodeLink.Accept() after close: conn = %v, err = %v", c, err)
	}
	// nl1 is not accepting connections - because it has LinkClient role
	// check Accept behaviour.
	c, err = nl1.Accept()
	if !(c == nil && err == ErrLinkNoListen) {
		t.Fatalf("NodeLink.Accept() on non-listening node link: conn = %v, err = %v", c, err)
	}
	xclose(nl1)

	// Close vs recvPkt on another side
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl2)
	})
	pkt, err = nl1.recvPkt()
	if !(pkt == nil && err == io.EOF) { // NOTE io.EOF on Read per io.Pipe
		t.Fatalf("NodeLink.recvPkt() after peer shutdown: pkt = %v  err = %v", pkt, err)
	}
	xwait(wg)
	xclose(nl1)

	// Close vs sendPkt on another side
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg = WorkGroup()
	wg.Gox(func() {
		tdelay()
		xclose(nl2)
	})
	pkt = &PktBuf{[]byte("data")}
	err = nl1.sendPkt(pkt)
	if err != io.ErrClosedPipe { // NOTE io.ErrClosedPipe on Write per io.Pipe
		t.Fatalf("NodeLink.sendPkt() after peer shutdown: pkt = %v  err = %v", pkt, err)
	}
	xwait(wg)
	xclose(nl1)

	// raw exchange
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)

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
	nl1, nl2 = _nodeLinkPipe(0, linkNoRecvSend)
	c = xnewconn(nl1)
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
	nl1, nl2 = _nodeLinkPipe(0, linkNoRecvSend)
	c = xnewconn(nl1)
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
	c11 := xnewconn(nl1)
	c12 := xnewconn(nl1)
	wg = WorkGroup()
	wg.Gox(func() {
		pkt, err := c11.Recv()
		if !(pkt == nil && err == ErrLinkClosed) {
			exc.Raisef("Conn.Recv() after NodeLink close: pkt = %v  err = %v", pkt, err)
		}
	})
	wg.Gox(func() {
		pkt := &PktBuf{[]byte("data")}
		err := c12.Send(pkt)
		if err != ErrLinkClosed {
			exc.Raisef("Conn.Send() after NodeLink close: err = %v", err)
		}
	})
	tdelay()
	xclose(nl1)
	xwait(wg)
	xclose(c11)
	xclose(c12)
	xclose(nl2)

	// NodeLink.Close vs Conn.Send/Recv and Accept on another side
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, 0)
	c21 := xnewconn(nl2)
	c22 := xnewconn(nl2)
	c23 := xnewconn(nl2)
	wg = WorkGroup()
	var errRecv error
	wg.Gox(func() {
		pkt, err := c21.Recv()
		want1 := io.EOF		  // if recvPkt wakes up due to peer close
		want2 := io.ErrClosedPipe // if recvPkt wakes up due to sendPkt wakes up first and closes nl1
		if !(pkt == nil && (err == want1 || err == want2)) {
			exc.Raisef("Conn.Recv after peer NodeLink shutdown: pkt = %v  err = %v", pkt, err)
		}

		errRecv = err
	})
	wg.Gox(func() {
		pkt := &PktBuf{[]byte("data")}
		err := c22.Send(pkt)
		want := io.ErrClosedPipe // always this in both due to peer close or recvPkt waking up and closing nl2
		if err != want {
			exc.Raisef("Conn.Send after peer NodeLink shutdown: %v", err)
		}

	})
	tdelay()
	xclose(nl1)
	xwait(wg)

	// XXX denoise vvv

	// NewConn after NodeLink stop
	c, err = nl2.NewConn()
	if err != ErrLinkDown {
		t.Fatalf("NewConn after NodeLink stop: %v", err)
	}

	// Recv/Send on another Conn
	pkt, err = c23.Recv()
	if !(pkt == nil && err == errRecv) {
		t.Fatalf("Conn.Recv 2 after peer NodeLink shutdown: pkt = %v  err = %v", pkt, err)
	}
	err = c23.Send(&PktBuf{[]byte("data")})
	if err != ErrLinkDown {
		t.Fatalf("Conn.Send 2 after peer NodeLink shutdown: %v", err)
	}

	// Recv/Send error on second call
	pkt, err = c21.Recv()
	if !(pkt == nil && err == ErrLinkDown) {
		t.Fatalf("Conn.Recv after NodeLink stop: pkt = %v  err = %v", pkt, err)
	}
	err = c22.Send(&PktBuf{[]byte("data")})
	if err != ErrLinkDown {
		t.Fatalf("Conn.Send after NodeLink stop: %v", err)
	}

	xclose(c23)
	// Recv/Send on closed Conn but not closed NodeLink
	pkt, err = c23.Recv()
	if !(pkt == nil && err == ErrClosedConn) {
		t.Fatalf("Conn.Recv after close but only stopped NodeLink: pkt = %v  err = %v", pkt, err)
	}
	err = c23.Send(&PktBuf{[]byte("data")})
	if err != ErrClosedConn {
		t.Fatalf("Conn.Send after close but only stopped NodeLink: %v", err)
	}

	xclose(nl2)
	// Recv/Send error after NodeLink close
	pkt, err = c21.Recv()
	if !(pkt == nil && err == ErrLinkClosed) {
		t.Fatalf("Conn.Recv after NodeLink stop: pkt = %v  err = %v", pkt, err)
	}
	err = c22.Send(&PktBuf{[]byte("data")})
	if err != ErrLinkClosed {
		t.Fatalf("Conn.Send after NodeLink stop: %v", err)
	}

	// NewConn after NodeLink close
	c, err = nl2.NewConn()
	if err != ErrLinkClosed {
		t.Fatalf("NewConn after NodeLink close: %v", err)
	}

	xclose(c21)
	xclose(c22)
	// Recv/Send error after Close & NodeLink shutdown
	pkt, err = c21.Recv()
	if !(pkt == nil && err == ErrClosedConn) {
		t.Fatalf("Conn.Recv after close and NodeLink close: pkt = %v  err = %v", pkt, err)
	}
	err = c22.Send(&PktBuf{[]byte("data")})
	if err != ErrClosedConn {
		t.Fatalf("Conn.Send after close and NodeLink close: %v", err)
	}


	// Conn accept + exchange
	nl1, nl2 = nodeLinkPipe()
	wg = WorkGroup()
	wg.Gox(func() {
		c := xaccept(nl2)

		pkt := xrecv(c)
		xverifyPkt(pkt, c.connId, 33, []byte("ping"))

		// change pkt a bit and send it back
		xsend(c, mkpkt(34, []byte("pong")))

		// one more time
		pkt = xrecv(c)
		xverifyPkt(pkt, c.connId, 35, []byte("ping2"))
		xsend(c, mkpkt(36, []byte("pong2")))

		xclose(c)
	})
	c = xnewconn(nl1)
	xsend(c, mkpkt(33, []byte("ping")))
	pkt = xrecv(c)
	xverifyPkt(pkt, c.connId, 34, []byte("pong"))
	xsend(c, mkpkt(35, []byte("ping2")))
	pkt = xrecv(c)
	xverifyPkt(pkt, c.connId, 36, []byte("pong2"))
	xwait(wg)

	xclose(c)
	xclose(nl1)
	xclose(nl2)

	// test 2 channels with replies coming in reversed time order
	nl1, nl2 = nodeLinkPipe()
	wg = WorkGroup()
	replyOrder := map[uint16]struct { // "order" in which to process requests
		start chan struct{}       // processing starts when start chan is ready
		next  uint16              // after processing this switch to next
	}{
		2: {make(chan struct{}), 1},
		1: {make(chan struct{}), 0},
	}
	close(replyOrder[2].start)

	wg.Gox(func() {
		for _ = range replyOrder {
			c := xaccept(nl2)

			wg.Gox(func() {
				pkt := xrecv(c)
				n := ntoh16(pkt.Header().MsgCode)
				x := replyOrder[n]

				// wait before it is our turn & echo pkt back
				<-x.start
				xsend(c, pkt)

				xclose(c)

				// tell next it can start
				if x.next != 0 {
					close(replyOrder[x.next].start)
				}
			})
		}
	})

	c1 := xnewconn(nl1)
	c2 := xnewconn(nl1)
	xsend(c1, mkpkt(1, []byte("")))
	xsend(c2, mkpkt(2, []byte("")))

	// replies must be coming in reverse order
	xechoWait := func(c *Conn, msgCode uint16) {
		pkt := xrecv(c)
		xverifyPkt(pkt, c.connId, msgCode, []byte(""))
	}
	xechoWait(c2, 2)
	xechoWait(c1, 1)
	xwait(wg)

	xclose(c1)
	xclose(c2)
	xclose(nl1)
	xclose(nl2)
}
