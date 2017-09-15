// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package neo
// Connection management. Tests

import (
	"bytes"
	"context"
	"io"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xsync"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
)

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

func xsendPkt(c interface { sendPkt(*PktBuf) error }, pkt *PktBuf) {
	err := c.sendPkt(pkt)
	exc.Raiseif(err)
}

func xrecvPkt(c interface { recvPkt() (*PktBuf, error) }) *PktBuf {
	pkt, err := c.recvPkt()
	exc.Raiseif(err)
	return pkt
}

func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

func xhandshake(ctx context.Context, c net.Conn, version uint32) {
	err := handshake(ctx, c, version)
	exc.Raiseif(err)
}

// xlinkError verifies that err is *LinkError and returns err.Err
func xlinkError(err error) error {
	le, ok := err.(*LinkError)
	if !ok {
		exc.Raisef("%#v is not *LinkError", err)
	}
	return le.Err
}

// xconnError verifies that err is *ConnError and returns err.Err
func xconnError(err error) error {
	ce, ok := err.(*ConnError)
	if !ok {
		exc.Raisef("%#v is not *ConnError", err)
	}
	return ce.Err
}

// Prepare PktBuf with content
func _mkpkt(connid uint32, msgcode uint16, payload []byte) *PktBuf {
	pkt := &PktBuf{make([]byte, pktHeaderLen + len(payload))}
	h := pkt.Header()
	h.ConnId = hton32(connid)
	h.MsgCode = hton16(msgcode)
	h.MsgLen = hton32(uint32(len(payload)))
	copy(pkt.Payload(), payload)
	return pkt
}

func (c *Conn) mkpkt(msgcode uint16, payload []byte) *PktBuf {
	// in Conn exchange connid is automatically set by Conn.sendPkt
	return _mkpkt(c.connId, msgcode, payload)
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
	if ntoh32(h.MsgLen) != uint32(len(payload)) {
		errv.Appendf("header: unexpected msglen %v  (want %v)", ntoh32(h.MsgLen), len(payload))
	}
	if !bytes.Equal(pkt.Payload(), payload) {
		errv.Appendf("payload differ:\n%s",
			pretty.Compare(string(payload), string(pkt.Payload())))
	}

	exc.Raiseif( errv.Err() )
}

// Verify PktBuf to match expected message
func xverifyPktMsg(pkt *PktBuf, connid uint32, msg Msg) {
	data := make([]byte, msg.neoMsgEncodedLen())
	msg.neoMsgEncode(data)
	xverifyPkt(pkt, connid, msg.neoMsgCode(), data)
}

// delay a bit
// needed e.g. to test Close interaction with waiting read or write
// (we cannot easily sync and make sure e.g. read is started and became asleep)
//
// XXX JM suggested to really wait till syscall starts this way:
// - via polling get traceback for thread that is going to call syscall and eventually block
// - if from that traceback we can see that blocking syscall is already called
//   -> this way we can know that it is already blocking and thus sleep-hack can be avoided
// this can be done via runtime/pprof -> "goroutine" predefined profile
func tdelay() {
	time.Sleep(1*time.Millisecond)
}

// create NodeLinks connected via net.Pipe
func _nodeLinkPipe(flags1, flags2 LinkRole) (nl1, nl2 *NodeLink) {
	node1, node2 := net.Pipe()
	nl1 = newNodeLink(node1, LinkClient | flags1)
	nl2 = newNodeLink(node2, LinkServer | flags2)
	return nl1, nl2
}

func nodeLinkPipe() (nl1, nl2 *NodeLink) {
	return _nodeLinkPipe(0, 0)
}

// XXX temp for cluster_test.go
// var NodeLinkPipe = nodeLinkPipe

func TestNodeLink(t *testing.T) {
	// TODO catch exception -> add proper location from it -> t.Fatal (see git-backup)

	//println("000")
	// Close vs recvPkt
	nl1, nl2 := _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg := &xsync.WorkGroup{}
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
	wg = &xsync.WorkGroup{}
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

	// {Close,CloseAccept} vs Accept
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		tdelay()
		xclose(nl2)
	})
	c, err := nl2.Accept()
	if !(c == nil && xlinkError(err) == ErrLinkClosed) {
		t.Fatalf("NodeLink.Accept() after close: conn = %v, err = %v", c, err)
	}
	wg.Gox(func() {
		tdelay()
		nl1.CloseAccept()
	})
	c, err = nl1.Accept()
	if !(c == nil && xlinkError(err) == ErrLinkNoListen) {
		t.Fatalf("NodeLink.Accept() after CloseAccept: conn = %v, err = %v", c, err)
	}
	xwait(wg)
	// nl1 is now not accepting connections - because it was CloseAccept'ed
	// check further Accept behaviour.
	c, err = nl1.Accept()
	if !(c == nil && xlinkError(err) == ErrLinkNoListen) {
		t.Fatalf("NodeLink.Accept() on non-listening node link: conn = %v, err = %v", c, err)
	}
	xclose(nl1)

	//println("111")
	// Close vs recvPkt on another side
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)
	wg = &xsync.WorkGroup{}
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
	wg = &xsync.WorkGroup{}
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

	//println("222")

	// raw exchange
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, linkNoRecvSend)

	wg, ctx := xsync.WorkGroupCtx(context.Background())
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
	wgclose := &xsync.WorkGroup{}
	wgclose.Gox(func() {
		<-ctx.Done()
		xclose(nl1)
		xclose(nl2)
	})

	xwait(wg)
	xwait(wgclose)

	//println("333")

	// ---- connections on top of nodelink ----

	// Close vs recvPkt
	nl1, nl2 = _nodeLinkPipe(0, linkNoRecvSend)
	c = xnewconn(nl1)
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		tdelay()
		xclose(c)
	})
	pkt, err = c.recvPkt()
	if !(pkt == nil && xconnError(err) == ErrClosedConn) {
		t.Fatalf("Conn.recvPkt() after close: pkt = %v  err = %v", pkt, err)
	}
	xwait(wg)
	xclose(nl1)
	xclose(nl2)

	//println("444")

	// Close vs sendPkt
	nl1, nl2 = _nodeLinkPipe(0, linkNoRecvSend)
	c = xnewconn(nl1)
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		tdelay()
		xclose(c)
	})
	pkt = c.mkpkt(0, []byte("data"))
	err = c.sendPkt(pkt)
	if xconnError(err) != ErrClosedConn {
		t.Fatalf("Conn.sendPkt() after close: err = %v", err)
	}
	xwait(wg)

	//println("555")

	// NodeLink.Close vs Conn.sendPkt/recvPkt
	c11 := xnewconn(nl1)
	c12 := xnewconn(nl1)
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		pkt, err := c11.recvPkt()
		if !(pkt == nil && xconnError(err) == ErrLinkClosed) {
			exc.Raisef("Conn.recvPkt() after NodeLink close: pkt = %v  err = %v", pkt, err)
		}
	})
	wg.Gox(func() {
		pkt := c12.mkpkt(0, []byte("data"))
		err := c12.sendPkt(pkt)
		if xconnError(err) != ErrLinkClosed {
			exc.Raisef("Conn.sendPkt() after NodeLink close: err = %v", err)
		}
	})
	tdelay()
	xclose(nl1)
	xwait(wg)
	xclose(c11)
	xclose(c12)
	xclose(nl2)

	//println(600)

	// NodeLink.Close vs Conn.sendPkt/recvPkt and Accept on another side
	nl1, nl2 = _nodeLinkPipe(linkNoRecvSend, 0)
	c21 := xnewconn(nl2)
	c22 := xnewconn(nl2)
	c23 := xnewconn(nl2)
	wg = &xsync.WorkGroup{}
	var errRecv error
	wg.Gox(func() {
		pkt, err := c21.recvPkt()
		want1 := io.EOF		  // if recvPkt wakes up due to peer close
		want2 := io.ErrClosedPipe // if recvPkt wakes up due to sendPkt wakes up first and closes nl1
		cerr := xconnError(err)
		if !(pkt == nil && (cerr == want1 || cerr == want2)) {
			exc.Raisef("Conn.recvPkt after peer NodeLink shutdown: pkt = %v  err = %v", pkt, err)
		}

		errRecv = cerr
	})
	wg.Gox(func() {
		pkt := c22.mkpkt(0, []byte("data"))
		err := c22.sendPkt(pkt)
		want := io.ErrClosedPipe // always this in both due to peer close or recvPkt waking up and closing nl2
		if xconnError(err) != want {
			exc.Raisef("Conn.sendPkt after peer NodeLink shutdown: %v", err)
		}

	})
	wg.Gox(func() {
		conn, err := nl2.Accept()
		if !(conn == nil && xlinkError(err) == ErrLinkDown) {
			exc.Raisef("Accept after peer NodeLink shutdown: conn = %v  err = %v", conn, err)
		}
	})
	tdelay()
	xclose(nl1)
	xwait(wg)

	//println(777)
	// XXX denoise vvv

	// NewConn after NodeLink shutdown
	c, err = nl2.NewConn()
	if xlinkError(err) != ErrLinkDown {
		t.Fatalf("NewConn after NodeLink shutdown: %v", err)
	}

	// Accept after NodeLink shutdown
	c, err = nl2.Accept()
	if xlinkError(err) != ErrLinkDown {
		t.Fatalf("Accept after NodeLink shutdown: conn = %v  err = %v", c, err)
	}

	// recvPkt/sendPkt on another Conn
	pkt, err = c23.recvPkt()
	if !(pkt == nil && xconnError(err) == errRecv) {
		t.Fatalf("Conn.recvPkt 2 after peer NodeLink shutdown: pkt = %v  err = %v", pkt, err)
	}
	err = c23.sendPkt(c23.mkpkt(0, []byte("data")))
	if xconnError(err) != ErrLinkDown {
		t.Fatalf("Conn.sendPkt 2 after peer NodeLink shutdown: %v", err)
	}

	// recvPkt/sendPkt error on second call
	pkt, err = c21.recvPkt()
	if !(pkt == nil && xconnError(err) == ErrLinkDown) {
		t.Fatalf("Conn.recvPkt after NodeLink shutdown: pkt = %v  err = %v", pkt, err)
	}
	err = c22.sendPkt(c22.mkpkt(0, []byte("data")))
	if xconnError(err) != ErrLinkDown {
		t.Fatalf("Conn.sendPkt after NodeLink shutdown: %v", err)
	}

	xclose(c23)
	// recvPkt/sendPkt on closed Conn but not closed NodeLink
	pkt, err = c23.recvPkt()
	if !(pkt == nil && xconnError(err) == ErrClosedConn) {
		t.Fatalf("Conn.recvPkt after close but only stopped NodeLink: pkt = %v  err = %v", pkt, err)
	}
	err = c23.sendPkt(c23.mkpkt(0, []byte("data")))
	if xconnError(err) != ErrClosedConn {
		t.Fatalf("Conn.sendPkt after close but only stopped NodeLink: %v", err)
	}

	xclose(nl2)
	// recvPkt/sendPkt NewConn/Accept error after NodeLink close
	pkt, err = c21.recvPkt()
	if !(pkt == nil && xconnError(err) == ErrLinkClosed) {
		t.Fatalf("Conn.recvPkt after NodeLink shutdown: pkt = %v  err = %v", pkt, err)
	}
	err = c22.sendPkt(c22.mkpkt(0, []byte("data")))
	if xconnError(err) != ErrLinkClosed {
		t.Fatalf("Conn.sendPkt after NodeLink shutdown: %v", err)
	}

	c, err = nl2.NewConn()
	if xlinkError(err) != ErrLinkClosed {
		t.Fatalf("NewConn after NodeLink close: %v", err)
	}
	c, err = nl2.Accept()
	if xlinkError(err) != ErrLinkClosed {
		t.Fatalf("Accept after NodeLink close: %v", err)
	}

	//println(888)

	xclose(c21)
	xclose(c22)
	// recvPkt/sendPkt error after Close & NodeLink shutdown
	pkt, err = c21.recvPkt()
	if !(pkt == nil && xconnError(err) == ErrClosedConn) {
		t.Fatalf("Conn.recvPkt after close and NodeLink close: pkt = %v  err = %v", pkt, err)
	}
	err = c22.sendPkt(c22.mkpkt(0, []byte("data")))
	if xconnError(err) != ErrClosedConn {
		t.Fatalf("Conn.sendPkt after close and NodeLink close: %v", err)
	}


	saveKeepClosed := connKeepClosed
	connKeepClosed = 10*time.Millisecond

	//println(999)
	//println()

	// Conn accept + exchange
	nl1, nl2 = nodeLinkPipe()
	nl1.CloseAccept()
	wg = &xsync.WorkGroup{}
	closed := make(chan int)
	wg.Gox(func() {
		c := xaccept(nl2)

		pkt := xrecvPkt(c)
		xverifyPkt(pkt, c.connId, 33, []byte("ping"))

		// change pkt a bit and send it back
		xsendPkt(c, c.mkpkt(34, []byte("pong")))

		// one more time
		pkt = xrecvPkt(c)
		xverifyPkt(pkt, c.connId, 35, []byte("ping2"))
		xsendPkt(c, c.mkpkt(36, []byte("pong2")))

		xclose(c)
		closed <- 1

		//println("X ααα")
		// once again as ^^^ but finish only with CloseRecv
		c2 := xaccept(nl2)
		//println("X ααα + 1")
		pkt = xrecvPkt(c2)
		//println("X ααα + 2")
		xverifyPkt(pkt, c2.connId, 41, []byte("ping5"))
		xsendPkt(c2, c2.mkpkt(42, []byte("pong5")))

		//println("X βββ")
		c2.CloseRecv()
		closed <- 2

		//println("X γγγ")

		// "connection refused" when trying to connect to not-listening peer
		c = xnewconn(nl2) // XXX should get error here?
		xsendPkt(c, c.mkpkt(38, []byte("pong3")))
		//println("X γγγ + 1")
		pkt = xrecvPkt(c)
		//println("X γγγ + 2")
		xverifyPktMsg(pkt, c.connId, errConnRefused)
		xsendPkt(c, c.mkpkt(40, []byte("pong4"))) // once again
		//println("X γγγ + 3")
		pkt = xrecvPkt(c)
		//println("X γγγ + 4")
		xverifyPktMsg(pkt, c.connId, errConnRefused)

		//println("X zzz")

		xclose(c)

	})

	//println("aaa")

	c1 := xnewconn(nl1)
	xsendPkt(c1, c1.mkpkt(33, []byte("ping")))
	pkt = xrecvPkt(c1)
	xverifyPkt(pkt, c1.connId, 34, []byte("pong"))
	xsendPkt(c1, c1.mkpkt(35, []byte("ping2")))
	pkt = xrecvPkt(c1)
	xverifyPkt(pkt, c1.connId, 36, []byte("pong2"))

	//println("111")
	// "connection closed" after peer closed its end
	<-closed
	//println("111 + closed")
	xsendPkt(c1, c1.mkpkt(37, []byte("ping3")))
	//println("111 + 1")
	pkt = xrecvPkt(c1)
	//println("111 + 2")
	xverifyPktMsg(pkt, c1.connId, errConnClosed)
	xsendPkt(c1, c1.mkpkt(39, []byte("ping4"))) // once again
	pkt = xrecvPkt(c1)
	//println("111 + 4")
	xverifyPktMsg(pkt, c1.connId, errConnClosed)
	// XXX also should get EOF on recv

	//println("222")
	// one more time but now peer does only .CloseRecv()
	c2 := xnewconn(nl1)
	xsendPkt(c2, c2.mkpkt(41, []byte("ping5")))
	pkt = xrecvPkt(c2)
	xverifyPkt(pkt, c2.connId, 42, []byte("pong5"))
	<-closed
	xsendPkt(c2, c2.mkpkt(41, []byte("ping6")))
	pkt = xrecvPkt(c2)
	xverifyPktMsg(pkt, c2.connId, errConnClosed)

	//println("333 z")
	xwait(wg)
	//println("444")

	// make sure entry for closed nl2.1 stays in nl2.connTab
	nl2.connMu.Lock()
	if cnl2 := nl2.connTab[1]; cnl2 == nil {
		t.Fatal("nl2.connTab[1] == nil  ; want \"closed\" entry")
	}
	nl2.connMu.Unlock()

	// make sure "closed" entry goes away after its time
	time.Sleep(3*connKeepClosed)
	nl2.connMu.Lock()
	if cnl2 := nl2.connTab[1]; cnl2 != nil {
		t.Fatalf("nl2.connTab[1] == %v after close time window  ; want nil", cnl2)
	}
	nl2.connMu.Unlock()

	//println("bbb")

	xclose(c1)
	xclose(c2)
	xclose(nl1)
	xclose(nl2)
	connKeepClosed = saveKeepClosed

	//println("\nsss")

	// test 2 channels with replies coming in reversed time order
	nl1, nl2 = nodeLinkPipe()
	wg = &xsync.WorkGroup{}
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
				pkt := xrecvPkt(c)
				n := ntoh16(pkt.Header().MsgCode)
				x := replyOrder[n]

				// wait before it is our turn & echo pkt back
				<-x.start
				xsendPkt(c, pkt)

				xclose(c)

				// tell next it can start
				if x.next != 0 {
					close(replyOrder[x.next].start)
				}
			})
		}
	})

	c1 = xnewconn(nl1)
	c2 = xnewconn(nl1)
	xsendPkt(c1, c1.mkpkt(1, []byte("")))
	xsendPkt(c2, c2.mkpkt(2, []byte("")))

	// replies must be coming in reverse order
	xechoWait := func(c *Conn, msgCode uint16) {
		pkt := xrecvPkt(c)
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


func TestHandshake(t *testing.T) {
	bg := context.Background()
	// handshake ok
	p1, p2 := net.Pipe()
	wg := &xsync.WorkGroup{}
	wg.Gox(func() {
		xhandshake(bg, p1, 1)
	})
	wg.Gox(func() {
		xhandshake(bg, p2, 1)
	})
	xwait(wg)
	xclose(p1)
	xclose(p2)

	// version mismatch
	p1, p2 = net.Pipe()
	var err1, err2 error
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		err1 = handshake(bg, p1, 1)
	})
	wg.Gox(func() {
		err2 = handshake(bg, p2, 2)
	})
	xwait(wg)
	xclose(p1)
	xclose(p2)

	err1Want := "pipe - pipe: handshake: protocol version mismatch: peer = 00000002  ; our side = 00000001"
	err2Want := "pipe - pipe: handshake: protocol version mismatch: peer = 00000001  ; our side = 00000002"

	if !(err1 != nil && err1.Error() == err1Want) {
		t.Errorf("handshake ver mismatch: p1: unexpected error:\nhave: %v\nwant: %v", err1, err1Want)
	}
	if !(err2 != nil && err2.Error() == err2Want) {
		t.Errorf("handshake ver mismatch: p2: unexpected error:\nhave: %v\nwant: %v", err2, err2Want)
	}

	// tx & rx problem
	p1, p2 = net.Pipe()
	err1, err2 = nil, nil
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		err1 = handshake(bg, p1, 1)
	})
	wg.Gox(func() {
		xclose(p2)
	})
	xwait(wg)
	xclose(p1)

	err11, ok := err1.(*HandshakeError)

	if !ok || !(err11.Err == io.ErrClosedPipe /* on Write */ || err11.Err == io.ErrUnexpectedEOF /* on Read */) {
		t.Errorf("handshake peer close: unexpected error: %#v", err1)
	}

	// ctx cancel
	p1, p2 = net.Pipe()
	ctx, cancel := context.WithCancel(bg)
	wg.Gox(func() {
		err1 = handshake(ctx, p1, 1)
	})
	tdelay()
	cancel()
	xwait(wg)
	xclose(p1)
	xclose(p2)

	err11, ok = err1.(*HandshakeError)

	if !ok || !(err11.Err == context.Canceled) {
		t.Errorf("handshake cancel: unexpected error: %#v", err1)
	}

}

// ---- recv1 mode ----

func xSend(c *Conn, msg Msg) {
	err := c.Send(msg)
	exc.Raiseif(err)
}

func xRecv(c *Conn) Msg {
	msg, err := c.Recv()
	exc.Raiseif(err)
	return msg
}

func xRecv1(l *NodeLink) Request {
	req, err := l.Recv1()
	exc.Raiseif(err)
	return req
}

func xSend1(l *NodeLink, msg Msg) {
	err := l.Send1(msg)
	exc.Raiseif(err)
}

func xverifyMsg(msg1, msg2 Msg) {
	if !reflect.DeepEqual(msg1, msg2) {
		exc.Raisef("messages differ:\n%s", pretty.Compare(msg1, msg2))
	}
}

func TestRecv1Mode(t *testing.T) {
	//println("000")
	// Send1
	nl1, nl2 := nodeLinkPipe()
	wg := &xsync.WorkGroup{}
	sync := make(chan int)
	wg.Gox(func() {
		defer func() {
			if e := recover(); e != nil {
				panic(e)
			}
		}()

		//println("X aaa")
		c := xaccept(nl2)
		//println("X aaa + 1")
		msg := xRecv(c)
		//println("X aaa + 2")
		xverifyMsg(msg, &Ping{})
		xSend(c, &Pong{})
		//println("X aaa + 3")
		msg = xRecv(c)
		//println("X aaa + 4")
		xverifyMsg(msg, errConnClosed)
		xclose(c)

		sync <- 1
		//println("X zzz")

		c = xaccept(nl2)
		msg = xRecv(c)
		//fmt.Println("X zzz + 1", c, msg)
		xverifyMsg(msg, &Error{ACK, "hello up there"})
		xSend(c, &Error{ACK, "hello to you too"})
		msg = xRecv(c)
		//println("X zzz + 2")
		xverifyMsg(msg, errConnClosed)
		//println("X zzz + 3")
		xclose(c)
	})

	//println("aaa")
	xSend1(nl1, &Ping{})

	// before next Send1 wait till peer receives errConnClosed from us
	// otherwise peer could be already in accept while our errConnClosed is received
	// and there is only one receiving thread there ^^^
	<-sync
	//println("bbb")
	xSend1(nl1, &Error{ACK, "hello up there"})
	//println("ccc")
	xwait(wg)

	//println("111\n")

	// Recv1: further packets with same connid are rejected with "connection closed"
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
		c := xnewconn(nl2)

		//println("aaa")
		xSend(c, &Ping{})
		//println("bbb")
		xSend(c, &Ping{})
		//println("ccc")
		msg := xRecv(c)
		//println("ddd")
		xverifyMsg(msg, errConnClosed)
	})

	_ = xRecv1(nl1)
	xwait(wg)

	// TODO link.Close vs Recv1
}

// ---- benchmarks ----

// rtt over chan - for comparision as base
func benchmarkChanRTT(b *testing.B, c12, c21 chan byte) {
	go func() {
		for {
			c, ok := <-c12
			if !ok {
				break
			}

			c21 <- c
		}
	}()

	for i := 0; i < b.N; i++ {
		c := byte(i)
		c12 <- c
		cc := <-c21
		if cc != c {
			b.Fatalf("sent %q != got %q", c, cc)
		}
	}

	close(c12)
}

func BenchmarkSyncChanRTT(b *testing.B) {
	benchmarkChanRTT(b, make(chan byte), make(chan byte))
}

func BenchmarkBufChanRTT(b *testing.B) {
	benchmarkChanRTT(b, make(chan byte, 1), make(chan byte, 1))
}

// rtt over (acceptq, rxq) & ack chans - base comparision for link.Accept + conn.Recv
func BenchmarkBufChanAXRXRTT(b *testing.B) {
	axq := make(chan chan byte)
	ack := make(chan byte)
	go func() {
		for {
			// accept
			rxq, ok := <-axq
			if !ok {
				break
			}

			// recv
			c := <-rxq

			// send back
			ack <- c
		}
	}()

	rxq := make(chan byte, 1) // buffered
	for i := 0; i < b.N; i++ {
		c := byte(i)
		axq <- rxq
		rxq <- c
		cc := <-ack
		if cc != c {
			b.Fatalf("sent %q != got %q", c, cc)
		}
	}

	close(axq)
}


var gosched = make(chan struct{})

// GoschedLocal is like runtime.Gosched but queus current goroutine on P-local
// runqueue instead of global runqueu.
// FIXME does not work - in the end goroutines appear on different Ps/Ms
func GoschedLocal() {
	go func() {
		gosched <- struct{}{}
	}()
	<-gosched
}

// rtt over net.Conn Read/Write
// if serveRecv=t - do RX path with additional serveRecv-style goroutine
func benchmarkNetConnRTT(b *testing.B, c1, c2 net.Conn, serveRecv bool, ghandoff bool) {
	buf1 := make([]byte, 1)
	buf2 := make([]byte, 1)

	// make func to recv from c into buf via selected rx strategy
	mkrecv := func(c net.Conn, buf []byte) func() (int, error) {
		var recv func() (int, error)
		if serveRecv {
			type rx struct {
				n   int
				erx error
			}
			rxq := make(chan rx, 1)
			rxghandoff := make(chan struct{})
			var serveRx func()
			serveRx = func() {
				for {
					n, erx := io.ReadFull(c, buf)
					//fmt.Printf("(go) %p rx -> %v %v\n", c, n, erx)
					rxq <- rx{n, erx}

					// good: reduce switch to receiver G latency
					// see comment about rxghandoff in serveRecv
					// in case of TCP/loopback saves ~5μs
					if ghandoff {
						<-rxghandoff
					}

					// stop on first error
					if erx != nil {
						return
					}

					if false {
						// bad - puts G in global runq and so it changes M
						runtime.Gosched()
					}
					if false {
						// bad - same as runtime.Gosched
						GoschedLocal()
					}

					if false {
						// bad - in the end Gs appear on different Ms
						go serveRx()
						return
					}
				}
			}

			go serveRx()

			recv = func() (int, error) {
				r := <-rxq
				if ghandoff {
					rxghandoff <- struct{}{}
				}
				return r.n, r.erx
			}

		} else {
			recv = func() (int, error) {
				return io.ReadFull(c, buf)
			}
		}
		return recv
	}

	recv1 := mkrecv(c1, buf1)
	recv2 := mkrecv(c2, buf2)


	b.ResetTimer()

	go func() {
		defer func() {
			//fmt.Printf("2: close\n")
			xclose(c2)
		}()

		for {
			n, erx := recv2()
			//fmt.Printf("2: rx %q\n", buf2[:n])
			if n > 0 {
				if n != len(buf2) {
					b.Fatalf("read -> %d bytes  ; want %d", n, len(buf2))
				}

				//fmt.Printf("2: tx %q\n", buf2)
				_, etx := c2.Write(buf2)
				if etx != nil {
					b.Fatal(etx)
				}
			}

			switch erx {
			case nil:
				// ok

			case io.ErrClosedPipe, io.EOF:	// net.Pipe, TCP
				return

			default:
				b.Fatal(erx) // XXX cannot call b.Fatal from non-main goroutine?
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		c := byte(i)
		buf1[0] = c
		//fmt.Printf("1: tx %q\n", buf1)
		_, err := c1.Write(buf1)
		if err != nil {
			b.Fatal(err)
		}

		n, err := recv1()
		//fmt.Printf("1: rx %q\n", buf1[:n])
		if !(n == len(buf1) && err == nil) {
			b.Fatalf("read back: n=%v  err=%v", n, err)
		}

		if buf1[0] != c {
			b.Fatalf("sent %q != got %q", c, buf1[0])
		}
	}

	//fmt.Printf("1: close\n")
	xclose(c1)
}

// rtt over net.Pipe - for comparision as base
func BenchmarkNetPipeRTT(b *testing.B) {
	c1, c2 := net.Pipe()
	benchmarkNetConnRTT(b, c1, c2, false, false)
}

func BenchmarkNetPipeRTTsr(b *testing.B) {
	c1, c2 := net.Pipe()
	benchmarkNetConnRTT(b, c1, c2, true, false)
}

func BenchmarkNetPipeRTTsrho(b *testing.B) {
	c1, c2 := net.Pipe()
	benchmarkNetConnRTT(b, c1, c2, true, true)
}

// xtcpPipe creates two TCP connections connected to each other via loopback
func xtcpPipe() (*net.TCPConn, *net.TCPConn) {
	// NOTE go sets TCP_NODELAY by default for TCP sockets
	l, err := net.Listen("tcp", "localhost:")
	exc.Raiseif(err)

	c1, err := net.Dial("tcp", l.Addr().String())
	exc.Raiseif(err)

	c2, err := l.Accept()
	exc.Raiseif(err)

	xclose(l)
	return c1.(*net.TCPConn), c2.(*net.TCPConn)
}

// rtt over TCP/loopback - for comparision as base
func BenchmarkTCPlo(b *testing.B) {
	c1, c2 := xtcpPipe()
	benchmarkNetConnRTT(b, c1, c2, false, false)
}

func BenchmarkTCPlosr(b *testing.B) {
	c1, c2 := xtcpPipe()
	benchmarkNetConnRTT(b, c1, c2, true, false)
}

func BenchmarkTCPlosrho(b *testing.B) {
	c1, c2 := xtcpPipe()
	benchmarkNetConnRTT(b, c1, c2, true, true)
}


// rtt over NodeLink via Ask1/Recv1
func benchmarkLinkRTT(b *testing.B, l1, l2 *NodeLink) {
	b.ResetTimer()

	go func() {
		defer xclose(l2)

		for {
			req, err := l2.Recv1()
			if err != nil {
				switch errors.Cause(err) {
				case ErrLinkDown:
					return

				default:
					b.Fatal(err)
				}
			}

			switch msg := req.Msg.(type) {
			default:
				b.Fatalf("read -> unexpected message %T", msg)

			case *GetObject:
				err = req.Reply(&AnswerObject{
					Oid:		msg.Oid,
					Serial:		msg.Serial,
					DataSerial:	msg.Tid,
				})
				if err != nil {
					b.Fatal(err)
				}
			}

			req.Close()
		}
	}()

	for i := 0; i < b.N; i++ {
		// NOTE keeping inside loop to simulate what happens in real Load
		get := &GetObject{}
		obj := &AnswerObject{}

		get.Oid = zodb.Oid(i)
		get.Serial = zodb.Tid(i+1)
		get.Tid = zodb.Tid(i+2)

		err := l1.Ask1(get, obj)
		if err != nil {
			b.Fatal(err)
		}

		if !(obj.Oid == get.Oid && obj.Serial == get.Serial && obj.DataSerial == get.Tid) {
			b.Fatalf("read back: %v  ; requested %v", obj, get)
		}

		// XXX must be obj.Release
		obj.Data.XRelease()
	}

	xclose(l1)
}

// XXX RTT over Conn.Send/Recv		(no msg encoding/decoding)
// XXX RTT over link.sendPkt/recvPkt	(no conn route)

// xlinkPipe creates two links interconnected to each other via c1 and c2
// XXX c1, c2 -> piper (who creates c1, c2) ?
// XXX overlap with nodeLinkPipe
func xlinkPipe(c1, c2 net.Conn) (*NodeLink, *NodeLink) {
	var l1, l2 *NodeLink

	wg := &xsync.WorkGroup{}
	wg.Gox(func() {
		l, err := Handshake(context.Background(), c1, LinkClient)
		exc.Raiseif(err)
		l1 = l
	})
	wg.Gox(func() {
		l, err := Handshake(context.Background(), c2, LinkServer)
		exc.Raiseif(err)
		l2 = l
	})
	xwait(wg)

	return l1, l2
}

func BenchmarkLinkNetPipeRTT(b *testing.B) {
	c1, c2 := net.Pipe()
	l1, l2 := xlinkPipe(c1, c2)
	benchmarkLinkRTT(b, l1, l2)
}

func BenchmarkLinkTCPRTT(b *testing.B) {
	c1, c2 := xtcpPipe()
	l1, l2 := xlinkPipe(c1, c2)
	benchmarkLinkRTT(b, l1, l2)
}
