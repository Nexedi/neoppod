// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package pipenet

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"

	"../../../xcommon/xsync"

	"lab.nexedi.com/kirr/go123/exc"
)

// we assume net.Pipe works ok; here we only test Listen/Accept/Dial routing
// XXX tests are ugly, non-robust and small coverage

type mklistener interface {
	Listen(string) (net.Listener, error)
}

func xlisten(n mklistener, laddr string) net.Listener {
	l, err := n.Listen(laddr)
	exc.Raiseif(err)
	return l
}

func xaccept(l net.Listener) net.Conn {
	c, err := l.Accept()
	exc.Raiseif(err)
	return c
}

type dialer interface {
	Dial(context.Context, string) (net.Conn, error)
}

func xdial(n dialer, addr string) net.Conn {
	c, err := n.Dial(context.Background(), addr)
	exc.Raiseif(err)
	return c
}

func xread(r io.Reader) string {
	buf := make([]byte, 4096)
	n, err := r.Read(buf)
	exc.Raiseif(err)
	return string(buf[:n])
}

func xwrite(w io.Writer, data string) {
	_, err := w.Write([]byte(data))
	exc.Raiseif(err)
}

func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

func assertEq(t *testing.T, a, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		fmt.Printf("not equal:\nhave: %v\nwant: %v\n", a, b)
		t.Errorf("not equal:\nhave: %v\nwant: %v", a, b)
		exc.Raise(0)
	}
}


func TestPipeNet(t *testing.T) {
	pnet := New("t")
	xaddr := func(addr string) *Addr {
		a, err := pnet.ParseAddr(addr)
		exc.Raiseif(err)
		return a
	}

	hα := pnet.Host("α")
	hβ := pnet.Host("β")

	_, err := hα.Dial(context.Background(), ":0")
	assertEq(t, err, &net.OpError{Op: "dial", Net: "pipet", Addr: xaddr("α:0"), Err: errConnRefused})

	l1 := xlisten(hα, "")
	assertEq(t, l1.Addr(), xaddr("α:0"))

	wg := &xsync.WorkGroup{}
	wg.Gox(func() {
		c1s := xaccept(l1)
		assertEq(t, c1s.LocalAddr(), xaddr("α:1"))
		assertEq(t, c1s.RemoteAddr(), xaddr("β:0"))

		assertEq(t, xread(c1s), "ping")
		xwrite(c1s, "pong")

		c2s := xaccept(l1)
		assertEq(t, c2s.LocalAddr(), xaddr("α:2"))
		assertEq(t, c2s.RemoteAddr(), xaddr("β:1"))

		assertEq(t, xread(c2s), "hello")
		xwrite(c2s, "world")
	})

	c1c := xdial(hβ, "α:0")
	assertEq(t, c1c.LocalAddr(), xaddr("β:0"))
	assertEq(t, c1c.RemoteAddr(), xaddr("α:1"))

	xwrite(c1c, "ping")
	assertEq(t, xread(c1c), "pong")

	c2c := xdial(hβ, "α:0")
	assertEq(t, c2c.LocalAddr(), xaddr("β:1"))
	assertEq(t, c2c.RemoteAddr(), xaddr("α:2"))

	xwrite(c2c, "hello")
	assertEq(t, xread(c2c), "world")

	xwait(wg)

	l2 := xlisten(hα, "")
	assertEq(t, l2.Addr(), xaddr("α:3"))
}
