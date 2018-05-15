// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package lonet

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"

	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
)


// FIXME dup from pipenet:
// ---- 8< ----
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
// ---- 8< ----

// TODO test go-go
// TODO test go-py

// XXX handshake:
// - ok,
// - econnrefused (no such host, port not listening)
// - network mismatch
// - invalid request


func TestLonet(t *testing.T) {
	// FIXME ~100% dup from TestPipeNet

	X := exc.Raiseif
	ctx := context.Background()

	subnet, err := Join(ctx, "")
	X(err)
	// XXX defer shutdown/rm this lonet fully?

	xaddr := func(addr string) *Addr {
		a, err := subnet.parseAddr(addr)
		X(err)
		return a
	}

	hα, err := subnet.NewHost(ctx, "α")
	X(err)

	hβ, err := subnet.NewHost(ctx, "β")
	X(err)

	assertEq(t, hα.Network(), subnet.Network())
	assertEq(t, hβ.Network(), subnet.Network())
	assertEq(t, hα.Name(), "α")
	assertEq(t, hβ.Name(), "β")

	l1, err := hα.Listen("")
	X(err)
	assertEq(t, l1.Addr(), xaddr("α:1"))

	// zero port always stays unused even after autobind
	_, err = hα.Dial(ctx, ":0")
	assertEq(t, err, &net.OpError{Op: "dial", Net: subnet.Network(), Addr: xaddr("α:0"), Err: errConnRefused})


	wg := &errgroup.Group{}
	wg.Go(exc.Funcx(func() {
		c1s := xaccept(l1)
		assertEq(t, c1s.LocalAddr(), xaddr("α:2"))
		assertEq(t, c1s.RemoteAddr(), xaddr("β:1"))

		assertEq(t, xread(c1s), "ping")
		xwrite(c1s, "pong")

		c2s := xaccept(l1)
		assertEq(t, c2s.LocalAddr(), xaddr("α:3"))
		assertEq(t, c2s.RemoteAddr(), xaddr("β:2"))

		assertEq(t, xread(c2s), "hello")
		xwrite(c2s, "world")
	}))

	c1c := xdial(hβ, "α:1")
	assertEq(t, c1c.LocalAddr(), xaddr("β:1"))
	assertEq(t, c1c.RemoteAddr(), xaddr("α:2"))

	xwrite(c1c, "ping")
	assertEq(t, xread(c1c), "pong")

	c2c := xdial(hβ, "α:1")
	assertEq(t, c2c.LocalAddr(), xaddr("β:2"))
	assertEq(t, c2c.RemoteAddr(), xaddr("α:3"))

	xwrite(c2c, "hello")
	assertEq(t, xread(c2c), "world")

	xwait(wg)

	l2 := xlisten(hα, ":0") // autobind again
	assertEq(t, l2.Addr(), xaddr("α:4"))
}
