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

package xnet
// network tracing
// XXX move to xnet/trace ?

import (
	"context"
	"net"
)

// NetTrace wraps underlying network with IO tracing layer
//
// Tracing is done via calling trace func right before corresponding packet
// is sent for Tx to underlying network. No synchronization for notification is
// performed - if one is required tracing func must implement such
// synchronization itself.
//
// only Tx events are traced:
// - because Write, contrary to Read, never writes partial data on non-error
// - because in case of pipenet tracing writes only is enough to get whole network exchange picture
func NetTrace(inner Network, trace func (t *TraceTx)) Network {
	return &netTrace{inner, trace}
}

// TraceTx is event corresponding to network transmission
type TraceTx struct {
	Src, Dst net.Addr
	Pkt      []byte
}

// netTrace wraps underlying Network such that whenever a connection is created
// it is wrapped with traceConn
type netTrace struct {
	inner Network
	trace func(t *TraceTx)
}

func (nt *netTrace) Network() string {
	return nt.inner.Network() // XXX + "+trace" ?
}

func (nt *netTrace) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := nt.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &traceConn{nt, c}, nil
}

func (nt *netTrace) Listen(laddr string) (net.Listener, error) {
	l, err := nt.inner.Listen(laddr)
	if err != nil {
		return nil, err
	}
	return &netTraceListener{nt, l}, nil
}

// netTraceListener wraps net.Listener to wrap accepted connections with traceConn
type netTraceListener struct {
	nt           *netTrace
	net.Listener
}

func (ntl *netTraceListener) Accept() (net.Conn, error) {
	c, err := ntl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &traceConn{ntl.nt, c}, nil
}

// traceConn wraps net.Conn and notifies tracer on Writes
type traceConn struct {
	nt       *netTrace
	net.Conn
}

func (tc *traceConn) Write(b []byte) (int, error) {
	t := &TraceTx{Src: tc.LocalAddr(), Dst: tc.RemoteAddr(), Pkt: b}
	tc.nt.trace(t)
	return tc.Conn.Write(b)
}
