// Copyright (C) 2017  Nexedi SA and Contributors.
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

package xnet
// network tracing

import (
	"context"
	"net"
)

// NetTrace wraps underlying networker with IO tracing layer
//
// Tracing is done via calling trace func right after corresponding networking
// event happenned.  No synchronization for notification is performed - if one
// is required tracing func must implement such synchronization itself.
//
// only initiation events are traced:
//
// 1. Tx only (no Rx):
//    - because Write, contrary to Read, never writes partial data on non-error
//    - because in case of pipenet tracing writes only is enough to get whole network exchange picture
//
// 2. Dial only (no Accept)
//    - for similar reasons.
//
// WARNING NetTrace functionality is currently very draft.
func NetTrace(inner Networker, tracer Tracer) Networker {
	return &netTrace{inner, tracer}
}

// Tracer is the interface that needs to be implemented by network trace receivers
type Tracer interface {
	TraceNetConnect(*TraceConnect)
	TraceNetListen(*TraceListen)
	TraceNetTx(*TraceTx)
}

// TraceConnect is event corresponding to network connection
type TraceConnect struct {
	// XXX also put networker?
	Src, Dst net.Addr
	Dialed   string
}

// TraceListen is event corresponding to network listening
type TraceListen struct {
	// XXX also put networker?
	Laddr net.Addr
}

// TraceTx is event corresponding to network transmission
type TraceTx struct {
	// XXX also put network somehow?
	Src, Dst net.Addr
	Pkt      []byte
}

// netTrace wraps underlying Networker such that whenever a connection is created
// it is wrapped with traceConn.
type netTrace struct {
	inner  Networker
	tracer Tracer
}

func (nt *netTrace) Network() string {
	return nt.inner.Network() // XXX + "+trace" ?
}

func (nt *netTrace) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// XXX +TraceNetDialPost ?
	c, err := nt.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	nt.tracer.TraceNetConnect(&TraceConnect{Src: c.LocalAddr(), Dst: c.RemoteAddr(), Dialed: addr})
	return &traceConn{nt, c}, nil
}

func (nt *netTrace) Listen(laddr string) (net.Listener, error) {
	// XXX +TraceNetListenPre ?
	l, err := nt.inner.Listen(laddr)
	if err != nil {
		return nil, err
	}
	nt.tracer.TraceNetListen(&TraceListen{Laddr: l.Addr()})
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
	// XXX +TraceNetTxPre ?
	n, err := tc.Conn.Write(b)
	if err == nil {
		tc.nt.tracer.TraceNetTx(&TraceTx{Src: tc.LocalAddr(), Dst: tc.RemoteAddr(), Pkt: b})
	}
	return n, err
}
