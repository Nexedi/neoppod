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

// Package xnet provides addons to std package net.
package xnet

import (
	"context"
	"net"

	"crypto/tls"
)

// Networker is interface representing access-point to a streaming network
type Networker interface {
	// Network returns name of the network
	Network() string

	// XXX +Addr() net.Addr	 -> address of this access-point on underlying network ?

	// Dial connects to addr on underlying network
	// see net.Dial for semantic details
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// Listen starts listening on local address laddr on underlying network access-point
	// see net.Listen for semantic details
	//
	// XXX also introduce xnet.Listener in which Accept() accepts also ctx?
	Listen(laddr string) (net.Listener, error)
}


// NetPlain creates Networker corresponding to regular network accessors from std package net.
//
// network is "tcp", "tcp4", "tcp6", "unix", etc...
func NetPlain(network string) Networker {
	return netPlain(network)
}

type netPlain string

func (n netPlain) Network() string {
	return string(n)
}

func (n netPlain) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{}
	return d.DialContext(ctx, string(n), addr)
}

func (n netPlain) Listen(laddr string) (net.Listener, error) {
	return net.Listen(string(n), laddr)
}

// NetTLS wraps underlying networker with TLS layer according to config.
//
// The config must be valid:
// - for tls.Client -- for Dial to work,
// - for tls.Server -- for Listen to work.
func NetTLS(inner Networker, config *tls.Config) Networker {
	return &netTLS{inner, config}
}

type netTLS struct {
	inner  Networker
	config *tls.Config
}

func (n *netTLS) Network() string {
	return n.inner.Network() + "+tls" // XXX is this a good idea?
}

func (n *netTLS) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := n.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return tls.Client(c, n.config), nil
}

func (n *netTLS) Listen(laddr string) (net.Listener, error) {
	l, err := n.inner.Listen(laddr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(l, n.config), nil
}
