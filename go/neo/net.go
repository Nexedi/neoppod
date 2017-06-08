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

package neo
// unified interface for accessing various kinds of networks

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"crypto/tls"
	"../xcommon/pipenet"
)


// Network represents interface to work with some kind of streaming network
//
// NOTE in NEO a node usually needs to both 1) listen and serve incoming
// connections, and 2) dial peers. For this reason the interface is not split
// into Dialer and Listener.
type Network interface {
	// Dial connects to addr on underlying network
	// see net.Dial for semantic details
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// Listen starts listening on local address laddr on underlying network
	// see net.Listen for semantic details
	Listen(laddr string) (net.Listener, error)
}


// NetPlain creates Network corresponding to regular network
// network is "tcp", "tcp4", "tcp6", "unix", etc...
func NetPlain(network string) Network {
	return netPlain(network)
}

type netPlain string

func (n netPlain) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{}
	return d.DialContext(ctx, string(n), addr)
}

func (n netPlain) Listen(laddr string) (net.Listener, error) {
	return net.Listen(string(n), laddr)
}

// NetPipe creates Network corresponding to in-memory pipenet
// name is anything valid according to pipenet.New rules
func NetPipe(name string) Network {
	return pipenet.New(name)
}


// NetTLS wraps underlying network with TLS layer according to config
// The config must be valid for both tls.Client and tls.Server for Dial and Listen to work
func NetTLS(inner Network, config *tls.Config) Network {
	return &netTLS{inner, config}
}

type netTLS struct {
	inner  Network
	config *tls.Config
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

// ----------------------------------------

// String formats Address to networked address string
func (addr Address) String() string {
	// XXX in py if .Host == "" -> whole Address is assumed to be empty

	// e.g. on unix, pipenet, etc there is no host/port split - the address
	// is single string which we put into .Host and set .Port=0
	switch addr.Port {
	case 0:
		return addr.Host
	default:
		return net.JoinHostPort(addr.Host, fmt.Sprintf("%d", addr.Port))
	}
}

// ParseAddress parses networked address (XXX of form host:port) into NEO Address
func ParseAddress(addr string) (Address, error) {
	// e.g. on unix, pipenet, etc there is no host/port split - the address
	// is single string which we put into .Host and set .Port=0

	// TODO detect :port presence ?
	// XXX pass net.Addr() here instead of string?

	host, portstr, err := net.SplitHostPort(hostport)
	if err != nil {
		return Address{}, err
	}
	// XXX also lookup portstr in /etc/services (net.LookupPort) ?
	port, err := strconv.ParseUint(portstr, 10, 16)
	if err != nil {
		return Address{}, &net.AddrError{Err: "invalid port", Addr: hostport}
	}

	return Address{Host: host, Port: uint16(port)}, nil
}
