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

// Package pipenet provides TCP-like synchronous in-memory network of net.Pipes.
//
// Addresses on pipenet are host:port pairs. A host is xnet.Networker and so
// can be worked with similarly to regular TCP network with Dial/Listen/Accept/...
//
// Example:
//
//	net := pipenet.New("")
//	h1 := net.Host("abc")
//	h2 := net.Host("def")
//
//	l, err := h1.Listen(":10")       // starts listening on address "abc:10"
//	go func() {
//		csrv, err := l.Accept()  // csrv will have LocalAddr "abc:10"
//	}()
//	ccli, err := h2.Dial("abc:10")   // ccli will have RemoteAddr "def:10"
//
// Pipenet might be handy for testing interaction of networked applications in 1
// process without going to OS networking stack.
package pipenet

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
)

const NetPrefix = "pipe" // pipenet package creates only "pipe*" networks

var (
	errNetClosed		= errors.New("network connection closed")
	errAddrAlreadyUsed	= errors.New("address already in use")
	errAddrNoListen		= errors.New("cannot listen on requested address")
	errConnRefused		= errors.New("connection refused")
)

// Addr represents address of a pipenet endpoint
type Addr struct {
	Net      string	// full network name, e.g. "pipe"
	Host     string // name of host access point on the network
	Port     int	// port on host
}

// Network implements synchronous in-memory network of pipes
type Network struct {
	// name of this network under "pipe" namespace -> e.g. ""
	// full network name will be reported as "pipe"+name
	name string

	// big network lock for everything dynamic under Network
	// (e.g. Host.socketv too)
	mu      sync.Mutex

	hostMap map[string]*Host
}

// Host represents named access point on Network
type Host struct {
	network *Network
	name    string

	// NOTE protected by Network.mu
	socketv []*socket // port -> listener | conn  ; [0] is always nil
}

var _ xnet.Networker = (*Host)(nil)

// socket represents one endpoint entry on Network
// it can be either already connected or listening
type socket struct {
	host	*Host	// host/port this socket is bound to
	port	int

	conn     *conn     // connection endpoint is here if != nil
	listener *listener // listener is waiting here if != nil
}

// conn represents one endpoint of connection created under Network
type conn struct {
	socket *socket
	peersk *socket // the other side of this connection

	net.Conn

	closeOnce sync.Once
}

// listener implements net.Listener for piped network
type listener struct {
	// network/host/port we are listening on
	socket *socket

	dialq    chan dialReq	// Dial requests to our port go here
	down     chan struct{}	// Close -> down=ready

	closeOnce sync.Once
}

// dialReq represents one dial request to listener
type dialReq struct {
	from *Host
	resp chan net.Conn
}

// ----------------------------------------

// New creates new pipenet Network
//
// name is name of this network under "pipe" namespace, e.g. "α" will give full network name "pipeα".
//
// New does not check whether network name provided is unique.
func New(name string) *Network {
	return &Network{name: name, hostMap: make(map[string]*Host)}
}

// Host returns network access point by name
//
// If there was no such host before it creates new one.
func (n *Network) Host(name string) *Host {
	n.mu.Lock()
	defer n.mu.Unlock()

	host := n.hostMap[name]
	if host == nil {
		host = &Host{network: n, name: name}
		n.hostMap[name] = host
	}

	return host
}

// resolveAddr resolves addr on the network from the host point of view
// must be called with Network.mu held
func (h *Host) resolveAddr(addr string) (host *Host, port int, err error) {
	a, err := h.network.ParseAddr(addr)
	if err != nil {
		return nil, 0, err
	}

	// local host if host name omitted
	if a.Host == "" {
		a.Host = h.name
	}

	host = h.network.hostMap[a.Host]
	if host == nil {
		return nil, 0, &net.AddrError{Err: "no such host", Addr: addr}
	}

	return host, a.Port, nil
}

// Listen starts new listener
//
// It either allocates free port if laddr is "" or with 0 port, or binds to laddr.
// Once listener is started, Dials could connect to listening address.
// Connection requests created by Dials could be accepted via Accept.
func (h *Host) Listen(laddr string) (net.Listener, error) {
	h.network.mu.Lock()
	defer h.network.mu.Unlock()

	var sk *socket

	if laddr == "" {
		laddr = ":0"
	}

	var netladdr net.Addr
	lerr := func(err error) error {
		return &net.OpError{Op: "listen", Net: h.Network(), Addr: netladdr, Err: err}
	}

	host, port, err := h.resolveAddr(laddr)
	if err != nil {
		return nil, lerr(err)
	}

	netladdr = &Addr{Net: h.Network(), Host: host.name, Port: port}

	if host != h {
		return nil, lerr(errAddrNoListen)
	}

	// find first free port if autobind requested
	if port == 0 {
		sk = h.allocFreeSocket()

	// else allocate socket in-place
	} else {
		// grow if needed
		for port >= len(h.socketv) {
			h.socketv = append(h.socketv, nil)
		}

		if h.socketv[port] != nil {
			return nil, lerr(errAddrAlreadyUsed)
		}

		sk = &socket{host: h, port: port}
		h.socketv[port] = sk
	}

	// create listener under socket
	l := &listener{
		socket:  sk,
		dialq:	 make(chan dialReq),
		down:	 make(chan struct{}),
	}
	sk.listener = l

	return l, nil
}

// Close closes the listener
// it interrupts all currently in-flight calls to Accept
func (l *listener) Close() error {
	l.closeOnce.Do(func() {
		close(l.down)

		sk := l.socket
		h := sk.host
		n := h.network

		n.mu.Lock()
		defer n.mu.Unlock()

		sk.listener = nil
		if sk.empty() {
			h.socketv[sk.port] = nil
		}
	})
	return nil
}

// Accept tries to connect to Dial called with addr corresponding to our listener
func (l *listener) Accept() (net.Conn, error) {
	h := l.socket.host
	n := h.network

	select {
	case <-l.down:
		return nil, &net.OpError{Op: "accept", Net: h.Network(), Addr: l.Addr(), Err: errNetClosed}

	case req := <-l.dialq:
		// someone dialed us - let's connect
		pc, ps := net.Pipe()

		// allocate sockets and register conns to Network under them
		n.mu.Lock()

		skc := req.from.allocFreeSocket()
		sks := h.allocFreeSocket()
		skc.conn = &conn{socket: skc, peersk: sks, Conn: pc}
		sks.conn = &conn{socket: sks, peersk: skc, Conn: ps}

		n.mu.Unlock()

		req.resp <- skc.conn
		return sks.conn, nil
	}
}

// Dial dials address on the network
//
// It tries to connect to Accept called on listener corresponding to addr.
func (h *Host) Dial(ctx context.Context, addr string) (net.Conn, error) {
	var netaddr net.Addr
	derr := func(err error) error {
		return &net.OpError{Op: "dial", Net: h.Network(), Addr: netaddr, Err: err}
	}

	n := h.network
	n.mu.Lock()

	host, port, err := h.resolveAddr(addr)
	if err != nil {
		n.mu.Unlock()
		return nil, derr(err)
	}

	netaddr = &Addr{Net: h.Network(), Host: host.name, Port: port}

	if port >= len(host.socketv) {
		n.mu.Unlock()
		return nil, derr(errConnRefused)
	}

	sks := host.socketv[port]
	if sks == nil || sks.listener == nil {
		n.mu.Unlock()
		return nil, derr(errConnRefused)
	}
	l := sks.listener

	// NOTE Accept is locking n.mu -> we must release n.mu before sending dial request
	n.mu.Unlock()

	resp := make(chan net.Conn)
	select {
	case <-ctx.Done():
		return nil, derr(ctx.Err())

	case <-l.down:
		return nil, derr(errConnRefused)

	case l.dialq <- dialReq{from: h, resp: resp}:
		return <-resp, nil
	}
}

// Close closes pipe endpoint and unregisters conn from Network
// All currently in-flight blocked IO is interrupted with an error
func (c *conn) Close() (err error) {
	c.closeOnce.Do(func() {
		err = c.Conn.Close()

		sk := c.socket
		h := sk.host
		n := h.network

		n.mu.Lock()
		defer n.mu.Unlock()

		sk.conn = nil
		if sk.empty() {
			h.socketv[sk.port] = nil
		}
	})

	return err
}

// LocalAddr returns address of local end of connection
func (c *conn) LocalAddr() net.Addr {
	return c.socket.addr()
}

// RemoteAddr returns address of remote end of connection
func (c *conn) RemoteAddr() net.Addr {
	return c.peersk.addr()
}


// ----------------------------------------

// allocFreeSocket finds first free port and allocates socket entry for it.
// must be called with Network.mu held
func (h *Host) allocFreeSocket() *socket {
	// find first free port
	port := 1 // never allocate port 0 - it is used for autobind on listen only
	for ; port < len(h.socketv); port++ {
		if h.socketv[port] == nil {
			break
		}
	}
	// if all busy it exits with port >= len(h.socketv)

	// grow if needed
	for port >= len(h.socketv) {
		h.socketv = append(h.socketv, nil)
	}

	sk := &socket{host: h, port: port}
	h.socketv[port] = sk
	return sk
}

// empty checks whether socket's both conn and listener are all nil
func (sk *socket) empty() bool {
	return sk.conn == nil && sk.listener == nil
}

// addr returns address corresponding to socket
func (sk *socket) addr() *Addr {
	h := sk.host
	return &Addr{Net: h.Network(), Host: h.name, Port: sk.port}
}

func (a *Addr) Network() string { return a.Net }
func (a *Addr) String() string { return net.JoinHostPort(a.Host, strconv.Itoa(a.Port)) }

// ParseAddr parses addr into pipenet address
func (n *Network) ParseAddr(addr string) (*Addr, error) {
	host, portstr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portstr)
	if err != nil || port < 0 {
		return nil, &net.AddrError{Err: "invalid port", Addr: addr}
	}
	return &Addr{Net: n.Network(), Host: host, Port: port}, nil
}

// Addr returns address where listener is accepting incoming connections
func (l *listener) Addr() net.Addr {
	return l.socket.addr()
}

// Network returns full network name of this network
func (n *Network) Network() string { return NetPrefix + n.name }

// Network returns full network name of underlying network
func (h *Host) Network() string { return h.network.Network() }
