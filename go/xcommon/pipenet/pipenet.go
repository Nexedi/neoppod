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

// Package pipenet provides synchronous in-memory network of net.Pipes
//
// TODO describe addressing scheme
//
// it is useful for testing networked applications interaction in 1 process
// without going to OS networking stack. XXX
package pipenet

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

const NetPrefix = "pipe" // pipenet package works only with "pipe*" networks

var (
	errBadNetwork		= errors.New("pipenet: invalid network")
	errBadAddress		= errors.New("invalid address")
	errNetNotFound		= errors.New("no such network")
	errNetClosed		= errors.New("network connection closed")
	errAddrAlreadyUsed	= errors.New("address already in use")
	errConnRefused		= errors.New("connection refused")
)

// Addr represents address of a pipenet endpoint
type Addr struct {
	network string	// full network name, e.g. "pipe"
	addr    string	// XXX -> port ? + including c/s ?
}

// Network implements synchronous in-memory network of pipes
// It can be worked with the same way a regular TCP network is handled with Dial/Listen/Accept/...
//
// Network must be created with New
type Network struct {
	// name of this network under "pipe" namespace -> e.g. ""
	// full network name will be reported as "pipe"+Name		XXX -> just full name ?
	Name string

	mu      sync.Mutex
	entryv  []*entry   // port -> listener | (conn, conn)
}

// entry represents one Network entry
// it can be either already connected (2 endpoints) or only listening (1 endpoint)
// anything from the above becomes nil when closed
type entry struct {
	network  *Network
	port     int

	pipev    [2]*conn  // connection endpoints are there if != nil
	listener *listener // listener is waiting here if != nil
}

// conn represents one endpoint of connection created under Network
type conn struct {
	entry    *entry
	endpoint int // 0 | 1 -> entry.pipev

	net.Conn

	closeOnce sync.Once
}

// listener implements net.Listener for piped network
type listener struct {
	// network/port we are listening on
	entry *entry

	dialq    chan chan net.Conn	// Dial requests to our port go here
	down     chan struct{}		// Close -> down=ready

	closeOnce sync.Once
}



// empty checks whether both 2 pipe endpoints and listener are nil
func (e *entry) empty() bool {
	return e.pipev[0] == nil && e.pipev[1] == nil && e.listener == nil
}

// addr returns address corresponding to entry
func (e *entry) addr() *Addr {
	return &Addr{network: e.network.netname(), addr: fmt.Sprintf("%d", e.port)}
}

func (a *Addr) Network() string { return a.network }
func (a *Addr) String() string { return a.addr }	// XXX Network() + ":" + a.addr ?
func (n *Network) netname() string { return NetPrefix + n.Name }



func (n *Network) Listen(laddr string) (net.Listener, error) {
	lerr := func(err error) error {
		return &net.OpError{Op: "listen", Net: n.netname(), Addr: &Addr{n.netname(), laddr}, Err: err}
	}

	// laddr must be empty or int >= 0
	port := -1
	if laddr != "" {
		port, err := strconv.Atoi(laddr)
		if err != nil || port < 0 {
			return nil, lerr(errBadAddress)
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// find first free port if it was not specified
	if port < 0 {
		for port = 0; port < len(n.entryv); port++ {
			if n.entryv[port] == nil {
				break
			}
		}
		// if all busy it exits with port == len(n.entryv)
	}

	// grow if needed
	for port >= len(n.entryv) {
		n.entryv = append(n.entryv, nil)
	}

	if n.entryv[port] != nil {
		return nil, lerr(errAddrAlreadyUsed)
	}

	e := &entry{network: n, port: port}
	l := &listener{
		entry:   e,
		dialq:	 make(chan chan net.Conn),
		down:	 make(chan struct{}),
	}
	e.listener = l
	n.entryv[port] = e

	return l, nil
}

// Close closes the listener
// it interrupts all currently in-flight calls to Accept
func (l *listener) Close() error {
	l.closeOnce.Do(func() {
		close(l.down)

		e := l.entry
		n := e.network

		n.mu.Lock()
		defer n.mu.Unlock()

		e.listener = nil
		if e.empty() {
			n.entryv[e.port] = nil
		}
	})
	return nil
}

// Accept tries to connect to Dial called with addr corresponding to our listener
func (l *listener) Accept() (net.Conn, error) {
	select {
	case <-l.down:
		return nil, &net.OpError{Op: "accept", Net: l.entry.network.netname(), Addr: l.Addr(), Err: errNetClosed}

	case resp := <-l.dialq:
		// someone dialed us - let's connect
		pc, ps := net.Pipe()

		// XXX allocate port and register to l.network.pipev


		resp <- pc
		return ps, nil
	}
}

// Dial tries to connect to Accept called on listener corresponding to addr
func (n *Network) Dial(addr string) (net.Conn, error) {
	derr := func(err error) error {
		return &net.OpError{Op: "dial", Net: n.netname(), Addr: &Addr{n.netname(), addr}, Err: err}
	}

	port, err := strconv.Atoi(addr)
	if err != nil || port < 0 {
		return nil, derr(errBadAddress)
	}

	n.mu.Lock()
	defer n.mu.Unlock()	// XXX ok to defer here?

	if port >= len(n.entryv) {
		return nil, derr(errConnRefused)	// XXX merge with vvv
	}

	e := n.entryv[port]
	if e == nil || e.listener == nil {
		return nil, derr(errConnRefused)	// XXX merge with ^^^
	}
	l := e.listener

	// NOTE listener is not locking n.mu -> it is ok to send/receive under mu - FIXME not correct
	// FIXME -> Accept needs to register new connection under n.mu
	resp := make(chan net.Conn)
	select {
	case <-l.down:
		return nil, derr(errConnRefused)

	case l.dialq <- resp:
		return <-resp, nil
	}
}

// Addr returns address where listener is accepting incoming connections
func (l *listener) Addr() net.Addr {
	// NOTE no +"l" suffix e.g. because Dial(l.Addr()) must work
	return l.entry.addr()
}

// Close closes pipe endpoint and unregisters conn from Network
// All currently in-flight blocking IO is interuppted with an error
func (c *conn) Close() (err error) {
	c.closeOnce.Do(func() {
		err = c.Conn.Close()

		e := c.entry
		n := e.network

		n.mu.Lock()
		defer n.mu.Unlock()

		e.pipev[c.endpoint] = nil

		if e.empty() {
			n.entryv[e.port] = nil
		}
	})

	return err
}

// LocalAddr returns address of local end of connection
// it is entry address + "c" (client) or "s" (server) suffix depending on
// whether pipe endpoint was created via Dial or Accept.
func (c *conn) LocalAddr() net.Addr {
	addr := c.entry.addr()
	addr.addr += string("cs"[c.endpoint])
	return addr
}

// RemoteAddr returns address of remote end of connection
// it is entry address + "c" or "s" suffix -- see LocalAddr for details
func (c *conn) RemoteAddr() net.Addr {
	addr := c.entry.addr()
	addr.addr += string("sc"[c.endpoint])
	return addr
}




// ----------------------------------------

var (
	netMu      sync.Mutex
	networks = map[string]*Network{} // netSuffix -> Network

	DefaultNet = New("")
)

// New creates, initializes and returns new pipenet Network
// network name is name of this network under "pipe" namesapce, e.g. ""
// network name must be unique - if not New will panic
func New(name string) *Network {
	netMu.Lock()
	defer netMu.Unlock()

	_, already := networks[name]
	if already {
		panic(fmt.Errorf("pipenet %q already registered", name))
	}

	n := &Network{Name: name}
	networks[name] = n
	return n
}

// lookupNet lookups Network by name
// name is full network name, e.g. "pipe"
func lookupNet(name string) (*Network, error) {
	if !strings.HasPrefix(NetPrefix, name) {
		return nil, errBadNetwork
	}

	netMu.Lock()
	defer netMu.Unlock()

	n := networks[strings.TrimPrefix(NetPrefix, name)]
	if n == nil {
		return nil, errNetNotFound
	}
	return n, nil
}

// Dial dials addr on a pipenet
// network should be full network name, e.g. "pipe"
func Dial(network, addr string) (net.Conn, error) {
	n, err := lookupNet(network)
	if err != nil {
		return nil, &net.OpError{Op: "dial", Net: network, Addr: &Addr{network, addr}, Err: err}
	}

	return n.Dial(addr)
}

// Listen starts listening on a pipenet address
// network should be full network name, e.g. "pipe"
func Listen(network, laddr string) (net.Listener, error) {
	n, err := lookupNet(network)
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: network, Addr: &Addr{network, laddr}, Err: err}
	}

	return n.Listen(laddr)
}
