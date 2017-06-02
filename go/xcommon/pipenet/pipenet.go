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

// Package pipenet provides in-memory network of net.Pipes
//
// TODO describe addressing scheme
//
// it is useful for testing networked applications interaction in 1 process
// without going to OS networking stack. XXX
package pipenet

import (
	"errors"
	"net"
)

const NetPrefix = "pipe" // pipenet package works only with "pipe*" networks

var errNetNotFound = errors.New("no such network")
//var errBadNetwork = errors.New("invalid network")
var errBadAddress = errors.New("invalid address")
var errAddrAlreadyUsed = errors.New("address already in use")
var errConnRefused = errors.New("connection refused")


// Network represents network of in-memory pipes
// It can be worked with the same way a regular TCP network is handled with Dial/Listen/Accept/...
//
// Network must be created with New
type Network struct {
	// name (suffix) of this network, e.g. ""
	// full network name will be reported as "pipe"+Name
	Name string

	mu      sync.Mutex
	pipev   []pipe		// port -> listener + net.Pipe (?)
//	listenv []chan dialReq // listener[port] is waiting here if != nil
}

// pipe represents one pipenet connection
// it can be either already connected (2 endpoints) or only listening (1 endpoint)	XXX
type pipe struct {
	// TODO
}

// Addr represents address of a pipe endpoint
type Addr struct {
	network *Network
	addr    string	// XXX -> port ? + including c/s ?
}

var _ net.Addr = (*Addr)(nil)

func (a *Addr) Network() string { return a.network.netname() }
func (a *Addr) String() string { return a.addr }	// XXX Network() + ":" + a.addr ?
func (n *Network) netname() string { return NetPrefix + n.Name }


// XXX do we need Conn wrapping net.Pipe ? (e.g. to override String())


func (n *Network) Listen(laddr string) (net.Listener, error) {
	lerr := func(err error) error {
		return &net.OpError{Op: "listen", Net: n.netname(), Addr: laddr, Err: err}
	}

	// laddr must be empty or int >= 0
	port := -1
	if laddr != "" {
		port, err := strconv.Atoi(laddr)
		if err != nil || port < 0 {
			return lerr(errBadAddress)
		}
	}

	n.mu.Lock()
	defer n.m.Unlock()

	// find first free port if it was not specified
	if port < 0 {
		for port = 0; port < len(n.listenv); port++ {
			if n.listenv[port] == nil {
				break
			}
		}
		// if all busy it exits with port == len(n.listenv)
	}

	// grow if needed
	for port >= len(n.listenv) {
		n.listenv = append(n.listenv, nil)
	}

	if n.listenv[port] != nil {
		return lerr(errAddrAlreadyUsed)
	}

	l := listener{...}
	n.listenv[port] = l...

	return &l
}

// listener implements net.Listener for piped network
type listener struct {
	network *Network // XXX needed ?
	port    int	// port we are listening on

	dialq    chan chan dialResp   // Dial requests to our port go here
	acceptq  chan chan acceptResp // Accept requests go here
	down     chan struct{}	      // Close -> down=ready
	downOnce sync.Once            // so Close several times is ok
}

var _ net.Listener = (*listener)nil

func (l *listener) Addr() Addr {
	return &Addr{net: l.net, addr: fmt.Sprintf("%d", l.port} // NOTE no c/s XXX -> +l ?
}

func (l *listener) Close() error {
	l.downOnce.Do(func() {
		close(l.down)
	})
}

// connected is response from listener to Dial and Accept
type connected struct {
	conn net.Conn
	err  error
}

func (l *listener) Accept() (net.Conn, error) {
	ch := make(chan connected)
	select {
	case l.down
		return ... "... closed"	// XXX

	case l.acceptq <- ch:
		resp := <-ch
		return resp.conn, resp.err
	}
}

func (n *Network) Dial(network, addr string) (net.Conn, error) {
	derr := func(err error) error {
		return &net.OpError{Op: "dial", Net: network, Addr: addr, Err: err}
	}

	port, err := strconv.Atoi(addr)
	if err != nil || port < 0 {
		return derr(errBadAddress)
	}

	n.mu.Lock()
	defer n.mu.Unlock()	// XXX ok to defer here?

	if port >= len(n.listenv) {
		return derr(errConnRefused)	// XXX merge with vvv
	}

	l := n.listenv[port]
	if l == nil {
		return derr(errConnRefused)	// XXX merge with ^^^
	}

	// NOTE listener is not locking n.mu -> it is ok to send/receive under mu
	ch := make(chan dialResp)
	select {
	case l.down:
		return derr(errConnRefused)

	case l.dialq <- ch:
		resp := <-ch
		return resp.conn, resp.err
	}
}



// ----------------------------------------


var (
	netMu      sync.Mutex
	networks = map[string]*Network{} // netsuffix -> Network

	DefaultNet = New("")
)

// New creates, initializes and returns new pipenet Network
// network name must be unique - if not New will panic
func New(name string) *Network {
	netMu.Lock()
	defer netMu.Unlock()

	_, already := networks[name]
	if already {
		panic(fmt.Errorf("pipenet %v already registered", name)
	}

	n := &Network{Name: name}
	networks[name] = n
	return n
}

// lookupNet lookups Network by name
// returns nil, if not found
func lookupNet(name string) *Network {
	netMu.Lock()
	defer netMu.Unlock()

	return networks[name]
}

// Dial dials addr on a pipenet
// network should be valid registered pipe network name
func Dial(network, addr string) (net.Conn, error) {
	n := lookupNet(network)
	if n == nil {
		return nil, &net.OpError{Op: "dial", Net: NetPrefix + network, Addr: addr, Err: err}
	}

	return n.Dial(addr)
}

// XXX text
func Listen(network, laddr string) (net.Listen, error) {
	n := lookupNet(network)
	if n == nil {
		return nil, &net.OpError{Op: "listen", Net: NetPrefix + network, Addr: addr, Err: err}
	}

	return n.Listen(netw, laddr)
}
