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

// FIXME kill dup from pipenet!

// Package lonet provides TCP network simulated on top of localhost TCP loopback.
//
// For testing distributed systems it is sometimes handy to imitate network of
// several TCP hosts. It is also handy that port allocated on Dial/Listen/Accept on
// that hosts be predictable - that would help tests to verify network events
// against expected sequence. When whole system could be imitated in 1 OS-level
// process, package lab.nexedi.com/kirr/go123/xnet/pipenet serves the task via
// providing TCP-like synchronous in-memory network of net.Pipes.  However
// pipenet cannot be used for cases where tested system consists of 2 or more
// OS-level processes. This is where lonet comes into play:
//
// Similarly to pipenet addresses on lonet are host:port pairs and several
// hosts could be created with different names. A host is xnet.Networker and
// so can be worked with similarly to regular TCP network with
// Dial/Listen/Accept.  Host's ports allocation is predictable: ports of a host
// are contiguous integer sequence starting from 1 that are all initially free,
// and whenever autobind is requested the first free port of the host will be
// used.
//
// Internally lonet network maintains registry of hosts so that lonet
// addresses could be resolved to OS-level addresses, for example α:1 and β:1
// to 127.0.0.1:4567 and 127.0.0.1:8765, and once lonet connection is
// established it becomes served by OS-level TCP connection over loopback.
//
// XXX several networks = possible. (or document in New?)
//
// Example:			TODO adjust
//
//	net := lonet.New("")	// XXX network name
//	h1 := net.Host("abc")	// XXX err
//	h2 := net.Host("def")	// ...
//
//	// XXX inject 127.0.0.1 to example...
//	// starts listening on address "abc:10" (which gets mapped to "127.0.0.1:xxx")
//	l, err := h1.Listen(":10")
//	go func() {
//		csrv, err := l.Accept()  // csrv will have LocalAddr "abc:10"
//	}()
//	ccli, err := h2.Dial("abc:10")   // ccli will have RemoteAddr "def:10"
//
// Once again lonet is similar to pipenet, but since it works via OS TCP stack
// it could be handy for testing networked application when there are several
// OS-level processes involved.
//
// See also shipped lonet.py for accessing lonet networks from Python.
package lonet

/*
const NetPrefix = "lonet" // lonet package creates only "lonet*" networks

// Addr represents address of a lonet endpoint.
type Addr struct {
	Net  string // full network name, e.g. "lonet"
	Host string // name of host access point on the network
	Port int    // port on host
}

// Network implements ... XXX
type Network struct {
	// name of this network under "lonet" namespace -> e.g. ""
	// full network name will be reported as "lonet"+name
	name string

	// big network lock for everything dynamic under Network
	// (e.g. Host.socketv too)	XXX
	mu sync.Mutex

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
	host *Host // host/port this socket is bound to
	port int

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

// XXX listener

// XXX dialReq

// ----------------------------------------

// New creates new lonet Network.
//
// name is name of this network under "lonet" namespace, e.g. "α" will give full network name "lonetα".
//
// New does not check whether network name provided is unique.
func New(name string) *Network {
	return &Network{name: name, hostMap: make(map[string]*Host)}
}

// Host returns network access point by name.
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
*/
