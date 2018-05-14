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

// Package lonet provides TCP network simulated on top of localhost TCP loopback.
//
// For testing distributed systems it is sometimes handy to imitate network of
// several TCP hosts. It is also handy that ports allocated on Dial/Listen/Accept on
// that hosts be predictable - that would help tests to verify network events
// against expected sequence. When whole system could be imitated in 1 OS-level
// process, package lab.nexedi.com/kirr/go123/xnet/pipenet serves the task via
// providing TCP-like synchronous in-memory network of net.Pipes.  However
// pipenet cannot be used for cases where tested system consists of 2 or more
// OS-level processes. This is where lonet comes into play:
//
// Similarly to pipenet addresses on lonet are host:port pairs and several
// hosts could be created with different names. A host is xnet.Networker and
// so can be worked with similarly to regular TCP network access-point with
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
//	net, err := lonet.Join(ctx, "mynet")
//	h1, err := net.NewHost("abc")
//	h2, err := net.NewHost("def")	// ...
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

// XXX document lonet organization, protocol

// > lonet "network" dial <src> <dst>
// < lonet "network" connected <dst'>
//                   E connrefused
//
//                   E wrong network|op|...
//
// - protocol error
// - wrong network
// - wrong op

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
)

const NetPrefix = "lonet" // lonet package creates only "lonet*" networks

// XXX errors

// Addr represents address of a lonet endpoint.
type Addr struct {
	Net  string // full network name, e.g. "lonet"
	Host string // name of host access point on the network
	Port int    // port on host
}

// SubNetwork represents one subnetwork of a lonet network.
//
// Multiple Hosts could be created on one subnetwork.
// There can be other subnetworks in the same process or in another OS-level processes.
//
// Host names are unique through whole lonet network.
type SubNetwork struct {
	// name of full network under "lonet" namespace -> e.g. ""
	// full network name will be reported as "lonet"+network.
	network string

	// registry of whole lonet network
	registry registry

	// OS-level listener of this subnetwork.
	// whenever connection to subnet's host is tried to be established it goes here.
	oslistener net.Listener

	// big subnetwork lock for everything dynamic under SubNetwork
	// (e.g. Host.socketv too)
	mu sync.Mutex

	hostMap map[string]*Host
}

// Host represents named access point on Network	XXX
type Host struct {
	subnet *SubNetwork
	name    string

	// NOTE protected by subnet.mu
	socketv []*socket // port -> listener | conn  ; [0] is always nil
}

var _ xnet.Networker = (*Host)(nil)

// socket represents one endpoint entry on Host.
//
// it can be either already connected or listening.
type socket struct {
	host *Host // host/port this socket is bound to
	port int

	conn     *conn     // connection endpoint is here if != nil
	listener *listener // listener is waiting here if != nil
}

// conn represents one endpoint of connection created under lonet network.
type conn struct {
	socket   *socket // local socket
	peerAddr *Addr   // lonet address of the remote side of this connection

	net.Conn

	closeOnce sync.Once
}

// listener implements net.Listener for Host.
type listener struct {
	// subnetwork/host/port we are listening on
	socket *socket

	dialq chan dialReq  // Dial requests to our port go here from OS-level listener
	down  chan struct{} // Close -> down=ready

	closeOnce sync.Once
}


// dialReq represents one dial request to listener.
//
// it comes after OS-level connection was accepted and lonet dial already
// request parsed locally.
type dialReq struct {
	from   Addr
	osconn net.Conn
	resp   chan Addr // accepted with this local address
}

// ----------------------------------------

// Join joins or creates new lonet network with given name.
//
// Network is the name of this network under "lonet" namespace, e.g. "α" will
// give full network name "lonetα".
//
// If network is "" new network with random unique name will be created.
//
// Join returns new subnetwork on the joined network.
func Join(ctx context.Context, network string) (_ *SubNetwork, err error) {
	defer xerr.Contextf(&err, "lonet: join %q", network)

	// create/join registry under /tmp/lonet/<network>/registry.db
	lonet := os.TempDir() + "/lonet"
	err = os.MkdirAll(lonet, 0777 | os.ModeSticky)
	if err != nil {
		return nil, err
	}

	var netdir string
	if network != "" {
		netdir = lonet + "/" + network
		err = os.MkdirAll(netdir, 0700)
	} else {
		// new with random name
		netdir, err = ioutil.TempDir(lonet, "")
		network = filepath.Base(netdir)
	}
	if err != nil {
		return nil, err
	}

	registry, err := openRegistrySQLite(ctx, netdir + "/registry.db", network)
	if err != nil {
		return nil, err
	}

	// start OS listener
	oslistener, err := net.Listen("tcp4", "127.0.0.1:")
	if err != nil {
		registry.Close()	// XXX lclose?
		return nil, err
	}

	// joined ok
	n := &SubNetwork{
		network:    network,
		registry:   registry,
		oslistener: oslistener,
		hostMap:    make(map[string]*Host),
	}
	go n.serve()

	return n, nil
}

// NewHost creates new lonet Host with given name.
//
// Serving of created host will be done though SubNetwork via which it was
// created.	XXX
//
// XXX errors
//	errHostDup	if host name was already registered.
func (n *SubNetwork) NewHost(ctx context.Context, name string) (*Host, error) {
	// announce host name to registry
	err := n.registry.Announce(ctx, name, n.oslistener.Addr().String())
	if err != nil {
		return nil, err // registry error has enough context
	}

	// announced ok -> host can be created
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.hostMap[name] != nil {
		panic(fmt.Sprintf(
			"lonet %q: new host %q: announced to registry but .hostMap already !empty",
			n.Network(), name))
	}

	host := &Host{subnet: n, name: name}
	n.hostMap[name] = host

	return host, nil
}

// XXX Host.resolveAddr

// XXX
func (h *Host) Listen(laddr string) (net.Listener, error) {
	panic("TODO")
}

// XXX
func (n *SubNetwork) serve() {	// XXX error?
	// wait for incoming OS connections and do lonet protocol handshake on them.
	// if successful - route handshaked connection to particular Host's listener.
	for {
		osconn, err := n.oslistener.Accept()
		if err != nil {
			// XXX mark subnet as down + notify all its hosts
			return
		}

		go func() {
			err := n.loaccept(osconn)	// XXX + ctx?
			if err != nil {
				log.Print(err)	// XXX ok?
			}
		}()
	}
}

// loaccept handles incoming OS-level connection.
//
// it performs lonet protocol handshake and if successfull further conveys
// accepted connection to lonet-level Accept.
func (n *SubNetwork) loaccept(osconn net.Conn) (err error) {
	defer xerr.Contextf(&err, "lonet %q: handshake", n.network)

	// read handshake line and parse it
	// XXX cancel handshake on ctx down
	line, err := readline(osconn, maxlen)
	if err != nil {
		return err
	}

	var network, src, dst string
	_, err = fmt.Sscanf(line, "lonet %q dial %s %s\n", &network, &src, &dst)
	if err != nil {
		ereply("protocol error")
		return err
	}

	if network != n.network {
		ereply("network mismatch")
		return fmt.Errorf("network mismatch")
	}

	asrc, err := ParseAddr(src)
	if err != nil {
		return ereply("src address invalid")
	}
	adst, err := ParseAddr(dst)
	if err != nil {
		return ereply("dst address invalid")
	}

	// check dst host:port in .hostMap
	n.mu.Lock()
	host := n.hostMap[adst.Host]

	if host == nil || adst.Port >= len(host.socketv) {
		n.mu.Unlock()
		return ereplyf(errConnRefused)
	}

	sk := host.socketv[adst.Port]
	if sk == nil || sk.listener == nil {
		n.mu.Unlock()
		return ereplyf(errConnRefused)
	}

	// there is listener corresponding to dst - let's connect it
	l := sk.listener
	n.mu.Unlock()

	resp := make(chan XXX)
	select {
	//case <-ctx.Done():
	//	...

	case <-l.down:
		return ereplyf(errConnRefused)

	case l.dialq <- dialReq{from: asrc, osconn: osconn, resp: resp}:
		// connection accepted
		acceptAddr := <-resp
		replyf("connected %s", acceptAddr)
	}
}


// readline reads 1 line from r up to maxlen bytes.
func readline(r io.Reader, maxlen int) (string, _ error) {
	buf1 := []byte{0}
	var line []byte
	for len(line) < maxlen {
		n, err := r.Read(buf1)
		if n == 1 {
			err = nil
		}
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}

		line = append(line, buf1...)
		if buf1[0] == '\n' {
			break
		}
	}

	return string(line), nil
}

// XXX
func (h *Host) Dial(ctx context.Context, addr string) (net.Conn, error) {
	panic("TODO")
}

// Close closes network endpoint and unregisters conn from Host.
//
// All currently in-flight blocked IO is interrupted with an error
func (c *conn) Close() (err error) {
	c.closeOnce.Do(func() {
		err = c.Conn.Close()

		sk := c.socket
		h := sk.host
		n := h.subnet

		n.mu.Lock()
		defer n.mu.Unlock()

		sk.conn = nil
		if sk.empty() {
			h.socketv[sk.port] = nil
		}
	})

	return err
}

// LocalAddr returns address of local end of connection.
func (c *conn) LocalAddr() net.Addr {
	return c.socket.addr()
}

// RemoteAddr returns address of remote end of connection.
func (c *conn) RemoteAddr() net.Addr {
	return c.peerAddr
}

// ----------------------------------------

// XXX Host.allocFreeSocket

// empty checks whether socket's both conn and listener are all nil.
// XXX recheck we really need this.
func (sk *socket) empty() bool {
	return sk.conn == nil && sk.listener == nil
}

// addr returns address corresponding to socket.
func (sk *socket) addr() *Addr {
	h := sk.host
	return &Addr{Net: h.Network(), Host: h.name, Port: sk.port}
}

func (a *Addr) Network() string { return a.Net }
func (a *Addr) String() string  { return net.JoinHostPort(a.Host, strconv.Itoa(a.Port)) }

// XXX ParseAddr

// Addr returns address where listener is accepting incoming connections.
func (l *listener) Addr() net.Addr {
	return l.socket.addr()
}

// Network returns full network name this subnetwork is part of.
func (n *SubNetwork) Network() string { return NetPrefix + n.network }

// Network returns full network name of underlying network.
func (h *Host) Network() string { return h.subnet.Network() }

// Name returns host name.
func (h *Host) Name() string { return h.name }
