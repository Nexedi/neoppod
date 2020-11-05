// Copyright (C) 2016-2020  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

package neonet
// link establishment

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/neo/go/internal/xio"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

// ---- Handshake ----

// XXX _Handshake may be needed to become public in case when we have already
// established raw connection and want to hand-over it to NEO. But currently we
// do not have such uses.

// _Handshake performs NEO protocol handshake just after raw connection between
// 2 nodes was established.
//
// On success raw connection is returned wrapped into NodeLink.
// On error raw connection is closed.
func _Handshake(ctx context.Context, conn net.Conn, role _LinkRole) (nl *NodeLink, err error) {
	err = handshake(ctx, conn, proto.Version)
	if err != nil {
		return nil, err
	}

	// handshake ok -> NodeLink
	return newNodeLink(conn, role), nil
}

// _HandshakeError is returned when there is an error while performing handshake.
type _HandshakeError struct {
	LocalAddr  net.Addr
	RemoteAddr net.Addr
	Err        error
}

func (e *_HandshakeError) Error() string {
	return fmt.Sprintf("%s - %s: handshake: %s", e.LocalAddr, e.RemoteAddr, e.Err.Error())
}

func handshake(ctx context.Context, conn net.Conn, version uint32) (err error) {
	// XXX simplify -> errgroup
	errch := make(chan error, 2)

	// tx handshake word
	txWg := sync.WaitGroup{}
	txWg.Add(1)
	go func() {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], version) // XXX -> hton32 ?
		_, err := conn.Write(b[:])
		// XXX EOF -> ErrUnexpectedEOF ?
		errch <- err
		txWg.Done()
	}()

	// rx handshake word
	go func() {
		var b [4]byte
		_, err := io.ReadFull(conn, b[:])
		err = xio.NoEOF(err) // can be returned with n = 0
		if err == nil {
			peerVersion := binary.BigEndian.Uint32(b[:]) // XXX -> ntoh32 ?
			if peerVersion != version {
				err = fmt.Errorf("protocol version mismatch: peer = %08x  ; our side = %08x", peerVersion, version)
			}
		}
		errch <- err
	}()

	connClosed := false
	defer func() {
		// make sure our version is always sent on the wire, if possible,
		// so that peer does not see just closed connection when on rx we see version mismatch.
		//
		// NOTE if cancelled tx goroutine will wake up without delay.
		txWg.Wait()

		// don't forget to close conn if returning with error + add handshake err context
		if err != nil {
			err = &_HandshakeError{conn.LocalAddr(), conn.RemoteAddr(), err}
			if !connClosed {
				conn.Close()
			}
		}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			conn.Close() // interrupt IO
			connClosed = true
			return ctx.Err()

		case err = <-errch:
			if err != nil {
				return err
			}
		}
	}

	// handshaked ok
	return nil
}


// ---- Dial & Listen at NodeLink level ----

// DialLink connects to address on given network, performs NEO protocol
// handshake and wraps the connection as NodeLink.
func DialLink(ctx context.Context, net xnet.Networker, addr string) (*NodeLink, error) {
	peerConn, err := net.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	return _Handshake(ctx, peerConn, _LinkClient)
}

// ListenLink starts listening on laddr for incoming connections and wraps them as NodeLink.
//
// The listener accepts only those connections that pass NEO protocol handshake.
func ListenLink(net xnet.Networker, laddr string) (LinkListener, error) {
	rawl, err := net.Listen(laddr)
	if err != nil {
		return nil, err
	}

	return NewLinkListener(rawl), nil
}

// NewLinkListener creates LinkListener which accepts connections from an inner
// net.Listener and wraps them as NodeLink.
//
// The listener accepts only those connections that pass NEO protocol handshake.
func NewLinkListener(inner net.Listener) LinkListener {
	l := &linkListener{
		l:       inner,
		acceptq: make(chan linkAccepted),
		closed:  make(chan struct{}),
	}
	go l.run()
	return l
}

// LinkListener is net.Listener adapted to return handshaked NodeLink on Accept.
type LinkListener interface {
	// from net.Listener:
	Close() error
	Addr() net.Addr

	// Accept returns new incoming connection wrapped into NodeLink.
	// It accepts only those connections which pass NEO protocol handshake.
	Accept() (*NodeLink, error)
}

// linkListener implements LinkListener.
type linkListener struct {
	l       net.Listener
	acceptq chan linkAccepted
	closed  chan struct{}
}

type linkAccepted struct {
	link *NodeLink
	err  error
}

func (l *linkListener) Close() error {
	err := l.l.Close()
	close(l.closed)
	return err
}

func (l *linkListener) run() {
	// context that cancels when listener stops
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	for {
		// stop on close
		select {
		case <-l.closed:
			return
		default:
		}

		// XXX add backpressure on too much incoming connections without client .Accept ?
		conn, err := l.l.Accept()
		go l.accept(runCtx, conn, err)
	}
}

func (l *linkListener) accept(ctx context.Context, conn net.Conn, err error) {
	link, err := l.accept1(ctx, conn, err)

	select {
	case l.acceptq <- linkAccepted{link, err}:
		// ok

	case <-l.closed:
		// shutdown
		if link != nil {
			link.Close()
		}
	}
}

func (l *linkListener) accept1(ctx context.Context, conn net.Conn, err error) (*NodeLink, error) {
	// XXX err ctx?

	if err != nil {
		return nil, err
	}

	// NOTE Handshake closes conn in case of failure
	link, err := _Handshake(ctx, conn, _LinkServer)
	if err != nil {
		return nil, err
	}

	return link, nil
}

func (l *linkListener) Accept() (*NodeLink, error) {
	select {
	case <-l.closed:
		// we know raw listener is already closed - return proper error about it
		_, err := l.l.Accept()
		return nil, err

	case a := <-l.acceptq:
		return a.link, a.err
	}
}

func (l *linkListener) Addr() net.Addr {
	return l.l.Addr()
}
