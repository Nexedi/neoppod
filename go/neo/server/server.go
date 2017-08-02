// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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

// Package server provides servers side of NEO.
package server

// common parts for organizing network servers

import (
//	"context"
//	"fmt"
	"net"

	"lab.nexedi.com/kirr/neo/go/neo"

	"lab.nexedi.com/kirr/go123/xerr"
)


// Listener wraps neo.Listener to return link on which identification was correctly requested XXX
// Create via Listen. XXX
type Listener struct {
	l       *neo.Listener
	acceptq chan accepted
	closed  chan struct {}
}

type accepted struct {
	conn  *neo.Conn
	idReq *neo.RequestIdentification
	err   error
}

func (l *Listener) Close() error {
	err := l.l.Close()
	close(l.closed)
	return err
}

func (l *Listener) run() {
	for {
		// stop on close
		select {
		case <-l.closed:
			return
		default:
		}

		// XXX add backpressure on too much incoming connections without client .Accept ?
		link, err := l.l.Accept()
		go l.accept(link, err)
	}
}

func (l *Listener) accept(link *neo.NodeLink, err error) {
	res := make(chan accepted, 1)
	go func() {
		conn, idReq, err := l.accept1(link, err)
		res <- accepted{conn, idReq, err}
	}()

	// wait for accept1 result & resend it to .acceptq
	// close link in case of listening cancel or error
	//
	// the only case when link stays alive is when acceptance was
	// successful and link ownership is passed to Accept.
	ok := false
	select {
	case <-l.closed:

	case a := <-res:
		select {
		case l.acceptq <- a:
			ok = (a.err == nil)

		case <-l.closed:
		}
	}

	if !ok {
		link.Close()
	}
}

func (l *Listener) accept1(link *neo.NodeLink, err0 error) (_ *neo.Conn, _ *neo.RequestIdentification, err  error) {
	if err0 != nil {
		return nil, nil, err0
	}

	defer xerr.Context(&err, "identify")

	// identify peer
	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept()
	if err != nil {
		return nil, nil, err
	}

	idReq := &neo.RequestIdentification{}
	_, err = conn.Expect(idReq)
	if err != nil {
		// XXX ok to let peer know error as is? e.g. even IO error on Recv?
		err2 := conn.Send(&neo.Error{neo.PROTOCOL_ERROR, err.Error()})
		err = xerr.Merge(err, err2)
		return nil, nil, err
	}

	return conn, idReq, nil
}

// Accept accepts incoming client connection.
//
// On success the link was handshaked and on returned Conn peer sent us
// RequestIdentification packet which we did not yet answer.
func (l *Listener) Accept() (*neo.Conn, *neo.RequestIdentification, error) {
	select{
	case <-l.closed:
		// we know raw listener is already closed - return proper error about it
		_, err := l.l.Accept()
		return nil, nil, err

	case a := <-l.acceptq:
		return a.conn, a.idReq, a.err
	}
}

func (l *Listener) Addr() net.Addr {
	return l.l.Addr()
}


/*
// Server is an interface that represents networked server
type Server interface {
	// ServeLink serves already established nodelink (connection) in a blocking way.
	// ServeLink is usually run in separate goroutine
	ServeLink(ctx context.Context, link *neo.NodeLink)
}

// Serve runs service on a listener
// - accept incoming connection on the listener
// - for every accepted connection spawn srv.ServeLink() in separate goroutine.
//
// the listener is closed when Serve returns.
func Serve(ctx context.Context, l *neo.Listener, srv Server) error {
	fmt.Printf("xxx: serving on %s ...\n", l.Addr())	// XXX 'xxx' -> ?

	// close listener when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - listener close will signal to all accepts to
	//   terminate with an error )
	// XXX dup -> utility
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX err = cancelled
		case <-retch:
		}
		l.Close() // XXX err
	}()

	// main Accept -> ServeLink loop
	for {
		link, err := l.Accept()
		if err != nil {
			// TODO err == closed <-> ctx was cancelled
			// TODO err -> net.Error && .Temporary() -> some throttling
			return err
		}

		// XXX close link when either cancelling or returning?
		// XXX only returning with error!
		go srv.ServeLink(ctx, link)
	}
}
*/

// ----------------------------------------

// XXX goes away?  (we need a func to make sure to recv RequestIdentification
// XXX	and pass it to server main logic - whether to accept it or not should be
// XXX 	programmed there)
//
// IdentifyPeer identifies peer on the link
// it expects peer to send RequestIdentification packet and replies with AcceptIdentification if identification passes.
// returns information about identified node or error.
func IdentifyPeer(link *neo.NodeLink, myNodeType neo.NodeType) (nodeInfo neo.RequestIdentification, err error) {
	defer xerr.Contextf(&err, "%s: identify", link)

	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept()
	if err != nil {
		return nodeInfo, err
	}
	defer func() {
		err2 := conn.Close()
		if err == nil {
			err = err2
			// XXX also clear nodeInfo ?
		}
	}()

	req := neo.RequestIdentification{}
	_, err = conn.Expect(&req)
	if err != nil {
		return nodeInfo, err
	}

	// TODO (.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

	// TODO hook here in logic to check identification request, assign nodeID etc

	err = conn.Send(&neo.AcceptIdentification{
		NodeType:	myNodeType,
		MyNodeUUID:	0,		// XXX
		NumPartitions:	1,		// XXX
		NumReplicas:	1,		// XXX
		YourNodeUUID:	req.NodeUUID,
	})

	if err != nil {
		return nodeInfo, err
	}

	return req, nil
}
