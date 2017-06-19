// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

package server
// common parts for organizing network servers

import (
	"context"
	"fmt"
	"net"

	"lab.nexedi.com/kirr/neo/go/neo"

	"lab.nexedi.com/kirr/go123/xerr"
)

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
func Serve(ctx context.Context, l net.Listener, srv Server) error {
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

	// main Accept -> Handshake -> ServeLink loop
	for {
		peerConn, err := l.Accept()
		if err != nil {
			// TODO err == closed <-> ctx was cancelled
			// TODO err -> net.Error && .Temporary() -> some throttling
			return err
		}

		go func() {
			link, err := neo.Handshake(ctx, peerConn, neo.LinkServer)
			if err != nil {
				fmt.Printf("xxx: %s\n", err)
				return
			}
			srv.ServeLink(ctx, link)
		}()
	}
}

/*
// ListenAndServe listens on network address and then calls Serve to handle incoming connections
// XXX unused -> goes away ?
func ListenAndServe(ctx context.Context, net neo.Network, laddr string, srv Server) error {
	l, err := net.Listen(laddr)
	if err != nil {
		return err
	}
	// TODO set keepalive on l
	return Serve(ctx, l, srv)
}
*/


// ----------------------------------------

// IdentifyPeer identifies peer on the link
// it expects peer to send RequestIdentification packet and replies with AcceptIdentification if identification passes.
// returns information about identified node or error.
// XXX recheck identification logic here
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
