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

package neo

// common parts for organizing network servers

import (
	"context"
//	"fmt"
//	"net"
	"sync"

	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/internal/log"

	"lab.nexedi.com/kirr/go123/xerr"
)

/*
// Server is an interface that represents networked server
type Server interface {
	// ServeLink serves already established nodelink (connection) in a blocking way.
	// ServeLink is usually run in separate goroutine
	ServeLink(ctx context.Context, link *neonet.NodeLink)
}

// Serve runs service on a listener
// - accept incoming connection on the listener
// - for every accepted connection spawn srv.ServeLink() in separate goroutine.
//
// the listener is closed when Serve returns.
func Serve(ctx context.Context, l *neo.Listener, srv Server) error {
	fmt.Printf("xxx: serving on %s ...\n", l.Addr())	// XXX 'xxx' -> ?
	defer xio.CloseWhenDone(ctx, l)()

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

/*
// FIXME kill vvv
// ----------------------------------------

// XXX goes away?  (we need a func to make sure to recv RequestIdentification
// XXX	and pass it to server main logic - whether to accept it or not should be
// XXX 	programmed there)
//
// IdentifyPeer identifies peer on the link
// it expects peer to send RequestIdentification packet and replies with AcceptIdentification if identification passes.
// returns information about identified node or error.
func IdentifyPeer(ctx context.Context, link *neonet.NodeLink, myNodeType proto.NodeType) (nodeInfo proto.RequestIdentification, err error) {
	defer xerr.Contextf(&err, "%s: identify", link)

	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept() //+ctx
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

	req := proto.RequestIdentification{}
	_, err = conn.Expect(&req)
	if err != nil {
		return nodeInfo, err
	}

	// TODO (.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

	// TODO hook here in logic to check identification request, assign nodeID etc

	err = conn.Send(&proto.AcceptIdentification{
		NodeType:	myNodeType,
		MyUUID:		0,		// XXX
		NumPartitions:	1,		// XXX
		NumReplicas:	1,		// XXX
		YourUUID:	req.UUID,
	})

	if err != nil {
		return nodeInfo, err
	}

	return req, nil
}
*/


// ----------------------------------------

// event: node connects
type nodeCome struct {
	req    *neonet.Request
	idReq  *proto.RequestIdentification // we received this identification request
}

/*
// event: node disconnects
type nodeLeave struct {
	node *neo.Node
}
*/



// reject sends rejective identification response and closes associated link
func reject(ctx context.Context, req *neonet.Request, resp proto.Msg) {
	// XXX cancel on ctx?
	// log.Info(ctx, "identification rejected") ?
	err1 := req.Reply(resp)
	err2 := req.Link().Close()
	err := xerr.Merge(err1, err2)
	if err != nil {
		log.Error(ctx, "reject:", err)
	}
}

// goreject spawns reject in separate goroutine properly added/done on wg
func goreject(ctx context.Context, wg *sync.WaitGroup, req *neonet.Request, resp proto.Msg) {
	wg.Add(1)
	defer wg.Done()
	go reject(ctx, req, resp)
}

// accept replies with acceptive identification response
// XXX spawn ping goroutine from here?
func accept(ctx context.Context, req *neonet.Request, resp proto.Msg) error {
	// XXX cancel on ctx
	err1 := req.Reply(resp)
	return err1	// XXX while trying to work on single conn
	//err2 := conn.Close()
	//return xerr.First(err1, err2)
}
