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

package neo
// common parts for organizing network servers

import (
	"context"
	"fmt"
)

// Server is an interface that represents networked server
type Server interface {
	// ServeLink serves already established nodelink (connection) in a blocking way.
	// ServeLink is usually run in separate goroutine
	ServeLink(ctx context.Context, link *NodeLink)
}

// Serve runs service on a listener
// - accept incoming connection on the listener
// - for every accepted connection spawn srv.ServeLink() in separate goroutine.
//
// the listener is closed when Serve returns.
func Serve(ctx context.Context, l *Listener, srv Server) error {
	fmt.Printf("xxx: serving on %s ...\n", l.Addr())	// XXX 'xxx' -> ?

	// close listener when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - listener close will signal to all accepts to
	//   terminate with an error )
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
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

		go srv.ServeLink(ctx, link)
	}
}

// ListenAndServe listens on network address and then calls Serve to handle incoming connections
// XXX split -> separate Listen() & Serve()
func ListenAndServe(ctx context.Context, net_, laddr string, srv Server) error {
	l, err := Listen(net_, laddr)
	if err != nil {
		return err
	}
	// TODO set keepalive on l
	// TODO if TLS config -> tls.NewListener()
	return Serve(ctx, l, srv)
}


// ----------------------------------------

// Identify identifies peer on the link
// it expects peer to send RequestIdentification packet and TODO
func Identify(link *NodeLink) (nodeInfo RequestIdentification /*TODO -> NodeInfo*/, err error) {
	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept()
	if err != nil {
		return nodeInfo, err	// XXX err ctx
	}
	defer func() {
		err2 := conn.Close()
		if err == nil {
			err = err2
			// XXX also clear nodeInfo ?
		}
	}()

	pkt, err := RecvAndDecode(conn)
	if err != nil {
		return nodeInfo, err	// XXX err ctx
	}

	switch pkt := pkt.(type) {
	default:
		return nodeInfo, fmt.Errorf("expected RequestIdentification  ; got %T", pkt)

	case *RequestIdentification:
		if pkt.ProtocolVersion != PROTOCOL_VERSION {
			// TODO also tell peer with Error
			return nodeInfo, fmt.Errorf("protocol version mismatch: peer = %d  ; our side = %d", pkt.ProtocolVersion, PROTOCOL_VERSION)
		}

		// TODO (.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

		err = EncodeAndSend(conn, &AcceptIdentification{
			NodeType:	pkt.NodeType,
			MyUUID:		0,		// XXX
			NumPartitions:	0,		// XXX
			NumReplicas:	0,		// XXX
			YourUUID:	pkt.UUID,
			Primary:	Address{},	// XXX
			//KnownMasterList:		// XXX
		})

		if err != nil {
			return nodeInfo, err
		}

		nodeInfo = *pkt
	}

	return nodeInfo, nil
}

// ----------------------------------------
// XXX place = ok ? not ok -> move out of here
// XXX naming for RecvAndDecode and EncodeAndSend

// RecvAndDecode receivs packet from conn and decodes it
func RecvAndDecode(conn *Conn) (interface{}, error) {	// XXX interface{} -> NEOEncoder ?
	pkt, err := conn.Recv()
	if err != nil {
		return nil, err
	}

	// TODO decode pkt
	return pkt, nil
}

// EncodeAndSend encodes pkt and send it to conn
func EncodeAndSend(conn *Conn, pkt NEOEncoder) error {
	msgCode, l := pkt.NEOEncodedInfo()
	l += PktHeadLen
	buf := PktBuf{make([]byte, l)}	// XXX -> freelist

	h := buf.Header()
	// h.ConnId will be set by conn.Send
	h.MsgCode = hton16(msgCode)
	h.Len = hton32(uint32(l))	// XXX casting: think again

	pkt.NEOEncode(buf.Payload())

	return conn.Send(&buf)	// XXX why pointer?
}
