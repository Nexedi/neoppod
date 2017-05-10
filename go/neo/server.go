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
	"reflect"
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

// IdentifyPeer identifies peer on the link
// it expects peer to send RequestIdentification packet and replies with AcceptIdentification if identification passes.
// returns information about identified node or error.
func IdentifyPeer(link *NodeLink, myNodeType NodeType) (nodeInfo RequestIdentification /*TODO -> NodeInfo*/, err error) {
	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept()
	if err != nil {
		return nodeInfo, err	// XXX err ctx
	}
	defer func() {
		err2 := conn.Close()
		if err == nil {
			err = err2	// XXX err ctx
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

	// XXX also handle Error

	case *RequestIdentification:
		if pkt.ProtocolVersion != PROTOCOL_VERSION {
			// TODO also tell peer with Error
			return nodeInfo, fmt.Errorf("protocol version mismatch: peer = %d  ; our side = %d", pkt.ProtocolVersion, PROTOCOL_VERSION)
		}

		// TODO (.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

		err = EncodeAndSend(conn, &AcceptIdentification{
			NodeType:	myNodeType,
			MyNodeID:	0,		// XXX
			NumPartitions:	0,		// XXX
			NumReplicas:	0,		// XXX
			YourNodeID:	pkt.NodeID,
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

// IdentifyMe identifies local node to remote peer
func IdentifyMe(link *NodeLink, nodeType NodeType /*XXX*/) (peerType NodeType, err error) {
	conn, err := link.NewConn()
	if err != nil {
		return peerType, err
	}
	defer func() {
		err2 := conn.Close()
		if err == nil && err2 != nil {
			err = err2	// XXX err ctx
			// XXX also reset peerType
		}
	}()

	err = EncodeAndSend(conn, &RequestIdentification{
		ProtocolVersion: PROTOCOL_VERSION,
		NodeType:	 nodeType,
		NodeID:		 0,			// XXX
		Address:	 Address{},		// XXX
		Name:		 "",			// XXX cluster name ?
		IdTimestamp:	 0,			// XXX
	})

	if err != nil {
		return peerType, err
	}

	pkt, err := RecvAndDecode(conn)
	if err != nil {
		return peerType, err
	}

	switch pkt := pkt.(type) {
	default:
		return peerType, fmt.Errorf("expected AcceptIdentification  ; got %T", pkt)

	// XXX also handle Error

	case *AcceptIdentification:
		return pkt.NodeType, nil
	}

}

// ----------------------------------------
// XXX place = ok ? not ok -> move out of here
// XXX naming for RecvAndDecode and EncodeAndSend

// RecvAndDecode receives packet from conn and decodes it
func RecvAndDecode(conn *Conn) (NEOEncoder, error) {	// XXX NEOEncoder -> interface{}
	pkt, err := conn.Recv()
	if err != nil {
		return nil, err
	}

	// decode packet
	// XXX maybe better generate switch on msgCode instead of reflect
	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)
	msgType := pktTypeRegistry[msgCode]
	if msgType == nil {
		return nil, fmt.Errorf("invalid msgCode (%d)", msgCode)	// XXX err context
	}

	// TODO use free-list for decoded packets + when possible decode in-place
	pktObj := reflect.New(msgType).Interface().(NEOCodec)
	_, err = pktObj.NEODecode(pkt.Payload())
	if err != nil {
		return nil, err	// XXX err ctx ?
	}

	return pktObj, nil
}

// EncodeAndSend encodes pkt and send it to conn
func EncodeAndSend(conn *Conn, pkt NEOEncoder) error {
	msgCode, l := pkt.NEOEncodedInfo()
	buf := PktBuf{make([]byte, PktHeadLen + l)}	// XXX -> freelist

	h := buf.Header()
	// h.ConnId will be set by conn.Send
	h.MsgCode = hton16(msgCode)
	h.MsgLen = hton32(uint32(l))	// XXX casting: think again

	pkt.NEOEncode(buf.Payload())

	return conn.Send(&buf)	// XXX why pointer?
}
