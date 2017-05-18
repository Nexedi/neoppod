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
	"net"
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
func Serve(ctx context.Context, l net.Listener, srv Server) error {
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

	// main Accept -> Handshake -> ServeLink loop
	for {
		peerConn, err := l.Accept()
		if err != nil {
			// TODO err == closed <-> ctx was cancelled
			// TODO err -> net.Error && .Temporary() -> some throttling
			return err
		}

		go func() {
			link, err := Handshake(ctx, peerConn, LinkServer)
			if err != nil {
				fmt.Printf("xxx: %s\n", err)
				return
			}
			srv.ServeLink(ctx, link)
		}()
	}
}

// ListenAndServe listens on network address and then calls Serve to handle incoming connections
// XXX split -> separate Listen() & Serve()
func ListenAndServe(ctx context.Context, net_, laddr string, srv Server) error {
	l, err := net.Listen(net_, laddr)
	if err != nil {
		return err
	}
	// TODO set keepalive on l
	// TODO if TLS config -> tls.NewListener()
	return Serve(ctx, l, srv)
}


// ----------------------------------------

// errcontextf adds formatted prefix context to *errp
// must be called under defer
func errcontextf(errp *error, format string, argv ...interface{}) {
	if *errp == nil {
		return
	}

	format += ": %s"
	argv = append(argv, *errp)
	*errp = fmt.Errorf(format, argv...)
}

// IdentifyPeer identifies peer on the link
// it expects peer to send RequestIdentification packet and replies with AcceptIdentification if identification passes.
// returns information about identified node or error.
func IdentifyPeer(link *NodeLink, myNodeType NodeType) (nodeInfo RequestIdentification /*TODO -> NodeInfo*/, err error) {
	defer errcontextf(&err, "%s: identify", link)

	/*
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: identify: %s", link, err)
		}
	}()
	*/

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

	pkt, err := RecvAndDecode(conn)
	if err != nil {
		return nodeInfo, err
	}

	switch pkt := pkt.(type) {
	default:
		return nodeInfo, fmt.Errorf("unexpected request: %T", pkt)

	// XXX also handle Error

	case *RequestIdentification:
		// TODO (.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

		err = EncodeAndSend(conn, &AcceptIdentification{
			NodeType:	myNodeType,
			MyNodeID:	0,		// XXX
			NumPartitions:	0,		// XXX
			NumReplicas:	0,		// XXX
			YourNodeID:	pkt.NodeID,
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
	defer errcontextf(&err, "%s: request identification", link)

	/*
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: request identification: %s", link, err)
		}
	}()
	*/

	conn, err := link.NewConn()
	if err != nil {
		return peerType, err
	}
	defer func() {
		err2 := conn.Close()
		if err == nil && err2 != nil {
			err = err2
			// XXX also reset peerType
		}
	}()

	err = EncodeAndSend(conn, &RequestIdentification{
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
		return peerType, fmt.Errorf("unexpected answer: %T", pkt)

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