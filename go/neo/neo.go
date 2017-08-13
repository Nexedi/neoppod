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

// Package neo and its children provide distributed object storage for ZODB.
//
// Package neo itself provides protocol definition and common infrastructure.
// See packages neo.client and neo.server for client and server sides respectively.
// XXX text
package neo

// XXX gotrace ... -> gotrace gen ...
//go:generate sh -c "go run ../xcommon/tracing/cmd/gotrace/{gotrace,util}.go ."

import (
	"context"
	"fmt"
	"net"

	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

const (
	//INVALID_UUID UUID = 0
	INVALID_TID  zodb.Tid = 1<<64 - 1            // 0xffffffffffffffff
	INVALID_OID  zodb.Oid = 1<<64 - 1

	// OID_LEN = 8
	// TID_LEN = 8
)


// NodeCommon is common data in all NEO nodes: Master, Storage & Client	XXX text
// XXX naming -> Node ?
type NodeCommon struct {
	MyInfo		NodeInfo	// XXX -> only NodeUUID
	ClusterName	string

	Net		xnet.Networker	// network AP we are sending/receiving on
	MasterAddr	string		// address of master	XXX -> Address ?

	NodeTab		NodeTable	// information about nodes in the cluster
	PartTab		PartitionTable	// information about data distribution in the cluster
}

// Dial connects to another node in the cluster
//
// It handshakes, requests identification and checks peer type. If successful returned are:
// - primary link connection which carried identification
// - accept identification reply
func (n *NodeCommon) Dial(ctx context.Context, peerType NodeType, addr string) (_ *Conn, _ *AcceptIdentification, err error) {
	link, err := DialLink(ctx, n.Net, addr)
	if err != nil {
		return nil, nil, err
	}

	defer xerr.Contextf(&err, "%s: request identification", link)
	// close link on error return
	// FIXME also close link on ctx cancel
	defer func() {
		if err != nil {
			link.Close()
		}
	}()

	conn, err := link.NewConn()
	if err != nil {
		return nil, nil, err
	}

	req := &RequestIdentification{
		NodeType:	 n.MyInfo.NodeType,
		NodeUUID:	 n.MyInfo.NodeUUID,
		Address:	 n.MyInfo.Address,
		ClusterName:	 n.ClusterName,
		IdTimestamp:	 n.MyInfo.IdTimestamp,	// XXX ok?
	}
	accept := &AcceptIdentification{}
	err = conn.Ask(req, accept)
	if err != nil {
		return nil, nil, err
	}

	if accept.NodeType != peerType {
		// XXX send Error to peer?
		return nil, nil, fmt.Errorf("accepted, but peer is not %v (identifies as %v)", peerType, accept.NodeType)
	}

	//accept.MyNodeUUID, link // XXX register .NodeTab? (or better LinkTab as NodeTab is driven by M)
	//accept.YourNodeUUID	// XXX M can tell us to change UUID -> take in effect

	return conn, accept, nil
}


// Listen starts listening at node's listening address.
// If the address is empty one new free is automatically selected.
// The node information about where it listens at is appropriately updated.
func (n *NodeCommon) Listen() (Listener, error) {
	// start listening
	ll, err := ListenLink(n.Net, n.MyInfo.Address.String())
	if err != nil {
		return nil, err	// XXX err ctx
	}

	// now we know our listening address (in case it was autobind before)
	// NOTE listen("tcp", ":1234") gives l.Addr 0.0.0.0:1234 and
	//      listen("tcp6", ":1234") gives l.Addr [::]:1234
	//	-> host is never empty
	addr, err := Addr(ll.Addr())
	if err != nil {
		// XXX -> panic here ?
		ll.Close()
		return nil, err	// XXX err ctx
	}

	n.MyInfo.Address = addr

	l := &listener{
		l:	 ll,
		acceptq: make(chan accepted),
		closed:  make(chan struct{}),
	}
	go l.run()

	return l, nil
}

// Listener is LinkListener adapted to return NodeLink with requested identification on Accept.
type Listener interface {
	LinkListener

	// Accept accepts incoming client connection.
	//
	// On success the link was handshaked and peer sent us RequestIdentification
	// packet which we did not yet answer.
	//
	// On success returned are:
	// - primary link connection which carried identification
	// - requested identification packet
	Accept() (*Conn, *RequestIdentification, error)
}

type listener struct {
	l       *LinkListener
	acceptq chan accepted
	closed  chan struct {}
}

type accepted struct {
	conn  *Conn
	idReq *RequestIdentification
	err   error
}

func (l *listener) Close() error {
	err := l.l.Close()
	close(l.closed)
	return err
}

func (l *listener) run() {
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

func (l *listener) accept(link *NodeLink, err error) {
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

func (l *listener) accept1(link *NodeLink, err0 error) (_ *Conn, _ *RequestIdentification, err error) {
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

	// NOTE NodeLink currently guarantees that after link.Accept() there is
	// at least 1 packet in accepted conn. This way the following won't
	// block/deadlock if packets with other ConnID comes.
	// Still it is a bit fragile.
	idReq := &RequestIdentification{}
	_, err = conn.Expect(idReq)
	if err != nil {
		// XXX ok to let peer know error as is? e.g. even IO error on Recv?
		err2 := conn.Send(&Error{PROTOCOL_ERROR, err.Error()})
		err = xerr.Merge(err, err2)
		return nil, nil, err
	}

	return conn, idReq, nil
}

func (l *listener) Accept() (*Conn, *RequestIdentification, error) {
	select{
	case <-l.closed:
		// we know raw listener is already closed - return proper error about it
		_, err := l.l.Accept()
		return nil, nil, err

	case a := <-l.acceptq:
		return a.conn, a.idReq, a.err
	}
}

func (l *listener) Addr() net.Addr {
	return l.l.Addr()
}

// TODO functions to update:
//	.PartTab	from NotifyPartitionTable msg
//	.NodeTab	from NotifyNodeInformation msg
//	.ClusterState	from NotifyClusterState msg
