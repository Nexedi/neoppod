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

//go:generate gotrace gen .

import (
	"context"
	"fmt"
	"net"
	"sync"

	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"
	//"lab.nexedi.com/kirr/neo/go/xcommon/xio"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

const (
	//INVALID_UUID UUID = 0

	// XXX -> zodb?
	INVALID_TID  zodb.Tid = 1<<64 - 1            // 0xffffffffffffffff
	INVALID_OID  zodb.Oid = 1<<64 - 1

	// OID_LEN = 8
	// TID_LEN = 8
)


// NodeApp is base for implementing NEO node applications.
//
// XXX -> internal?
type NodeApp struct {
	MyInfo		NodeInfo
	ClusterName	string

	Net		xnet.Networker	// network AP we are sending/receiving on
	MasterAddr	string		// address of master	XXX -> Address ?

	StateMu		sync.RWMutex	// <- XXX just embed?
	NodeTab		*NodeTable	// information about nodes in the cluster
	PartTab		*PartitionTable	// information about data distribution in the cluster
	ClusterState	ClusterState	// master idea about cluster state

	// should be set by user so NodeApp can notify when master tells this node to shutdown
	OnShutdown	func()
}

// NewNodeApp creates new node application
func NewNodeApp(net xnet.Networker, typ NodeType, clusterName, masterAddr, serveAddr string) *NodeApp {
	// convert serveAddr into neo format
	addr, err := AddrString(net.Network(), serveAddr)
	if err != nil {
		panic(err)	// XXX
	}

	app := &NodeApp{
		MyInfo:		NodeInfo{Type: typ, Addr: addr, IdTime: IdTimeNone},
		ClusterName:	clusterName,
		Net:		net,
		MasterAddr:	masterAddr,

		NodeTab:	&NodeTable{},
		PartTab:	&PartitionTable{},
		ClusterState:	-1, // invalid
	}

	app.NodeTab.nodeApp = app
	return app
}

// Dial connects to another node in the cluster.
//
// It handshakes, requests identification and checks peer type. If successful returned are:
// - established link
// - accept identification reply
//
// Dial does not update .NodeTab or its node entries in any way.
// For establishing links to peers present in .NodeTab use Node.Dial.
func (app *NodeApp) Dial(ctx context.Context, peerType NodeType, addr string) (_ *NodeLink, _ *AcceptIdentification, err error) {
	defer task.Runningf(&ctx, "dial %v (%v)", addr, peerType)(&err)

	link, err := DialLink(ctx, app.Net, addr)
	if err != nil {
		return nil, nil, err
	}

	log.Info(ctx, "dialed ok; requesting identification...")
	defer xerr.Contextf(&err, "%s: request identification", link)
	// close link on error or FIXME: ctx cancel
	//cleanup := xio.CloseWhenDone(ctx, link)
	defer func() {
		if err != nil {
			// FIXME wrong - err=nil -> goroutine still left hanging waiting
			// for ctx and will close link if dial ctx closes
			// cleanup()

			lclose(ctx, link)
		}
	}()

	req := &RequestIdentification{
		NodeType:	app.MyInfo.Type,
		UUID:		app.MyInfo.UUID,
		Address:	app.MyInfo.Addr,
		ClusterName:	app.ClusterName,
		IdTime:		app.MyInfo.IdTime,	// XXX ok?
	}
	accept := &AcceptIdentification{}
	// FIXME error if peer sends us something with another connID
	// (currently we ignore and serveRecv will deadlock)
	//
	// XXX solution could be:
	// link.CloseAccept()
	// link.Ask1(req, accept)
	// link.Listen()
	// XXX but there is a race window in between recv in ask and listen
	// start, and if peer sends new connection in that window it will be rejected.
	//
	// TODO thinking.
	err = link.Ask1(req, accept)
	if err != nil {
		return nil, nil, err
	}

	// XXX vvv move out of here (e.g. to DialPeer) if we are not checking everthing in full here?
	if accept.NodeType != peerType {
		// XXX send Error to peer?
		return nil, nil, fmt.Errorf("accepted, but peer is not %v (identifies as %v)", peerType, accept.NodeType)
	}

	// XXX accept.MyUUID, link // XXX register .NodeTab? (or better LinkTab as NodeTab is driven by M)
	// XXX accept.YourUUID	// XXX M can tell us to change UUID -> take in effect
	// XXX accept.NumPartitions, ... wrt app.node.PartTab

	log.Info(ctx, "identification accepted")
	return link, accept, nil
}


// Listen starts listening at node's listening address.
//
// If the address is empty one new free is automatically selected.
// The node information about where it listens at is appropriately updated.
func (app *NodeApp) Listen() (Listener, error) {
	// start listening
	ll, err := ListenLink(app.Net, app.MyInfo.Addr.String())
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

	app.MyInfo.Addr = addr

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
	// from LinkListener:
	Close() error
	Addr() net.Addr

	// Accept accepts incoming client connection.
	//
	// On success the link was handshaked and peer sent us RequestIdentification
	// packet which we did not yet answer.
	//
	// On success returned are:
	// - original peer request that carried identification
	// - requested identification packet
	//
	// After successful accept it is the caller responsibility to reply the request.
	//
	// NOTE established link is Request.Link().
	Accept(ctx context.Context) (*Request, *RequestIdentification, error)
}

type listener struct {
	l       LinkListener
	acceptq chan accepted
	closed  chan struct {}
}

type accepted struct {
	req   *Request
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
		// XXX do not let err go to .accept() - handle here? (but here
		// we do not know with which severety and context to log)
		link, err := l.l.Accept()
		go l.accept(link, err)
	}
}

func (l *listener) accept(link *NodeLink, err error) {
	res := make(chan accepted, 1)
	go func() {
		req, idReq, err := l.accept1(context.Background(), link, err)	// XXX ctx cancel on l close?
		res <- accepted{req, idReq, err}
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

	if !ok && link != nil {
		link.Close()
	}
}

func (l *listener) accept1(ctx context.Context, link *NodeLink, err0 error) (_ *Request, _ *RequestIdentification, err error) {
	if err0 != nil {
		return nil, nil, err0
	}

	defer xerr.Context(&err, "identify")	// XXX -> task.ErrContext?

	// identify peer
	// the first conn must come with RequestIdentification packet
	req, err := link.Recv1(/*ctx*/)
	if err != nil {
		return nil, nil, err
	}

	switch msg := req.Msg.(type) {
	case *RequestIdentification:
		return &req, msg, nil
	}

	emsg := &Error{PROTOCOL_ERROR, fmt.Sprintf("unexpected message %T", req.Msg)}
	req.Reply(emsg)	// XXX err
	return nil, nil, emsg
}

func (l *listener) Accept(ctx context.Context) (*Request, *RequestIdentification, error) {
	select{
	case <-l.closed:
		// we know raw listener is already closed - return proper error about it
		_, err := l.l.Accept()
		return nil, nil, err

	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case a := <-l.acceptq:
		return a.req, a.idReq, a.err
	}
}

func (l *listener) Addr() net.Addr {
	return l.l.Addr()
}

// ----------------------------------------

// UpdateNodeTab applies updates to .NodeTab from message and logs changes appropriately.
func (app *NodeApp) UpdateNodeTab(ctx context.Context, msg *NotifyNodeInformation) {
	// XXX msg.IdTime ?
	for _, nodeInfo := range msg.NodeList {
		log.Infof(ctx, "node update: %v", nodeInfo)
		app.NodeTab.Update(nodeInfo)

		// XXX we have to provide IdTime when requesting identification to other peers
		// (e.g. Spy checks this is what master broadcast them and if not replis "unknown by master")
		if nodeInfo.UUID == app.MyInfo.UUID {
			// XXX recheck locking
			// XXX do .MyInfo = nodeInfo ?
			app.MyInfo.IdTime = nodeInfo.IdTime

			// FIXME hack - better it be separate command and handled cleanly
			if nodeInfo.State == DOWN {
				log.Info(ctx, "master told us to shutdown")
				log.Flush()
				app.OnShutdown()
				// os.Exit(1)
				return
			}
		}
	}

	// FIXME logging under lock (if caller took e.g. .StateMu before applying updates)
	log.Infof(ctx, "full nodetab:\n%s", app.NodeTab)
}

// UpdatePartTab applies updates to .PartTab from message and logs changes appropriately.
func (app *NodeApp) UpdatePartTab(ctx context.Context, msg *SendPartitionTable) {
	pt := PartTabFromDump(msg.PTid, msg.RowList)
	// XXX logging under lock
	log.Infof(ctx, "parttab update: %v", pt)
	app.PartTab = pt
}

// UpdateClusterState applies update to .ClusterState from message and logs change appropriately.
func (app *NodeApp) UpdateClusterState(ctx context.Context, msg *NotifyClusterState) {
	// XXX loging under lock
	log.Infof(ctx, "state update: %v", msg.State)
	app.ClusterState.Set(msg.State)
}
