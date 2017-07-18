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

package server
// storage node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
)

// XXX fmt -> log

// Storage is NEO storage server application
type Storage struct {
	// XXX move -> nodeCommon?
	// ---- 8< ----
	myInfo		neo.NodeInfo	// XXX -> only Address + NodeUUID ?
	clusterName	string

	net		xnet.Networker	// network AP we are sending/receiving on
	masterAddr	string		// address of master
	// ---- 8< ----

	// context for providing operational service
	// it is renewed every time master tells us StartOpertion, so users
	// must read it initially only once under opMu
	opMu  sync.Mutex
	opCtx context.Context

	zstor zodb.IStorage // underlying ZODB storage	XXX temp ?
}

// NewStorage creates new storage node that will listen on serveAddr and talk to master on masterAddr
// The storage uses zstor as underlying backend for storing data.
// Use Run to actually start running the node.
func NewStorage(cluster, masterAddr, serveAddr string, net xnet.Networker, zstor zodb.IStorage) *Storage {
	// convert serveAddr into neo format
	addr, err := neo.AddrString(net.Network(), serveAddr)
	if err != nil {
		panic(err)	// XXX
	}

	stor := &Storage{
			myInfo:		neo.NodeInfo{NodeType: neo.STORAGE, Address: addr},
			clusterName:	cluster,
			net:		net,
			masterAddr:	masterAddr,
			zstor:		zstor,
	}

	// operational context is initially done (no service should be provided)
	noOpCtx, cancel := context.WithCancel(context.Background())
	stor.opCtx = noOpCtx
	cancel()

	return stor
}


// Run starts storage node and runs it until either ctx is cancelled or master
// commands it to shutdown.
func (stor *Storage) Run(ctx context.Context) error {
	// XXX dup wrt Master.Run
	// start listening
	l, err := stor.net.Listen(stor.myInfo.Address.String())		// XXX ugly
	if err != nil {
		return err // XXX err ctx
	}

	// now we know our listening address (in case it was autobind before)
	// NOTE listen("tcp", ":1234") gives l.Addr 0.0.0.0:1234 and
	//      listen("tcp6", ":1234") gives l.Addr [::]:1234
	//	-> host is never empty
	addr, err := neo.Addr(l.Addr())
	if err != nil {
		// XXX -> panic here ?
		return err	// XXX err ctx
	}

	stor.myInfo.Address = addr

	wg := sync.WaitGroup{}

	serveCtx, serveCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = Serve(serveCtx, l, stor)
		_ = err	// XXX what to do with err ?
	}()

	err = stor.talkMaster(ctx)
	serveCancel()
	wg.Wait()

	return err // XXX err ctx
}

// talkMaster connects to master, announces self and receives notifications and commands
// XXX and notifies master about ? (e.g. StartOperation -> NotifyReady)
// it tries to persist master link reconnecting as needed
//
// it always return an error - either due to cancel or commannd from master to shutdown
func (stor *Storage) talkMaster(ctx context.Context) error {
	// XXX errctx

	for {
		fmt.Printf("stor: master(%v): connecting\n", stor.masterAddr) // XXX info
		err := stor.talkMaster1(ctx)
		fmt.Printf("stor: master(%v): %v\n", stor.masterAddr, err)

		// TODO if err = shutdown -> return
		// XXX handle shutdown command from master

		// throttle reconnecting / exit on cancel
		select {
		case <-ctx.Done():
			return ctx.Err()

		// XXX 1s hardcoded -> move out of here
		case <-time.After(1*time.Second):
			// ok
		}
	}
}

// talkMaster1 does 1 cycle of connect/talk/disconnect to master
// it returns error describing why such cycle had to finish
// XXX distinguish between temporary problems and non-temporary ones?
func (stor *Storage) talkMaster1(ctx context.Context) error {
	Mlink, err := neo.Dial(ctx, stor.net, stor.masterAddr)
	if err != nil {
		return err
	}

	// TODO Mlink.Close() on return / cancel

	// request identification this way registering our node to master
	accept, err := neo.IdentifyWith(neo.MASTER, Mlink, stor.myInfo, stor.clusterName)
	if err != nil {
		return err
	}

	// XXX add master UUID -> nodeTab ? or master will notify us with it himself ?

	if !(accept.NumPartitions == 1 && accept.NumReplicas == 1) {
		return fmt.Errorf("TODO for 1-storage POC: Npt: %v  Nreplica: %v", accept.NumPartitions, accept.NumReplicas)
	}

	if accept.YourNodeUUID != stor.myInfo.NodeUUID {
		fmt.Printf("stor: %v: master told us to have UUID=%v\n", Mlink, accept.YourNodeUUID)
		stor.myInfo.NodeUUID = accept.YourNodeUUID
		// XXX notify anyone?
	}

	// now handle notifications and commands from master
	// FIXME wrong - either keep conn as one used from identification or accept from listening
	conn, err := Mlink.NewConn()
	if err != nil { panic(err) }	// XXX

	for {
		notify, err := conn.Recv()
		if err != nil {
			// XXX TODO
		}

		_ = notify	// XXX temp
	}





}

// ServeLink serves incoming node-node link connection
// XXX +error return?
func (stor *Storage) ServeLink(ctx context.Context, link *neo.NodeLink) {
	fmt.Printf("stor: %s: serving new node\n", link)

	// close link when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - link.Close will signal to all current IO to
	//   terminate with an error )
	// XXX dup -> utility
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX tell peers we are shutting down?
			// XXX ret err = ctx.Err()
		case <-retch:
		}
		fmt.Printf("stor: %v: closing link\n", link)
		link.Close()	// XXX err
	}()

	// XXX only accept clients
	// XXX only accept when operational (?)
	nodeInfo, err := IdentifyPeer(link, neo.STORAGE)
	if err != nil {
		fmt.Printf("stor: %v\n", err)
		return
	}

	var serveConn func(context.Context, *neo.Conn)
	switch nodeInfo.NodeType {
	case neo.CLIENT:
		serveConn = stor.ServeClient

	default:
		// XXX vvv should be reply to peer
		fmt.Printf("stor: %v: unexpected peer type: %v\n", link, nodeInfo.NodeType)
		return
	}

	// identification passed, now serve other requests
	for {
		conn, err := link.Accept()
		if err != nil {
			fmt.Printf("stor: %v\n", err)	// XXX err ctx
			break
		}

		// XXX wrap conn close to happen here, not in ServeClient ?
		go serveConn(ctx, conn)
	}

	// TODO wait all spawned serveConn
}


// ServeClient serves incoming connection on which peer identified itself as client
// XXX +error return?
func (stor *Storage) ServeClient(ctx context.Context, conn *neo.Conn) {
	fmt.Printf("stor: %s: serving new client conn\n", conn)

	// rederive ctx from ctx and .operationCtx (which is cancelled when M tells us StopOperation)
	// XXX -> xcontext ?
	ctx, opCancel := context.WithCancel(ctx)
	go func() {
		// cancel ctx when global operation context is cancelled
		stor.opMu.Lock()
		opCtx := stor.opCtx
		stor.opMu.Unlock()
		select {
		case <-opCtx.Done():
			opCancel()

		case <-ctx.Done():
			// noop - to avoid goroutine leak
		}
	}()

	// close connection when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - conn.Close will signal to current IO to
	//   terminate with an error )
	// XXX dup -> utility
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX tell client we are shutting down?
			// XXX ret err = cancelled ?
		case <-retch:
		}
		fmt.Printf("stor: %v: closing client conn\n", conn)
		conn.Close()	// XXX err
	}()

	for {
		req, err := conn.Recv()
		if err != nil {
			return	// XXX log / err / send error before closing
		}

		switch req := req.(type) {
		case *neo.GetObject:
			xid := zodb.Xid{Oid: req.Oid}
			if req.Serial != neo.INVALID_TID {
				xid.Tid = req.Serial
				xid.TidBefore = false
			} else {
				xid.Tid = req.Tid
				xid.TidBefore = true
			}

			var reply neo.Msg
			data, tid, err := stor.zstor.Load(xid)
			if err != nil {
				// TODO translate err to NEO protocol error codes
				reply = neo.ErrEncode(err)
			} else {
				reply = &neo.AnswerGetObject{
						Oid:	xid.Oid,
						Serial: tid,

						Compression: false,
						Data: data,
						// XXX .CheckSum

						// XXX .NextSerial
						// XXX .DataSerial
					}
			}

			conn.Send(reply)	// XXX err

		case *neo.LastTransaction:
			var reply neo.Msg

			lastTid, err := stor.zstor.LastTid()
			if err != nil {
				reply = neo.ErrEncode(err)
			} else {
				reply = &neo.AnswerLastTransaction{lastTid}
			}

			conn.Send(reply)	// XXX err

		//case *ObjectHistory:
		//case *StoreObject:

		default:
			panic("unexpected packet")	// XXX
		}

		//req.Put(...)
	}
}
