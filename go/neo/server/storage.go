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
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"

	"lab.nexedi.com/kirr/go123/xerr"
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
	// must read it initially only once under opMu via opCtxRead
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

	// start serving incoming connections
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = Serve(serveCtx, l, stor)
		_ = err	// XXX what to do with err ?
	}()

	// connect to master and get commands and updates from it
	err = stor.talkMaster(ctx)

	// we are done - shutdown
	serveCancel()
	wg.Wait()

	return err // XXX err ctx
}

// talkMaster connects to master, announces self and receives commands and notifications
// it tries to persist master link reconnecting as needed
//
// it always returns an error - either due to cancel or commannd from master to shutdown
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
func (stor *Storage) talkMaster1(ctx context.Context) (err error) {
	Mlink, err := neo.Dial(ctx, stor.net, stor.masterAddr)
	if err != nil {
		return err
	}

	// close Mlink on return / cancel
	defer func() {
		errClose := Mlink.Close()
		err = xerr.First(err, errClose)
	}()

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
	}

	// now handle notifications and commands from master
	var Mconn *neo.Conn
	for {
		// accept next connection from master. only 1 connection is served at any given time
		// XXX every new connection from master means previous connection was closed
		// XXX how to do so and stay compatible to py?
		//
		// XXX or simply use only the first connection and if M decides
		// to cancel - close whole nodelink and S reconnects?
		if Mconn != nil {
			Mconn.Close()	// XXX err
			Mconn = nil
		}

		Mconn, err = Mlink.Accept()
		if err != nil {
			return err // XXX ?
		}

		err = stor.m1initialize(ctx, Mconn)
		if err != nil {
			fmt.Println("stor: %v: master: %v", err)
			// XXX recheck closing Mconn
			continue // retry initializing
		}

		err = stor.m1serve(ctx, Mconn)
		fmt.Println("stor: %v: master: %v", err)
		// XXX check if it was command to shutdown and if so break
		continue // retry from initializing
	}

	return nil	// XXX err
}

// m1initialize drives storage by master messages during initialization phase
//
// Initialization includes master retrieving info for cluster recovery and data
// verification before starting operation. Initialization finishes either
// successfully with receiving master commanding to start operation, or
// unsuccessfully with connection closing indicating initialization was
// cancelled or some other error.
//
// return error indicates:
// - nil:  initialization was ok and a command came from master to start operation
// - !nil: initialization was cancelled or failed somehow
func (stor *Storage) m1initialize(ctx context.Context, Mconn *neo.Conn) (err error) {
	defer xerr.Context(&err, "init")

	for {
		// XXX abort on ctx (XXX or upper?)
		msg, err := Mconn.Recv()
		if err != nil {
			return err
		}

		switch msg.(type) {
		default:
			return fmt.Errorf("unexpected message: %T", msg)

		case *neo.StartOperation:
			// ok, transition to serve
			return nil

		case *neo.Recovery:
			err = Mconn.Send(&neo.AnswerRecovery{
				PTid:		0, // XXX stub
				BackupTid:	neo.INVALID_TID,
				TruncateTid:	neo.INVALID_TID})

		case *neo.AskPartitionTable:
			// TODO read and send M locally-saved PT (ptid, []PtRow)

		case *neo.LockedTransactions:
			// XXX r/o stub
			err = Mconn.Send(&neo.AnswerLockedTransactions{})

		// TODO AskUnfinishedTransactions

		case *neo.LastIDs:
			lastTid, zerr1 := stor.zstor.LastTid()
			lastOid, zerr2 := stor.zstor.LastOid()
			if zerr := xerr.First(zerr1, zerr2); zerr != nil {
				return zerr
			}

			err = Mconn.Send(&neo.AnswerLastIDs{LastTid: lastTid, LastOid: lastOid})

		case *neo.NotifyPartitionTable:
			// TODO save locally what M told us


		case *neo.NotifyClusterState:
			// TODO .clusterState = ...	XXX what to do with it?

		case *neo.NotifyNodeInformation:
			// XXX check for myUUID and consider it a command (like neo/py) does?
			// TODO update .nodeTab

		}

		if err != nil {
			return err
		}
	}
}

// m1serve drives storage by master messages during service hase
//
// it always returns with an error describing why serve has to be stopped -
// either due to master commanding us to stop, or context cancel or some other
// error.
func (stor *Storage) m1serve(ctx context.Context, Mconn *neo.Conn) (err error) {
	defer xerr.Context(&err, "serve")

	// refresh stor.opCtx and cancel it when we finish so that client
	// handlers know they need to stop operating
	opCtx, opCancel := context.WithCancel(ctx)
	stor.opMu.Lock()
	stor.opCtx = opCtx
	stor.opMu.Unlock()
	defer opCancel()

	// reply M we are ready
	err = Mconn.Send(&neo.NotifyReady{})
	if err != nil {
		return err
	}

	for {
		// XXX abort on ctx (XXX or upper?)
		msg, err := Mconn.Recv()
		if err != nil {
			return err
		}

		switch msg.(type) {
		default:
			return fmt.Errorf("unexpected message: %T", msg)

		case *neo.StopOperation:
			return fmt.Errorf("stop requested")

		// TODO commit related messages
		}
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
		serveConn = stor.serveClient

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

		// XXX wrap conn close to happen here, not in serveClient ?
		go serveConn(ctx, conn)
	}

	// TODO wait all spawned serveConn
}

// withWhileOperational derives new context from ctx which will be cancelled, when either
// - ctx is cancelled, or
// - master tells us to stop operational service
func (stor *Storage) withWhileOperational(ctx context.Context) (context.Context, context.CancelFunc) {
	stor.opMu.Lock()
	opCtx := stor.opCtx
	stor.opMu.Unlock()

	return xcontext.Merge(ctx, opCtx)
}

// serveClient serves incoming connection on which peer identified itself as client
// the connection is closed when serveClient returns
// XXX +error return?
func (stor *Storage) serveClient(ctx context.Context, conn *neo.Conn) {
	fmt.Printf("stor: %s: serving new client conn\n", conn)

	// rederive ctx to be also cancelled if M tells us StopOperation
	ctx, cancel := stor.withWhileOperational(ctx)
	defer cancel()

	// main work to serve
	done := make(chan error, 1)
	go func() {
		for {
			err := stor.serveClient1(conn)
			if err != nil {
				done <- err
				break
			}
		}
	}()

	// close connection when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - conn.Close will signal to current IO to
	//   terminate with an error )
	var err error
	select {
	case <-ctx.Done():
		// XXX tell client we are shutting down?
		// XXX should we also wait for main work to finish?
		err = ctx.Err()

	case err = <-done:
	}

	fmt.Printf("stor: %v: %v\n", conn, err)
	// XXX vvv -> defer ?
	fmt.Printf("stor: %v: closing client conn\n", conn)
	conn.Close()	// XXX err
}

// serveClient1 serves 1 request from a client
func (stor *Storage) serveClient1(conn *neo.Conn) error {
	req, err := conn.Recv()
	if err != nil {
		return err	// XXX log / err / send error before closing
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

	return nil
}
