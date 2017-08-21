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
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"

	"lab.nexedi.com/kirr/go123/xerr"
)

// Storage is NEO node that keeps data and provides read/write access to it
type Storage struct {
	node neo.NodeCommon

	// context for providing operational service
	// it is renewed every time master tells us StartOpertion, so users
	// must read it initially only once under opMu via opCtxRead
	opMu  sync.Mutex
	opCtx context.Context

	// TODO storage layout:
	//	meta/
	//	data/
	//	    1 inbox/	(commit queues)
	//	    2 ? (data.fs)
	//	    3. packed/
	zstor zodb.IStorage // underlying ZODB storage	XXX -> directly work with fs1 & friends
}

// NewStorage creates new storage node that will listen on serveAddr and talk to master on masterAddr.
// The storage uses zstor as underlying backend for storing data.
// Use Run to actually start running the node.
func NewStorage(cluster, masterAddr, serveAddr string, net xnet.Networker, zstor zodb.IStorage) *Storage {
	// convert serveAddr into neo format
	// XXX -> new.NewNode() ?
	addr, err := neo.AddrString(net.Network(), serveAddr)
	if err != nil {
		panic(err)	// XXX
	}

	stor := &Storage{
			node:	neo.NodeCommon{
				MyInfo:		neo.NodeInfo{Type: neo.STORAGE, Addr: addr},
				ClusterName:	cluster,
				Net:		net,
				MasterAddr:	masterAddr,
			},

			zstor:		zstor,
	}

	// operational context is initially done (no service should be provided)
	noOpCtx, cancel := context.WithCancel(context.Background())
	cancel()
	stor.opCtx = noOpCtx

	return stor
}


// Run starts storage node and runs it until either ctx is cancelled or master
// commands it to shutdown.
func (stor *Storage) Run(ctx context.Context) error {
	// start listening
	l, err := stor.node.Listen()
	if err != nil {
		return err // XXX err ctx
	}

	defer runningf(&ctx, "storage(%v)", l.Addr())(&err)

	// start serving incoming connections
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// XXX dup from master
		for serveCtx.Err() == nil {
			conn, idReq, err := l.Accept()
			if err != nil {
				// TODO log / throttle
				continue
			}

			_ = idReq

			select {
			//case m.nodeCome <- nodeCome{conn, idReq, nil/*XXX kill*/}:
			//	// ok

			case <-serveCtx.Done():
				// shutdown
				lclose(serveCtx, conn.Link())
				return
			}
		}
	}()

	// connect to master and get commands and updates from it
	err = stor.talkMaster(ctx)
	// XXX log err?

	// we are done - shutdown
	serveCancel()
	wg.Wait()

	return err // XXX err ctx
}

// --- connect to master and let it direct us ---

// talkMaster connects to master, announces self and receives commands and notifications.
// it tries to persist master link reconnecting as needed
//
// it always returns an error - either due to cancel or command from master to shutdown
func (stor *Storage) talkMaster(ctx context.Context) (err error) {
	defer runningf(&ctx, "talk master(%v)", stor.node.MasterAddr)(&err)

	for {
		err := stor.talkMaster1(ctx)
		log.Error(ctx, err)

		// TODO if err = shutdown -> return

		// exit on cancel / throttle reconnecting 
		select {
		case <-ctx.Done():
			return ctx.Err()

		// XXX 1s hardcoded -> move out of here
		case <-time.After(1*time.Second):
			// ok
		}
	}
}

// talkMaster1 does 1 cycle of connect/talk/disconnect to master.
// it returns error describing why such cycle had to finish
// XXX distinguish between temporary problems and non-temporary ones?
func (stor *Storage) talkMaster1(ctx context.Context) (err error) {
	// XXX put logging into Dial?
	log.Info(ctx, "connecting ...")
	Mconn, accept, err := stor.node.Dial(ctx, neo.MASTER, stor.node.MasterAddr)
	if err != nil {
		// FIXME it is not only identification - e.g. ECONNREFUSED
		log.Info(ctx, "identification rejected")	// XXX ok here? (err is logged above)
		return err
	}

	log.Info(ctx, "identification accepted")
	Mlink := Mconn.Link()

	// close Mlink on return / cancel
	retch := make(chan struct{})
	defer func() {
		err2 := Mlink.Close()
		err = xerr.First(err, err2)
		close(retch)
	}()

	// XXX add master UUID -> nodeTab ? or master will notify us with it himself ?

	if !(accept.NumPartitions == 1 && accept.NumReplicas == 1) {
		return fmt.Errorf("TODO for 1-storage POC: Npt: %v  Nreplica: %v", accept.NumPartitions, accept.NumReplicas)
	}

	// XXX -> node.Dial ?
	if accept.YourNodeUUID != stor.node.MyInfo.UUID {
		log.Infof(ctx, "master told us to have uuid=%v", accept.YourNodeUUID)
		stor.node.MyInfo.UUID = accept.YourNodeUUID
	}


	// accept next connection from master. only 1 connection is served at any given time.
	// every new connection from master means talk over previous connection is cancelled.
	// XXX recheck compatibility with py
	type accepted struct {conn *neo.Conn; err error}
	acceptq := make(chan accepted)
	go func () {
		// XXX (temp ?) disabled not to let S accept new connections
		// reason: not (yet ?) clear how to allow listen on dialed link without
		// missing immediate sends or deadlocks if peer does not follow
		// expected protocol exchange (2 receive paths: Recv & Accept)
		return

		for {
			conn, err := Mlink.Accept()

			select {
			case acceptq <- accepted{conn, err}:
			case <-retch:
				return
			}

			if err != nil {
				log.Error(ctx, err)
				return
			}
		}
	}()

	// handle notifications and commands from master
	talkq := make(chan error, 1)
	for {
		// wait for next connection from master if talk over previous one finished.
		// XXX rafactor all this into SingleTalker ? (XXX ServeSingle ?)
		if Mconn == nil {
			select {
			case a := <-acceptq:
				if a.err != nil {
					return a.err
				}
				Mconn = a.conn

			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// one talk cycle for master to drive us
		// puts error after talk finishes -> talkq
		talk := func() error {
			// let master initialize us. If successful this ends with StartOperation command.
			err := stor.m1initialize(ctx, Mconn)
			if err != nil {
				log.Error(ctx, err)
				return err
			}

			// we got StartOperation command. Let master drive us during servicing phase.
			err = stor.m1serve(ctx, Mconn)
			log.Error(ctx, err)
			return err
		}
		go func() {
			talkq <- talk()
		}()

		// next connection / talk finished / cancel
		select {
		case a := <-acceptq:
			lclose(ctx, Mconn) // wakeup/cancel current talk
			<-talkq            // wait till it finish
			if a.err != nil {
				return a.err
			}
			Mconn = a.conn     // proceed next cycle on accepted conn

		case err = <-talkq:
			// XXX check for shutdown command
			lclose(ctx, Mconn)
			Mconn = nil // now wait for accept to get next Mconn

		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
	defer runningf(&ctx, "init %v", Mconn)(&err)

	for {
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
				PTid:		stor.node.PartTab.PTid,
				BackupTid:	neo.INVALID_TID,
				TruncateTid:	neo.INVALID_TID})

		case *neo.AskPartitionTable:
			// TODO initially read PT from disk
			err = Mconn.Send(&neo.AnswerPartitionTable{
				PTid:	 stor.node.PartTab.PTid,
				RowList: stor.node.PartTab.Dump()})

		case *neo.LockedTransactions:
			// XXX r/o stub
			err = Mconn.Send(&neo.AnswerLockedTransactions{})

		// TODO AskUnfinishedTransactions

		case *neo.LastIDs:
			lastTid, zerr1 := stor.zstor.LastTid()
			lastOid, zerr2 := stor.zstor.LastOid()
			if zerr := xerr.First(zerr1, zerr2); zerr != nil {
				return zerr	// XXX send the error to M
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

		// XXX move Mconn.Send here and ^^^ only prepare reply?
		if err != nil {
			return err
		}
	}
}

// m1serve drives storage by master messages during service phase
//
// Service is regular phase serving requests from clients to load/save object,
// handling transaction commit (with master) and syncing data with other
// storage nodes (XXX correct?).
//
// it always returns with an error describing why serve has to be stopped -
// either due to master commanding us to stop, or context cancel or some other
// error.
func (stor *Storage) m1serve(ctx context.Context, Mconn *neo.Conn) (err error) {
	defer runningf(&ctx, "serve %v", Mconn)(&err)

	// refresh stor.opCtx and cancel it when we finish so that client
	// handlers know they need to stop operating as master told us to do so.
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

// --- serve incoming connections from other nodes ---

// ServeLink serves incoming node-node link connection
// XXX +error return?
func (stor *Storage) ServeLink(ctx context.Context, link *neo.NodeLink) {
	log.Infof(ctx, "%s: serving new node", link)	// XXX -> running?

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
		log.Info(ctx, "%v: closing link", link)
		link.Close()	// XXX err
	}()

	// XXX only accept clients
	// XXX only accept when operational (?)
	nodeInfo, err := IdentifyPeer(link, neo.STORAGE)
	if err != nil {
		log.Error(ctx, err)
		return
	}

	var serveConn func(context.Context, *neo.Conn)
	switch nodeInfo.NodeType {
	case neo.CLIENT:
		serveConn = stor.serveClient

	default:
		// XXX vvv should be reply to peer
		log.Errorf(ctx, "%v: unexpected peer type: %v", link, nodeInfo.NodeType)
		return
	}

	// identification passed, now serve other requests
	for {
		conn, err := link.Accept()
		if err != nil {
			log.Error(ctx, err)
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
	log.Infof(ctx, "%s: serving new client conn", conn)	// XXX -> running?

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

	log.Infof(ctx, "%v: %v", conn, err)
	// XXX vvv -> defer ?
	log.Infof(ctx, "%v: closing client conn", conn)
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
