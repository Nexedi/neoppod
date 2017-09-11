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
	"crypto/sha1"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"
	"lab.nexedi.com/kirr/neo/go/xcommon/xio"

	"lab.nexedi.com/kirr/go123/xerr"
)

// Storage is NEO node that keeps data and provides read/write access to it via network.
type Storage struct {
	node *neo.NodeApp

	// context for providing operational service
	// it is renewed every time master tells us StartOpertion, so users
	// must read it initially only once under opMu via withWhileOperational.
	opMu  sync.Mutex
	opCtx context.Context

	// TODO storage layout:
	//	meta/
	//	data/
	//	    1 inbox/	(commit queues)
	//	    2 ? (data.fs)
	//	    3 packed/	(deltified objects)
	zstor zodb.IStorage // underlying ZODB storage	XXX -> directly work with fs1 & friends

	//nodeCome chan nodeCome	// node connected
}

// NewStorage creates new storage node that will listen on serveAddr and talk to master on masterAddr.
//
// The storage uses zstor as underlying backend for storing data.
// Use Run to actually start running the node.
func NewStorage(clusterName, masterAddr, serveAddr string, net xnet.Networker, zstor zodb.IStorage) *Storage {
	stor := &Storage{
		node:  neo.NewNodeApp(net, neo.STORAGE, clusterName, masterAddr, serveAddr),
		zstor: zstor,
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

	defer task.Runningf(&ctx, "storage(%v)", l.Addr())(&err)

	// start serving incoming connections
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)

	//stor.node.OnShutdown = serveCancel
	// XXX hack: until ctx cancel is not handled properly by Recv/Send
	stor.node.OnShutdown = func() {
		serveCancel()
		lclose(ctx, l)
	}

	wg.Add(1)
	go func(ctx context.Context) (err error) {
		defer wg.Done()
		defer task.Running(&ctx, "accept")(&err)

		// XXX dup from master
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			req, idReq, err := l.Accept(ctx)
			if err != nil {
				log.Error(ctx, err)	// XXX throttle?
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				stor.serveLink(ctx, req, idReq) // XXX ignore err? -> logged
			}()


			// // handover to main driver
			// select {
			// case stor.nodeCome <- nodeCome{req, idReq}:
			// 	// ok
			//
			// case <-ctx.Done():
			// 	// shutdown
			// 	lclose(ctx, req.Link())
			// 	continue
			// }
		}
	}(serveCtx)

	// connect to master and get commands and updates from it
	//err = stor.talkMaster(ctx)
	err = stor.talkMaster(serveCtx)		// XXX hack for shutdown
	// XXX log err?

	// we are done - shutdown
	serveCancel()
	wg.Wait()

	return err // XXX err ctx
}

// --- connect to master and let it drive us ---

// talkMaster connects to master, announces self and receives commands and notifications.
// it tries to persist master link reconnecting as needed.
//
// it always returns an error - either due to cancel or command from master to shutdown.
func (stor *Storage) talkMaster(ctx context.Context) (err error) {
	defer task.Runningf(&ctx, "talk master(%v)", stor.node.MasterAddr)(&err)

	// XXX dup wrt Client.talkMaster
	for {
		err := stor.talkMaster1(ctx)
		log.Warning(ctx, err)	// XXX Warning ok? -> Error?

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
//
// it returns error describing why such cycle had to finish.
// XXX distinguish between temporary problems and non-temporary ones?
func (stor *Storage) talkMaster1(ctx context.Context) (err error) {
	// XXX dup in Client.talkMaster1 ?
	mlink, accept, err := stor.node.Dial(ctx, neo.MASTER, stor.node.MasterAddr)
	if err != nil {
		return err
	}

	defer xio.CloseWhenDone(ctx, mlink)()

	// XXX add master UUID -> nodeTab ? or master will notify us with it himself ?

	if !(accept.NumPartitions == 1 && accept.NumReplicas == 1) {
		return fmt.Errorf("TODO for 1-storage POC: Npt: %v  Nreplica: %v", accept.NumPartitions, accept.NumReplicas)
	}

	// XXX -> node.Dial ?
	if accept.YourUUID != stor.node.MyInfo.UUID {
		log.Infof(ctx, "master told us to have uuid=%v", accept.YourUUID)
		stor.node.MyInfo.UUID = accept.YourUUID
	}

	// handle notifications and commands from master

	// let master initialize us. If successful this ends with StartOperation command.
	reqStart, err := stor.m1initialize(ctx, mlink)
	if err != nil {
		//log.Error(ctx, err)
		return err
	}

	// we got StartOperation command. Let master drive us during servicing phase.
	err = stor.m1serve(ctx, reqStart)
	//log.Error(ctx, err)
	return err
}

// m1initialize drives storage by master messages during initialization phase
//
// Initialization includes master retrieving info for cluster recovery and data
// verification before starting operation. Initialization finishes either
// successfully with receiving master command to start operation, or
// unsuccessfully with connection closing indicating initialization was
// cancelled or some other error.
//
// return error indicates:
// - nil:  initialization was ok and a command came from master to start operation.
// - !nil: initialization was cancelled or failed somehow.
func (stor *Storage) m1initialize(ctx context.Context, mlink *neo.NodeLink) (reqStart *neo.Request, err error) {
	defer task.Runningf(&ctx, "init %v", mlink)(&err)

	for {
		req, err := mlink.Recv1()
		if err != nil {
			return nil, err
		}

		// XXX vvv move Send out of reply preparing logic

		switch msg := req.Msg.(type) {
		default:
			return nil, fmt.Errorf("unexpected message: %T", msg)

		case *neo.StartOperation:
			// ok, transition to serve
			return &req, nil

		case *neo.Recovery:
			err = req.Reply(&neo.AnswerRecovery{
				PTid:		stor.node.PartTab.PTid,
				BackupTid:	neo.INVALID_TID,
				TruncateTid:	neo.INVALID_TID})

		case *neo.AskPartitionTable:
			// TODO initially read PT from disk
			err = req.Reply(&neo.AnswerPartitionTable{
				PTid:	 stor.node.PartTab.PTid,
				RowList: stor.node.PartTab.Dump()})

		case *neo.LockedTransactions:
			// XXX r/o stub
			err = req.Reply(&neo.AnswerLockedTransactions{})

		// TODO AskUnfinishedTransactions

		case *neo.LastIDs:
			lastTid, zerr1 := stor.zstor.LastTid(ctx)
			lastOid, zerr2 := stor.zstor.LastOid(ctx)
			if zerr := xerr.First(zerr1, zerr2); zerr != nil {
				return nil, zerr	// XXX send the error to M
			}

			err = req.Reply(&neo.AnswerLastIDs{LastTid: lastTid, LastOid: lastOid})

		case *neo.SendPartitionTable:
			// TODO M sends us whole PT -> save locally
			stor.node.UpdatePartTab(ctx, msg)	// XXX lock?

		case *neo.NotifyPartitionChanges:
			// TODO M sends us δPT -> save locally?

		case *neo.NotifyNodeInformation:
			// XXX check for myUUID and consider it a command (like neo/py) does?
			stor.node.UpdateNodeTab(ctx, msg)	// XXX lock?

		case *neo.NotifyClusterState:
			stor.node.UpdateClusterState(ctx, msg)	// XXX lock? what to do with it?
		}

		// XXX move req.Reply here and ^^^ only prepare reply
		if err != nil {
			return nil, err
		}

		req.Close() // XXX err?
	}
}

// m1serve drives storage by master messages during service phase.
//
// Service is regular phase serving requests from clients to load/save objects,
// handling transaction commit (with master) and syncing data with other
// storage nodes (XXX correct?).
//
// it always returns with an error describing why serve has to be stopped -
// either due to master commanding us to stop, or context cancel or some other
// error.
func (stor *Storage) m1serve(ctx context.Context, reqStart *neo.Request) (err error) {
	mlink := reqStart.Link()
	defer task.Runningf(&ctx, "serve %v", mlink)(&err)

	// refresh stor.opCtx and cancel it when we finish so that client
	// handlers know they need to stop operating as master told us to do so.
	opCtx, opCancel := context.WithCancel(ctx)
	stor.opMu.Lock()
	stor.opCtx = opCtx
	stor.opMu.Unlock()
	defer opCancel()

	// reply M we are ready
	// XXX according to current neo/py this is separate send - not reply - and so we do here
	err = reqStart.Reply(&neo.NotifyReady{})
	if err != nil {
		return err
	}

	for {
		// XXX abort on ctx (XXX or upper?)
		req, err := mlink.Recv1()
		if err != nil {
			return err
		}

		req.Close() // XXX stub, err

		switch msg := req.Msg.(type) {
		default:
			return fmt.Errorf("unexpected message: %T", msg)

		case *neo.StopOperation:
			return fmt.Errorf("stop requested")

		// XXX SendPartitionTable?
		// XXX NotifyPartitionChanges?

		case *neo.NotifyNodeInformation:
			stor.node.UpdateNodeTab(ctx, msg)	// XXX lock?

		case *neo.NotifyClusterState:
			stor.node.UpdateClusterState(ctx, msg)	// XXX lock? what to do with it?

		// TODO commit related messages
		}
	}
}

// --- serve incoming connections from other nodes ---

// identify processes identification request from connected peer.
func (stor *Storage) identify(idReq *neo.RequestIdentification) (neo.Msg, bool) {
	// XXX stub: we accept clients and don't care about their UUID
	if idReq.NodeType != neo.CLIENT {
		return &neo.Error{neo.PROTOCOL_ERROR, "only clients are accepted"}, false
	}
	if idReq.ClusterName != stor.node.ClusterName {
		return &neo.Error{neo.PROTOCOL_ERROR, "cluster name mismatch"}, false
	}

	// check operational
	stor.opMu.Lock()
	operational := (stor.opCtx.Err() == nil)
	stor.opMu.Unlock()

	if !operational {
		return &neo.Error{neo.NOT_READY, "cluster not operational"}, false
	}

	return &neo.AcceptIdentification{
		NodeType:	stor.node.MyInfo.Type,
		MyUUID:		stor.node.MyInfo.UUID,		// XXX lock wrt update
		NumPartitions:	1,	// XXX
		NumReplicas:	1,	// XXX
		YourUUID:	idReq.UUID,
	}, true
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


// serveLink serves incoming node-node link connection
func (stor *Storage) serveLink(ctx context.Context, req *neo.Request, idReq *neo.RequestIdentification) (err error) {
	link := req.Link()
	defer task.Runningf(&ctx, "serve %s", link)(&err)
	defer xio.CloseWhenDone(ctx, link)()

	// first process identification
	idResp, ok := stor.identify(idReq)
	if !ok {
		reject(ctx, req, idResp)	// XXX log?
		return nil
	}

	err = accept(ctx, req, idResp)
	if err != nil {
		return err
	}

	// client passed identification, now serve other requests
	log.Info(ctx, "identification accepted")	// FIXME must be in identify?

	// rederive ctx to be also cancelled if M tells us StopOperation
	ctx, cancel := stor.withWhileOperational(ctx)
	defer cancel()

	wg := sync.WaitGroup{}	// XXX -> errgroup?
	for {
		req, err := link.Recv1()
		if err != nil {
			switch errors.Cause(err) {
			// XXX closed by main or peer down
			// XXX review
			case neo.ErrLinkDown, neo.ErrLinkClosed:
				log.Info(ctx, err)
				// ok

			default:
				log.Error(ctx, err)
			}
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			stor.serveClient(ctx, req)
		}()
	}

	wg.Wait()
	return nil
}

// serveClient serves incoming client request.
//
// XXX +error return?
//
// XXX version that reuses goroutine to serve next client requests
// XXX for py compatibility (py has no way to tell us Conn is closed)
func (stor *Storage) serveClient(ctx context.Context, req neo.Request) {
	link := req.Link()

	for {
		resp := stor.serveClient1(ctx, req.Msg)
		err := req.Reply(resp)
		if err != nil {
			log.Error(ctx, err)
			return
		}

		// keep on going in the same goroutine to avoid goroutine creation overhead
		// TODO += timeout -> go away if inactive
		req, err = link.Recv1()
		if err != nil {
			switch errors.Cause(err) {
			// XXX closed by main or peer down - all logged by main called
			// XXX review
			case neo.ErrLinkDown, neo.ErrLinkClosed:
				// ok

			default:
				log.Error(ctx, err)
			}
			return
		}
	}
}

// serveClient1 prepares response for 1 request from client
func (stor *Storage) serveClient1(ctx context.Context, req neo.Msg) (resp neo.Msg) {
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

		data, tid, err := stor.zstor.Load(ctx, xid)
		if err != nil {
			// translate err to NEO protocol error codes
			return neo.ErrEncode(err)
		}

		return &neo.AnswerObject{
			Oid:	xid.Oid,
			Serial: tid,

			Compression:	false,
			Data:		data,
			Checksum:	sha1.Sum(data),	// XXX computing every time

			// XXX .NextSerial
			// XXX .DataSerial
		}

	case *neo.LastTransaction:
		lastTid, err := stor.zstor.LastTid(ctx)
		if err != nil {
			return neo.ErrEncode(err)
		}

		return &neo.AnswerLastTransaction{lastTid}

	//case *ObjectHistory:
	//case *StoreObject:

	default:
		return &neo.Error{neo.PROTOCOL_ERROR, fmt.Sprintf("unexpected message %T", req)}
	}

	//req.Put(...)
}



// ----------------------------------------

// serveClient serves incoming connection on which peer identified itself as client
// the connection is closed when serveClient returns
// XXX +error return?
//
// XXX version that keeps 1 goroutine per 1 Conn
// XXX unusable until Conn.Close signals peer
/*
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
*/
