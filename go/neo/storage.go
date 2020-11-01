// Copyright (C) 2016-2020  Nexedi SA and Contributors.
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
// storage node

import (
	"context"
	"fmt"
	stdnet "net"
	"sync"
	"time"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/neo/storage"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/internal/log"
	"lab.nexedi.com/kirr/neo/go/internal/task"
	xxcontext "lab.nexedi.com/kirr/neo/go/internal/xcontext"
	"lab.nexedi.com/kirr/neo/go/internal/xio"

	"lab.nexedi.com/kirr/go123/xcontext"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
)

// Storage is NEO node that keeps data and provides read/write access to it via network.
//
// Storage implements only NEO protocol logic with data being persisted via provided storage.Backend.
type Storage struct {
	node *NodeApp

	// context for providing operational service
	// it is renewed every time master tells us StartOpertion, so users
	// must read it initially only once under opMu via withWhileOperational.
	opMu  sync.Mutex
	opCtx context.Context

	back storage.Backend

	//nodeCome chan nodeCome	// node connected
}

// NewStorage creates new storage node that will talk to master on masterAddr.
//
// The storage uses back as underlying backend for storing data.
// Use Run to actually start running the node.
func NewStorage(clusterName, masterAddr string, net xnet.Networker, back storage.Backend) *Storage {
	stor := &Storage{
		node: NewNodeApp(net, proto.STORAGE, clusterName, masterAddr),
		back: back,
	}

	// operational context is initially done (no service should be provided)
	noOpCtx, cancel := context.WithCancel(context.Background())
	cancel()
	stor.opCtx = noOpCtx

	return stor
}


// Run starts storage node and runs it until either ctx is cancelled or master
// commands it to shutdown.
//
// The storage will be serving incoming connections on l.
func (stor *Storage) Run(ctx context.Context, l stdnet.Listener) (err error) {
	addr := l.Addr()
	defer task.Runningf(&ctx, "storage(%v)", addr)(&err)

	// update our serving address in node
	naddr, err := proto.Addr(addr)
	if err != nil {
		return err
	}
	stor.node.MyInfo.Addr = naddr

	// wrap listener with link / identificaton hello checker
	ll := neonet.NewLinkListener(l)
	lli := requireIdentifyHello(ll)

	// start serving incoming connections
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)

	//stor.node.OnShutdown = serveCancel
	// XXX hack: until ctx cancel is not handled properly by Recv/Send
	stor.node.OnShutdown = func() {
		serveCancel()
		lclose(ctx, lli)
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

			req, idReq, err := lli.Accept(ctx)
			if err != nil {
				if !xxcontext.Canceled(err) {
					log.Error(ctx, err)	// XXX throttle?
				}
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
	// FIXME dup in Client.talkMaster1
	mlink, accept, err := stor.node.Dial(ctx, proto.MASTER, stor.node.MasterAddr)
	if err != nil {
		return err
	}

	defer xio.CloseWhenDone(ctx, mlink)()

	// XXX add master UUID -> nodeTab ? or master will notify us with it himself ?

// XXX move -> SetNumReplicas handler
/*
	// NumReplicas: neo/py meaning for n(replica) = `n(real-replica) - 1`
	if !(accept.NumPartitions == 1 && accept.NumReplicas == 0) {
		return fmt.Errorf("TODO for 1-storage POC: Npt: %v  Nreplica: %v", accept.NumPartitions, accept.NumReplicas)
	}
*/

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
func (stor *Storage) m1initialize(ctx context.Context, mlink *neonet.NodeLink) (reqStart *neonet.Request, err error) {
	defer task.Runningf(&ctx, "init %v", mlink)(&err)

	for {
		req, err := mlink.Recv1()
		if err != nil {
			return nil, err
		}
		err = stor.m1initialize1(ctx, req)
		if err == cmdStart {
			// start - transition to serve
			return &req, nil
		}
		req.Close()
		if err != nil {
			return nil, err
		}
	}
}

var cmdStart = errors.New("start requested")

// m1initialize1 handles one message from master from under m1initialize
func (stor *Storage) m1initialize1(ctx context.Context, req neonet.Request) error {
	// XXX vvv move Send out of reply preparing logic
	var err error

	switch msg := req.Msg.(type) {
	default:
		return fmt.Errorf("unexpected message: %T", msg)

	case *proto.StartOperation:
		// ok, transition to serve
		return cmdStart

	case *proto.Recovery:
		err = req.Reply(&proto.AnswerRecovery{
			PTid:		stor.node.PartTab.PTid,
			BackupTid:	proto.INVALID_TID,
			TruncateTid:	proto.INVALID_TID})

	case *proto.AskPartitionTable:
		// TODO initially read PT from disk
		err = req.Reply(&proto.AnswerPartitionTable{
			PTid:	     stor.node.PartTab.PTid,
			NumReplicas: 0, // FIXME hardcoded; NEO/py uses this as n(replica)-1
			RowList:     stor.node.PartTab.Dump()})

	case *proto.LockedTransactions:
		// XXX r/o stub
		err = req.Reply(&proto.AnswerLockedTransactions{})

	// TODO AskUnfinishedTransactions

	case *proto.LastIDs:
		lastTid, zerr1 := stor.back.LastTid(ctx)
		lastOid, zerr2 := stor.back.LastOid(ctx)
		if zerr := xerr.First(zerr1, zerr2); zerr != nil {
			return zerr	// XXX send the error to M
		}

		err = req.Reply(&proto.AnswerLastIDs{LastTid: lastTid, LastOid: lastOid})

	case *proto.SendPartitionTable:
		// TODO M sends us whole PT -> save locally
		stor.node.UpdatePartTab(ctx, msg)	// XXX lock?  XXX handle msg.NumReplicas

	case *proto.NotifyPartitionChanges:
		// TODO M sends us δPT -> save locally?

	case *proto.NotifyNodeInformation:
		// XXX check for myUUID and consider it a command (like neo/py) does?
		stor.node.UpdateNodeTab(ctx, msg)	// XXX lock?

	case *proto.NotifyClusterState:
		stor.node.UpdateClusterState(ctx, msg)	// XXX lock? what to do with it?
	}

	// XXX move req.Reply here and ^^^ only prepare reply
	return err
}

// m1serve drives storage by master messages during service phase.
//
// Service is regular phase serving requests from clients to load/save objects,
// handling transaction commit (with master) and syncing data with other
// storage nodes.
//
// it always returns with an error describing why serve had to be stopped -
// either due to master commanding us to stop, or context cancel or some other
// error.
func (stor *Storage) m1serve(ctx context.Context, reqStart *neonet.Request) (err error) {
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
	// XXX NEO/py sends NotifyReady on another conn; we patched py; see 4eaaf186 for context
	err = reqStart.Reply(&proto.NotifyReady{})
	reqStart.Close()
	if err != nil {
		return err
	}

	for {
		// XXX abort on ctx (XXX or upper?)
		req, err := mlink.Recv1()
		if err != nil {
			return err
		}
		err = stor.m1serve1(ctx, req)
		req.Close()
		if err != nil {
			return err
		}
	}
}

// m1serve1 handles one message from master under m1serve
func (stor *Storage) m1serve1(ctx context.Context, req neonet.Request) error {
	switch msg := req.Msg.(type) {
	default:
		return fmt.Errorf("unexpected message: %T", msg)

	case *proto.StopOperation:
		return fmt.Errorf("stop requested")

	// XXX SendPartitionTable?
	// XXX NotifyPartitionChanges?

	case *proto.NotifyNodeInformation:
		stor.node.UpdateNodeTab(ctx, msg)	// XXX lock?

	case *proto.NotifyClusterState:
		stor.node.UpdateClusterState(ctx, msg)	// XXX lock? what to do with it?

	// TODO commit related messages
	}

	return nil
}

// --- serve incoming connections from other nodes ---

// identify processes identification request from connected peer.
func (stor *Storage) identify(idReq *proto.RequestIdentification) (proto.Msg, bool) {
	// XXX stub: we accept clients and don't care about their UUID
	if idReq.NodeType != proto.CLIENT {
		return &proto.Error{proto.PROTOCOL_ERROR, "only clients are accepted"}, false
	}
	if idReq.ClusterName != stor.node.ClusterName {
		return &proto.Error{proto.PROTOCOL_ERROR, "cluster name mismatch"}, false
	}

	// check operational
	stor.opMu.Lock()
	operational := (stor.opCtx.Err() == nil)
	stor.opMu.Unlock()

	if !operational {
		return &proto.Error{proto.NOT_READY, "cluster not operational"}, false
	}

	return &proto.AcceptIdentification{
		NodeType:	stor.node.MyInfo.Type,
		MyUUID:		stor.node.MyInfo.UUID,		// XXX lock wrt update
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


// serveLink serves incoming node-node link connection.
func (stor *Storage) serveLink(ctx context.Context, req *neonet.Request, idReq *proto.RequestIdentification) (err error) {
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
			case neonet.ErrLinkDown, neonet.ErrLinkClosed:
				log.Info(ctx, err)
				// ok

			default:
				log.Error(ctx, err)
			}
			return err
		}

		// XXX this go + link.Recv1() in serveClient arrange for N(goroutine) ↑
		// with O(1/nreq) rate (i.e. N(goroutine, nreq) ~ ln(nreq)).
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
func (stor *Storage) serveClient(ctx context.Context, req neonet.Request) {
	link := req.Link()

	for {
		resp := stor.serveClient1(ctx, req.Msg)
		err := req.Reply(resp)
		req.Close()
		if err != nil {
			log.Error(ctx, err)
			return
		}

		// XXX hack -> resp.Release()
		// XXX req.Msg release too?
		if resp, ok := resp.(*proto.AnswerObject); ok {
			resp.Data.Release()
		}

		// keep on going in the same goroutine to avoid goroutine creation overhead
		// TODO += timeout -> go away if inactive
		req, err = link.Recv1()
		if err != nil {
			switch errors.Cause(err) {
			// XXX closed by main or peer down - all logged by main called
			// XXX review
			case neonet.ErrLinkDown, neonet.ErrLinkClosed:
				// ok

			default:
				log.Error(ctx, err)
			}
			return
		}
	}
}

// serveClient1 prepares response for 1 request from client
func (stor *Storage) serveClient1(ctx context.Context, req proto.Msg) (resp proto.Msg) {
	switch req := req.(type) {
	case *proto.GetObject:
		xid := zodb.Xid{Oid: req.Oid}
		if req.At != proto.INVALID_TID {
			xid.At = req.At
		} else {
			xid.At = before2At(req.Before)
		}

		obj, err := stor.back.Load(ctx, xid)
		if err != nil {
			// translate err to NEO protocol error codes
			return proto.ZODBErrEncode(err)
		}

		// compatibility with py side:
		// for loadSerial - check we have exact hit - else "nodata"
		if req.At != proto.INVALID_TID {
		        if obj.Serial != req.At {
				return &proto.Error{
					Code:    proto.OID_NOT_FOUND,
					Message: fmt.Sprintf("%s: no data with serial %s", xid.Oid, req.At),
				}
		        }
		}

		return obj

	case *proto.LastTransaction:
		lastTid, err := stor.back.LastTid(ctx)
		if err != nil {
			return proto.ZODBErrEncode(err)
		}

		return &proto.AnswerLastTransaction{lastTid}

	//case *ObjectHistory:
	//case *StoreObject:

	default:
		return &proto.Error{proto.PROTOCOL_ERROR, fmt.Sprintf("unexpected message %T", req)}
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
