// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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
// master node

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/go123/xnet"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"
	"lab.nexedi.com/kirr/neo/go/xcommon/xio"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	node *neo.NodeApp

	// master manages node and partition tables and broadcast their updates
	// to all nodes in cluster

	// last allocated oid & tid
	// XXX how to start allocating oid from 0, not 1 ?
	// TODO mu
	lastOid zodb.Oid
	lastTid zodb.Tid

	// channels controlling main driver
	ctlStart    chan chan error	// request to start cluster
	ctlStop     chan chan struct{}	// request to stop  cluster
	ctlShutdown chan chan error	// request to shutdown cluster XXX with ctx ?

	// channels from workers directly serving peers to main driver
	nodeCome     chan nodeCome	// node connected	XXX -> acceptq?
//	nodeLeave    chan nodeLeave	// node disconnected	XXX -> don't need

	// so tests could override
	monotime func() float64
}


// NewMaster creates new master node that will listen on serveAddr.
//
// Use Run to actually start running the node.
func NewMaster(clusterName, serveAddr string, net xnet.Networker) *Master {
	m := &Master{
		node: neo.NewNodeApp(net, neo.MASTER, clusterName, serveAddr, serveAddr),

		ctlStart:	make(chan chan error),
		ctlStop:	make(chan chan struct{}),
		ctlShutdown:	make(chan chan error),

		nodeCome:	make(chan nodeCome),
//		nodeLeave:	make(chan nodeLeave),

		monotime:	monotime,
	}

	return m
}

// Start requests cluster to eventually transition into running state.
//
// It returns an error if such transition is not currently possible to begin
// (e.g. partition table is not operational).
// It returns nil if the transition began.
//
// NOTE upon successful return cluster is not yet in running state - the transition will
// take time and could be also automatically aborted due to cluster environment change (e.g.
// a storage node goes down).
func (m *Master) Start() error {
	ech := make(chan error)
	m.ctlStart <- ech
	return <-ech
}

// Stop requests cluster to eventually transition into recovery state.
func (m *Master) Stop()  {
	ech := make(chan struct{})
	m.ctlStop <- ech
	<-ech
}

// Shutdown requests all known nodes in the cluster to stop.
// XXX + master's run to finish ?
func (m *Master) Shutdown() error {
	panic("TODO")
}


// setClusterState sets .clusterState and notifies subscribers.
func (m *Master) setClusterState(state neo.ClusterState) {
	m.node.ClusterState.Set(state)

	// TODO notify subscribers
}


// Run starts master node and runs it until ctx is cancelled or fatal error.
func (m *Master) Run(ctx context.Context) (err error) {
	// start listening
	l, err := m.node.Listen()
	if err != nil {
		return err	// XXX err ctx
	}

	defer task.Runningf(&ctx, "master(%v)", l.Addr())(&err)

	m.node.MasterAddr = l.Addr().String()
	naddr, err := neo.Addr(l.Addr())
	if err != nil {
		// must be ok since l.Addr() is valid since it is listening
		// XXX panic -> errors.Wrap?
		panic(err)
	}

	m.node.MyInfo = neo.NodeInfo{
		Type:	neo.MASTER,
		Addr:	naddr,
		UUID:	m.allocUUID(neo.MASTER),
		State:	neo.RUNNING,
		IdTime:	neo.IdTimeNone,	// XXX ok?
	}

	// update nodeTab with self
	m.node.NodeTab.Update(m.node.MyInfo)


	// accept incoming connections and pass them to main driver
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func(ctx context.Context) (err error) {
		defer wg.Done()
		defer task.Running(&ctx, "accept")(&err)

		// XXX dup in storage
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			req, idReq, err := l.Accept(ctx)
			if err != nil {
				if !xcontext.Canceled(err) {
					log.Error(ctx, err)	// XXX throttle?
				}
				continue
			}

			// for storages the only incoming connection is for RequestIdentification
			// and then master only drives it. So close accept as noone will be
			// listening for it on our side anymore.
			switch idReq.NodeType {
			case neo.CLIENT:
				// ok

			case neo.STORAGE:
				fallthrough
			default:
				req.Link().CloseAccept()
			}

			// handover to main driver
			select {
			case m.nodeCome <- nodeCome{req, idReq}:
				// ok

			case <-ctx.Done():
				// shutdown
				lclose(ctx, req.Link())
				continue
			}
		}
	}(serveCtx)

	// main driving logic
	err = m.runMain(ctx)

	serveCancel()
	lclose(ctx, l)
	wg.Wait()

	return err
}

// runMain is the process which implements main master cluster management logic: node tracking, cluster
// state updates, scheduling data movement between storage nodes etc.
func (m *Master) runMain(ctx context.Context) (err error) {
	defer task.Running(&ctx, "main")(&err)

	// NOTE Run's goroutine is the only mutator of nodeTab, partTab and other cluster state

	// XXX however since clients request state reading we should use node.StateMu?
	// XXX -> better rework protocol so that master pushes itself (not
	//     being pulled) to clients everything they need.

	for ctx.Err() == nil {
		// recover partition table from storages and wait till enough
		// storages connects us so that we can see the partition table
		// can be operational.
		//
		// Successful recovery means all ^^^ preconditions are met and
		// a command came to us to start the cluster.
		err := m.recovery(ctx)
		if err != nil {
			//log.Error(ctx, err)
			return err // recovery cancelled
		}

		// make sure transactions on storages are properly finished, in
		// case previously it was unclean shutdown.
		err = m.verify(ctx)
		if err != nil {
			//log.Error(ctx, err)
			continue // -> recovery
		}

		// provide service as long as partition table stays operational
		err = m.service(ctx)
		if err != nil {
			//log.Error(ctx, err)
			continue // -> recovery
		}

		// XXX shutdown request ?
	}

	return ctx.Err()
}


// Cluster Recovery
// ----------------
//
// - starts from potentially no storage nodes known
// - accept connections from storage nodes
// - retrieve and recover latest previously saved partition table from storages
// - monitor whether partition table becomes operational wrt currently up nodeset
// - if yes - finish recovering upon receiving "start" command		XXX or autostart
// - start is also allowed if storages connected and say there is no partition
//   table saved to them (empty new cluster case).

// storRecovery is result of 1 storage node passing recovery phase
type storRecovery struct {
	stor    *neo.Node
	partTab *neo.PartitionTable
	err     error

	// XXX + backup_tid, truncate_tid ?
}

// recovery drives cluster during recovery phase
//
// when recovery finishes error indicates:
// - nil:  recovery was ok and a command came for cluster to start
// - !nil: recovery was cancelled
func (m *Master) recovery(ctx context.Context) (err error) {
	defer task.Running(&ctx, "recovery")(&err)

	m.setClusterState(neo.ClusterRecovering)
	ctx, rcancel := context.WithCancel(ctx)
	defer rcancel()

//trace:event traceMasterStartReady(m *Master, ready bool)
	readyToStart := false

	recovery := make(chan storRecovery)
	inprogress := 0 // in-progress stor recoveries
	wg := &sync.WaitGroup{}

	// start recovery on all storages we are currently in touch with
	// XXX close links to clients
	for _, stor := range m.node.NodeTab.StorageList() {
		if stor.State > neo.DOWN {	// XXX state cmp ok ? XXX or stor.Link != nil ?
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()
				storCtlRecovery(ctx, stor, recovery)
			}()
		}
	}

loop:
	for {
		select {
		// new connection comes in
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX only accept storages -> PENDING */)

			if node == nil {
				goreject(ctx, wg, n.req, resp)
				break
			}

			// if new storage arrived - start recovery on it too
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := accept(ctx, n.req, resp)
				if err != nil {
					recovery <- storRecovery{stor: node, err: err}
					return
				}

				// start recovery
				storCtlRecovery(ctx, node, recovery)
			}()

		// a storage node came through recovery - let's see whether
		// ptid ↑ and if so we should take partition table from there
		//
		// FIXME after a storage recovers, it could go away while
		//	 recovery for others is in progress -> monitor this.
		case r := <-recovery:
			inprogress--

			if r.err != nil {
				log.Error(ctx, r.err)

				if !xcontext.Canceled(errors.Cause(r.err)) {
					r.stor.CloseLink(ctx)
					r.stor.SetState(neo.DOWN)
				}

			} else {
				// we are interested in latest partTab
				// NOTE during recovery no one must be subscribed to
				// partTab so it is ok to simply change whole m.partTab
				if r.partTab.PTid > m.node.PartTab.PTid {
					m.node.PartTab = r.partTab
				}
			}

			// update indicator whether cluster currently can be operational or not
			var ready bool
			if m.node.PartTab.PTid == 0 {
				// new cluster - allow startup if we have some storages passed
				// recovery and there is no in-progress recovery running
				nup := 0
				for _, stor := range m.node.NodeTab.StorageList() {
					if stor.State > neo.DOWN {
						nup++
					}
				}
				ready = (nup > 0 && inprogress == 0)

			} else {
				ready = m.node.PartTab.OperationalWith(m.node.NodeTab)	// XXX + node state
			}

			if readyToStart != ready {
				readyToStart = ready
				traceMasterStartReady(m, ready)
			}


		// request to start the cluster - if ok we exit replying ok
		// if not ok - we just reply not ok
		case ech := <-m.ctlStart:
			if readyToStart {
				log.Infof(ctx, "start command - we are ready")
				// reply "ok to start" after whole recovery finishes

				// XXX ok? we want to retrieve all recovery information first?
				// XXX or initially S is in PENDING state and
				// transitions to RUNNING only after successful recovery?

				rcancel()
				defer func() {
					// XXX can situation change while we are shutting down?
					// XXX -> recheck logic with checking PT operational ^^^
					// XXX    (depending on storages state)
					ech <- nil
				}()
				break loop
			}

			log.Infof(ctx, "start command - err - we are not ready")
			ech <- fmt.Errorf("start: cluster is non-operational")

		case ech := <-m.ctlStop:
			close(ech) // ok; we are already recovering

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	// wait all workers to finish (which should come without delay since it was cancelled)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

loop2:
	for {
		select {
		case r := <-recovery:
			// XXX dup wrt <-recovery handler above
			log.Error(ctx, r.err)

			if !xcontext.Canceled(errors.Cause(r.err)) {
				r.stor.CloseLink(ctx)
				r.stor.SetState(neo.DOWN)
			}

		case <-done:
			break loop2
		}
	}

	if err != nil {
		return err
	}

	// recovery successful - we are starting

	// S PENDING -> RUNNING
	// XXX recheck logic is ok for when starting existing cluster
	for _, stor := range m.node.NodeTab.StorageList() {
		if stor.State == neo.PENDING {
			stor.SetState(neo.RUNNING)
		}
	}

	// if we are starting for new cluster - create partition table
	if m.node.PartTab.PTid == 0 {
		// XXX -> m.nodeTab.StorageList(State > DOWN)
		storv := []*neo.Node{}
		for _, stor := range m.node.NodeTab.StorageList() {
			if stor.State > neo.DOWN {
				storv = append(storv, stor)
			}
		}
		m.node.PartTab = neo.MakePartTab(1 /* XXX hardcoded */, storv)
		m.node.PartTab.PTid = 1
		log.Infof(ctx, "creating new partition table: %s", m.node.PartTab)
	}

	return nil
}

// storCtlRecovery drives a storage node during cluster recovering state
// it retrieves various ids and partition table from as stored on the storage
func storCtlRecovery(ctx context.Context, stor *neo.Node, res chan storRecovery) {
	var err error
	defer func() {
		if err == nil {
			return
		}

		// on error provide feedback to storRecovery chan
		res <- storRecovery{stor: stor, err: err}
	}()
	slink := stor.Link()
	defer task.Runningf(&ctx, "%s: stor recovery", slink.RemoteAddr())(&err)

	// XXX cancel on ctx

	recovery := neo.AnswerRecovery{}
	err = slink.Ask1(&neo.Recovery{}, &recovery)
	if err != nil {
		return
	}

	resp := neo.AnswerPartitionTable{}
	err = slink.Ask1(&neo.AskPartitionTable{}, &resp)
	if err != nil {
		return
	}

	// reconstruct partition table from response
	pt := neo.PartTabFromDump(resp.PTid, resp.RowList)
	res <- storRecovery{stor: stor, partTab: pt}
}


var errStopRequested   = stderrors.New("stop requested")
var errClusterDegraded = stderrors.New("cluster became non-operatonal")


// Cluster Verification (data recovery)
// ------------------------------------
//
// - starts with operational partition table
// - saves recovered partition table to all storages
// - asks all storages for partly finished transactions and decides cluster-wide
//   which such transactions need to be either finished or rolled back.
// - executes decided finishes and rollbacks on the storages.
// - retrieve last ids from storages along the way.
// - once we are done without losing too much storages in the process (so that
//   partition table is still operational) we are ready to enter servicing state.

// verify drives cluster during verification phase
//
// when verify finishes error indicates:
// - nil:  verification completed ok; cluster is ready to enter running state
// - !nil: verification failed; cluster needs to be reset to recovery state
//
// prerequisite for start: .partTab is operational wrt .nodeTab
func (m *Master) verify(ctx context.Context) (err error) {
	defer task.Running(&ctx, "verify")(&err)

	m.setClusterState(neo.ClusterVerifying)
	ctx, vcancel := context.WithCancel(ctx)
	defer vcancel()

	verify := make(chan storVerify)
	inprogress := 0
	wg := &sync.WaitGroup{}

	// NOTE we don't reset m.lastOid / m.lastTid to 0 in the beginning of verification
	//      XXX (= py), rationale=?

	// start verification on all storages we are currently in touch with
	for _, stor := range m.node.NodeTab.StorageList() {
		if stor.State > neo.DOWN {	// XXX state cmp ok ? XXX or stor.Link != nil ?
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()
				storCtlVerify(ctx, stor, m.node.PartTab, verify)
			}()
		}
	}

loop:
	for inprogress > 0 {
		select {
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX only accept storages -> known ? RUNNING : PENDING */)

			if node == nil {
				goreject(ctx, wg, n.req, resp)
				break
			}

			// new storage arrived - start verification on it too
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := accept(ctx, n.req, resp)
				if err != nil {
					verify <- storVerify{stor: node, err: err}
					return
				}

				storCtlVerify(ctx, node, m.node.PartTab, verify)
			}()

		/*
		case n := <-m.nodeLeave:
			n.node.SetState(neo.DOWN)

			// if cluster became non-operational - we cancel verification
			if !m.node.PartTab.OperationalWith(m.node.NodeTab) {
				// XXX ok to instantly cancel? or better
				// graceful shutdown in-flight verifications?
				vcancel()
				err = errClusterDegraded
				break loop
			}
		*/

		// a storage node came through verification - adjust our last{Oid,Tid} if ok
		// on error check - whether cluster became non-operational and stop verification if so
		//
		// FIXME actually implement logic to decide to finish/rollback transactions
		case v := <-verify:
			inprogress--

			if v.err != nil {
				log.Error(ctx, v.err)

				if !xcontext.Canceled(errors.Cause(v.err)) {
					v.stor.CloseLink(ctx)
					v.stor.SetState(neo.DOWN)
				}

				// check partTab is still operational
				// if not -> cancel to go back to recovery
				if !m.node.PartTab.OperationalWith(m.node.NodeTab) {
					vcancel()
					err = errClusterDegraded
					break loop
				}
			} else {
				if v.lastOid > m.lastOid {
					m.lastOid = v.lastOid
				}
				if v.lastTid > m.lastTid {
					m.lastTid = v.lastTid
				}
			}

		case ech := <-m.ctlStart:
			ech <- nil // we are already starting

		case ech := <-m.ctlStop:
			close(ech) // ok
			err = errStopRequested
			break loop

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	// wait all workers to finish (which should come without delay since it was cancelled)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

loop2:
	for {
		select {
		case v := <-verify:
			// XXX dup wrt <-verify handler above
			log.Error(ctx, v.err)

			if !xcontext.Canceled(errors.Cause(v.err)) {
				v.stor.CloseLink(ctx)
				v.stor.SetState(neo.DOWN)
			}

		case <-done:
			break loop2
		}
	}

	return err
}

// storVerify is result of a storage node passing verification phase
type storVerify struct {
	stor    *neo.Node
	lastOid zodb.Oid
	lastTid zodb.Tid
	err	error
}

// storCtlVerify drives a storage node during cluster verifying (= starting) state
func storCtlVerify(ctx context.Context, stor *neo.Node, pt *neo.PartitionTable, res chan storVerify) {
	// XXX link.Close on err
	// XXX cancel on ctx

	var err error
	defer func() {
		if err != nil {
			res <- storVerify{stor: stor, err: err}
		}
	}()
	slink := stor.Link()
	defer task.Runningf(&ctx, "%s: stor verify", slink)(&err)

	// send just recovered parttab so storage saves it
	err = slink.Send1(&neo.SendPartitionTable{
		PTid:    pt.PTid,
		RowList: pt.Dump(),
	})
	if err != nil {
		return
	}

	locked := neo.AnswerLockedTransactions{}
	err = slink.Ask1(&neo.LockedTransactions{}, &locked)
	if err != nil {
		return
	}

	if len(locked.TidDict) > 0 {
		// TODO vvv
		err = fmt.Errorf("TODO: non-ø locked txns: %v", locked.TidDict)
		return
	}

	last := neo.AnswerLastIDs{}
	err = slink.Ask1(&neo.LastIDs{}, &last)
	if err != nil {
		return
	}

	// send results to driver
	res <- storVerify{stor: stor, lastOid: last.LastOid, lastTid: last.LastTid}
}


// Cluster Running (operational service)
// -------------------------------------
//
// - starts with operational parttab and all present storage nodes consistently
//   either finished or rolled-back partly finished transactions.
// - monitor storages come & go and if parttab becomes non-operational leave to recovery.
// - provide service to clients while we are here.
//
// TODO also plan data movement on new storage nodes appearing

// serviceDone is the error returned after service-phase node handling is finished
type serviceDone struct {
	node *neo.Node
	err  error
}

// service drives cluster during running state
//
// TODO document error meanings on return
//
// prerequisite for start: .partTab is operational wrt .nodeTab and verification passed
func (m *Master) service(ctx context.Context) (err error) {
	defer task.Running(&ctx, "service")(&err)

	m.setClusterState(neo.ClusterRunning)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serviced := make(chan serviceDone)
	wg := &sync.WaitGroup{}

	// spawn per-storage service driver
	for _, stor := range m.node.NodeTab.StorageList() {
		if stor.State == neo.RUNNING {	// XXX note PENDING - not adding to service; ok?
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := storCtlService(ctx, stor)
				serviced <- serviceDone{node: stor, err: err}
			}()
		}
	}

loop:
	for {
		select {
		// new connection comes in
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX accept everyone */)

			if node == nil {
				goreject(ctx, wg, n.req, resp)
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				err = accept(ctx, n.req, resp)
				if err != nil {
					serviced <- serviceDone{node: node, err: err}
					return
				}

				switch node.Type {
				case neo.STORAGE:
					err = storCtlService(ctx, node)

				case neo.CLIENT:
					err = m.serveClient(ctx, node)

				// XXX ADMIN
				}

				serviced <- serviceDone{node: node, err: err}
			}()

		case d := <-serviced:
			// TODO if S goes away -> check partTab still operational -> if not - recovery
			_ = d

		/*
		// XXX who sends here?
		case n := <-m.nodeLeave:
			n.node.SetState(neo.DOWN)

			// if cluster became non-operational - cancel service
			if !m.node.PartTab.OperationalWith(m.node.NodeTab) {
				err = errClusterDegraded
				break loop
			}
		*/


		// XXX what else ?	(-> txn control at least)

		case ech := <-m.ctlStart:
			ech <- nil // we are already started

		case ech := <-m.ctlStop:
			close(ech) // ok
			err = fmt.Errorf("stop requested")
			// XXX tell storages to stop
			break loop

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}


	// XXX wait all spawned service workers

	return err
}

// storCtlService drives a storage node during cluster service state
func storCtlService(ctx context.Context, stor *neo.Node) (err error) {
	slink := stor.Link()
	defer task.Runningf(&ctx, "%s: stor service", slink.RemoteAddr())(&err)

	// XXX send nodeTab ?
	// XXX send clusterInformation ?

	// XXX current neo/py does StartOperation / NotifyReady as separate
	//     sends, not exchange on the same conn. - fixed
	ready := neo.NotifyReady{}
	err = slink.Ask1(&neo.StartOperation{Backup: false}, &ready)
	//err = slink.Send1(&neo.StartOperation{Backup: false})
	//if err != nil {
	//	return err
	//}
	//req, err := slink.Recv1()
	//if err != nil {
	//	return err
	//}
	//req.Close()	XXX must be after req handling
	//switch msg := req.Msg.(type) {
	//case *neo.NotifyReady:
	//	// ok
	//case *neo.Error:
	//	return msg
	//default:
	//	return fmt.Errorf("unexpected message %T", msg)
	//}


	// now wait in a loop
	// XXX this should be also controlling transactions
	for {
		select {
		// XXX stub
		case <-time.After(1*time.Second):
			//println(".")

		case <-ctx.Done():
			// XXX also send StopOperation?
			// XXX close link?
			return ctx.Err()	// XXX ok?
		}
	}
}

// serveClient serves incoming client link
func (m *Master) serveClient(ctx context.Context, cli *neo.Node) (err error) {
	clink := cli.Link()
	defer task.Runningf(&ctx, "%s: client service", clink.RemoteAddr())(&err)

	wg, ctx := errgroup.WithContext(ctx)
	defer xio.CloseWhenDone(ctx, clink)()	// XXX -> cli.CloseLink?

	// M -> C notifications about cluster state
	wg.Go(func() error {
		return m.keepPeerUpdated(ctx, clink)
	})

	// M <- C requests handler
	wg.Go(func() error {
		for {
			req, err := clink.Recv1()
			if err != nil {
				return err
			}

			resp := m.serveClient1(ctx, req.Msg)
			err = req.Reply(resp)
			req.Close()
			if err != nil {
				return err
			}
		}
	})

	return wg.Wait()
}

// serveClient1 prepares response for 1 request from client
func (m *Master) serveClient1(ctx context.Context, req neo.Msg) (resp neo.Msg) {
	switch req := req.(type) {
	case *neo.AskPartitionTable:
		m.node.StateMu.RLock()
		rpt := &neo.AnswerPartitionTable{
			PTid:	 m.node.PartTab.PTid,
			RowList: m.node.PartTab.Dump(),
		}
		m.node.StateMu.RUnlock()
		return rpt

	case *neo.LastTransaction:
		// XXX lock
		return &neo.AnswerLastTransaction{m.lastTid}

	default:
		return &neo.Error{neo.PROTOCOL_ERROR, fmt.Sprintf("unexpected message %T", req)}
	}
}

// ----------------------------------------

// keepPeerUpdated sends cluster state updates to peer on the link
func (m *Master) keepPeerUpdated(ctx context.Context, link *neo.NodeLink) (err error) {
	// link should be already in parent ctx (XXX and closed on cancel ?)
	defer task.Runningf(&ctx, "keep updated")(&err)

	// first lock cluster state to get its first consistent snapshot and
	// atomically subscribe to updates
	m.node.StateMu.RLock()

	//clusterState := m.node.ClusterState
	// XXX ^^^ + subscribe

	nodev := m.node.NodeTab.All()
	nodeiv := make([]neo.NodeInfo, len(nodev))
	for i, node := range nodev {
		// NOTE .NodeInfo is data not pointers - so won't change after we copy it to nodeiv
		nodeiv[i] = node.NodeInfo
	}

	// XXX RLock is not enough for subscribe - right?
	nodech, nodeUnsubscribe := m.node.NodeTab.SubscribeBuffered()

	m.node.StateMu.RUnlock()

	// don't forget to unsubscribe when we are done
	defer func() {
		m.node.StateMu.RLock()	// XXX rlock not enough for unsubscribe
		// XXX ClusterState unsubscribe
		nodeUnsubscribe()
		m.node.StateMu.RUnlock()
	}()

	// ok now we have state snapshot and subscription channels.
	// first send the snapshot.

	// XXX +ClusterState
	err = link.Send1(&neo.NotifyNodeInformation{
		IdTime:   neo.IdTimeNone,	// XXX what here?
		NodeList: nodeiv,
	})
	if err != nil {
		return err
	}

	// now proxy the updates until we are done
	for {
		var msg neo.Msg

		select {
		case <-ctx.Done():
			return ctx.Err()

		// XXX ClusterState

		case nodeiv = <-nodech:
			msg = &neo.NotifyNodeInformation{
				IdTime:   neo.IdTimeNone, // XXX what here?
				NodeList: nodeiv,
			}
		}

		// XXX vvv don't allow it to send very slowly and thus our
		// buffered subscription channel to grow up indefinitely.
		// XXX -> if it is too slow - just close the link.
		err = link.Send1(msg)
		if err != nil {
			return err
		}
	}
}

// ----------------------------------------

// identify processes identification request of just connected node and either accepts or declines it.
//
// If node identification is accepted .nodeTab is updated and corresponding node entry is returned.
// Response message is constructed but not send back not to block the caller - it is
// the caller responsibility to send the response to node which requested identification.
func (m *Master) identify(ctx context.Context, n nodeCome) (node *neo.Node, resp neo.Msg) {
	// XXX also verify ? :
	// - NodeType valid
	// - IdTime ?

	uuid := n.idReq.UUID
	nodeType := n.idReq.NodeType

	err := func() *neo.Error {
		if n.idReq.ClusterName != m.node.ClusterName {
			return &neo.Error{neo.PROTOCOL_ERROR, "cluster name mismatch"}
		}

		if uuid == 0 {
			uuid = m.allocUUID(nodeType)
		}
		// XXX uuid < 0 (temporary) -> reallocate if conflict ?

		// XXX check uuid matches NodeType

		node = m.node.NodeTab.Get(uuid)
		if node != nil {
			// reject - uuid is already occupied by someone else
			// XXX check also for down state - it could be the same node reconnecting
			return &neo.Error{neo.PROTOCOL_ERROR, fmt.Sprintf("uuid %v already used by another node", uuid)}
		}

		// accept only certain kind of nodes depending on .clusterState, e.g.
		// XXX ok to have this logic inside identify? (better provide from outside ?)
		switch nodeType {
		case neo.CLIENT:
			if m.node.ClusterState != neo.ClusterRunning {
				return &neo.Error{neo.NOT_READY, "cluster not operational"}
			}

		case neo.STORAGE:
			// ok

		// TODO +master, admin
		default:
			return &neo.Error{neo.PROTOCOL_ERROR, fmt.Sprintf("not accepting node type %v", nodeType)}
		}

		return nil
	}()

	subj := fmt.Sprintf("identify: %s (%s)", n.req.Link().RemoteAddr(), n.idReq.UUID)
	if err != nil {
		log.Infof(ctx, "%s: rejecting: %s", subj, err)
		return nil, err
	}

	log.Infof(ctx, "%s: accepting as %s", subj, uuid)

	accept := &neo.AcceptIdentification{
			NodeType:	neo.MASTER,
			MyUUID:		m.node.MyInfo.UUID,
			NumPartitions:	1,	// FIXME hardcoded
			NumReplicas:	1,	// FIXME hardcoded
			YourUUID:	uuid,
		}

	// update nodeTab
	var nodeState neo.NodeState
	switch nodeType {
	case neo.STORAGE:
		// FIXME py sets to RUNNING/PENDING depending on cluster state
		nodeState = neo.PENDING

	default:
		nodeState = neo.RUNNING
	}

	nodeInfo := neo.NodeInfo{
		Type:	nodeType,
		Addr:	n.idReq.Address,
		UUID:	uuid,
		State:	nodeState,
		IdTime:	neo.IdTime(m.monotime()),
	}

	node = m.node.NodeTab.Update(nodeInfo) // NOTE this notifies all nodeTab subscribers
	node.SetLink(n.req.Link())
	return node, accept
}

// allocUUID allocates new node uuid for a node of kind nodeType
// XXX it is bad idea for master to assign uuid to coming node
// -> better nodes generate really unique UUID themselves and always show with them
func (m *Master) allocUUID(nodeType neo.NodeType) neo.NodeUUID {
	for num := int32(1); num < 1<<24; num++ {
		uuid := neo.UUID(nodeType, num)
		if m.node.NodeTab.Get(uuid) == nil {
			return uuid
		}
	}

	panic("all uuid allocated ???")	// XXX more robust ?
}
