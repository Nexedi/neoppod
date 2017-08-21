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
//	"math"
	"sync"
	"time"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"

	"lab.nexedi.com/kirr/go123/xerr"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	node neo.NodeCommon

	// last allocated oid & tid
	// XXX how to start allocating oid from 0, not 1 ?
	lastOid zodb.Oid
	lastTid zodb.Tid

	// master manages node and partition tables and broadcast their updates
	// to all nodes in cluster
///*
	stateMu      sync.RWMutex	// XXX recheck: needed ?
	nodeTab      *neo.NodeTable
	partTab      *neo.PartitionTable	// XXX ^ is also in node
	clusterState neo.ClusterState
//*/
	clusterInfo neo.ClusterInfo

	// channels controlling main driver
	ctlStart    chan chan error	// request to start cluster
	ctlStop     chan chan struct{}	// request to stop  cluster
	ctlShutdown chan chan error	// request to shutdown cluster XXX with ctx ?

	// channels from workers directly serving peers to main driver
	nodeCome     chan nodeCome	// node connected	XXX -> acceptq?
	nodeLeave    chan nodeLeave	// node disconnected	XXX -> don't need

	// so tests could override
	monotime func() float64
}


// event: node connects
type nodeCome struct {
	conn   *neo.Conn
	idReq  *neo.RequestIdentification // we received this identification request
}

// event: node disconnects
type nodeLeave struct {
	node *neo.Node
}

// NewMaster creates new master node that will listen on serveAddr.
// Use Run to actually start running the node.
func NewMaster(clusterName, serveAddr string, net xnet.Networker) *Master {
	// convert serveAddr into neo format
	addr, err := neo.AddrString(net.Network(), serveAddr)
	if err != nil {
		panic(err)	// XXX
	}

	m := &Master{
		node: neo.NodeCommon{
			MyInfo:		neo.NodeInfo{Type: neo.MASTER, Addr: addr},
			ClusterName:	clusterName,
			Net:		net,
			MasterAddr:	serveAddr,	// XXX ok?
		},

		nodeTab:	&neo.NodeTable{},
		partTab:	&neo.PartitionTable{},

		ctlStart:	make(chan chan error),
		ctlStop:	make(chan chan struct{}),
		ctlShutdown:	make(chan chan error),

		nodeCome:	make(chan nodeCome),
		nodeLeave:	make(chan nodeLeave),

		monotime:	monotime,
	}

	m.clusterState = -1 // invalid
	return m
}

// Start requests cluster to eventually transition into running state
// it returns an error if such transition is not currently possible to begin (e.g. partition table is not operational)
// it returns nil if the transition began.
// NOTE upon successful return cluster is not yet in running state - the transition will
//      take time and could be also automatically aborted due to cluster environment change (e.g.
//      a storage node goes down)
func (m *Master) Start() error {
	ech := make(chan error)
	m.ctlStart <- ech
	return <-ech
}

// Stop requests cluster to eventually transition into recovery state
func (m *Master) Stop()  {
	ech := make(chan struct{})
	m.ctlStop <- ech
	<-ech
}

// Shutdown requests all known nodes in the cluster to stop
// XXX + master's run to finish ?
func (m *Master) Shutdown() error {
	panic("TODO")
}


// setClusterState sets .clusterState and notifies subscribers
func (m *Master) setClusterState(state neo.ClusterState) {
	m.clusterState.Set(state)

	// TODO notify subscribers
}


// Run starts master node and runs it until ctx is cancelled or fatal error
func (m *Master) Run(ctx context.Context) (err error) {
	// start listening
	l, err := m.node.Listen()
	if err != nil {
		return err	// XXX err ctx
	}

	defer runningf(&ctx, "master(%v)", l.Addr())(&err)

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
		IdTimestamp:	0,	// XXX ok?
	}

	// update nodeTab with self
	m.nodeTab.Update(m.node.MyInfo, nil /*XXX ok? we are not connecting to self*/)


	// accept incoming connections and pass them to main driver
	wg := sync.WaitGroup{}
	serveCtx, serveCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// XXX dup in storage
		for serveCtx.Err() == nil {
			conn, idReq, err := l.Accept()
			if err != nil {
				// TODO log / throttle
				continue
			}

			select {
			case m.nodeCome <- nodeCome{conn, idReq}:
				// ok

			case <-serveCtx.Done():
				// shutdown
				lclose(serveCtx, conn.Link())
				return
			}
		}
	}()

	// main driving logic
	err = m.runMain(ctx)

	serveCancel()
	lclose(ctx, l)
	wg.Wait()

	return err
}

// runMain is the process which implements main master cluster management logic: node tracking, cluster
// state updates, scheduling data movement between storage nodes etc
func (m *Master) runMain(ctx context.Context) (err error) {
	defer running(&ctx, "main")(&err)

	// NOTE Run's goroutine is the only mutator of nodeTab, partTab and other cluster state

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
	defer running(&ctx, "recovery")(&err)

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
	for _, stor := range m.nodeTab.StorageList() {
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
			// XXX set node.State = PENDING

			if node == nil {
				goreject(ctx, wg, n.conn, resp)
				break
			}

			// if new storage arrived - start recovery on it too
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := m.accept(ctx, n.conn, resp)
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
					// XXX dup wrt vvv (loop2)
					log.Infof(ctx, "%v: closing link", r.stor.Link)

					// close stor link / update .nodeTab
					lclose(ctx, r.stor.Link)
					m.nodeTab.SetNodeState(r.stor, neo.DOWN)
				}

			} else {
				// we are interested in latest partTab
				// NOTE during recovery no one must be subscribed to
				// partTab so it is ok to simply change whole m.partTab
				if r.partTab.PTid > m.partTab.PTid {
					m.partTab = r.partTab
				}
			}

			// update indicator whether cluster currently can be operational or not
			var ready bool
			if m.partTab.PTid == 0 {
				// new cluster - allow startup if we have some storages passed
				// recovery and there is no in-progress recovery running
				nup := 0
				for _, stor := range m.nodeTab.StorageList() {
					if stor.State > neo.DOWN {
						nup++
					}
				}
				ready = (nup > 0 && inprogress == 0)

			} else {
				ready = m.partTab.OperationalWith(m.nodeTab)	// XXX + node state
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
				// XXX -> r.stor.CloseLink(ctx) ?
				log.Infof(ctx, "%v: closing link", r.stor.Link)

				// close stor link / update .nodeTab
				lclose(ctx, r.stor.Link)
				m.nodeTab.SetNodeState(r.stor, neo.DOWN)
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
	for _, stor := range m.nodeTab.StorageList() {
		if stor.State == neo.PENDING {
			m.nodeTab.SetNodeState(stor, neo.RUNNING)
		}
	}

	// if we are starting for new cluster - create partition table
	if m.partTab.PTid == 0 {
		log.Infof(ctx, "creating new partition table")
		// XXX -> m.nodeTab.StorageList(State > DOWN)
		storv := []*neo.Node{}
		for _, stor := range m.nodeTab.StorageList() {
			if stor.State > neo.DOWN {
				storv = append(storv, stor)
			}
		}
		m.partTab = neo.MakePartTab(1 /* XXX hardcoded */, storv)
		m.partTab.PTid = 1
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
	defer runningf(&ctx, "%s: stor recovery", stor.Link.RemoteAddr())(&err)

	conn := stor.Conn
	// conn, err := stor.Link.NewConn()
	// if err != nil {
	// 	return
	// }
	// defer func() {
	// 	err2 := conn.Close()
	// 	err = xerr.First(err, err2)
	// }()

	// XXX cancel on ctx

	recovery := neo.AnswerRecovery{}
	err = conn.Ask(&neo.Recovery{}, &recovery)
	if err != nil {
		return
	}

	resp := neo.AnswerPartitionTable{}
	err = conn.Ask(&neo.AskPartitionTable{}, &resp)
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
// - once we are done without loosing too much storages in the process (so that
//   partition table is still operational) we are ready to enter servicing state.

// verify drives cluster during verification phase
//
// when verify finishes error indicates:
// - nil:  verification completed ok; cluster is ready to enter running state
// - !nil: verification failed; cluster needs to be reset to recovery state
//
// prerequisite for start: .partTab is operational wrt .nodeTab
func (m *Master) verify(ctx context.Context) (err error) {
	defer running(&ctx, "verify")(&err)

	m.setClusterState(neo.ClusterVerifying)
	ctx, vcancel := context.WithCancel(ctx)
	defer vcancel()

	verify := make(chan storVerify)
	inprogress := 0
	wg := &sync.WaitGroup{}

	// NOTE we don't reset m.lastOid / m.lastTid to 0 in the beginning of verification
	//      XXX (= py), rationale=?

	// start verification on all storages we are currently in touch with
	for _, stor := range m.nodeTab.StorageList() {
		if stor.State > neo.DOWN {	// XXX state cmp ok ? XXX or stor.Link != nil ?
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()
				storCtlVerify(ctx, stor, m.partTab, verify)
			}()
		}
	}

loop:
	for inprogress > 0 {
		select {
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX only accept storages -> known ? RUNNING : PENDING */)

			if node == nil {
				goreject(ctx, wg, n.conn, resp)
				break
			}

			// new storage arrived - start verification on it too
			inprogress++
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := m.accept(ctx, n.conn, resp)
				if err != nil {
					verify <- storVerify{stor: node, err: err}
					return
				}

				storCtlVerify(ctx, node, m.partTab, verify)
			}()

		case n := <-m.nodeLeave:
			m.nodeTab.SetNodeState(n.node, neo.DOWN)

			// if cluster became non-operational - we cancel verification
			if !m.partTab.OperationalWith(m.nodeTab) {
				// XXX ok to instantly cancel? or better
				// graceful shutdown in-flight verifications?
				vcancel()
				err = errClusterDegraded
				break loop
			}

		// a storage node came through verification - adjust our last{Oid,Tid} if ok
		// on error check - whether cluster became non-operational and stop verification if so
		//
		// FIXME actually implement logic to decide to finish/rollback transactions
		case v := <-verify:
			inprogress--

			if v.err != nil {
				log.Error(ctx, v.err)

				if !xcontext.Canceled(errors.Cause(v.err)) {
					// XXX dup wrt recovery ^^^
					log.Infof(ctx, "%s: closing link", v.stor.Link)

					// mark storage as non-working in nodeTab
					lclose(ctx, v.stor.Link)
					m.nodeTab.SetNodeState(v.stor, neo.DOWN)
				}

				// check partTab is still operational
				// if not -> cancel to go back to recovery
				if !m.partTab.OperationalWith(m.nodeTab) {
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
				log.Infof(ctx, "%v: closing link", v.stor.Link)

				// close stor link / update .nodeTab
				lclose(ctx, v.stor.Link)
				m.nodeTab.SetNodeState(v.stor, neo.DOWN)
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
	defer runningf(&ctx, "%s: stor verify", stor.Link)(&err)

	conn := stor.Conn

	// send just recovered parttab so storage saves it
	err = conn.Send(&neo.NotifyPartitionTable{
		PTid:    pt.PTid,
		RowList: pt.Dump(),
	})
	if err != nil {
		return
	}

	locked := neo.AnswerLockedTransactions{}
	err = conn.Ask(&neo.LockedTransactions{}, &locked)
	if err != nil {
		return
	}

	if len(locked.TidDict) > 0 {
		// TODO vvv
		err = fmt.Errorf("TODO: non-ø locked txns: %v", locked.TidDict)
		return
	}

	last := neo.AnswerLastIDs{}
	err = conn.Ask(&neo.LastIDs{}, &last)
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
	defer running(&ctx, "service")(&err)

	m.setClusterState(neo.ClusterRunning)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serviced := make(chan serviceDone)
	wg := &sync.WaitGroup{}

	// spawn per-storage service driver
	for _, stor := range m.nodeTab.StorageList() {
		if stor.State == neo.RUNNING {	// XXX note PENDING - not adding to service; ok?
			wg.Add(1)
			go func() {
				defer wg.Done()
				storCtlService(ctx, stor, serviced)
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
				goreject(ctx, wg, n.conn, resp)
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				err = m.accept(ctx, n.conn, resp)
				if err != nil {
					serviced <- serviceDone{node: node, err: err}
					return
				}

				switch node.Type {
				case neo.STORAGE:
					storCtlService(ctx, node, serviced)

				//case neo.CLIENT:
				//	serveClient(ctx, node, serviced)

				// XXX ADMIN
				}
			}()

		case d := <-serviced:
			// TODO if S goes away -> check partTab still operational -> if not - recovery

		// XXX who sends here?
		case n := <-m.nodeLeave:
			m.nodeTab.SetNodeState(n.node, neo.DOWN)

			// if cluster became non-operational - cancel service
			if !m.partTab.OperationalWith(m.nodeTab) {
				err = errClusterDegraded
				break loop
			}


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

	return err
}

// storCtlService drives a storage node during cluster service state
func storCtlService(ctx context.Context, stor *neo.Node, done chan serviceDone) {
	err := storCtlService1(ctx, stor)
	done <- serviceDone{node: stor, err: err}
}

func storCtlService1(ctx context.Context, stor *neo.Node) (err error) {
	defer runningf(&ctx, "%s: stor service", stor.Link.RemoteAddr())(&err)

	conn := stor.Conn

	// XXX send nodeTab ?
	// XXX send clusterInformation ?

	ready := neo.NotifyReady{}
	err = conn.Ask(&neo.StartOperation{Backup: false}, &ready)
	if err != nil {
		return err
	}

	// now wait in a loop
	// XXX this should be also controlling transactions
	for {
		select {
		// XXX stub
		case <-time.After(1*time.Second):
			println(".")

		case <-ctx.Done():
			// XXX also send StopOperation?
			// XXX close link?
			return ctx.Err()	// XXX ok?
		}
	}
}

// ----------------------------------------

// identify processes identification request of just connected node and either accepts or declines it.
// If node identification is accepted .nodeTab is updated and corresponding node entry is returned.
// Response message is constructed but not send back not to block the caller - it is
// the caller responsibility to send the response to node which requested identification.
func (m *Master) identify(ctx context.Context, n nodeCome) (node *neo.Node, resp neo.Msg) {
	// XXX also verify ? :
	// - NodeType valid
	// - IdTimestamp ?

	uuid := n.idReq.NodeUUID
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

		node = m.nodeTab.Get(uuid)
		if node != nil {
			// reject - uuid is already occupied by someone else
			// XXX check also for down state - it could be the same node reconnecting
			return &neo.Error{neo.PROTOCOL_ERROR, fmt.Sprintf("uuid %v already used by another node", uuid)}
		}

		// XXX accept only certain kind of nodes depending on .clusterState, e.g.
		switch nodeType {
		case neo.CLIENT:
			return &neo.Error{neo.NOT_READY, "cluster not operational"}

		// XXX ...
		}

		return nil
	}()

	subj := fmt.Sprintf("identify: %s (%s)", n.conn.Link().RemoteAddr(), n.idReq.NodeUUID)
	if err != nil {
		log.Infof(ctx, "%s: rejecting: %s", subj, err)
		return nil, err
	}

	log.Infof(ctx, "%s: accepting as %s", subj, uuid)

	accept := &neo.AcceptIdentification{
			NodeType:	neo.MASTER,
			MyNodeUUID:	m.node.MyInfo.UUID,
			NumPartitions:	1,	// FIXME hardcoded
			NumReplicas:	1,	// FIXME hardcoded
			YourNodeUUID:	uuid,
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
		IdTimestamp:	m.monotime(),
	}

	node = m.nodeTab.Update(nodeInfo, n.conn) // NOTE this notifies all nodeTab subscribers
	return node, accept
}

// reject sends rejective identification response and closes associated link
func reject(ctx context.Context, conn *neo.Conn, resp neo.Msg) {
	// XXX cancel on ctx?
	// XXX log?
	err1 := conn.Send(resp)
	err2 := conn.Close()
	err3 := conn.Link().Close()
	err := xerr.Merge(err1, err2, err3)
	if err != nil {
		log.Error(ctx, "reject:", err)
	}
}

// goreject spawns reject in separate goroutine properly added/done on wg
func goreject(ctx context.Context, wg *sync.WaitGroup, conn *neo.Conn, resp neo.Msg) {
	wg.Add(1)
	defer wg.Done()
	go reject(ctx, conn, resp)
}

// accept sends acceptive identification response and closes conn
// XXX if problem -> .nodeLeave
// XXX spawn ping goroutine from here?
func (m *Master) accept(ctx context.Context, conn *neo.Conn, resp neo.Msg) error {
	// XXX cancel on ctx
	err1 := conn.Send(resp)
	return err1	// XXX while trying to work on single conn
	//err2 := conn.Close()
	//return xerr.First(err1, err2)
}

// allocUUID allocates new node uuid for a node of kind nodeType
// XXX it is bad idea for master to assign uuid to coming node
// -> better nodes generate really unique UUID themselves and always show with them
func (m *Master) allocUUID(nodeType neo.NodeType) neo.NodeUUID {
	for num := int32(1); num < 1<<24; num++ {
		uuid := neo.UUID(nodeType, num)
		if m.nodeTab.Get(uuid) == nil {
			return uuid
		}
	}

	panic("all uuid allocated ???")	// XXX more robust ?
}
