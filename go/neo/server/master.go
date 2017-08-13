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
	nodeTab      neo.NodeTable
	partTab      neo.PartitionTable
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
			MyInfo:		neo.NodeInfo{NodeType: neo.MASTER, Address: addr},
			ClusterName:	clusterName,
			Net:		net,
			MasterAddr:	serveAddr,	// XXX ok?
		},

		ctlStart:	make(chan chan error),
		ctlStop:	make(chan chan struct{}),
		ctlShutdown:	make(chan chan error),

		nodeCome:	make(chan nodeCome),
		nodeLeave:	make(chan nodeLeave),
	}

	m.node.MyInfo.NodeUUID = m.allocUUID(neo.MASTER)
	// TODO update nodeTab with self
	m.clusterState = neo.ClusterRecovering	// XXX no elections - we are the only master

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
	m.clusterState = state

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

// storRecovery is result of 1 storage node passing recovery phase
type storRecovery struct {
	stor    *neo.Node
	partTab neo.PartitionTable
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

	readyToStart := false
	recovery := make(chan storRecovery)
	wg := sync.WaitGroup{}

	// start recovery on all storages we are currently in touch with
	// XXX close links to clients
	for _, stor := range m.nodeTab.StorageList() {
		if stor.NodeState > neo.DOWN {	// XXX state cmp ok ? XXX or stor.Link != nil ?
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

			// if new storage arrived - start recovery on it too
			wg.Add(1)
			go func() {
				defer wg.Done()

				if node == nil {
					m.reject(ctx, n.conn, resp)
					return
				}

				err := m.accept(ctx, n.conn, resp)
				if err != nil {
					// XXX move this m.nodeLeave <- to accept() ?
					recovery <- storRecovery{stor: node, err: err}
					return
				}

				// start recovery
				storCtlRecovery(ctx, node, recovery)
			}()

		// a storage node came through recovery - let's see whether
		// ptid ↑ and if so we should take partition table from there
		case r := <-recovery:
			if r.err != nil {
				log.Error(ctx, r.err)

				if !xcontext.Canceled(errors.Cause(r.err)) {
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
			readyToStart = m.partTab.OperationalWith(&m.nodeTab)	// XXX + node state

			// XXX handle case of new cluster - when no storage reports valid parttab
			// XXX -> create new parttab


		// request to start the cluster - if ok we exit replying ok
		// if not ok - we just reply not ok
		case ech := <-m.ctlStart:
			if readyToStart {
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

	for {
		select {
		case r := <-recovery:
			// XXX dup wrt <-recovery handler above
			log.Error(ctx, r.err)

			if !xcontext.Canceled(errors.Cause(r.err)) {
				// XXX not so ok

				// log / close node link; update NT
			}

		case <-done:
			break
		}
	}

	return err
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

	conn, err := stor.Link.NewConn()
	if err != nil {
		return
	}
	defer func() {
		err2 := conn.Close()
		err = xerr.First(err, err2)
	}()

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
	pt := neo.PartitionTable{}
	pt.PTid = resp.PTid
	for _, row := range resp.RowList {
		i := row.Offset
		for i >= uint32(len(pt.PtTab)) {
			pt.PtTab = append(pt.PtTab, []neo.PartitionCell{})
		}

		//pt.PtTab[i] = append(pt.PtTab[i], row.CellList...)
		for _, cell := range row.CellList {
			pt.PtTab[i] = append(pt.PtTab[i], neo.PartitionCell{
					NodeUUID:  cell.NodeUUID,
					CellState: cell.CellState,
				})
		}
	}

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

// verify drives cluster via verification phase
//
// when verify finishes error indicates:
// - nil:  verification completed ok; cluster is ready to enter running state
// - !nil: verification failed; cluster needs to be reset to recovery state
//
// prerequisite for start: .partTab is operational wrt .nodeTab
func (m *Master) verify(ctx context.Context) (err error) {
	defer running(&ctx, "verify")(&err)

	m.setClusterState(neo.ClusterVerifying)
	vctx, vcancel := context.WithCancel(ctx)
	defer vcancel()

	verify := make(chan storVerify)
	inprogress := 0

	// verification = ask every storage to verify and wait for all them to complete
	// XXX "all them" -> "enough of them"?

	// NOTE we don't reset m.lastOid / m.lastTid to 0 in the beginning of verification
	//      XXX (= py), rationale=?

	// start verification on all storages we are currently in touch with
	for _, stor := range m.nodeTab.StorageList() {
		if stor.NodeState > neo.DOWN {	// XXX state cmp ok ? XXX or stor.Link != nil ?
			inprogress++
			go storCtlVerify(vctx, stor, verify)
		}
	}

loop:
	for inprogress > 0 {
		select {
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX only accept storages -> known ? RUNNING : PENDING */)
			// XXX handle resp ^^^ like in recover
			_, ok := resp.(*neo.AcceptIdentification)
			if !ok {
				break
			}

			// new storage arrived - start verification on it too
			// XXX ok? or it must first go through recovery check?
			inprogress++
			go storCtlVerify(vctx, node, verify)

		case n := <-m.nodeLeave:
			m.nodeTab.SetNodeState(n.node, neo.DOWN)

			// if cluster became non-operational - we cancel verification
			if !m.partTab.OperationalWith(&m.nodeTab) {
				// XXX ok to instantly cancel? or better
				// graceful shutdown in-flight verifications?
				vcancel()
				err = errClusterDegraded
				break loop
			}

		// a storage node came through verification - adjust our last{Oid,Tid} if ok
		// on error check - whether cluster became non-operational and stop verification if so
		case v := <-verify:
			inprogress--

			if v.err != nil {
				log.Error(ctx, v.err)

				// mark storage as non-working in nodeTab
				m.nodeTab.SetNodeState(v.node, neo.DOWN)

				// check partTab is still operational
				// if not -> cancel to go back to recovery
				if !m.partTab.OperationalWith(&m.nodeTab) {
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

			// XXX if m.partTab.OperationalWith(&.nodeTab, RUNNING) -> break (ok)


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

	// consume left verify responses (which should come without delay since it was cancelled)
	for ; inprogress > 0; inprogress-- {
		<-verify
	}

	return err
}

// storVerify is result of a storage node passing verification phase
type storVerify struct {
	node    *neo.Node
	lastOid zodb.Oid
	lastTid zodb.Tid
//	link    *neo.NodeLink	// XXX -> Node
	err	error
}

// storCtlVerify drives a storage node during cluster verifying (= starting) state
func storCtlVerify(ctx context.Context, stor *neo.Node, res chan storVerify) {
	// XXX link.Close on err
	// XXX cancel on ctx

	var err error
	defer func() {
		if err != nil {
			res <- storVerify{node: stor, err: err}
		}
	}()
	defer runningf(&ctx, "%s: stor verify", stor.Link)(&err)

	// FIXME stub
	conn, _ := stor.Link.NewConn()

	// XXX NotifyPT (so storages save locally recovered PT)

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
	res <- storVerify{node: stor, lastOid: last.LastOid, lastTid: last.LastTid}
}


// Cluster Running
// ---------------
//
// - starts with operational parttab and all present storage nodes consistently
//   either finished or rolled-back partly finished transactions
// - monitor storages come & go and if parttab becomes non-operational leave to recovery
// - provide service to clients while we are here
//
// TODO also plan data movement on new storage nodes appearing

// service drives cluster during running state
//
// TODO document error meanings on return
//
// prerequisite for start: .partTab is operational wrt .nodeTab and verification passed
func (m *Master) service(ctx context.Context) (err error) {
	defer running(&ctx, "service")(&err)

	// XXX we also need to tell storages StartOperation first
	m.setClusterState(neo.ClusterRunning)

	// XXX spawn per-storage driver about nodetab

loop:
	for {
		select {
		// a node connected and requests identification
		case n := <-m.nodeCome:
			node, resp := m.identify(ctx, n, /* XXX accept everyone */)

			//state := m.clusterState
			_ = node
			_ = resp
/*
			wg.Add(1)
			go func() {
				defer wg.Done()

				if !ok {
					reject(n.conn, resp)
					return
				}

				err = accept(n.conn, resp)
				if err {
					// XXX
				}

				switch node.NodeType {
				case STORAGE:
					switch state {
					case ClusterRecovery:
						storCtlRecovery(xxx, node, recovery)

					case ClusterVerifying, ClusterRunning:
						storCtlVerify(xxx, node, verify)

					// XXX ClusterStopping
					}

				case CLIENT:
					// TODO
				}
			}()
*/

/*
		// a storage node came through recovery - if we are still recovering let's see whether
		// ptid ↑ and if so we should take partition table from there
		case r := <-recovery:
			if m.ClusterState == ClusterRecovering {
				if r.err != nil {
					// XXX err ctx?
					// XXX log here or in producer?
					m.logf("%v", r.err)

					// TODO close stor link / update .nodeTab
					break
				}

				// we are interested in latest partTab
				// NOTE during recovery no one must be subscribed to
				// partTab so it is ok to simply change whole m.partTab
				if r.partTab.PTid > m.partTab.PTid {
					m.partTab = r.partTab
				}

				// XXX handle case of new cluster - when no storage reports valid parttab
				// XXX -> create new parttab

				// XXX update something indicating cluster currently can be operational or not ?
			}

			// proceed to storage verification if we want the
			// storage to eventually join operations
			switch m.ClusterState {
			case ClusterVerifying:
			case ClusterRunning:
				wg.Add(1)
				go func() {
					defer wg.Done()
					storCtlVerify(xxx, r.node, verify)
				}()

			// XXX note e.g. ClusterStopping - not starting anything on the storage
			}

		// a storage node came through verification - adjust our last{Oid,Tid} if ok
		// on error check - whether cluster became non-operational and reset to recovery if so
		// XXX was "stop verification if so"
		case v := <-verify:
			if v.err != nil {
				m.logf("verify: %v", v.err)

				// mark storage as non-working in nodeTab
				// FIXME better -> v.node.setState(DOWN) ?
				//m.nodeTab.UpdateLinkDown(v.link)
				m.nodeTab.UpdateDown(v.node)

				// check partTab is still operational
				// if not -> cancel to go back to recovery
				if !m.partTab.OperationalWith(&m.nodeTab) {
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

			// XXX if m.partTab.OperationalWith(&.nodeTab, RUNNING) -> break (ok)
*/

		case n := <-m.nodeLeave:
			m.nodeTab.SetNodeState(n.node, neo.DOWN)

			// if cluster became non-operational - cancel service
			if !m.partTab.OperationalWith(&m.nodeTab) {
				err = errClusterDegraded
				break loop
			}


		// XXX what else ?	(-> txn control at least)

/*
		case ech := <-m.ctlStart:
			switch m.clusterState {
			case neo.ClusterVerifying, neo.ClusterRunning:
				ech <- nil // we are already started

			case neo.ClusterRecovering:
				if m.partTab.OperationalWith(&m.nodeTab) {
					// reply "ok to start" after whole recovery finishes

					// XXX ok? we want to retrieve all recovery information first?
					// XXX or initially S is in PENDING state and
					// transitions to RUNNING only after successful recovery?

					rcancel()
					defer func() {
						ech <- nil
					}()
					break loop
				}

				ech <- fmt.Errorf("start: cluster is non-operational")


			// XXX case ClusterStopping:
			}
*/

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
			MyNodeUUID:	m.node.MyInfo.NodeUUID,
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
		NodeType:	nodeType,
		Address:	n.idReq.Address,
		NodeUUID:	uuid,
		NodeState:	nodeState,
		IdTimestamp:	monotime(),
	}

	node = m.nodeTab.Update(nodeInfo, n.conn.Link()) // NOTE this notifies all nodeTab subscribers
	return node, accept
}

// reject sends rejective identification response and closes associated link
func (m *Master) reject(ctx context.Context, conn *neo.Conn, resp neo.Msg) {
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

// accept sends acceptive identification response and closes conn
// XXX if problem -> .nodeLeave
// XXX spawn ping goroutine from here?
func (m *Master) accept(ctx context.Context, conn *neo.Conn, resp neo.Msg) error {
	// XXX cancel on ctx
	err1 := conn.Send(resp)
	err2 := conn.Close()
	return xerr.First(err1, err2)
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

/*	XXX goes away
// ServeLink serves incoming node-node link connection
// XXX +error return?
func (m *Master) ServeLink(ctx context.Context, link *neo.NodeLink) {
	logf := func(format string, argv ...interface{}) {
		m.logf("%s: " + format + "\n", append([]interface{}{link}, argv...)...)
	}

	logf("serving new node")

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
		logf("closing link")
		link.Close()	// XXX err
	}()

	// identify peer
	// the first conn must come with RequestIdentification packet
	conn, err := link.Accept()
	if err != nil {
		logf("identify: %v", err)
		return
	}

	idReq := &neo.RequestIdentification{}
	_, err = conn.Expect(idReq)
	if err != nil {
		logf("identify: %v", err)
		// XXX ok to let peer know error as is? e.g. even IO error on Recv?
		err = conn.Send(&neo.Error{neo.PROTOCOL_ERROR, err.Error()})
		if err != nil {
			logf("failed to send error: %v", err)
		}
		return
	}

	// convey identification request to master and we are done here - the master takes on the torch
	m.nodeCome <- nodeCome{conn, idReq, nilXXX kill}

//////////////////////////////////////////////////
	// if master accepted this node - don't forget to notify when it leaves
	_, rejected := idResp.(error)
	if !rejected {
		defer func() {
			m.nodeLeave <- nodeLeave{link}
		}()
	}

	// let the peer know identification result
	err = conn.Send(idResp)
	if err != nil {
		return
	}

	// nothing to do more here if identification was not accepted
	if rejected {
		logf("identify: %v", idResp)
		return
	}

	logf("identify: accepted")

	// FIXME vvv must be notified only after recovering is done
	//       (while recovering is in progress we must _not_ send partition table updates to S)
	// XXX on successful identification master should also give us:
	// - full snapshots of nodeTab, partTab and clusterState
	// - buffered notification channel subscribed to changes of ^^^
	// - unsubscribe func	XXX needed? -> nodeLeave is enough

	// ----------------------------------------
	// XXX recheck vvv

	// XXX temp hack
	connNotify := conn

	// subscribe to nodeTab/partTab/clusterState and notify peer with updates
	m.stateMu.Lock()


	nodeCh, nodeUnsubscribe := m.nodeTab.SubscribeBuffered()
	_ = nodeUnsubscribe
	//partCh, partUnsubscribe := m.partTab.SubscribeBuffered()
	// TODO cluster subscribe
	//clusterCh := make(chan ClusterState)

	//m.clusterNotifyv = append(m.clusterNotifyv, clusterCh)

	// NotifyPartitionTable	PM -> S, C
	// PartitionChanges	PM -> S, C	// subset of NotifyPartitionTable (?)
	// NotifyNodeIntormation PM -> *

	// TODO read initial nodeTab/partTab while still under lock
	// TODO send later this initial content to peer

	// TODO notify about cluster state changes
	// ClusterInformation	(PM -> * ?)
	m.stateMu.Unlock()

	go func() {
		var msg neo.Msg

		for {
			select {
			case <-ctx.Done():
				// TODO unsubscribe
				// XXX we are not draining on cancel - how to free internal buffer ?
				return

			case nodeUpdateV := <-nodeCh:
				msg = &neo.NotifyNodeInformation{
					IdTimestamp: math.NaN(),	// XXX
					NodeList:    nodeUpdateV,
				}

			//case clusterState = <-clusterCh:
			//	changed = true
			}

			err = connNotify.Send(msg)
			if err != nil {
				// XXX err
			}
		}
	}()


	// identification passed, now serve other requests

	// client: notify + serve requests
	m.ServeClient(ctx, link)

	// storage:
	m.DriveStorage(ctx, link)
/////////////////
}
*/

// ServeClient serves incoming connection on which peer identified itself as client
// XXX +error return?
//func (m *Master) ServeClient(ctx context.Context, conn *neo.Conn) {
func (m *Master) ServeClient(ctx context.Context, link *neo.NodeLink) {
	// TODO
}


// ---- internal requests for storage driver ----

// XXX goes away
/*
// storageRecovery asks storage driver to extract cluster recovery information from storage
type storageRecovery struct {
	resp chan PartitionTable	// XXX +err ?
}

// storageVerify asks storage driver to perform verification (i.e. "data recovery") operation
type storageVerify struct {
	// XXX what is result ?
}

// storageStartOperation asks storage driver to start storage node operating
type storageStartOperation struct {
	resp chan error // XXX
}

// storageStopOperation asks storage driver to stop storage node operating
type storageStopOperation struct {
	resp chan error
}
*/

// DriveStorage serves incoming connection on which peer identified itself as storage
//
// There are 2 connections:
// - notifications: unidirectional M -> S notifications (nodes, parttab, cluster state)
// - control: bidirectional M <-> S
//
// In control communication master always drives the exchange - talking first
// with e.g. a command or request and expects corresponding answer
//
// XXX +error return?
func (m *Master) DriveStorage(ctx context.Context, link *neo.NodeLink) {
	// ? >UnfinishedTransactions
	// ? <AnswerUnfinishedTransactions	(none currently)

	// TODO go for notify chan

	for {
		select {
		case <-ctx.Done():
			return	// XXX recheck

		// // request from master to do something
		// case mreq := <-xxx:
		// 	switch mreq := mreq.(type) {
		// 	case storageRecovery:

		// 	case storageVerify:
		// 		// TODO

		// 	case storageStartOperation:
		// 		// XXX timeout ?

		// 		// XXX -> chat2 ?
		// 		err = neo.EncodeAndSend(conn, &StartOperation{Backup: false /* XXX hardcoded */})
		// 		if err != nil {
		// 			// XXX err
		// 		}

		// 		pkt, err := RecvAndDecode(conn)
		// 		if err != nil {
		// 			// XXX err
		// 		}

		// 		switch pkt := pkt.(type) {
		// 		default:
		// 			err = fmt.Errorf("unexpected answer: %T", pkt)

		// 		case *NotifyReady:
		// 		}

		// 		// XXX better in m.nodeq ?
		// 		mreq.resp <- err	// XXX err ctx


		// 	case storageStopOperation:
		// 		// TODO
		// 	}
		}
	}

	// RECOVERY (master.recovery.RecoveryManager + master.handlers.identification.py)
	// --------
	// """
        // Recover the status about the cluster. Obtain the last OID, the last
        // TID, and the last Partition Table ID from storage nodes, then get
        // back the latest partition table or make a new table from scratch,
        // if this is the first time.
        // A new primary master may also arise during this phase.
	// """
	//
	// m.clusterState = Recovering
	// m.partTab.clear()
	//
	// - wait for S nodes to connect and process recovery phases on them
	// - if pt.filled() - we are starting an existing cluster
	// - else if autostart and N(S, connected) >= min_autosart -> starting new cluster
	// - (handle truncation if .trancate_tid is set)
	//
	// >Recovery
	// <AnswerRecovery		(ptid, backup_tid, truncate_tid)
	//
	// >PartitionTable
	// <AnswerPartitionTable	(ptid, []{pid, []cell}
	//
	// NOTE ^^^ need to collect PT from all storages and choose one with highest ptid
	// NOTE same for backup_tid & truncate_tid
	//
	//
	// # neoctl start
	// # (via changing nodeTab and relying on broadcast distribution ?)
	// >NotifyNodeInformation	(S1.state=RUNNING)
	// # S: "I was told I'm RUNNING"	XXX ^^^ -> StartOperation
	//
	// # (via changing m.clusterState and relying on broadcast ?)
	// >NotifyClusterInformation	(cluster_state=VERIFYING)
	//
	// # (via changing partTab and relying on broadcast ?) -> no sends whole PT initially
	// >NotifyPartitionTable	(ptid=1, `node 0: S1, R`)
	// # S saves PT info locally	XXX -> after StartOperation ?
	//
	//
	// VERIFICATION (master.verification.py)
	// ------------
	//
	// # M asks about unfinished transactions
	// >AskLockedTransactions
	// <AnswerLockedTransactions	{} ttid -> tid	# in example we have empty
	//
	// >LastIDs
	// <AnswerLastIDs		(last_oid, last_tid)
	//
	// # (via changing m.clusterState and relying on broadcast ?)
	// >NotifyClusterInformation	(cluster_state=RUNNING) XXX -> StartOperation
	//
	// >StartOperation
	// <NotifyReady
	// XXX only here we can update nodeTab with S1.state=RUNNING
	//
	// ...
	//
	// StopOperation	PM -> S
}

func (m *Master) ServeAdmin(ctx context.Context, conn *neo.Conn) {
	// TODO
}

func (m *Master) ServeMaster(ctx context.Context, conn *neo.Conn) {
	// TODO  (for elections)
}
