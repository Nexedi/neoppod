// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

package neo
// master node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"

	"../zodb"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	clusterName  string
	nodeUUID     NodeUUID

	// last allocated oid & tid
	// XXX how to start allocating oid from 0, not 1 ?
	lastOid zodb.Oid
	lastTid zodb.Tid

	// master manages node and partition tables and broadcast their updates
	// to all nodes in cluster
	stateMu      sync.RWMutex	// XXX recheck: needed ?
	nodeTab      NodeTable
	partTab      PartitionTable
	clusterState ClusterState

	// channels controlling main driver
	ctlStart    chan ctlStart	// request to start cluster
	ctlStop     chan ctlStop	// request to stop  cluster
	ctlShutdown chan chan error	// request to shutdown cluster XXX with ctx too ?

	wantToStart chan chan error	// main -> recovery

	// channels from various workers to main driver
	nodeCome     chan nodeCome	// node connected
	nodeLeave    chan nodeLeave	// node disconnected
}

type ctlStart struct {
	// XXX +ctx ?
	resp chan error
}

type ctlStop struct {
	// XXX +ctx ?
	resp chan error
}

// node connects
type nodeCome struct {
	link   *NodeLink
	idReq  RequestIdentification // we received this identification request
	idResp chan NEOEncoder	     // what we reply (AcceptIdentification | Error)
}

// node disconnects
type nodeLeave struct {
	link *NodeLink
	// XXX TODO
}

func NewMaster(clusterName string) *Master {
	m := &Master{clusterName: clusterName}
	m.nodeUUID = m.allocUUID(MASTER)
	// TODO update nodeTab with self
	m.clusterState = ClusterRecovering	// XXX no elections - we are the only master
	go m.run(context.TODO())	// XXX ctx

	return m
}


// XXX NotifyNodeInformation to all nodes whenever nodetab changes

// XXX -> Start(), Stop()

// Start requests cluster to eventually transition into running state
// it returns an error if such transition is not currently possible (e.g. partition table is not operational)
// it returns nil if the transition began.
// NOTE upon successfull return cluster is not yet in running state - the transition will
//      take time and could be also automatically aborted due to cluster environment change (e.g.
//      a storage node goes down)
func (m *Master) Start() error {
	ch := make(chan error)
	m.ctlStart <- ctlStart{ch}
	return <-ch
}

// Stop requests cluster to eventually transition into recovery state
// XXX should be always possible ?
func (m *Master) Stop() error {
	ch := make(chan error)
	m.ctlStop <- ctlStop{ch}
	return <-ch
}

// Shutdown requests all known nodes in the cluster to stop
func (m *Master) Shutdown() error {
	panic("TODO")
}

func (m *Master) setClusterState(state ClusterState) {
	m.clusterState = state
	// TODO notify subscribers
}


func (m *Master) xxx(ctx ...) {
	var err error

	for ctx.Err() == nil {
		err = recovery(ctx)
		if err != nil {
			return // XXX
		}

		// successful recovery -> verify
		err = verify(ctx)
		if err != nil {
			continue // -> recovery
		}

		// successful verify -> service
		err = service(ctx)
		if err != nil {
			// XXX what about shutdown ?
			continue // -> recovery
		}
	}
}


// run is a process which implements main master cluster management logic: node tracking, cluster
// state updates, scheduling data movement between storage nodes etc
func (m *Master) run(ctx context.Context) {

	go m.recovery(ctx)

	for {
		select {
		case <-ctx.Done():
			// XXX -> shutdown
			panic("TODO")

		// command to start cluster
		case c := <-m.ctlStart:
			if m.clusterState != ClusterRecovering {
				// start possible only from recovery
				// XXX err ctx
				c.resp <- fmt.Errorf("start: inappropriate current state: %v", m.clusterState)
				break
			}

			ch := make(chan error)
			select {
			case <-ctx.Done():
				// XXX how to avoid checking this ctx.Done everywhere?
				c.resp <- ctx.Err()
				panic("TODO")

			case m.wantToStart <- ch:
			}
			err := <-ch
			c.resp <- err
			if err != nil {
				break
			}

			// recovery said it is ok to start and finished - launch verification
			m.setClusterState(ClusterVerifying)
			go m.verify(ctx)

		// command to stop cluster
		case <-m.ctlStop:
			// TODO

		// command to shutdown
		case <-m.ctlShutdown:
			// TODO
		}
	}

}


// Cluster Recovery
// ----------------
//
// - starts from potentially no storage nodes known
// - accept connections from storage nodes
// - retrieve and recovery latest previously saved partition table from storages
// - monitor whether partition table becomes operational wrt currently up nodeset
// - if yes - finish recovering upon receiving "start" command

// recovery drives cluster during recovery phase
//
// when recovery finishes error indicates:
// - nil:  recovery was ok and a command came for cluster to start
// - !nil: recovery was cancelled
func (m *Master) recovery(ctx context.Context) error {
	recovery := make(chan storRecovery)
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	inprogress := 0

	// start recovery on all storages we are currently in touch with
	for _, stor := range m.nodeTab.StorageList() {
		if stor.Info.NodeState > DOWN {	// XXX state cmp ok ?
			inprogress++
			go storCtlRecovery(rctx, stor.Link, recovery)
		}
	}

loop:
	for {
		select {
		case n := <-m.nodeCome:
			node, ok := m.accept(n, /* XXX do not accept clients */)
			if !ok {
				break
			}

			// new storage arrived - start recovery on it too
			inprogress++
			go storCtlRecovery(rctx, node.Link, recovery)

		case n := <-m.nodeLeave:
			m.nodeTab.UpdateLinkDown(n.link)
			// XXX update something indicating cluster currently can be operational or not ?

		// a storage node came through recovery - let's see whether
		// ptid ↑ and if so we should take partition table from there
		case r := <-recovery:
			inprogress--

			if r.err != nil {
				// XXX err ctx?
				// XXX log here or in producer?
				fmt.Printf("master: %v\n", r.err)
				break
			}

			// we are interested in latest partTab
			// NOTE during recovery no one must be subscribed to
			// partTab so it is ok to simply change whole m.partTab
			if r.partTab.ptid > m.partTab.ptid {
				m.partTab = r.partTab
			}

			// XXX update something indicating cluster currently can be operational or not ?


		// request from master: "I want to start - ok?" - if ok we reply ok and exit
		// if not ok - we just reply not ok
		//case s := <-m.wantToStart:
		case c := <-m.ctlStart:
			if m.partTab.OperationalWith(&m.nodeTab) {
				// reply "ok to start" after whole recovery finishes

				// XXX ok? we want to retreive all recovery information first?
				// XXX or initially S is in PENDING state and
				// transitions to RUNNING only after successful
				// recovery?

				rcancel()
				defer func() {
					c.resp <- nil
				}()
				break loop
			}

			s <- fmt.Errorf("start: cluster is non-operational")

		case c := <-m.ctlStop:
			c.resp <- nil // we are already recovering

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	// consume left recovery responces (which should come without delay since it was cancelled)
	for ; inprogress > 0; inprogress-- {
		<-recovery
	}

	// XXX err
}

// storRecovery is result of a storage node passing recovery phase
type storRecovery struct {
	partTab PartitionTable
	// XXX + backup_tid, truncate_tid ?

	err error
}

// storCtlRecovery drives a storage node during cluster recovering state
// it retrieves various ids and parition table from as stored on the storage
func storCtlRecovery(ctx context.Context, link *NodeLink, res chan storRecovery) {
	var err error
	defer func() {
		if err == nil {
			return
		}

		// XXX on err still provide feedback to storRecovery chan ?
		res <- storRecovery{err: err}

		/*
		fmt.Printf("master: %v", err)

		// this must interrupt everything connected to stor node and
		// thus eventually result in nodeLeave event to main driver
		link.Close()
		*/
	}()
	defer errcontextf(&err, "%s: stor recovery", link)

	conn, err := link.NewConn()	// FIXME bad
	if err != nil {
		return
	}
	// XXX cancel on ctx

	recovery := AnswerRecovery{}
	err = Ask(conn, &Recovery{}, &recovery)
	if err != nil {
		return
	}

	resp := AnswerPartitionTable{}
	err = Ask(conn, &X_PartitionTable{}, &resp)
	if err != nil {
		return
	}

	// reconstruct partition table from response
	pt := PartitionTable{}
	pt.ptid = resp.PTid
	for _, row := range resp.RowList {
		i := row.Offset
		for i >= uint32(len(pt.ptTab)) {
			pt.ptTab = append(pt.ptTab, []PartitionCell{})
		}

		//pt.ptTab[i] = append(pt.ptTab[i], row.CellList...)
		for _, cell := range row.CellList {
			pt.ptTab[i] = append(pt.ptTab[i], PartitionCell{
					NodeUUID:  cell.NodeUUID,
					CellState: cell.CellState,
				})
		}
	}

	res <- storRecovery{partTab: pt}
}


// Cluster Verification
// --------------------
//
// - starts with operational parttab
// - tell all storages to perform data verificaion (TODO) and retreive last ids
// - once we are done without loosing too much storages in the process (so that
//   parttab is still operational) we are ready to enter servicing state.

// verify is a process that drives cluster via verification phase
//
// prerequisite for start: .partTab is operational wrt .nodeTab
func (m *Master) verify(ctx context.Context) error { //, storv []*NodeLink) error {
	// XXX ask every storage for verify and wait for _all_ them to complete?

	var err error
	verify := make(chan storVerify)
	vctx, vcancel := context.WithCancel(ctx)
	defer vcancel()
	inprogress := 0

	// XXX do we need to reset m.lastOid / m.lastTid to 0 in the beginning?

	// start verification on all storages we are currently in touch with
	for _, stor := range m.nodeTab.StorageList() {
		inprogress++
		go storCtlVerify(vctx, stor.Link, verify)
	}

loop:
	for inprogress > 0 {
		select {
		case n := <-m.nodeCome:
			// TODO

		case n := <-m.nodeLeave:
			// TODO

		case v := <-verify:
			inprogress--

			if v.err != nil {
				fmt.Printf("master: %v\n", v.err) // XXX err ctx
				// XXX mark S as non-working in nodeTab

				// check partTab is still operational
				// if not -> cancel to go back to recovery
				if m.partTab.OperationalWith(&m.nodeTab) {
					vcancel()
					err = fmt.Errorf("cluster became non-operational in the process")
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


		case c := <-m.ctlStart:
			c.resp <- nil // we are already starting

		case c := <-m.ctlStop:
			c.resp <- nil // ok
			err = fmt.Errorf("stop requested")
			break loop

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	if err != nil {
		// XXX -> err = fmt.Errorf("... %v", err)
		fmt.Printf("master: verify: %v\n", err)

		// consume left verify responses (which should come without delay since it was cancelled)
		for ; inprogress > 0; inprogress-- {
			<-verify
		}
	}

	// XXX -> return via channel ?
	return err
}

// storVerify is result of a storage node passing verification phase
type storVerify struct {
	lastOid zodb.Oid
	lastTid zodb.Tid
	err	error
}

// storCtlVerify drives a storage node during cluster verifying (= starting) state
func storCtlVerify(ctx context.Context, link *NodeLink, res chan storVerify) {
	// XXX err context + link.Close on err
	// XXX cancel on ctx

	var err error
	defer func() {
		if err != nil {
			res <- storVerify{err: err}
		}
	}()
	defer errcontextf(&err, "%s: verify", link)

	// FIXME stub
	conn, _ := link.NewConn()

	locked := AnswerLockedTransactions{}
	err = Ask(conn, &LockedTransactions{}, &locked)
	if err != nil {
		return
	}

	if len(locked.TidDict) > 0 {
		// TODO vvv
		err = fmt.Errorf("TODO: non-ø locked txns: %v", locked.TidDict)
		return
	}

	last := AnswerLastIDs{}
	err = Ask(conn, &LastIDs{}, &last)
	if err != nil {
		return
	}

	// send results to driver
	res <- storVerify{lastOid: last.LastOid, lastTid: last.LastTid}
}


// Cluster Running
// ---------------
//
// - starts with operational parttab and (enough ?) present storage nodes passed verification
// - monitor storages come & go and if parttab becomes non-operational leave to recovery
// - provide service to clients while we are here
//
// TODO also plan data movement on new storage nodes appearing

// service is the process that drives cluster during running state
//
func (m *Master) service(ctx context.Context) {

loop:
	for {
		select {
		case n := <-m.nodeCome:
			// TODO

		case n := <-m.nodeLeave:
			// TODO


		// XXX what else ?	(-> txn control at least)

		case c := <-m.ctlStart:
			c.resp <- nil // we are already started

		case c := <-m.ctlStop:
			c.resp <- nil // ok
			err = fmt.Errorf("stop requested")
			break loop

		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	if err != nil {
		// TODO
	}

	return err
}

// accept processes identification request of just connected node and either accepts or declines it
// if node identification is accepted nodeTab is updated and corresponding node entry is returned
func (m *Master) accept(n nodeCome) (node *Node, ok bool) {
	// XXX also verify ? :
	// - NodeType valid
	// - IdTimestamp ?

	if n.idReq.ClusterName != m.clusterName {
		n.idResp <- &Error{PROTOCOL_ERROR, "cluster name mismatch"} // XXX
		return nil, false
	}

	nodeType := n.idReq.NodeType

	uuid := n.idReq.NodeUUID
	if uuid == 0 {
		uuid = m.allocUUID(nodeType)
	}
	// XXX uuid < 0 (temporary) -> reallocate if conflict ?

	node = m.nodeTab.Get(uuid)
	if node != nil {
		// reject - uuid is already occupied by someone else
		// XXX check also for down state - it could be the same node reconnecting
		n.idResp <- &Error{PROTOCOL_ERROR, "uuid %v already used by another node"} // XXX
		return nil, false
	}

	// XXX accept only certain kind of nodes depending on .clusterState, e.g.
	switch nodeType {
	case CLIENT:
		n.idResp <- &Error{NOT_READY, "cluster not operational"}

	// XXX ...
	}


	n.idResp <- &AcceptIdentification{
			NodeType:	MASTER,
			MyNodeUUID:	m.nodeUUID,
			NumPartitions:	1,	// FIXME hardcoded
			NumReplicas:	1,	// FIXME hardcoded
			YourNodeUUID:	uuid,
		}

	// update nodeTab
	var nodeState NodeState
	switch nodeType {
	case STORAGE:
		// FIXME py sets to RUNNING/PENDING depending on cluster state
		nodeState = PENDING

	default:
		nodeState = RUNNING
	}

	nodeInfo := NodeInfo{
		NodeType:	nodeType,
		Address:	n.idReq.Address,
		NodeUUID:	uuid,
		NodeState:	nodeState,
		IdTimestamp:	monotime(),
	}

	node = m.nodeTab.Update(nodeInfo, n.link) // NOTE this notifies al nodeTab subscribers
	return node, true
}

// allocUUID allocates new node uuid for a node of kind nodeType
// XXX it is bad idea for master to assign uuid to coming node
// -> better nodes generate really uniquie UUID themselves and always show with them
func (m *Master) allocUUID(nodeType NodeType) NodeUUID {
	// see NodeUUID & NodeUUID.String for details
	// XXX better to keep this code near to ^^^ (e.g. attached to NodeType)
	// XXX but since whole uuid assign idea is not good - let's keep it dirty here
	typ := int(nodeType & 7) << (24 + 4) // note temp=0
	for num := 1; num < 1<<24; num++ {
		uuid := NodeUUID(typ | num)
		if m.nodeTab.Get(uuid) == nil {
			return uuid
		}
	}

	panic("all uuid allocated ???")	// XXX more robust ?
}

// ServeLink serves incoming node-node link connection
// XXX +error return?
func (m *Master) ServeLink(ctx context.Context, link *NodeLink) {
	logf := func(format string, argv ...interface{}) {
		fmt.Printf("master: %s: " + format + "\n", append([]interface{}{link}, argv...))
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

	idReq := RequestIdentification{}
	err = Expect(conn, &idReq)
	if err != nil {
		logf("identify: %v", err)
		// XXX ok to let peer know error as is? e.g. even IO error on Recv?
		err = EncodeAndSend(conn, &Error{PROTOCOL_ERROR, err.Error()})
		if err != nil {
			logf("failed to send error: %v", err)
		}
		return
	}

	// convey identification request to master
	idRespCh := make(chan NEOEncoder)
	m.nodeCome <- nodeCome{link, idReq, idRespCh}
	idResp := <-idRespCh

	// if master accepted this node - don't forget to notify when it leaves
	_, rejected := idResp.(error)
	if !rejected {
		defer func() {
			m.nodeLeave <- nodeLeave{link}
		}()
	}

	// let the peer know identification result
	err = EncodeAndSend(conn, idResp)
	if err != nil {
		return
	}

	// nothing to do more here if identification was not accepted
	if rejected {
		logf("identify: %v", idResp)
		return
	}

	logf("identify: accepted")

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
		var pkt NEOEncoder

		for {
			select {
			case <-ctx.Done():
				// TODO unsubscribe
				// XXX we are not draining on cancel - how to free internal buffer ?
				return

			case nodeUpdateV := <-nodeCh:
				pkt = &NotifyNodeInformation{
					IdTimestamp: math.NaN(),	// XXX
					NodeList:    nodeUpdateV,
				}

			//case clusterState = <-clusterCh:
			//	changed = true
			}

			err = EncodeAndSend(connNotify, pkt)
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
}

// ServeClient serves incoming connection on which peer identified itself as client
// XXX +error return?
//func (m *Master) ServeClient(ctx context.Context, conn *Conn) {
func (m *Master) ServeClient(ctx context.Context, link *NodeLink) {
	// TODO
}


// ---- internal requests for storage driver ----

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

// storageStopOperation asks storage driver to stop storage node oerating
type storageStopOperation struct {
	resp chan error
}

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
func (m *Master) DriveStorage(ctx context.Context, link *NodeLink) {
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
		// 		err = EncodeAndSend(conn, &StartOperation{Backup: false /* XXX hardcoded */})
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
	// # (via changing partTab and relying on broadcast ?)
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

func (m *Master) ServeAdmin(ctx context.Context, conn *Conn) {
	// TODO
}

func (m *Master) ServeMaster(ctx context.Context, conn *Conn) {
	// TODO  (for elections)
}

// ----------------------------------------

const masterSummary = "run master node"

// TODO options:
// cluster, masterv ...

func masterUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: neo master [options]
Run NEO master node.
`)

	// FIXME use w (see flags.SetOutput)
}

func masterMain(argv []string) {
	var bind string
	var cluster string

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { masterUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	flags.StringVar(&bind, "bind", bind, "address to serve on")
	flags.StringVar(&cluster, "cluster", cluster, "cluster name")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}

	masterSrv := NewMaster(cluster)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	// TODO + TLS
	err := ListenAndServe(ctx, "tcp", bind, masterSrv)	// XXX "tcp" hardcoded
	if err != nil {
		log.Fatal(err)
	}
}
