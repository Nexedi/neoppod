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
	"time"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	clusterName  string
	nodeUUID     NodeUUID // my node uuid; XXX init somewhere

	// master manages node and partition tables and broadcast their updates
	// to all nodes in cluster
	stateMu      sync.RWMutex
	nodeTab      NodeTable
	partTab      PartitionTable
	clusterState ClusterState

	// channels from various workers to main driver
	nodeCome     chan nodeCome	// node connected
	nodeLeave    chan nodeLeave	// node disconnected
	storRecovery chan storRecovery	// storage node passed recovery
}


// node connects
type nodeCome struct {
	link   *NodeLink
	idReq  RequestIdentification // we received this identification request
	idResp chan NEOEncoder	     // what we reply (AcceptIdentification | Error)
}

// node disconnects
type nodeLeave struct {
	// TODO
}

// storage node passed recovery phase
type storRecovery struct {
	partTab PartitionTable
	// XXX + lastOid, lastTid, backup_tid, truncate_tid ?
}

func NewMaster(clusterName string) *Master {
	m := &Master{clusterName: clusterName}
	m.SetClusterState(RECOVERING) // XXX no elections - we are the only master

	return m
}


// XXX NotifyNodeInformation to all nodes whenever nodetab changes

func (m *Master) SetClusterState(state ClusterState) {
	m.clusterState = state
	// XXX actions ?
}

// monotime returns time passed since program start
// it uses monothonic time and is robust to OS clock adjustments
// XXX place?
func monotime() float64 {
	// time.Sub uses monotonic clock readings for the difference
	return time.Now().Sub(tstart).Seconds()
}

var tstart time.Time = time.Now()

// run implements main master cluster management logic: node tracking, cluster
// state updates, scheduling data movement between storage nodes etc
func (m *Master) run(ctx context.Context) {

	// current function to ask/control a storage depending on current cluster state and master idea
	// + associated context covering all storage nodes
	storCtl := m.storCtlRecovery
	storCtlCtx, storCtlCancel := context.WithCancel(ctx)

	for {
		select {
		case <-ctx.Done():
			panic("TODO")

		// node connects & requests identification
		case n := <-m.nodeCome:
			nodeInfo, ok := m.accept(n)

			if !(ok && nodeInfo.NodeType == STORAGE) {
				break
			}

			// new storage node joined cluster
			switch m.clusterState {
			case RECOVERING:
			}

			// XXX consider .clusterState change

			// launch current storage control work on the new node
			go storCtl(storCtlCtx, n.link)

			// TODO consider adjusting partTab

		// node disconnects
		case _ = <-m.nodeLeave:
			// TODO

		// a storage node came through recovery - let's see whether
		// ptid ↑ and if so we should take partition table from there
		case r := <-m.storRecovery:
			if r.partTab.ptid > m.partTab.ptid {
				m.partTab = r.partTab
				// XXX also transfer subscribers ?
				// XXX or during recovery no one must be subscribed to partTab ?
			}
		}
	}

	_ = storCtlCancel	// XXX
}

// accept processes identification request of just connected node and either accepts or declines it
// if node identification is accepted nodeTab is updated and corresponding nodeInfo is returned
func (m *Master) accept(n nodeCome) (nodeInfo NodeInfo, ok bool) {
	// XXX also verify ? :
	// - NodeType valid
	// - IdTimestamp ?

	if n.idReq.ClusterName != m.clusterName {
		n.idResp <- &Error{PROTOCOL_ERROR, "cluster name mismatch"} // XXX
		return
	}

	nodeType := n.idReq.NodeType

	uuid := n.idReq.NodeUUID
	if uuid == 0 {
		uuid = m.allocUUID(nodeType)
	}
	// XXX uuid < 0 (temporary) -> reallocate if conflict ?

	node := m.nodeTab.Get(uuid)
	if node != nil {
		// reject - uuid is already occupied by someone else
		// XXX check also for down state - it could be the same node reconnecting
		n.idResp <- &Error{PROTOCOL_ERROR, "uuid %v already used by another node"} // XXX
		return
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

	nodeInfo = NodeInfo{
		NodeType:	nodeType,
		Address:	n.idReq.Address,
		NodeUUID:	uuid,
		NodeState:	nodeState,
		IdTimestamp:	monotime(),
	}

	m.nodeTab.Update(nodeInfo) // NOTE this notifies al nodeTab subscribers

	return nodeInfo, true
}

// storCtlRecovery drives a storage node during cluster recoving state
// TODO text
func (m *Master) storCtlRecovery(ctx context.Context, link *NodeLink) {
	var err error
	defer func() {
		if err == nil {
			return
		}

		fmt.Printf("master: %v", err)

		// this must interrupt everything connected to stor node and
		// thus eventually result in nodeLeave event to main driver
		link.Close()
	}()
	defer errcontextf(&err, "%s: stor recovery", link)

	conn, err := link.NewConn()	// FIXME bad
	if err != nil {
		return
	}

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

	m.storRecovery <- storRecovery{partTab: pt}
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
	fmt.Printf("master: %s: serving new node\n", link)

	//var node *Node

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
		fmt.Printf("master: %v: closing link\n", link)
		link.Close()	// XXX err
	}()

	// identify
	// XXX add logic to verify/assign nodeID and do other requested identification checks
	nodeInfo, err := IdentifyPeer(link, MASTER)
	if err != nil {
		fmt.Printf("master: %v\n", err)
		return
	}

	// TODO get connNotify as conn left after identification
	connNotify, err := link.NewConn()
	if err != nil {
		panic("TODO")	// XXX
	}

	// notify main logic node connects/disconnects
	_ = nodeInfo
	/*
	node = &Node{nodeInfo, link}
	m.nodeq <- node
	defer func() {
		node.state = DOWN
		m.nodeq <- node
	}()
	*/


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
