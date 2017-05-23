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
	"os"
	"sync"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	clusterName  string

	// master manages node and partition tables and broadcast their updates
	// to all nodes in cluster
	stateMu      sync.RWMutex
	nodeTab      NodeTable
	partTab      PartitionTable
	clusterState ClusterState

	//nodeEventQ     chan ...	// for node connected / disconnected events


	//txnCommittedQ chan ... // TODO for when txn is committed
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


// run implements main master cluster management logic: node tracking, cluster
// state updates, scheduling data movement between storage nodes etc
/*
func (m *Master) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			panic("TODO")

		case nodeEvent := <-m.nodeEventQ:
			// TODO update nodeTab

			// add info to nodeTab
			m.nodeTab.Lock()
			m.nodeTab.Add(&Node{nodeInfo, link})
			m.nodeTab.Unlock()

			// TODO notify nodeTab changes

			// TODO consider adjusting partTab

			// TODO consider how this maybe adjust cluster state


		//case txnCommitted := <-m.txnCommittedQ:

		}
	}
}
*/

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
		for {
			select {
			case <-ctx.Done():
				// TODO unsubscribe
				// XXX we are not draining on cancel - how to free internal buffer ?
				return

			case nodeUpdateV := <-nodeCh:
				// TODO
				_ = nodeUpdateV

			//case clusterState = <-clusterCh:
			//	changed = true
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
