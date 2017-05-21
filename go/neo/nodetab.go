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
// node management & node table

import (
	"sync"
)

// NodeTable represents all nodes in a cluster
//
// Usually Master maintains such table and provides it to other nodes to know
// each other but in general use-cases can be different.
//
// XXX vvv is about Master=main use-case:
//
// - Primary Master view of cluster
// - M tracks changes to nodeTab as nodes appear (connected to M) and go (disconnected from M)
// - M regularly broadcasts nodeTab content updates(?) to all nodes
//   This way all nodes stay informed about their peers in cluster
//
// UC:
//	- C needs to connect/talk to a storage by uuid
//	- C needs to initially connect to PM (?)
//	- S pulls from other S
//
//
// XXX [] of
//	.nodeUUID
//	.nodeType
//	.nodeState
//	.listenAt	ip:port | ø	// ø - if client or down(?)
//
//	- - - - - - -
//
//	.comesFrom	?(ip:port)		connected to PM from here
//						XXX ^^^ needed ? nodeLink has this info
//
//	.nodeLink | nil				M's node link to this node
//	.notifyConn | nil			conn left after identification phase
//						M uses this to send notifications to the node
//
//
// .acceptingUpdates (?)	- whether it is ok to update nodeTab (e.g. if
//	master is shutting down it first sends all nodes something but has to make
// 	sure not to accept new connections	-> XXX not needed - just stop listening
// 	first.
//
// NodeTable zero value is valid empty node table.
type NodeTable struct {
	// users have to care locking explicitly
	sync.RWMutex

	nodev []Node


	ver int // ↑ for versioning	XXX do we need this?
}

// Node represents a node entry in NodeTable
type Node struct {
	Info NodeInfo	// XXX extract ?

	Link *NodeLink	// link to this node; =nil if not connected	XXX do we need it here ?
	// XXX identified or not ?
}


// UpdateNode updates information about a node
func (nt *NodeTable) UpdateNode(nodeInfo NodeInfo) {
	// TODO
}

// XXX ? ^^^ UpdateNode is enough ?
func (nt *NodeTable) Add(node *Node) {
	// XXX check node is already there
	// XXX pass/store node by pointer ?
	nt.nodev = append(nt.nodev, *node)

	// TODO notify all nodelink subscribers about new info
}

// TODO subscribe for changes on Add ?  (notification via channel)



func (nt *NodeTable) String() string {
	//nt.RLock()		// FIXME -> it must be client
	//defer nt.RUnlock()

	buf := bytes.Buffer{}

	for node := range nl.nodev {
		// XXX recheck output
		fmt.Fprintf(&buf, "%s (%s)\t%s\t%s\n", node.NodeID, node.NodeType, node.NodeState, node.Address)
	}

	return buf.String()
}
