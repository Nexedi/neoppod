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
	"bytes"
	"fmt"
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
// XXX once a node was added to NodeTable its entry never deleted: if e.g. a
// connection to node is lost associated entry is marked as having DOWN (XXX or UNKNOWN ?) node
// state.
//
// NodeTable zero value is valid empty node table.
type NodeTable struct {
	// users have to care locking explicitly
	sync.RWMutex

	nodev   []*Node
	notifyv []chan NodeInfo // subscribers

	ver int // ↑ for versioning	XXX do we need this?
}

// Node represents a node entry in NodeTable
type Node struct {
	Info NodeInfo	// XXX extract ?

	Link *NodeLink	// link to this node; =nil if not connected	XXX do we need it here ?
	// XXX identified or not ?
}


// Subscribe subscribes to NodeTable updates
// it returns a channel via which updates will be delivered and unsubscribe function
//
// XXX locking: client for subscribe/unsubscribe	XXX ok?
func (nt *NodeTable) Subscribe() (ch chan NodeInfo, unsubscribe func()) {
	ch = make(chan NodeInfo)		// XXX how to specify ch buf size if needed ?
	nt.notifyv = append(nt.notifyv, ch)

	unsubscribe = func() {
		for i, c := range nt.notifyv {
			if c == ch {
				nt.notifyv = append(nt.notifyv[:i], nt.notifyv[i+1:]...)
				close(ch)
				return
			}
		}
		panic("XXX unsubscribe not subscribed channel")
	}

	return ch, unsubscribe
}

// SubscribeBufferred subscribes to NodeTable updates without blocking updater
// it returns a channel via which updates are delivered and unsubscribe function
// the updates will be sent to destination in non-blocking way - if destination
// channel is not ready they will be bufferred.
// it is the caller reponsibility to make sure such buffering does not grow up
// to infinity - via e.g. detecting stuck connections and unsubscribing on shutdown
//
// XXX locking: client for subscribe/unsubscribe	XXX ok?
func (nt *NodeTable) SubscribeBuffered() (ch chan []NodeInfo, unsubscribe func()) {
	in, unsubscribe := nt.Subscribe()
	ch = make(chan []NodeInfo)

	go func() {
		var updatev []NodeInfo
		shutdown := false

		for {
			out := ch
			if len(updatev) == 0 {
				if shutdown {
					// nothing to send and source channel closed
					// -> close destination and stop
					close(ch)
					break
				}
				out = nil
			}

			select {
			case update, ok := <-in:
				if !ok {
					shutdown = true
					break
				}

				// FIXME merge updates as same node could be updated several times
				updatev = append(updatev, update)

			case out <- updatev:
				updatev = nil
			}
		}
	}()

	return ch, unsubscribe
}


// UpdateNode updates information about a node
func (nt *NodeTable) UpdateNode(nodeInfo NodeInfo) {
	// TODO
}

// XXX ? ^^^ UpdateNode is enough ?
func (nt *NodeTable) Add(node *Node) {
	// XXX check node is already there
	// XXX pass/store node by pointer ?
	nt.nodev = append(nt.nodev, node)

	// TODO notify all nodelink subscribers about new info
}

// TODO subscribe for changes on Add ?  (notification via channel)


// Lookup finds node by nodeID
func (nt *NodeTable) Lookup(uuid NodeUUID) *Node {
	// FIXME linear scan
	for _, node := range nt.nodev {
		if node.Info.NodeUUID == uuid {
			return node
		}
	}
	return nil
}

// XXX LookupByAddress ?


func (nt *NodeTable) String() string {
	//nt.RLock()		// FIXME -> it must be client
	//defer nt.RUnlock()

	buf := bytes.Buffer{}

	for _, node := range nt.nodev {
		// XXX recheck output
		i := node.Info
		fmt.Fprintf(&buf, "%s (%s)\t%s\t%s\n", i.NodeUUID, i.NodeType, i.NodeState, i.Address)
	}

	return buf.String()
}
