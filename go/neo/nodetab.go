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

// Node represents a node from local-node point of view
type Node struct {
	Info NodeInfo

	Link *NodeLink	// link to this node; =nil if not connected
	// XXX identified or not ?
}


// NodeTable represents all known nodes in a cluster from local-node point of view
type NodeTable struct {
	// users have to care locking explicitly
	sync.Mutex	// XXX -> RWMutex ?
}

// UpdateNode updates information about a node
func (nt *NodeTable) UpdateNode(nodeInfo NodeInfo) {
	// TODO
}

// XXX ? ^^^ UpdateNode is enough ?
func (nt *NodeTable) Add(node *Node) {
	// TODO
}

// TODO subscribe for changes on Add ?  (notification via channel)
