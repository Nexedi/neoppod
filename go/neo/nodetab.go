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

package neo
// node management & node table

import (
	"bytes"
	"fmt"
	//"sync"
)

// NodeTable represents known nodes in a cluster
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
//	.UUID
//	.Type
//	.State
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
	//sync.RWMutex	XXX needed ?

	//storv	[]*Node // storages
	nodev   []*Node // all other nodes	-> *Peer
	notifyv []chan NodeInfo // subscribers

	//ver int // ↑ for versioning	XXX do we need this?
}


// // special error indicating dial is currently in progress
// var errDialInprogress = errors.New("dialing...")

// even if dialing a peer failed, we'll attempt redial after this timeout
const δtRedial = 3 * time.Second

// Peer represents a peer node in the cluster.
type Peer struct {
	NodeInfo	// .uuid, .addr, ...

	// link to this peer
	linkMu		sync.Mutex
	link		*NodeLink	// link to peer or nil if not connected
//	linkErr		error		// dialing gave this error
	dialT		time.Time	// dialing finished at this time
//	linkReady	chan struct{}	// becomes ready after dial finishes; reinitialized at each redial

	dialing		*dialReady	// dialer notifies waiters via this; reinitialized at each redial; nil while not dialing
}

type dialReady struct {
	link	*NodeLink
	err	error
	ready	chan struct{}
}

// Connect returns link to this peer.
//
// If the link was not yet established Connect dials the peer appropriately,
// handshakes, requests identification and checks that identification reply is
// as expected.
func (p *Peer) Connect(ctx context.Context) (*NodeLink, error) {
	// XXX p.State != RUNNING
	// XXX p.Addr  != ""

	p.linkMu.Lock()

	// ok if already connected
	if link := p.link; link != nil {
		p.linkMu.Unlock()
		return link, nil
	}

	// if dial is already in progress - wait for its completion
	if dialing := p.dialing; dialing != nil {
		p.linkMu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-dialing.ready:
			return dialed.link, dialed.err
		}
	}


	// otherwise this goroutine becomes responsible for (re)dialing the peer
	dialing = &dialReady{ready: make(chan struct{})}
	p.dialing = dialing

	// start dialing - in singleflight
	p.linkMu.Unlock()
	go func() {
		// throttle redialing if too fast
		δt := time.Now().Sub(dialT)
		if δt < δtRedial && !dialT.IsZero() {
			select {
			case <-ctx.Done():
				// XXX -> return nil, ctx.Err()

			case <-time.After(δtRedial - δt):
				// ok
			}
		}

		conn0, accept, err := Dial(ctx, p.Type, p.Addr)
		if err != nil {
			// XXX -> return nil, err
		}

		// XXX accept.NodeType	== p.Type
		// XXX accept.MyUUID	== p.UUID
		// XXX accept.YourUUID	== (what has been given us by master)
		// XXX accept.Num{Partitions,Replicas} == (what is expected - (1,1) currently)

		p.link = link
		p.linkErr = err
		p.dialT = time.Now()

		dialing.link = link
		dialing.err = err
		close(dialing.ready)
	}()

	<-dialing.ready
	return dialing.link, dialing.err
}









//trace:event traceNodeChanged(nt *NodeTable, n *Node)

// Node represents a node entry in NodeTable
type Node struct {
	NodeInfo
	// XXX have Node point to -> NodeTable?

	// XXX decouple vvv from Node ?

	// link to this node; =nil if not connected
	Link *NodeLink

	// XXX not yet sure it is good idea
	Conn *Conn	// main connection

	// XXX something indicating in-flight connecting/identification
	// (wish Link != nil means connected and identified)
}


// Get finds node by uuid.
func (nt *NodeTable) Get(uuid NodeUUID) *Node {
	// FIXME linear scan
	for _, node := range nt.nodev {
		if node.UUID == uuid {
			return node
		}
	}
	return nil
}

// XXX GetByAddress ?

// Update updates information about a node.
//
// it returns corresponding node entry for convenience
func (nt *NodeTable) Update(nodeInfo NodeInfo, conn *Conn /*XXX better link *NodeLink*/) *Node {
	node := nt.Get(nodeInfo.UUID)
	if node == nil {
		node = &Node{}
		nt.nodev = append(nt.nodev, node)
	}

	node.NodeInfo = nodeInfo
	node.Conn = conn
	if conn != nil {
		node.Link = conn.Link()
	}

	traceNodeChanged(nt, node)

	nt.notify(node.NodeInfo)
	return node
}


/*
// GetByLink finds node by node-link
// XXX is this a good idea ?
func (nt *NodeTable) GetByLink(link *NodeLink) *Node {
	// FIXME linear scan
	for _, node := range nt.nodev {
		if node.Link == link {
			return node
		}
	}
	return nil
}
*/

// XXX doc
func (nt *NodeTable) SetNodeState(node *Node, state NodeState) {
	node.State = state
	traceNodeChanged(nt, node)
	nt.notify(node.NodeInfo)
}

/*
// UpdateLinkDown updates information about corresponding to link node and marks it as down
// it returns corresponding node entry for convenience
// XXX is this a good idea ?
func (nt *NodeTable) UpdateLinkDown(link *NodeLink) *Node {
	node := nt.GetByLink(link)
	if node == nil {
		// XXX vvv not good
		panic("nodetab: UpdateLinkDown: no corresponding entry")
	}

	nt.SetNodeState(node, DOWN)
	return node
}
*/


// StorageList returns list of all storages in node table
func (nt *NodeTable) StorageList() []*Node {
	// FIXME linear scan
	sl := []*Node{}
	for _, node := range nt.nodev {
		if node.Type == STORAGE {
			sl = append(sl, node)
		}
	}
	return sl
}

func (nt *NodeTable) String() string {
	//nt.RLock()		// FIXME -> it must be client
	//defer nt.RUnlock()

	buf := bytes.Buffer{}

	// XXX also for .storv
	for _, n := range nt.nodev {
		// XXX recheck output
		fmt.Fprintf(&buf, "%s (%s)\t%s\t%s\n", n.UUID, n.Type, n.State, n.Addr)
	}

	return buf.String()
}


// notify notifies NodeTable subscribers that nodeInfo was updated
func (nt *NodeTable) notify(nodeInfo NodeInfo) {
	// XXX rlock for .notifyv ?
	for _, notify := range nt.notifyv {
		notify <- nodeInfo
	}
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

// SubscribeBuffered subscribes to NodeTable updates without blocking updater
// it returns a channel via which updates are delivered and unsubscribe function
// the updates will be sent to destination in non-blocking way - if destination
// channel is not ready they will be buffered.
// it is the caller responsibility to make sure such buffering does not grow up
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
