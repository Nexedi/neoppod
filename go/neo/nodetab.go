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
	"context"
	"fmt"
	"sync"
	"time"

	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"

	"lab.nexedi.com/kirr/neo/go/internal/log"
	"lab.nexedi.com/kirr/neo/go/internal/task"
)

// NodeTable represents known nodes in a cluster.
//
// It is
//
//	UUID -> *Node
//
// mapping listing known nodes and associating their uuid with information
// about a node.
//
// Master maintains such table and provides it to its peers to know each other:
//
//	- Primary Master view of cluster.
// 	- M tracks changes to nodeTab as nodes appear (connected to M) and go (disconnected from M).
// 	- M regularly broadcasts nodeTab content updates(?) to all nodes.
// 	  This way all nodes stay informed about their peers in cluster.
//
// Usage examples:
//
//	- C needs to connect/talk to a storage by uuid
//	  (the uuid itself is obtained from PartitionTable by oid).
//	- S pulls from other S.
//
// NOTE once a node was added to NodeTable its entry is never deleted: if e.g.
// a connection to node is lost associated entry is marked as having DOWN (XXX
// or UNKNOWN ?) node state.
//
// NodeTable zero value is valid empty node table.
type NodeTable struct {
	// XXX for Node.Dial to work. see also comments vvv near "peer link"
	nodeApp *NodeApp

	// users have to care locking explicitly
	//sync.RWMutex	XXX needed ?

	//storv	[]*Node // storages
	nodev   []*Node // all other nodes
	notifyv []chan proto.NodeInfo // subscribers
}

//trace:event traceNodeChanged(nt *NodeTable, n *Node)

// Node represents a peer node in the cluster.
//
// XXX name as Peer?
type Node struct {
	nodeTab *NodeTable // this node is part of

	proto.NodeInfo // .type, .addr, .uuid, ...	XXX also protect by mu?

	linkMu sync.Mutex
	link   *neonet.NodeLink // link to this peer; nil if not connected
	dialT  time.Time // last dial finished at this time

	// dialer notifies waiters via this; reinitialized at each redial; nil while not dialing
	//
	// NOTE duplicates .link to have the following properties:
	//
	// 1. all waiters of current in-progress dial wakeup immediately after
	//    dial completes and get link/error from dial result.
	//
	// 2. any .Link() that sees .link=nil starts new redial with throttle
	//    to make sure peer is dialed not faster than δtRedial.
	//
	// (if we do not have dialing.link waiter will need to relock
	//  peer.linkMu and for some waiters chances are another .Link()
	//  already started redialing and they will have to wait again)
	dialing *dialed

//	// live connection pool that user provided back here via .PutConn()
//	connPool []*Conn
}

// Len returns N(entries) in the table.
func (nt *NodeTable) Len() int {
	return len(nt.nodev)
}

// All returns all entries in the table as one slice.
// XXX -> better iter?
func (nt *NodeTable) All() []*Node {
	return nt.nodev
}

// Get finds node by uuid.
func (nt *NodeTable) Get(uuid proto.NodeUUID) *Node {
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
// it returns corresponding node entry for convenience.
func (nt *NodeTable) Update(nodeInfo proto.NodeInfo) *Node {
	node := nt.Get(nodeInfo.UUID)
	if node == nil {
		node = &Node{nodeTab: nt}
		nt.nodev = append(nt.nodev, node)
	}

	node.NodeInfo = nodeInfo
	/*
	node.Conn = conn
	if conn != nil {
		node.Link = conn.Link()
	}
	*/

	// XXX close link if .state becomes DOWN ?

	traceNodeChanged(nt, node)

	nt.notify(node.NodeInfo)
	return node
}

// StorageList returns list of all storages in node table
func (nt *NodeTable) StorageList() []*Node {
	// FIXME linear scan
	sl := []*Node{}
	for _, node := range nt.nodev {
		if node.Type == proto.STORAGE {
			sl = append(sl, node)
		}
	}
	return sl
}


// XXX doc
func (n *Node) SetState(state proto.NodeState) {
	n.State = state
	traceNodeChanged(n.nodeTab, n)
	n.nodeTab.notify(n.NodeInfo)
}



func (nt *NodeTable) String() string {
	buf := bytes.Buffer{}

	// XXX also for .storv
	for _, n := range nt.nodev {
		// XXX recheck output
		fmt.Fprintf(&buf, "%s (%s)\t%s\t%s\t@ %s\n", n.UUID, n.Type, n.State, n.Addr, n.IdTime)
	}

	return buf.String()
}

// ---- subscription to nodetab updates ----

// notify notifies NodeTable subscribers that nodeInfo was updated
func (nt *NodeTable) notify(nodeInfo proto.NodeInfo) {
	// XXX rlock for .notifyv ?
	for _, notify := range nt.notifyv {
		notify <- nodeInfo
	}
}

// Subscribe subscribes to NodeTable updates.
//
// It returns a channel via which updates will be delivered and function to unsubscribe.
//
// XXX locking: client for subscribe/unsubscribe	XXX ok?
func (nt *NodeTable) Subscribe() (ch chan proto.NodeInfo, unsubscribe func()) {
	ch = make(chan proto.NodeInfo)		// XXX how to specify ch buf size if needed ?
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

// SubscribeBuffered subscribes to NodeTable updates without blocking updater.
//
// It returns a channel via which updates are delivered and unsubscribe function.
// The updates will be sent to destination in non-blocking way - if destination
// channel is not ready they will be buffered.
// It is the caller responsibility to make sure such buffering does not grow up
// to infinity - via e.g. detecting stuck connections and unsubscribing on shutdown.
//
// XXX locking: client for subscribe/unsubscribe	XXX ok?
func (nt *NodeTable) SubscribeBuffered() (ch chan []proto.NodeInfo, unsubscribe func()) {
	in, unsubscribe := nt.Subscribe()
	ch = make(chan []proto.NodeInfo)

	go func() {
		var updatev []proto.NodeInfo
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

// ---- peer link ----

// TODO review peer link dialing / setting / accepting.
//
//	Keep in mind that in NEO in general case it is not client/server but peer-to-peer
//	e.g. when two S establish a link in between then to exchange/sync data.
//
//	Also the distinction between S and M should go away as every S should
//	be taught to also become M (and thus separate M nodes go away
//	completely) with constant reelection being happening in the background
//	like in raft.

// SetLink sets link to peer node.
// XXX
//
// See also: Link, CloseLink, Dial.
func (p *Node) SetLink(link *neonet.NodeLink) {
	// XXX see Link about locking - whether it is needed here or not
	p.linkMu.Lock()
	p.link = link
	p.linkMu.Unlock()
}

// Link returns current link to peer node.
//
// If the link is not yet established - Link returns nil.
//
// See also: Dial.
func (p *Node) Link() *neonet.NodeLink {
	// XXX do we need lock here?
	// XXX usages where Link is used (contrary to Dial) there is no need for lock
	p.linkMu.Lock()
	link := p.link
	p.linkMu.Unlock()
	return link
}

// CloseLink closes link to peer and sets it to nil.
func (p *Node) CloseLink(ctx context.Context) {
	p.linkMu.Lock()
	link := p.link
	p.link = nil
	p.dialing = nil // XXX what if dialing is in progress?
	p.linkMu.Unlock()

	if link != nil {
		log.Infof(ctx, "%v: closing link", link)
		err := link.Close()
		if err != nil {
			log.Error(ctx, err)
		}
	}

}

// dial does low-level work to dial peer
// XXX p.* reading without lock - ok?
// XXX app.MyInfo without lock - ok?
func (p *Node) dial(ctx context.Context) (_ *neonet.NodeLink, err error) {
	defer task.Runningf(&ctx, "connect %s", p.UUID)(&err)	// XXX "connect" good word here?

	app := p.nodeTab.nodeApp
	link, accept, err := app.Dial(ctx, p.Type, p.Addr.String())
	if err != nil {
		return nil, err
	}

	// verify peer identifies as what we expect
	switch {
	// type is already checked by app.Dial

	case accept.MyUUID != p.UUID:
		err = fmt.Errorf("connected, but peer's uuid is not %v (identifies as %v)", p.UUID, accept.MyUUID)

	case accept.YourUUID != app.MyInfo.UUID:
		err = fmt.Errorf("connected, but peer gives us uuid %v (our is %v)", accept.YourUUID, app.MyInfo.UUID)

	// XXX Node.Dial is currently used by Client only.
	// XXX For Client it would be not correct to check #partition only at
	// XXX connection time, but it has to be also checked after always as every
	// XXX operation could coincide with cluster reconfiguration.
	//
	// FIXME for now we simply don't check N(p)
	//
	// XXX NumReplicas: neo/py meaning for n(replica) = `n(real-replica) - 1`
	/*
	case !(accept.NumPartitions == 1 && accept.NumReplicas == 0):
		err = fmt.Errorf("connected but TODO peer works with !1x1 partition table.")
	*/
	}

	if err != nil {
		//log.Errorif(ctx, link.Close())
		lclose(ctx, link)
		link = nil
	}

	return link, err
}

// even if dialing a peer failed, we'll attempt redial after this timeout
const δtRedial = 3 * time.Second

// dialed is result of dialing a peer.
type dialed struct {
	link	*neonet.NodeLink
	err	error
	ready	chan struct{}
}

// Dial establishes link to peer node.
//
// If the link was not yet established Dial dials the peer appropriately,
// handshakes, requests identification and checks that identification reply is
// as expected.
//
// Several Dial calls may be done in parallel - in any case only 1 link-level
// dial will be made and others will share established link.
//
// In case Dial returns an error - future Dial will attempt to reconnect with
// "don't reconnect too fast" throttling.
func (p *Node) Dial(ctx context.Context) (*neonet.NodeLink, error) {
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
			return dialing.link, dialing.err
		}
	}

	// otherwise this goroutine becomes responsible for (re)dialing the peer

	// XXX p.State != RUNNING
	// XXX p.Addr  != ""

	dialT := p.dialT
	dialing := &dialed{ready: make(chan struct{})}
	p.dialing = dialing
	p.linkMu.Unlock()

	go func() {
		link, err := func() (*neonet.NodeLink, error) {
			// throttle redialing if too fast
			δt := time.Now().Sub(dialT)
			if δt < δtRedial && !dialT.IsZero() {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()

				case <-time.After(δtRedial - δt):
					// ok
				}
			}

			link, err := p.dial(ctx)
			dialT = time.Now()
			return link, err
		}()

		p.linkMu.Lock()
		p.link = link
		p.dialT = dialT
		p.dialing = nil
		p.linkMu.Unlock()

		dialing.link = link
		dialing.err = err
		close(dialing.ready)
	}()

	<-dialing.ready
	return dialing.link, dialing.err
}

// Conn returns conn to the peer.	XXX -> DialConn ?
//
// If there is no link established - conn first dials peer (see Dial).
//
// For established link Conn either creates new connection over the link,
// XXX (currently inactive) or gets one from the pool of unused connections (see PutConn).
func (p *Node) Conn(ctx context.Context) (*neonet.Conn, error) {
	var err error

/*
	p.linkMu.Lock()
	if l := len(p.connPool); l > 0 {
		conn := p.connPool[l-1]
		p.connPool = p.connPool[:l-1]
		p.linkMu.Unlock()
		return conn, nil
	}
*/

	// connection poll is empty - let's create new connection from .link
	link := p.link
	p.linkMu.Unlock()

	// we might need to (re)dial
	if link == nil {
		link, err = p.Dial(ctx)
		if err != nil {
			return nil, err
		}
	}

	return link.NewConn()
}

/*
// PutConn saves c in the pool of unused connections.
//
// Since connections saved into pool can be reused by other code, after
// PutConn call the caller must not use the connection directly.
//
// PutConn ignores connections not created for current peer link.
func (p *Peer) PutConn(c *Conn) {
	p.linkMu.Lock()

	// NOTE we can't panic on p.link != c.Dial() - reason is: p.link can change on redial
	if p.link == c.Dial() {
		p.connPool = append(p.connPool, c)
	}

	p.linkMu.Unlock()
}
*/
