// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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
// NEO test events and their routing to be used under tracetest.

import (
	"fmt"
	"net"
	"sync"

	"lab.nexedi.com/kirr/neo/go/xcommon/xtracing/tracetest"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

// ---- events ----

// NOTE to ease testing we use strings only to reprsent addresses or where
// event happenned - not e.g. net.Addr or *NodeTab.

// xnet.TraceConnect
// event: network connection was made
type eventNetConnect struct {
	Src, Dst string
	Dialed   string
}

// xnet.TraceListen
// event: node starts listening
//
// XXX  we don't actually need this event - nodes always start with already provided listener
// TODO -> remove.
type eventNetListen struct {
	Laddr	string
}

// event: tx via neo.Conn
type eventNeoSend struct {
	Src, Dst string
	ConnID   uint32
	Msg	 proto.Msg
}

// event: cluster state changed
type eventClusterState struct {
	//Ptr   *neo.ClusterState // pointer to variable which holds the state
	Where string
	State proto.ClusterState
}

func clusterState(where string, v proto.ClusterState) *eventClusterState {
	return &eventClusterState{where, v}
}

// event: nodetab entry changed
type eventNodeTab struct {
	//NodeTab  unsafe.Pointer	// *neo.NodeTable XXX not to noise test diff
	Where    string		// host of running node	XXX ok? XXX -> TabName?
	NodeInfo proto.NodeInfo
}

// event: master ready to start changed
type eventMStartReady struct {
	//Master  unsafe.Pointer // *Master XXX not to noise test diff
	Where   string		// host (XXX name) of running node
	Ready   bool
}

func masterStartReady(where string, ready bool) *eventMStartReady {
	//return &eventMStartReady{unsafe.Pointer(m), ready}
	return &eventMStartReady{where, ready}
}

// TODO eventPartTab

// ---- shortcuts ----

// shortcut for net connect event
func netconnect(src, dst, dialed string) *eventNetConnect {
	return &eventNetConnect{Src: src, Dst: dst, Dialed: dialed}
}

func netlisten(laddr string) *eventNetListen {
	return &eventNetListen{Laddr: laddr}
}

// shortcut for net tx event over nodelink connection
func conntx(src, dst string, connid uint32, msg proto.Msg) *eventNeoSend {
	return &eventNeoSend{Src: src, Dst: dst, ConnID: connid, Msg: msg}
}

// shortcut for address
func xnaddr(addr string) proto.Address {
	if addr == "" {
		return proto.Address{}
	}
	// during testing we pretend addr is always of host:port form
	a, err := proto.AddrString("tcp", addr)
	if err != nil {
		panic(err)
	}
	return a
}

// shortcut for NodeInfo
func nodei(laddr string, typ proto.NodeType, num int32, state proto.NodeState, idtime proto.IdTime) proto.NodeInfo {
	return proto.NodeInfo{
		Type:   typ,
		Addr:   xnaddr(laddr),
		UUID:   proto.UUID(typ, num),
		State:  state,
		IdTime: idtime,
	}
}

// shortcut for nodetab change
func δnode(where string, laddr string, typ proto.NodeType, num int32, state proto.NodeState, idtime proto.IdTime) *eventNodeTab {
	return &eventNodeTab{
		Where:    where,
		NodeInfo: nodei(laddr, typ, num, state, idtime),
	}
}


// ---- events routing ----

// EventRouter implements NEO-specific routing of events to trace test channels.
//
// A test has to define routing rules using BranchNode, BranchState and BranchLink XXX
type EventRouter struct {
	mu sync.Mutex

	defaultq *tracetest.SyncChan

	// events specific to particular node - e.g. node starts listening,
	// state on that node changes, etc...
	byNode map[string /*host*/]*tracetest.SyncChan

	// state on host changes. Takes precendece over byNode.
	//
	// XXX not needed? ( I was once considering state change events on C to
	//	be routed to MC, because state change on C is due to M sends.
	//	However everything is correct if we put those C state changes on to
	//	byNode("C") and simply verify events on tMC and then tC in that order.
	//	keeping events local to C on tC, not tMC helps TestCluster to
	//	organize trace channels in uniform way )
	byState map[string /*host*/]*tracetest.SyncChan

	// event on a-b link
	byLink map[string /*host-host*/]*linkDst

	// who connected who, so that it is possible to determine by looking at
	// connID who initiated the exchange.
	connected map[string /*addr-addr*/]bool
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		defaultq:	tracetest.NewSyncChan("default"),
		byNode:		make(map[string]*tracetest.SyncChan),
		byState:	make(map[string]*tracetest.SyncChan),
		byLink:		make(map[string]*linkDst),
		connected:	make(map[string]bool),
	}
}

func (r *EventRouter) AllRoutes() []*tracetest.SyncChan {
	rtset := map[*tracetest.SyncChan]int{}
	rtset[r.defaultq] = 1
	for _, dst := range r.byNode {
		rtset[dst] = 1
	}
	for _, dst := range r.byState {
		rtset[dst] = 1
	}
	for _, ldst := range r.byLink {
		rtset[ldst.a] = 1
		rtset[ldst.b] = 1
	}

	var rtv []*tracetest.SyncChan
	for dst := range rtset {
		rtv = append(rtv, dst)
	}
	return rtv
}

// hostport splits addr of for "host:port" into host and port.
//
// if the address has not the specified form returned are:
// - host = addr
// - port = ""
func hostport(addr string) (host string, port string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}

// host returns hostname-only part from addr.
//
// see also hostport
func host(addr string) string {
	host, _ := hostport(addr)
	return host
}

// Route routes events according to rules specified via Branch*().
func (r *EventRouter) Route(event interface{}) (dst *tracetest.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch ev := event.(type) {
	// networking
	case *eventNetListen:
		dst = r.byNode[host(ev.Laddr)]

	case *eventNetConnect:
		link := host(ev.Src) + "-" + host(ev.Dst)
		ldst := r.byLink[link]
		if ldst != nil {
			dst = ldst.a
		}

		// remember who dialed and who was listening so that when
		// seeing eventNeoSend we can determine by connID who initiated
		// the exchange.
		//
		// remember with full host:port addresses, since potentially
		// there can be several connections and a->b and a<-b at the
		// same time. Having port around will allow to see which one it
		// actually is.
		r.connected[ev.Src + "-" + ev.Dst] = true

	case *eventNeoSend:
		var ldst *linkDst

		// find out link and cause dst according to ConnID and who connected to who
		a, b := host(ev.Src), host(ev.Dst)
		switch {
		case r.connected[ev.Src + "-" + ev.Dst]:
			ldst = r.byLink[a+"-"+b]

		case r.connected[ev.Dst + "-" + ev.Src]:
			ldst = r.byLink[b+"-"+a]

		default:
			// FIXME bad - did not seen connect
			panic("TODO")
		}

		if ldst == nil {
			break // link not branched
		}

		// now as ldst.a corresponds to who was dialer and ldst.b
		// corresponds to who was listener, we can route by ConnID.
		// (see neo.newNodeLink for details)
		if ev.ConnID % 2 == 1 {
			dst = ldst.a
		} else {
			dst = ldst.b
		}

	// state changes
	case *eventNodeTab:
		dst = r.routeState(ev.Where)

	case *eventClusterState:
		dst = r.routeState(ev.Where)

	case *eventMStartReady:
		dst = r.routeState(ev.Where)
	}

	if dst == nil {
		dst = r.defaultq
	}
	return dst
}

// routeState routes event corresponding to state change on host
func (r *EventRouter) routeState(host string) (dst *tracetest.SyncChan) {
	// lookup dst by state rules
	dst = r.byState[host]
	if dst != nil {
		return dst
	}

	// fallback to by node rules
	return r.byNode[host]
}

// BranchNode branches events corresponding to host.
func (r *EventRouter) BranchNode(host string, dst *tracetest.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, already := r.byNode[host]; already {
		panic(fmt.Sprintf("event router: node %q already branched", host))
	}

	r.byNode[host] = dst
}

// BranchState branches events corresponding to state changes on host.
//
// XXX not needed?
func (r *EventRouter) BranchState(host string, dst *tracetest.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, already := r.byState[host]; already {
		panic(fmt.Sprintf("event router: state on node %q already branched", host))
	}

	r.byState[host] = dst
}

// BranchLink branches events corresponding to link in between a-b.
//
// Link should be of "a-b" form with b listening and a dialing.
//
// Event with networking cause root coming from a go to dsta, and with
// networking cause root coming from b - go to dstb.
func (r *EventRouter) BranchLink(link string, dsta, dstb *tracetest.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, already := r.byLink[link]; already {
		panic(fmt.Sprintf("event router: link %q already branched", link))
	}

	// XXX verify b-a not registered too ?

	r.byLink[link] = &linkDst{dsta, dstb}
}

// linkDst represents destination for events on a network link.
//
// Events go to either a or b depending on which side initiated particular
// connection on top of the link.
type linkDst struct {
	a *tracetest.SyncChan // net cause was on dialer
	b *tracetest.SyncChan // net cause was on listener
}

