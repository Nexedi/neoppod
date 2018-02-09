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
// test interaction between nodes

//go:generate gotrace gen .

import (
	//"bytes"
	"context"
	//"crypto/sha1"
	//"io"
	"net"
	//"reflect"
	"sync"
	"testing"
//	"unsafe"

	"golang.org/x/sync/errgroup"

	//"github.com/kylelemons/godebug/pretty"

	"lab.nexedi.com/kirr/neo/go/neo"
	//"lab.nexedi.com/kirr/neo/go/neo/client"
	//"lab.nexedi.com/kirr/neo/go/neo/internal/common"

	//"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/neo/go/xcommon/xtracing/tsync"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/tracing"
	//"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xnet/pipenet"

	"fmt"
	//"time"
)

// ---- events used in tests ----

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
type eventNetListen struct {
	Laddr	string
}

// event: tx via neo.Conn
type eventNeoSend struct {
	Src, Dst string
	ConnID   uint32
	Msg	 neo.Msg
}

// event: cluster state changed
type eventClusterState struct {
	//Ptr   *neo.ClusterState // pointer to variable which holds the state
	Where string
	State neo.ClusterState
}

func clusterState(where string, v neo.ClusterState) *eventClusterState {
	return &eventClusterState{where, v}
}

// event: nodetab entry changed
type eventNodeTab struct {
	//NodeTab  unsafe.Pointer	// *neo.NodeTable XXX not to noise test diff
	Where    string		// host of running node	XXX ok? XXX -> TabName?
	NodeInfo neo.NodeInfo
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

// ---- events routing ----

// EventRouter implements NEO-specific routing of events to trace test channels.
type EventRouter struct {
	mu sync.Mutex

	defaultq *tsync.SyncChan

	// events specific to particular node - e.g. node starts listening
	byNode map[string /*host*/]*tsync.SyncChan

	byLink map[string /*host-host*/]*linkDst

	connected map[string /*addr-addr*/]bool
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		defaultq:	tsync.NewSyncChan("default"),
		byNode:		make(map[string]*tsync.SyncChan),
		byLink:		make(map[string]*linkDst),
		connected:	make(map[string]bool),
	}
}

func (r *EventRouter) AllRoutes() []*tsync.SyncChan {
	rtset := map[*tsync.SyncChan]int{}
	rtset[r.defaultq] = 1
	for _, dst := range r.byNode {
		rtset[dst] = 1
	}
	for _, ldst := range r.byLink {
		rtset[ldst.a] = 1
		rtset[ldst.b] = 1
	}

	var rtv []*tsync.SyncChan
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

// Route routes events according to rules specified via Branch*()
func (r *EventRouter) Route(event interface{}) (dst *tsync.SyncChan) {
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

		if ev.ConnID % 2 == 1 {
			dst = ldst.a
		} else {
			dst = ldst.b
		}

	// state changes
	case *eventNodeTab:
		dst = r.byNode[ev.Where]

	case *eventClusterState:
		dst = r.byNode[ev.Where]

	case *eventMStartReady:
		dst = r.byNode[ev.Where]
	}

	if dst == nil {
		dst = r.defaultq
	}
	return dst
}

// XXX doc
func (r *EventRouter) BranchNode(host string, dst *tsync.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, already := r.byNode[host]; already {
		panic(fmt.Sprintf("event router: node %q already branched", host))
	}

	r.byNode[host] = dst
}

// BranchLink branches events corresponding to link in between a-b.
//
// Link should be of "a-b" form with b listening and a dialing.
//
// Event with networkiing cause root coming from a go to dsta, and with
// networking cause root coming from b - go to dstb.
func (r *EventRouter) BranchLink(link string, dsta, dstb *tsync.SyncChan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, already := r.byLink[link]; already {
		panic(fmt.Sprintf("event router: link %q already branched", link))
	}

	// XXX verify b-a not registered too ?

	r.byLink[link] = &linkDst{dsta, dstb}
}

type linkDst struct {
	a *tsync.SyncChan // net cause was on dialer
	b *tsync.SyncChan // net cause was on listener
}

// ---- trace probes, etc -> events -> dispatcher ----

// TraceCollector connects to NEO-specific trace points via probes and sends events to dispatcher.
type TraceCollector struct {
	pg *tracing.ProbeGroup
	d  *tsync.EventDispatcher

	node2Name	   map[*neo.NodeApp]string
	nodeTab2Owner	   map[*neo.NodeTable]string
	clusterState2Owner map[*neo.ClusterState]string
}

func NewTraceCollector(dispatch *tsync.EventDispatcher) *TraceCollector {
	return &TraceCollector{
		pg:	&tracing.ProbeGroup{},
		d:	dispatch,

		node2Name:		make(map[*neo.NodeApp]string),
		nodeTab2Owner:		make(map[*neo.NodeTable]string),
		clusterState2Owner:	make(map[*neo.ClusterState]string),
	}
}

//trace:import "lab.nexedi.com/kirr/neo/go/neo"

// Attach attaches the tracer to appropriate trace points.
func (t *TraceCollector) Attach() {
	tracing.Lock()
	//neo_traceMsgRecv_Attach(t.pg, t.traceNeoMsgRecv)
	neo_traceMsgSendPre_Attach(t.pg, t.traceNeoMsgSendPre)
	neo_traceClusterStateChanged_Attach(t.pg, t.traceClusterState)
	neo_traceNodeChanged_Attach(t.pg, t.traceNode)
	traceMasterStartReady_Attach(t.pg, t.traceMasterStartReady)
	tracing.Unlock()
}

func (t *TraceCollector) Detach() {
	t.pg.Done()
}

// RegisterNode lets the tracer know ptr-to-node-state -> node name relation.
//
// This way it can translate e.g. *NodeTable -> owner node name when creating
// corresponding event.
func (t *TraceCollector) RegisterNode(node *neo.NodeApp, name string) {
	tracing.Lock()
	defer tracing.Unlock()

	// XXX verify there is no duplicate names
	// XXX verify the same pointer is not registerd twice
	t.node2Name[node] = name
	t.nodeTab2Owner[node.NodeTab] = name
	t.clusterState2Owner[&node.ClusterState] = name
}


func (t *TraceCollector) TraceNetConnect(ev *xnet.TraceConnect)	{
	t.d.Dispatch(&eventNetConnect{
		Src:	ev.Src.String(),
		Dst:	ev.Dst.String(),
		Dialed: ev.Dialed,
	})
}

func (t *TraceCollector) TraceNetListen(ev *xnet.TraceListen)	{
	t.d.Dispatch(&eventNetListen{Laddr: ev.Laddr.String()})
}

func (t *TraceCollector) TraceNetTx(ev *xnet.TraceTx)		{} // we use traceNeoMsgSend instead

func (t *TraceCollector) traceNeoMsgSendPre(l *neo.NodeLink, connID uint32, msg neo.Msg) {
	t.d.Dispatch(&eventNeoSend{l.LocalAddr().String(), l.RemoteAddr().String(), connID, msg})
}

func (t *TraceCollector) traceClusterState(cs *neo.ClusterState) {
	//t.d.Dispatch(&eventClusterState{cs, *cs})
	where := t.clusterState2Owner[cs]
	t.d.Dispatch(&eventClusterState{where, *cs})
}

func (t *TraceCollector) traceNode(nt *neo.NodeTable, n *neo.Node) {
	//t.d.Dispatch(&eventNodeTab{unsafe.Pointer(nt), n.NodeInfo})
	where := t.nodeTab2Owner[nt]
	t.d.Dispatch(&eventNodeTab{where, n.NodeInfo})
}

func (t *TraceCollector) traceMasterStartReady(m *Master, ready bool) {
	//t.d.Dispatch(masterStartReady(m, ready))
	where := t.node2Name[m.node]
	t.d.Dispatch(&eventMStartReady{where, ready})
}

// ----------------------------------------

// M drives cluster with 1 S & C through recovery -> verification -> service -> shutdown
func TestMasterStorage(t *testing.T) {
	rt	 := NewEventRouter()
	dispatch := tsync.NewEventDispatcher(rt)
	tracer   := NewTraceCollector(dispatch)

	net := pipenet.New("testnet")	// test network

	tracer.Attach()
	defer tracer.Detach()

	// by default events go to g
	//g := tsync.NewEventChecker(t, dispatch, rt.defaultq)



	// shortcut for addresses
	xaddr := func(addr string) *pipenet.Addr {
		a, err := net.ParseAddr(addr)
		exc.Raiseif(err)
		return a
	}
	xnaddr := func(addr string) neo.Address {
		if addr == "" {
			return neo.Address{}
		}
		a, err := neo.Addr(xaddr(addr))
		exc.Raiseif(err)
		return a
	}

	// // shortcut for net tx event
	// // XXX -> NetTx ?
	// nettx := func(src, dst, pkt string) *xnet.TraceTx {
	// 	return &xnet.TraceTx{Src: xaddr(src), Dst: xaddr(dst), Pkt: []byte(pkt)}
	// }

	// shortcut for net connect event
	// XXX -> NetConnect ?
	//netconnect := func(src, dst, dialed string) *xnet.TraceConnect {
	//	return &xnet.TraceConnect{Src: xaddr(src), Dst: xaddr(dst), Dialed: dialed}
	//}
	netconnect := func(src, dst, dialed string) *eventNetConnect {
		return &eventNetConnect{Src: src, Dst: dst, Dialed: dialed}
	}

	//netlisten := func(laddr string) *xnet.TraceListen {
	//	return &xnet.TraceListen{Laddr: xaddr(laddr)}
	//}

	netlisten := func(laddr string) *eventNetListen {
		return &eventNetListen{Laddr: laddr}
	}

	// shortcut for net tx event over nodelink connection
	//conntx := func(src, dst string, connid uint32, msg neo.Msg) *eventNeoSend {
	//	return &eventNeoSend{Src: xaddr(src), Dst: xaddr(dst), ConnID: connid, Msg: msg}
	//}
	conntx := func(src, dst string, connid uint32, msg neo.Msg) *eventNeoSend {
		return &eventNeoSend{Src: src, Dst: dst, ConnID: connid, Msg: msg}
	}

	// shortcut for NodeInfo
	nodei := func(laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtime neo.IdTime) neo.NodeInfo {
		return neo.NodeInfo{
			Type:   typ,
			Addr:   xnaddr(laddr),
			UUID:   neo.UUID(typ, num),
			State:  state,
			IdTime: idtime,
		}
	}

	// shortcut for nodetab change
//	node := func(x *neo.NodeApp, laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtime neo.IdTime) *eventNodeTab {
//		return &eventNodeTab{
//			NodeTab:  unsafe.Pointer(x.NodeTab),
//			NodeInfo: nodei(laddr, typ, num, state, idtime),
//		}
//	}

	node := func(where string, laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtime neo.IdTime) *eventNodeTab {
		return &eventNodeTab{
			Where:    where,
			NodeInfo: nodei(laddr, typ, num, state, idtime),
		}
	}

	// XXX -> M = testenv.NewMaster("m")  (mkhost, chan, register to tracer ...)
	// XXX ----//---- S, C

	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)
//	Chost := xnet.NetTrace(net.Host("c"), tracer)

	cM  := tsync.NewSyncChan("m.main") // trace of events local to M
	cS  := tsync.NewSyncChan("s.main") // trace of events local to S XXX with cause root also on S
	cMS := tsync.NewSyncChan("m-s")    // trace of events with cause root being m -> s send
	cSM := tsync.NewSyncChan("s-m")    // trace of events with cause root being s -> m send

	tM := tsync.NewEventChecker(t, dispatch, cM)
	tS := tsync.NewEventChecker(t, dispatch, cS)
	tMS := tsync.NewEventChecker(t, dispatch, cMS)
	tSM := tsync.NewEventChecker(t, dispatch, cSM)

	rt.BranchNode("m", cM)
	rt.BranchNode("s", cS)
	rt.BranchLink("s-m", cSM, cMS)

	// cluster nodes
	M := NewMaster("abc1", ":1", Mhost)
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")
	S := NewStorage("abc1", "m:1", ":1", Shost, zstor)

	// let tracer know how to map state addresses to node names
	tracer.RegisterNode(M.node, "m")	// XXX better Mhost.Name() ?
	tracer.RegisterNode(S.node, "s")



	gwg := &errgroup.Group{}

	// start master
	Mclock := &vclock{}
	M.monotime = Mclock.monotime
	Mctx, Mcancel := context.WithCancel(bg)
	gox(gwg, func() {
		err := M.Run(Mctx)
		fmt.Println("M err: ", err)
		exc.Raiseif(err)
	})

	// start storage
	Sctx, Scancel := context.WithCancel(bg)
	gox(gwg, func() {
		err := S.Run(Sctx)
		fmt.Println("S err: ", err)
		exc.Raiseif(err)
	})

	// >>> trace >>>

	// M starts listening
	tM.Expect(netlisten("m:1"))
	tM.Expect(node("m", "m:1", neo.MASTER, 1, neo.RUNNING, neo.IdTimeNone))
	tM.Expect(clusterState("m", neo.ClusterRecovering))

	// TODO create C; C tries connect to master - rejected ("not yet operational")

	// S starts listening
	tS.Expect(netlisten("s:1"))

	// S connects M
	tSM.Expect(netconnect("s:2", "m:2",  "m:1"))
	tSM.Expect(conntx("s:2", "m:2", 1, &neo.RequestIdentification{
		NodeType:	neo.STORAGE,
		UUID:		0,
		Address:	xnaddr("s:1"),
		ClusterName:	"abc1",
		IdTime:		neo.IdTimeNone,
	}))

	// XXX try chaning order ^ v
	tM.Expect(node("m", "s:1", neo.STORAGE, 1, neo.PENDING, 0.01))

	tSM.Expect(conntx("m:2", "s:2", 1, &neo.AcceptIdentification{
		NodeType:	neo.MASTER,
		MyUUID:		neo.UUID(neo.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourUUID:	neo.UUID(neo.STORAGE, 1),
	}))

	// TODO test ID rejects (uuid already registered, ...)

	// M starts recovery on S
	tMS.Expect(conntx("m:2", "s:2", 0, &neo.Recovery{}))
	tMS.Expect(conntx("s:2", "m:2", 0, &neo.AnswerRecovery{
		// empty new node
		PTid:		0,
		BackupTid:	neo.INVALID_TID,
		TruncateTid:	neo.INVALID_TID,
	}))

	tMS.Expect(conntx("m:2", "s:2", 2, &neo.AskPartitionTable{}))
	tMS.Expect(conntx("s:2", "m:2", 2, &neo.AnswerPartitionTable{
		PTid:		0,
		RowList:	[]neo.RowInfo{},
	}))

	// M ready to start: new cluster, no in-progress S recovery
	tM.Expect(masterStartReady("m", true))

	// <<< trace <<<

	_ = Mcancel
	_ = Scancel
	return
}

/*
	// M <- start cmd
	wg := &errgroup.Group{}
	gox(wg, func() {
		err := M.Start()
		exc.Raiseif(err)
	})

	tc.Expect(node(M.node, "s:1", neo.STORAGE, 1, neo.RUNNING, 0.01))
	xwait(wg)

	// XXX M.partTab <- S1

	// M starts verification
	tc.Expect(clusterState(&M.node.ClusterState, neo.ClusterVerifying))

	tc.Expect(conntx("m:2", "s:2", 4, &neo.SendPartitionTable{
		PTid:		1,
		RowList:	[]neo.RowInfo{
			{0, []neo.CellInfo{{neo.UUID(neo.STORAGE, 1), neo.UP_TO_DATE}}},
		},
	}))

	tc.Expect(conntx("m:2", "s:2", 6, &neo.LockedTransactions{}))
	tc.Expect(conntx("s:2", "m:2", 6, &neo.AnswerLockedTransactions{
		TidDict: nil,	// map[zodb.Tid]zodb.Tid{},
	}))

	lastOid, err1 := zstor.LastOid(bg)
	lastTid, err2 := zstor.LastTid(bg)
	exc.Raiseif(xerr.Merge(err1, err2))
	tc.Expect(conntx("m:2", "s:2", 8, &neo.LastIDs{}))
	tc.Expect(conntx("s:2", "m:2", 8, &neo.AnswerLastIDs{
		LastOid: lastOid,
		LastTid: lastTid,
	}))

	// XXX M -> S ClusterInformation(VERIFICATION) ?

	// TODO there is actually txn to finish
	// TODO S leave at verify
	// TODO S join at verify
	// TODO M.Stop() while verify

	// verification ok; M start service
	tc.Expect(clusterState(&M.node.ClusterState, neo.ClusterRunning))
	// TODO ^^^ should be sent to S

	tc.Expect(conntx("m:2", "s:2", 10, &neo.StartOperation{Backup: false}))
	tc.Expect(conntx("s:2", "m:2", 10, &neo.NotifyReady{}))


	// TODO S leave while service
	// TODO S join while service
	// TODO M.Stop while service

	// create client
	C := client.NewClient("abc1", "m:1", Chost)

	// C connects M
	tc.Expect(netconnect("c:1", "m:3",  "m:1"))
	tc.Expect(conntx("c:1", "m:3", 1, &neo.RequestIdentification{
		NodeType:	neo.CLIENT,
		UUID:		0,
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTime:		neo.IdTimeNone,
	}))

	tc.Expect(node(M.node, "", neo.CLIENT, 1, neo.RUNNING, 0.02))

	tc.Expect(conntx("m:3", "c:1", 1, &neo.AcceptIdentification{
		NodeType:	neo.MASTER,
		MyUUID:		neo.UUID(neo.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourUUID:	neo.UUID(neo.CLIENT, 1),
	}))

	// C asks M about PT
	// FIXME this might come in parallel with vvv "C <- M NotifyNodeInformation C1,M1,S1"
	tc.Expect(conntx("c:1", "m:3", 3, &neo.AskPartitionTable{}))
	tc.Expect(conntx("m:3", "c:1", 3, &neo.AnswerPartitionTable{
		PTid:		1,
		RowList:	[]neo.RowInfo{
			{0, []neo.CellInfo{{neo.UUID(neo.STORAGE, 1), neo.UP_TO_DATE}}},
		},
	}))

	// C <- M NotifyNodeInformation C1,M1,S1
	// FIXME this might come in parallel with ^^^ "C asks M about PT"
	tc.Expect(conntx("m:3", "c:1", 0, &neo.NotifyNodeInformation{
		IdTime:		neo.IdTimeNone,	// XXX ?
		NodeList:	[]neo.NodeInfo{
			nodei("m:1", neo.MASTER,  1, neo.RUNNING, neo.IdTimeNone),
			nodei("s:1", neo.STORAGE, 1, neo.RUNNING, 0.01),
			nodei("",    neo.CLIENT,  1, neo.RUNNING, 0.02),
		},
	}))

	Cnode := *(**neo.NodeApp)(unsafe.Pointer(C)) // XXX hack, fragile
	tc.Expect(node(Cnode, "m:1", neo.MASTER,  1, neo.RUNNING, neo.IdTimeNone))
	tc.Expect(node(Cnode, "s:1", neo.STORAGE, 1, neo.RUNNING, 0.01))
	tc.Expect(node(Cnode, "",    neo.CLIENT,  1, neo.RUNNING, 0.02))


	// C asks M about last tid	XXX better master sends it itself on new client connected
	wg = &errgroup.Group{}
	gox(wg, func() {
		cLastTid, err := C.LastTid(bg)
		exc.Raiseif(err)

		if cLastTid != lastTid {
			exc.Raisef("C.LastTid -> %v  ; want %v", cLastTid, lastTid)
		}
	})

	tc.Expect(conntx("c:1", "m:3", 5, &neo.LastTransaction{}))
	tc.Expect(conntx("m:3", "c:1", 5, &neo.AnswerLastTransaction{
		Tid: lastTid,
	}))

	xwait(wg)

	// C starts loading first object ...
	wg = &errgroup.Group{}
	xid1 := zodb.Xid{Oid: 1, At: zodb.TidMax}
	buf1, serial1, err := zstor.Load(bg, xid1)
	exc.Raiseif(err)
	gox(wg, func() {
		buf, serial, err := C.Load(bg, xid1)
		exc.Raiseif(err)

		if !(bytes.Equal(buf.Data, buf1.Data) && serial==serial1) {
			exc.Raisef("C.Load(%v) ->\ndata:\n%s\nserial:\n%s\n", xid1,
				pretty.Compare(buf1.Data, buf.Data), pretty.Compare(serial1, serial))
		}
	})

	// ... -> connects to S
	tc.Expect(netconnect("c:2", "s:3",  "s:1"))
	tc.Expect(conntx("c:2", "s:3", 1, &neo.RequestIdentification{
		NodeType:	neo.CLIENT,
		UUID:		neo.UUID(neo.CLIENT, 1),
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTime:		0.02,
	}))

	tc.Expect(conntx("s:3", "c:2", 1, &neo.AcceptIdentification{
		NodeType:	neo.STORAGE,
		MyUUID:		neo.UUID(neo.STORAGE, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourUUID:	neo.UUID(neo.CLIENT, 1),
	}))

	// ... -> GetObject(xid1)
	tc.Expect(conntx("c:2", "s:3", 3, &neo.GetObject{
		Oid:	xid1.Oid,
		Tid:	common.At2Before(xid1.At),
		Serial: neo.INVALID_TID,
	}))
	tc.Expect(conntx("s:3", "c:2", 3, &neo.AnswerObject{
		Oid:		xid1.Oid,
		Serial:		serial1,
		NextSerial:	neo.INVALID_TID,
		Compression:	false,
		Data:		buf1,
		DataSerial:	0,		// XXX
		Checksum:	sha1.Sum(buf1.Data),
	}))

	xwait(wg)

	// verify NextSerial is properly returned in AnswerObject via trace-loading prev. revision of obj1
	// (XXX we currently need NextSerial for neo/py client cache)
	wg = &errgroup.Group{}
	xid1prev := zodb.Xid{Oid: 1, At: serial1 - 1}
	buf1prev, serial1prev, err := zstor.Load(bg, xid1prev)
	exc.Raiseif(err)
	gox(wg, func() {
		buf, serial, err := C.Load(bg, xid1prev)
		exc.Raiseif(err)

		if !(bytes.Equal(buf.Data, buf1prev.Data) && serial==serial1prev) {
			exc.Raisef("C.Load(%v) ->\ndata:\n%s\nserial:\n%s\n", xid1prev,
				pretty.Compare(buf1prev.Data, buf.Data), pretty.Compare(serial1prev, serial))
		}
	})

	// ... -> GetObject(xid1prev)
	tc.Expect(conntx("c:2", "s:3", 5, &neo.GetObject{
		Oid:	xid1prev.Oid,
		Tid:	serial1,
		Serial: neo.INVALID_TID,
	}))
	tc.Expect(conntx("s:3", "c:2", 5, &neo.AnswerObject{
		Oid:		xid1prev.Oid,
		Serial:		serial1prev,
		NextSerial:	serial1,
		Compression:	false,
		Data:		buf1prev,
		DataSerial:	0,		// XXX
		Checksum:	sha1.Sum(buf1prev.Data),
	}))

	xwait(wg)


	// C loads every other {<,=}serial:oid - established link is reused
	ziter := zstor.Iterate(bg, 0, zodb.TidMax)

	// XXX hack: disable tracing early so that C.Load() calls do not deadlock
	// TODO refactor cluster creation into func
	// TODO move client all loading tests into separate test where tracing will be off
	pg.Done()

	for {
		_, dataIter, err := ziter.NextTxn(bg)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ziter.NextTxn: %v", err)
		}

		for {
			datai, err := dataIter.NextData(bg)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ziter.NextData: %v", err)
			}

			// TODO also test GetObject(tid=ø, serial=...) which originate from loadSerial on py side
			xid := zodb.Xid{Oid: datai.Oid, At: datai.Tid}

			buf, serial, err := C.Load(bg, xid)
			if datai.Data != nil {
				if !(bytes.Equal(buf.Data, datai.Data) && serial == datai.Tid && err == nil) {
					t.Fatalf("load: %v:\nhave: %v %v %q\nwant: %v nil %q",
						xid, serial, err, buf.Data, datai.Tid, datai.Data)
				}
			} else {
				// deleted
				errWant := &zodb.OpError{
					URL:  C.URL(),
					Op:   "load",
					Args: xid,
					Err:  &zodb.NoDataError{Oid: xid.Oid, DeletedAt: datai.Tid},
				}
				if !(buf == nil && serial == 0 && reflect.DeepEqual(err, errWant)) {
					t.Fatalf("load: %v ->\nbuf:\n%s\nserial:\n%s\nerr:\n%s\n", xid,
						pretty.Compare(nil, buf),
						pretty.Compare(0, serial),
						pretty.Compare(errWant, err))
				}
			}
		}
	}





	// TODO S.Stop() or Scancel()
	// expect:
	// M.nodeTab -= S
	// M.clusterState	<- RECOVERY
	// ...

	// TODO Scancel -> S down - test how M behaves

	// TODO test M.recovery starting back from verification/service
	// (M needs to resend to all storages recovery messages just from start)

	time.Sleep(100*time.Millisecond) // XXX temp so net tx'ers could actually tx
	return

	Mcancel()	// FIXME ctx cancel not fully handled
	Scancel()	// ---- // ----
	xwait(gwg)
}
*/


/*
func benchmarkGetObject(b *testing.B, Mnet, Snet, Cnet xnet.Networker, benchit func(xcload1 func())) {
	// create test cluster	<- XXX factor to utility func
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")

	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)
	defer wg.Wait()
	defer cancel()

	// spawn M
	M := NewMaster("abc1", "", Mnet)

	// XXX to wait for "M listens at ..." & "ready to start" -> XXX add something to M api?
	tracer := &TraceRouter{tsync.NewSyncChan()}
	tc := tsync.NewEventChecker(b, tracer.SyncChan)
	pg := &tracing.ProbeGroup{}
	tracing.Lock()
	pnode := neo_traceNodeChanged_Attach(nil, tracer.traceNode)
	traceMasterStartReady_Attach(pg, tracer.traceMasterStartReady)
	tracing.Unlock()

	wg.Go(func() error {
		return M.Run(ctx)
	})

	// determing M serving address	XXX better with M api
	ev := tracer.Recv()
	mnode, ok := ev.Event.(*eventNodeTab)
	if !ok {
		b.Fatal("after M start: got %T  ; want eventNodeTab", ev.Event)
	}
	Maddr := mnode.NodeInfo.Addr.String()

	tracing.Lock()
	pnode.Detach()
	tracing.Unlock()
	ev.Ack()

	// now after we know Maddr create S & C and start S serving
	S := NewStorage("abc1", Maddr, "", Snet, zstor)
	C := client.NewClient("abc1", Maddr, Cnet)

	wg.Go(func() error {
		return S.Run(ctx)
	})

	// command M to start
	tc.Expect(masterStartReady(M, true))	// <- XXX better with M api
	pg.Done()

	err := M.Start()
	if err != nil {
		b.Fatal(err)
	}

	xid1 := zodb.Xid{Oid: 1, At: zodb.TidMax}

	buf1, serial1, err := zstor.Load(ctx, xid1)
	if err != nil {
		b.Fatal(err)
	}

	// C.Load(xid1)
	xcload1 := func() {
		cbuf1, cserial1, err := C.Load(ctx, xid1)
		if err != nil {
			b.Fatal(err)
		}

		if !(bytes.Equal(cbuf1.Data, buf1.Data) && cserial1 == serial1) {
			b.Fatalf("C.Load first -> %q %v  ; want %q %v", cbuf1.Data, cserial1, buf1.Data, serial1)
		}

		cbuf1.Release()
	}

	// do first C.Load - this also implicitly waits for M & S to come up
	// and C to connect to M and S.
	xcload1()

	// now start the benchmark
	b.ResetTimer()

	benchit(xcload1)
}

func benchmarkGetObjectSerial(b *testing.B, Mnet, Snet, Cnet xnet.Networker) {
	benchmarkGetObject(b, Mnet, Snet, Cnet, func(xcload1 func()) {
		for i := 0; i < b.N; i++ {
			xcload1()
		}
	})
}

func benchmarkGetObjectParallel(b *testing.B, Mnet, Snet, Cnet xnet.Networker) {
	benchmarkGetObject(b, Mnet, Snet, Cnet, func(xcload1 func()) {
		//println()
		b.SetParallelism(32) // we are io-bound
		b.RunParallel(func(pb *testing.PB) {
			//print(".")
			for pb.Next() {
				xcload1()
			}
		})
	})
}

func BenchmarkGetObjectNetPipe(b *testing.B) {
	net := pipenet.New("testnet")
	Mhost := net.Host("m")
	Shost := net.Host("s")
	Chost := net.Host("c")
	benchmarkGetObjectSerial(b, Mhost, Shost, Chost)
}

func BenchmarkGetObjectNetPipeParallel(b *testing.B) {
	net := pipenet.New("testnet")
	Mhost := net.Host("m")
	Shost := net.Host("s")
	Chost := net.Host("c")
	benchmarkGetObjectParallel(b, Mhost, Shost, Chost)
}

func BenchmarkGetObjectTCPlo(b *testing.B) {
	net := xnet.NetPlain("tcp")
	benchmarkGetObjectSerial(b, net, net, net)
}

func BenchmarkGetObjectTCPloParallel(b *testing.B) {
	net := xnet.NetPlain("tcp")
	benchmarkGetObjectParallel(b, net, net, net)
}
*/
