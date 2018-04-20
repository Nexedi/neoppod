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
// test interaction between nodes

//go:generate gotrace gen .

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"reflect"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/kylelemons/godebug/pretty"

	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/neo/storage"

	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/neo/go/xcommon/xtracing/tracetest"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/tracing"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xnet/pipenet"

	"time"
)

// ---- trace probes, etc -> events -> dispatcher ----

// TraceCollector connects to NEO-specific trace points via probes and sends events to dispatcher.
type TraceCollector struct {
	pg *tracing.ProbeGroup
	d  interface { Dispatch(interface{}) }

	node2Name	   map[*NodeApp]string
	nodeTab2Owner	   map[*NodeTable]string
	clusterState2Owner map[*proto.ClusterState]string
}

func NewTraceCollector(dispatch interface { Dispatch(interface{}) }) *TraceCollector {
	return &TraceCollector{
		pg:	&tracing.ProbeGroup{},
		d:	dispatch,

		node2Name:		make(map[*NodeApp]string),
		nodeTab2Owner:		make(map[*NodeTable]string),
		clusterState2Owner:	make(map[*proto.ClusterState]string),
	}
}

//trace:import "lab.nexedi.com/kirr/neo/go/neo/neonet"
//trace:import "lab.nexedi.com/kirr/neo/go/neo/proto"

// Attach attaches the tracer to appropriate trace points.
func (t *TraceCollector) Attach() {
	tracing.Lock()
	//neo_traceMsgRecv_Attach(t.pg, t.traceNeoMsgRecv)
	neonet_traceMsgSendPre_Attach(t.pg, t.traceNeoMsgSendPre)
	proto_traceClusterStateChanged_Attach(t.pg, t.traceClusterState)
	traceNodeChanged_Attach(t.pg, t.traceNode)
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
func (t *TraceCollector) RegisterNode(node *NodeApp, name string) {
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

func (t *TraceCollector) traceNeoMsgSendPre(l *neonet.NodeLink, connID uint32, msg proto.Msg) {
	t.d.Dispatch(&eventNeoSend{l.LocalAddr().String(), l.RemoteAddr().String(), connID, msg})
}

func (t *TraceCollector) traceClusterState(cs *proto.ClusterState) {
	//t.d.Dispatch(&eventClusterState{cs, *cs})
	where := t.clusterState2Owner[cs]
	t.d.Dispatch(&eventClusterState{where, *cs})
}

func (t *TraceCollector) traceNode(nt *NodeTable, n *Node) {
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

// test-wrapper around Storage - to automatically listen by address, not provided listener.
type tStorage struct {
	*Storage
	serveAddr string
}

func tNewStorage(clusterName, masterAddr, serveAddr string, net xnet.Networker, back storage.Backend) *tStorage {
	return &tStorage{
		Storage:   NewStorage(clusterName, masterAddr, net, back),
		serveAddr: serveAddr,
	}
}

func (s *tStorage) Run(ctx context.Context) error {
	l, err := s.node.Net.Listen(s.serveAddr)
	if err != nil {
		return err
	}

	return s.Storage.Run(ctx, l)
}

// test-wrapper around Master - to automatically listen by address, not provided listener.
type tMaster struct {
	*Master
	serveAddr string
}

func tNewMaster(clusterName, serveAddr string, net xnet.Networker) *tMaster {
	return &tMaster{
		Master:   NewMaster(clusterName, net),
		serveAddr: serveAddr,
	}
}

func (m *tMaster) Run(ctx context.Context) error {
	l, err := m.node.Net.Listen(m.serveAddr)
	if err != nil {
		return err
	}

	return m.Master.Run(ctx, l)
}



// ----------------------------------------

// M drives cluster with 1 S & C through recovery -> verification -> service -> shutdown
func TestMasterStorage(t *testing.T) {
	rt	 := NewEventRouter()
	dispatch := tracetest.NewEventDispatcher(rt)
	tracer   := NewTraceCollector(dispatch)

	net := pipenet.New("testnet")	// test network

	tracer.Attach()
	defer tracer.Detach()

	// shortcut for addresses
	xaddr := func(addr string) *pipenet.Addr {
		a, err := net.ParseAddr(addr)
		exc.Raiseif(err)
		return a
	}
	xnaddr := func(addr string) proto.Address {
		if addr == "" {
			return proto.Address{}
		}
		a, err := proto.Addr(xaddr(addr))
		exc.Raiseif(err)
		return a
	}

	// shortcut for net connect event
	netconnect := func(src, dst, dialed string) *eventNetConnect {
		return &eventNetConnect{Src: src, Dst: dst, Dialed: dialed}
	}

	netlisten := func(laddr string) *eventNetListen {
		return &eventNetListen{Laddr: laddr}
	}

	// shortcut for net tx event over nodelink connection
	conntx := func(src, dst string, connid uint32, msg proto.Msg) *eventNeoSend {
		return &eventNeoSend{Src: src, Dst: dst, ConnID: connid, Msg: msg}
	}

	// shortcut for NodeInfo
	nodei := func(laddr string, typ proto.NodeType, num int32, state proto.NodeState, idtime proto.IdTime) proto.NodeInfo {
		return proto.NodeInfo{
			Type:   typ,
			Addr:   xnaddr(laddr),
			UUID:   proto.UUID(typ, num),
			State:  state,
			IdTime: idtime,
		}
	}

	// shortcut for nodetab change
	node := func(where string, laddr string, typ proto.NodeType, num int32, state proto.NodeState, idtime proto.IdTime) *eventNodeTab {
		return &eventNodeTab{
			Where:    where,
			NodeInfo: nodei(laddr, typ, num, state, idtime),
		}
	}

	// XXX -> M = testenv.NewMaster("m")  (mkhost, chan, register to tracer ...)
	// XXX ----//---- S, C

	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)
	Chost := xnet.NetTrace(net.Host("c"), tracer)

	cM  := tracetest.NewSyncChan("m.main") // trace of events local to M
	cS  := tracetest.NewSyncChan("s.main") // trace of events local to S XXX with cause root also on S
//	cC  := tracetest.NewSyncChan("c.main")
	cMS := tracetest.NewSyncChan("m-s")    // trace of events with cause root being m -> s send
	cSM := tracetest.NewSyncChan("s-m")    // trace of events with cause root being s -> m send
	cMC := tracetest.NewSyncChan("m-c")	   // ----//---- m -> c
	cCM := tracetest.NewSyncChan("c-m")    // ----//---- c -> m
	cCS := tracetest.NewSyncChan("c-s")    // ----//---- c -> s

	tM := tracetest.NewEventChecker(t, dispatch, cM)
	tS := tracetest.NewEventChecker(t, dispatch, cS)
//	tC := tracetest.NewEventChecker(t, dispatch, cC)	// XXX no need
	tMS := tracetest.NewEventChecker(t, dispatch, cMS)
	tSM := tracetest.NewEventChecker(t, dispatch, cSM)
	tMC := tracetest.NewEventChecker(t, dispatch, cMC)
	tCM := tracetest.NewEventChecker(t, dispatch, cCM)
	tCS := tracetest.NewEventChecker(t, dispatch, cCS)


	rt.BranchNode("m", cM)
	rt.BranchNode("s", cS)
	rt.BranchLink("s-m", cSM, cMS)
	rt.BranchLink("c-m", cCM, cMC)
	rt.BranchLink("c-s", cCS, rt.defaultq /* S never pushes to C */)
	rt.BranchState("s", cMS) // state on S is controlled by M
	rt.BranchState("c", cMC) // state on C is controlled by M

	// cluster nodes
	M := tNewMaster("abc1", ":1", Mhost)
	zstor := xfs1stor("../zodb/storage/fs1/testdata/1.fs")
	zback := xfs1back("../zodb/storage/fs1/testdata/1.fs")
	S := tNewStorage("abc1", "m:1", ":1", Shost, zback)
	C := newClient("abc1", "m:1", Chost)

	// let tracer know how to map state addresses to node names
	tracer.RegisterNode(M.node, "m")	// XXX better Mhost.Name() ?
	tracer.RegisterNode(S.node, "s")
	tracer.RegisterNode(C.node, "c")



	gwg := &errgroup.Group{}

	// ----------------------------------------

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

	// trace

	// M starts listening
	tM.Expect(netlisten("m:1"))
	tM.Expect(node("m", "m:1", proto.MASTER, 1, proto.RUNNING, proto.IdTimeNone))
	tM.Expect(clusterState("m", proto.ClusterRecovering))

	// TODO create C; C tries connect to master - rejected ("not yet operational")

	// S starts listening
	tS.Expect(netlisten("s:1"))

	// S connects M
	tSM.Expect(netconnect("s:2", "m:2",  "m:1"))
	tSM.Expect(conntx("s:2", "m:2", 1, &proto.RequestIdentification{
		NodeType:	proto.STORAGE,
		UUID:		0,
		Address:	xnaddr("s:1"),
		ClusterName:	"abc1",
		IdTime:		proto.IdTimeNone,
	}))

	tM.Expect(node("m", "s:1", proto.STORAGE, 1, proto.PENDING, 0.01))

	tSM.Expect(conntx("m:2", "s:2", 1, &proto.AcceptIdentification{
		NodeType:	proto.MASTER,
		MyUUID:		proto.UUID(proto.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	0,
		YourUUID:	proto.UUID(proto.STORAGE, 1),
	}))

	// TODO test ID rejects (uuid already registered, ...)

	// M starts recovery on S
	tMS.Expect(conntx("m:2", "s:2", 0, &proto.Recovery{}))
	tMS.Expect(conntx("s:2", "m:2", 0, &proto.AnswerRecovery{
		// empty new node
		PTid:		0,
		BackupTid:	proto.INVALID_TID,
		TruncateTid:	proto.INVALID_TID,
	}))

	tMS.Expect(conntx("m:2", "s:2", 2, &proto.AskPartitionTable{}))
	tMS.Expect(conntx("s:2", "m:2", 2, &proto.AnswerPartitionTable{
		PTid:		0,
		RowList:	[]proto.RowInfo{},
	}))

	// M ready to start: new cluster, no in-progress S recovery
	tM.Expect(masterStartReady("m", true))


	// ----------------------------------------

	// M <- start cmd
	wg := &errgroup.Group{}
	gox(wg, func() {
		err := M.Start()
		exc.Raiseif(err)
	})

	// trace

	tM.Expect(node("m", "s:1", proto.STORAGE, 1, proto.RUNNING, 0.01))
	xwait(wg)

	// XXX M.partTab <- S1

	// M starts verification
	tM.Expect(clusterState("m", proto.ClusterVerifying))

	tMS.Expect(conntx("m:2", "s:2", 4, &proto.SendPartitionTable{
		PTid:		1,
		RowList:	[]proto.RowInfo{
			{0, []proto.CellInfo{{proto.UUID(proto.STORAGE, 1), proto.UP_TO_DATE}}},
		},
	}))

	tMS.Expect(conntx("m:2", "s:2", 6, &proto.LockedTransactions{}))
	tMS.Expect(conntx("s:2", "m:2", 6, &proto.AnswerLockedTransactions{
		TidDict: nil,	// map[zodb.Tid]zodb.Tid{},
	}))

	lastOid, err1 := zstor.LastOid(bg)
	lastTid, err2 := zstor.LastTid(bg)
	exc.Raiseif(xerr.Merge(err1, err2))
	tMS.Expect(conntx("m:2", "s:2", 8, &proto.LastIDs{}))
	tMS.Expect(conntx("s:2", "m:2", 8, &proto.AnswerLastIDs{
		LastOid: lastOid,
		LastTid: lastTid,
	}))

	// XXX M -> S ClusterInformation(VERIFICATION) ?

	// TODO there is actually txn to finish
	// TODO S leave at verify
	// TODO S join at verify
	// TODO M.Stop() while verify

	// verification ok; M start service
	tM.Expect(clusterState("m", proto.ClusterRunning))
	// TODO ^^^ should be sent to S

	tMS.Expect(conntx("m:2", "s:2", 10, &proto.StartOperation{Backup: false}))
	tMS.Expect(conntx("s:2", "m:2", 10, &proto.NotifyReady{}))

	// TODO S leave while service
	// TODO S join while service
	// TODO M.Stop while service


	// ----------------------------------------

	// XXX try starting client from the beginning

	// start client
	Cctx, Ccancel := context.WithCancel(bg)
	gox(gwg, func() {
		err := C.run(Cctx)
		fmt.Println("C err: ", err)
		exc.Raiseif(err)
	})

	// trace

	// C connects M
	tCM.Expect(netconnect("c:1", "m:3",  "m:1"))
	tCM.Expect(conntx("c:1", "m:3", 1, &proto.RequestIdentification{
		NodeType:	proto.CLIENT,
		UUID:		0,
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTime:		proto.IdTimeNone,
	}))

	tM.Expect(node("m", "", proto.CLIENT, 1, proto.RUNNING, 0.02))

	tCM.Expect(conntx("m:3", "c:1", 1, &proto.AcceptIdentification{
		NodeType:	proto.MASTER,
		MyUUID:		proto.UUID(proto.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	0,
		YourUUID:	proto.UUID(proto.CLIENT, 1),
	}))

	// C asks M about PT
	// NOTE this might come in parallel with vvv "C <- M NotifyNodeInformation C1,M1,S1"
	tCM.Expect(conntx("c:1", "m:3", 3, &proto.AskPartitionTable{}))
	tCM.Expect(conntx("m:3", "c:1", 3, &proto.AnswerPartitionTable{
		PTid:		1,
		RowList:	[]proto.RowInfo{
			{0, []proto.CellInfo{{proto.UUID(proto.STORAGE, 1), proto.UP_TO_DATE}}},
		},
	}))

	// C <- M NotifyNodeInformation C1,M1,S1
	// NOTE this might come in parallel with ^^^ "C asks M about PT"
	tMC.Expect(conntx("m:3", "c:1", 0, &proto.NotifyNodeInformation{
		IdTime:		proto.IdTimeNone,	// XXX ?
		NodeList:	[]proto.NodeInfo{
			nodei("m:1", proto.MASTER,  1, proto.RUNNING, proto.IdTimeNone),
			nodei("s:1", proto.STORAGE, 1, proto.RUNNING, 0.01),
			nodei("",    proto.CLIENT,  1, proto.RUNNING, 0.02),
		},
	}))

	tMC.Expect(node("c", "m:1", proto.MASTER,  1, proto.RUNNING, proto.IdTimeNone))
	tMC.Expect(node("c", "s:1", proto.STORAGE, 1, proto.RUNNING, 0.01))
	tMC.Expect(node("c", "",    proto.CLIENT,  1, proto.RUNNING, 0.02))


	// ----------------------------------------

	// C asks M about last tid	XXX better master sends it itself on new client connected
	wg = &errgroup.Group{}
	gox(wg, func() {
		cLastTid, err := C.LastTid(bg)
		exc.Raiseif(err)

		if cLastTid != lastTid {
			exc.Raisef("C.LastTid -> %v  ; want %v", cLastTid, lastTid)
		}
	})

	tCM.Expect(conntx("c:1", "m:3", 5, &proto.LastTransaction{}))
	tCM.Expect(conntx("m:3", "c:1", 5, &proto.AnswerLastTransaction{
		Tid: lastTid,
	}))

	xwait(wg)


	// ----------------------------------------

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

	// trace

	// ... -> connects to S
	tCS.Expect(netconnect("c:2", "s:3",  "s:1"))
	tCS.Expect(conntx("c:2", "s:3", 1, &proto.RequestIdentification{
		NodeType:	proto.CLIENT,
		UUID:		proto.UUID(proto.CLIENT, 1),
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTime:		0.02,
	}))

	tCS.Expect(conntx("s:3", "c:2", 1, &proto.AcceptIdentification{
		NodeType:	proto.STORAGE,
		MyUUID:		proto.UUID(proto.STORAGE, 1),
		NumPartitions:	1,
		NumReplicas:	0,
		YourUUID:	proto.UUID(proto.CLIENT, 1),
	}))

	// ... -> GetObject(xid1)
	tCS.Expect(conntx("c:2", "s:3", 3, &proto.GetObject{
		Oid:	xid1.Oid,
		Tid:	at2Before(xid1.At),
		Serial: proto.INVALID_TID,
	}))
	tCS.Expect(conntx("s:3", "c:2", 3, &proto.AnswerObject{
		Oid:		xid1.Oid,
		Serial:		serial1,
		NextSerial:	proto.INVALID_TID,
		Compression:	false,
		Data:		buf1,
		DataSerial:	0,		// XXX
		Checksum:	sha1.Sum(buf1.Data),
	}))


	xwait(wg)


	// ----------------------------------------

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
	tCS.Expect(conntx("c:2", "s:3", 5, &proto.GetObject{
		Oid:	xid1prev.Oid,
		Tid:	serial1,
		Serial: proto.INVALID_TID,
	}))
	tCS.Expect(conntx("s:3", "c:2", 5, &proto.AnswerObject{
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
	tracer.Detach()

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
	Ccancel()	// ---- // ----
	xwait(gwg)
}


// dispatch1 dispatched directly to single output channel
//
// XXX hack - better we don't need it.
// XXX -> with testenv.MkCluster() we won't need it
type tdispatch1 struct {
	outch *tracetest.SyncChan
}

func (d tdispatch1) Dispatch(event interface{}) {
	d.outch.Send(event)
}

func benchmarkGetObject(b *testing.B, Mnet, Snet, Cnet xnet.Networker, benchit func(xcload1 func())) {
	// create test cluster	<- XXX factor to utility func
	zback := xfs1back("../zodb/storage/fs1/testdata/1.fs")

	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)
	defer wg.Wait()
	defer cancel()

	// spawn M
	M := tNewMaster("abc1", "", Mnet)

	// XXX to wait for "M listens at ..." & "ready to start" -> XXX add something to M api?
	cG	  := tracetest.NewSyncChan("main")
	tG	  := tracetest.NewEventChecker(b, nil /* XXX */, cG)

	tracer    := NewTraceCollector(tdispatch1{cG})
	tracing.Lock()
	pnode := traceNodeChanged_Attach(nil, tracer.traceNode)
	traceMasterStartReady_Attach(tracer.pg, tracer.traceMasterStartReady)
	tracing.Unlock()

	wg.Go(func() error {
		return M.Run(ctx)
	})

	// determing M serving address	XXX better with M api
	ev := cG.Recv()
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
	S := tNewStorage("abc1", Maddr, "", Snet, zback)
	C := NewClient("abc1", Maddr, Cnet)

	wg.Go(func() error {
		return S.Run(ctx)
	})

	// command M to start
	tG.Expect(masterStartReady("m", true))	// <- XXX better with M api
	tracer.Detach()

	err := M.Start()
	if err != nil {
		b.Fatal(err)
	}

	xid1 := zodb.Xid{Oid: 1, At: zodb.TidMax}

	obj1, err := zback.Load(ctx, xid1)
	if err != nil {
		b.Fatal(err)
	}
	buf1, serial1 := obj1.Data, obj1.Serial

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
