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
	"bytes"
	"context"
	"crypto/sha1"
	"io"
	"math"
	"net"
	"reflect"
	"testing"
	"unsafe"

	"golang.org/x/sync/errgroup"

	"github.com/kylelemons/godebug/pretty"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/neo/client"
	"lab.nexedi.com/kirr/neo/go/neo/internal/common"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/neo/go/xcommon/xtesting"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/tracing"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xnet/pipenet"

	"fmt"
	"time"
)

// XXX dup from connection_test
func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

func gox(wg interface { Go(func() error) }, xf func()) {
	wg.Go(exc.Funcx(xf))
}

func xfs1stor(path string) *fs1.FileStorage {
	zstor, err := fs1.Open(bg, path)
	exc.Raiseif(err)
	return zstor
}

var bg = context.Background()

// XXX tracer which can collect tracing events from net + TODO master/storage/etc...
// XXX naming
type MyTracer struct {
	*xtesting.SyncTracer
}

func (t *MyTracer) TraceNetConnect(ev *xnet.TraceConnect)	{ t.Trace1(ev) }
func (t *MyTracer) TraceNetListen(ev *xnet.TraceListen)		{ t.Trace1(ev) }
func (t *MyTracer) TraceNetTx(ev *xnet.TraceTx)			{}	// { t.Trace1(ev) }

//type traceNeoRecv struct {conn *neo.Conn; msg neo.Msg}
//func (t *MyTracer) traceNeoConnRecv(c *neo.Conn, msg neo.Msg)	{ t.Trace1(&traceNeoRecv{c, msg}) }

// tx via neo.Conn
type traceNeoSend struct {
	Src, Dst net.Addr
	ConnID   uint32
	Msg	 neo.Msg
}
func (t *MyTracer) traceNeoMsgSendPre(l *neo.NodeLink, connID uint32, msg neo.Msg) {
	t.Trace1(&traceNeoSend{l.LocalAddr(), l.RemoteAddr(), connID, msg})
}

// cluster state changed
type traceClusterState struct {
	Ptr   *neo.ClusterState // pointer to variable which holds the state
	State neo.ClusterState
}
func (t *MyTracer) traceClusterState(cs *neo.ClusterState) {
	t.Trace1(&traceClusterState{cs, *cs})
}
func clusterState(cs *neo.ClusterState, v neo.ClusterState) *traceClusterState {
	return &traceClusterState{cs, v}
}

// nodetab entry changed
type traceNode struct {
	NodeTab  unsafe.Pointer	// *neo.NodeTable XXX not to noise test diff
	NodeInfo neo.NodeInfo
}
func (t *MyTracer) traceNode(nt *neo.NodeTable, n *neo.Node) {
	t.Trace1(&traceNode{unsafe.Pointer(nt), n.NodeInfo})
}

// master ready to start changed
type traceMStartReady struct {
	Master  unsafe.Pointer // *Master XXX not to noise test diff
	Ready   bool
}
func (t *MyTracer) traceMasterStartReady(m *Master, ready bool) {
	t.Trace1(masterStartReady(m, ready))
}
func masterStartReady(m *Master, ready bool) *traceMStartReady {
	return &traceMStartReady{unsafe.Pointer(m), ready}
}


// vclock is a virtual clock
// XXX place -> util?
type vclock struct {
	t float64
}

func (c *vclock) monotime() float64 {
	c.t += 1E-2
	return c.t
}

func (c *vclock) tick() {	// XXX do we need tick?
	t := math.Ceil(c.t)
	if !(t > c.t) {
		t += 1
	}
	c.t = t
}

//trace:import "lab.nexedi.com/kirr/neo/go/neo"

// M drives cluster with 1 S & C through recovery -> verification -> service -> shutdown
func TestMasterStorage(t *testing.T) {
	tracer := &MyTracer{xtesting.NewSyncTracer()}
	tc := xtesting.NewTraceChecker(t, tracer.SyncTracer)

	net := pipenet.New("testnet")	// test network

	pg := &tracing.ProbeGroup{}
	defer pg.Done()

	tracing.Lock()
	//neo_traceMsgRecv_Attach(pg, tracer.traceNeoMsgRecv)
	neo_traceMsgSendPre_Attach(pg, tracer.traceNeoMsgSendPre)
	neo_traceClusterStateChanged_Attach(pg, tracer.traceClusterState)
	neo_traceNodeChanged_Attach(pg, tracer.traceNode)
	traceMasterStartReady_Attach(pg, tracer.traceMasterStartReady)
	tracing.Unlock()


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
	netconnect := func(src, dst, dialed string) *xnet.TraceConnect {
		return &xnet.TraceConnect{Src: xaddr(src), Dst: xaddr(dst), Dialed: dialed}
	}

	netlisten := func(laddr string) *xnet.TraceListen {
		return &xnet.TraceListen{Laddr: xaddr(laddr)}
	}

	// shortcut for net tx event over nodelink connection
	conntx := func(src, dst string, connid uint32, msg neo.Msg) *traceNeoSend {
		return &traceNeoSend{Src: xaddr(src), Dst: xaddr(dst), ConnID: connid, Msg: msg}
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
	node := func(x *neo.NodeApp, laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtime neo.IdTime) *traceNode {
		return &traceNode{
			NodeTab:  unsafe.Pointer(x.NodeTab),
			NodeInfo: nodei(laddr, typ, num, state, idtime),
		}
	}


	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)
	Chost := xnet.NetTrace(net.Host("c"), tracer)

	gwg := &errgroup.Group{}

	// start master
	Mclock := &vclock{}
	M := NewMaster("abc1", ":1", Mhost)
	M.monotime = Mclock.monotime
	Mctx, Mcancel := context.WithCancel(bg)
	gox(gwg, func() {
		err := M.Run(Mctx)
		fmt.Println("M err: ", err)
		exc.Raiseif(err)
	})

	// M starts listening
	tc.Expect(netlisten("m:1"))
	tc.Expect(node(M.node, "m:1", neo.MASTER, 1, neo.RUNNING, neo.IdTimeNone))
	tc.Expect(clusterState(&M.node.ClusterState, neo.ClusterRecovering))

	// TODO create C; C tries connect to master - rejected ("not yet operational")

	// start storage
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")
	S := NewStorage("abc1", "m:1", ":1", Shost, zstor)
	Sctx, Scancel := context.WithCancel(bg)
	gox(gwg, func() {
		err := S.Run(Sctx)
		fmt.Println("S err: ", err)
		exc.Raiseif(err)
	})

	// S starts listening
	tc.Expect(netlisten("s:1"))

	// S connects M
	tc.Expect(netconnect("s:2", "m:2",  "m:1"))
	tc.Expect(conntx("s:2", "m:2", 1, &neo.RequestIdentification{
		NodeType:	neo.STORAGE,
		UUID:		0,
		Address:	xnaddr("s:1"),
		ClusterName:	"abc1",
		IdTime:		neo.IdTimeNone,
	}))

	tc.Expect(node(M.node, "s:1", neo.STORAGE, 1, neo.PENDING, 0.01))

	tc.Expect(conntx("m:2", "s:2", 1, &neo.AcceptIdentification{
		NodeType:	neo.MASTER,
		MyUUID:		neo.UUID(neo.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourUUID:	neo.UUID(neo.STORAGE, 1),
	}))

	// TODO test ID rejects (uuid already registered, ...)

	// M starts recovery on S
	tc.Expect(conntx("m:2", "s:2", 0, &neo.Recovery{}))
	tc.Expect(conntx("s:2", "m:2", 0, &neo.AnswerRecovery{
		// empty new node
		PTid:		0,
		BackupTid:	neo.INVALID_TID,
		TruncateTid:	neo.INVALID_TID,
	}))

	tc.Expect(conntx("m:2", "s:2", 2, &neo.AskPartitionTable{}))
	tc.Expect(conntx("s:2", "m:2", 2, &neo.AnswerPartitionTable{
		PTid:		0,
		RowList:	[]neo.RowInfo{},
	}))

	// M ready to start: new cluster, no in-progress S recovery
	tc.Expect(masterStartReady(M, true))

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
		NextSerial:	0,		// XXX
		Compression:	false,
		Data:		buf1,
		DataSerial:	0,		// XXX
		Checksum:	sha1.Sum(buf1.Data),
	}))

	xwait(wg)

	// C loads every other {<,=}serial:oid - established link is reused
	ziter := zstor.Iterate(0, zodb.TidMax)

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
				errWant := &zodb.LoadError{
					URL: C.URL(),
					Xid: xid,
					Err: &zodb.NoDataError{Oid: xid.Oid, DeletedAt: datai.Tid},
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
	tracer := &MyTracer{xtesting.NewSyncTracer()}
	tc := xtesting.NewTraceChecker(b, tracer.SyncTracer)
	pg := &tracing.ProbeGroup{}
	tracing.Lock()
	pnode := neo_traceNodeChanged_Attach(nil, tracer.traceNode)
	traceMasterStartReady_Attach(pg, tracer.traceMasterStartReady)
	tracing.Unlock()

	wg.Go(func() error {
		return M.Run(ctx)
	})

	// determing M serving address	XXX better with M api
	ev := tracer.Get1()
	mnode, ok := ev.Event.(*traceNode)
	if !ok {
		b.Fatal("after M start: got %T  ; want traceNode", ev.Event)
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
