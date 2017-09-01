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

// XXX gotrace ... -> gotrace gen ...
//go:generate sh -c "go run ../../xcommon/tracing/cmd/gotrace/{gotrace,util}.go ."

import (
	"bytes"
	"context"
	//"io"
	"math"
	"net"
	//"reflect"
	"testing"
	"unsafe"

	"github.com/kylelemons/godebug/pretty"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/neo/client"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/neo/go/xcommon/tracing"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xsync"
	"lab.nexedi.com/kirr/neo/go/xcommon/xtesting"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"

	"fmt"
	"time"
)

// XXX dup from connection_test
func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
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
func (t *MyTracer) traceNeoConnSendPre(c *neo.Conn, msg neo.Msg) {
	t.Trace1(&traceNeoSend{c.Link().LocalAddr(), c.Link().RemoteAddr(), c.ConnID(), msg})
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
	//neo_traceConnRecv_Attach(pg, tracer.traceNeoConnRecv)
	neo_traceConnSendPre_Attach(pg, tracer.traceNeoConnSendPre)
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
	nodei := func(laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtstamp float64) neo.NodeInfo {
		return neo.NodeInfo{
			Type:  typ,
			Addr:  xnaddr(laddr),
			UUID:  neo.UUID(typ, num),
			State: state,
			IdTimestamp: idtstamp,
		}
	}

	// shortcut for nodetab change
	node := func(x *neo.NodeApp, laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtstamp float64) *traceNode {
		return &traceNode{
			NodeTab:  unsafe.Pointer(x.NodeTab),
			NodeInfo: nodei(laddr, typ, num, state, idtstamp),
		}
	}


	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)
	Chost := xnet.NetTrace(net.Host("c"), tracer)

	gwg := &xsync.WorkGroup{}

	// start master
	Mclock := &vclock{}
	M := NewMaster("abc1", ":1", Mhost)
	M.monotime = Mclock.monotime
	Mctx, Mcancel := context.WithCancel(bg)
	gwg.Gox(func() {
		err := M.Run(Mctx)
		fmt.Println("M err: ", err)
		exc.Raiseif(err)
	})

	// M starts listening
	tc.Expect(netlisten("m:1"))
	tc.Expect(node(M.node, "m:1", neo.MASTER, 1, neo.RUNNING, 0.0))
	tc.Expect(clusterState(&M.node.ClusterState, neo.ClusterRecovering))

	// TODO create C; C tries connect to master - rejected ("not yet operational")

	// start storage
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")
	S := NewStorage("abc1", "m:1", ":1", Shost, zstor)
	Sctx, Scancel := context.WithCancel(bg)
	gwg.Gox(func() {
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
		IdTimestamp:	0,
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
	wg := &xsync.WorkGroup{}
	wg.Gox(func() {
		err := M.Start()
		exc.Raiseif(err)
	})

	tc.Expect(node(M.node, "s:1", neo.STORAGE, 1, neo.RUNNING, 0.01))
	xwait(wg)

	// XXX M.partTab <- S1

	// M starts verification
	tc.Expect(clusterState(&M.node.ClusterState, neo.ClusterVerifying))

	tc.Expect(conntx("m:2", "s:2", 4, &neo.NotifyPartitionTable{
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
		IdTimestamp:	0,
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
		IdTimestamp:	0,	// XXX ?
		NodeList:	[]neo.NodeInfo{
			nodei("m:1", neo.MASTER,  1, neo.RUNNING, 0.00),
			nodei("s:1", neo.STORAGE, 1, neo.RUNNING, 0.01),
			nodei("",    neo.CLIENT,  1, neo.RUNNING, 0.02),
		},
	}))

	Cnode := *(**neo.NodeApp)(unsafe.Pointer(C)) // XXX hack, fragile
	tc.Expect(node(Cnode, "m:1", neo.MASTER,  1, neo.RUNNING, 0.00))
	tc.Expect(node(Cnode, "s:1", neo.STORAGE, 1, neo.RUNNING, 0.01))
	tc.Expect(node(Cnode, "",    neo.CLIENT,  1, neo.RUNNING, 0.02))


	// C asks M about last tid	XXX better master sends it itself on new client connected
	wg = &xsync.WorkGroup{}
	wg.Gox(func() {
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

	// C starts loading first object -> connects to S
	wg = &xsync.WorkGroup{}
	xid1 := zodb.Xid{Oid: 1, XTid: zodb.XTid{Tid: zodb.TidMax, TidBefore: true}}
	data1, serial1, err := zstor.Load(bg, xid1)
	exc.Raiseif(err)
	wg.Gox(func() {
		data, serial, err := C.Load(bg, xid1)
		exc.Raiseif(err)

		if !(bytes.Equal(data, data1) && serial==serial1) {
			exc.Raisef("C.Load(%v) ->\ndata:\n%s\nserial:\n%s\n", xid1,
				pretty.Compare(data1, data), pretty.Compare(serial1, serial))
		}
	})

	tc.Expect(netconnect("c:2", "s:3",  "s:1"))
	tc.Expect(conntx("c:2", "s:3", 1, &neo.RequestIdentification{
		NodeType:	neo.CLIENT,
		UUID:		neo.UUID(neo.CLIENT, 1),
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTimestamp:	0,	// XXX ?
	}))
	println("222")

	tc.Expect(conntx("s:3", "c:2", 1, &neo.AcceptIdentification{
		NodeType:	neo.STORAGE,
		MyUUID:		neo.UUID(neo.STORAGE, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourUUID:	neo.UUID(neo.CLIENT, 1),
	}))

	println("333")






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

// basic interaction between Client -- Storage
func _TestClientStorage(t *testing.T) {
	// XXX temp disabled
	return

/*
	Cnl, Snl := NodeLinkPipe()
	wg := &xsync.WorkGroup{}

	Sctx, Scancel := context.WithCancel(bg)

	net := pipenet.New("")	// XXX here? (or a bit above?)
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")	// XXX +readonly
	S := NewStorage("cluster", "Maddr", "Saddr", net, zstor)
	wg.Gox(func() {
		S.ServeLink(Sctx, Snl)
		// XXX + test error return
	})

	C, err := client.NewClient(Cnl)
	if err != nil {
		t.Fatalf("creating/identifying client: %v", err)
	}

	// verify LastTid
	lastTidOk, err := zstor.LastTid()
	if err != nil {
		t.Fatalf("zstor: lastTid: %v", err)
	}

	lastTid, err := C.LastTid()
	if !(lastTid == lastTidOk && err == nil) {
		t.Fatalf("C.LastTid -> %v, %v  ; want %v, nil", lastTid, err, lastTidOk)
	}

	// verify Load for all {<,=}serial:oid
	ziter := zstor.Iterate(zodb.Tid(0), zodb.TidMax)

	for {
		_, dataIter, err := ziter.NextTxn()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ziter.NextTxn: %v", err)
		}

		for {
			datai, err := dataIter.NextData()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ziter.NextData: %v", err)
			}

			for _, tidBefore := range []bool{false, true} {
				xid := zodb.Xid{Oid: datai.Oid} // {=,<}tid:oid
				xid.Tid = datai.Tid
				xid.TidBefore = tidBefore
				if tidBefore {
					xid.Tid++
				}

				data, tid, err := C.Load(xid)
				if datai.Data != nil {
					if !(bytes.Equal(data, datai.Data) && tid == datai.Tid && err == nil) {
						t.Fatalf("load: %v:\nhave: %v %v %q\nwant: %v nil %q",
							xid, tid, err, data, datai.Tid, datai.Data)
					}
				} else {
					// deleted
					errWant := &zodb.ErrXidMissing{xid}
					if !(data == nil && tid == 0 && reflect.DeepEqual(err, errWant)) {
						t.Fatalf("load: %v:\nhave: %v, %#v, %#v\nwant: %v, %#v, %#v",
							xid, tid, err, data, zodb.Tid(0), errWant, []byte(nil))
					}
				}
			}

		}

	}


	// TODO Iterate (not yet implemented for NEO)

	// shutdown storage
	// XXX wait for S to shutdown + verify shutdown error
	Scancel()

	xwait(wg)
*/
}
