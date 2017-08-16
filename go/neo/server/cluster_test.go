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
	//"bytes"
	"context"
	//"io"
	"net"
	//"reflect"
	"testing"
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/neo"
	//"lab.nexedi.com/kirr/neo/go/neo/client"

	//"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/neo/go/xcommon/tracing"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet"
	"lab.nexedi.com/kirr/neo/go/xcommon/xsync"
	"lab.nexedi.com/kirr/neo/go/xcommon/xtesting"

	"lab.nexedi.com/kirr/go123/exc"

	"fmt"
)

// XXX dup from connection_test
func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

func xfs1stor(path string) *fs1.FileStorage {
	zstor, err := fs1.Open(context.Background(), path)
	exc.Raiseif(err)
	return zstor
}


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
	NodeTab  unsafe.Pointer	// *neo.NodeTable XXX not to noise test diff output
	NodeInfo neo.NodeInfo
}
func (t *MyTracer) traceNode(nt *neo.NodeTable, n *neo.Node) {
	t.Trace1(&traceNode{unsafe.Pointer(nt), n.NodeInfo})
}


//trace:import "lab.nexedi.com/kirr/neo/go/neo"

// M drives cluster with 1 S through recovery -> verification -> service -> shutdown
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
	tracing.Unlock()


	// shortcut for addresses
	xaddr := func(addr string) *pipenet.Addr {
		a, err := net.ParseAddr(addr)
		exc.Raiseif(err)
		return a
	}
	xnaddr := func(addr string) neo.Address {
		a, err := neo.Addr(xaddr(addr))
		exc.Raiseif(err)
		return a
	}

	// shortcut for net tx event
	// XXX -> NetTx ?
	nettx := func(src, dst, pkt string) *xnet.TraceTx {
		return &xnet.TraceTx{Src: xaddr(src), Dst: xaddr(dst), Pkt: []byte(pkt)}
	}

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

	// shortcut for nodetab change
	node := func(nt *neo.NodeTable, laddr string, typ neo.NodeType, num int32, state neo.NodeState, idtstamp float64) *traceNode {
		return &traceNode{
			NodeTab: unsafe.Pointer(nt),
			NodeInfo: neo.NodeInfo{
				NodeType:    typ,
				Address:     xnaddr(laddr),
				NodeUUID:    neo.UUID(typ, num),
				NodeState:   state,
				IdTimestamp: idtstamp,
			},
		}
	}


	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)

	wg := &xsync.WorkGroup{}

	// start master
	M := NewMaster("abc1", ":1", Mhost)
	Mctx, Mcancel := context.WithCancel(context.Background())
	wg.Gox(func() {
		err := M.Run(Mctx)
		fmt.Println("M err: ", err)
		_ = err // XXX
	})


	// expect:
	tc.Expect(netlisten("m:1"))

	tc.Expect(node(M.nodeTab, "m:1", neo.MASTER, 1, neo.RUNNING, 0.0))
	tc.Expect(clusterState(&M.clusterState, neo.ClusterRecovering))

	// start storage
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")
	S := NewStorage("abc1", "m:1", ":1", Shost, zstor)
	Sctx, Scancel := context.WithCancel(context.Background())
	wg.Gox(func() {
		err := S.Run(Sctx)
		fmt.Println("S err: ", err)
		_ = err	// XXX
	})

	// expect:
	tc.Expect(netlisten("s:1"))
	tc.Expect(netconnect("s:2", "m:2",  "m:1"))

	//tc.ExpectPar(
	//	nettx("s:1", "m:1", "\x00\x00\x00\x01"),	// handshake
	//	nettx("m:1", "s:1", "\x00\x00\x00\x01"),
	//)
	_ = nettx

	tc.Expect(conntx("s:2", "m:2", 1, &neo.RequestIdentification{
		NodeType:	neo.STORAGE,
		NodeUUID:	0,
		Address:	xnaddr("s:1"),
		ClusterName:	"abc1",
		IdTimestamp:	0,
	}))

	tc.Expect(node(M.nodeTab, "s:1", neo.STORAGE, 1, neo.PENDING, 0.0)) // XXX t

	tc.Expect(conntx("m:2", "s:2", 1, &neo.AcceptIdentification{
		NodeType:	neo.MASTER,
		MyNodeUUID:	neo.UUID(neo.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	1,
		YourNodeUUID:	neo.UUID(neo.STORAGE, 1),
	}))

	// TODO test ID rejects

	// M starts recovery on S
	tc.Expect(conntx("m:2", "s:2", 1, &neo.Recovery{}))
	tc.Expect(conntx("s:2", "m:2", 1, &neo.AnswerRecovery{
		// empty new node
		PTid:		0,
		BackupTid:	neo.INVALID_TID,
		TruncateTid:	neo.INVALID_TID,
	}))

	tc.Expect(conntx("m:2", "s:2", 1, &neo.AskPartitionTable{}))
	tc.Expect(conntx("s:2", "m:2", 1, &neo.AnswerPartitionTable{
		PTid:		0,
		RowList:	[]neo.RowInfo{},
	}))

	// XXX M.partTab <- ...
	// XXX updated something cluster currently can be operational

	// XXX temp
	return

	err := M.Start()
	exc.Raiseif(err)

	// expect:
	// M.clusterState	<- VERIFICATION			+ TODO it should be sent to S
	// M -> S	.? LockedTransactions{}
	// M <- S	.? AnswerLockedTransactions{...}
	// M -> S	.? LastIDs{}
	// M <- S	.? AnswerLastIDs{...}

	//							+ TODO S leave at verify
	//							+ TODO S join at verify
	//							+ TODO M.Stop() while verify

	// expect:
	// M.clusterState	<- RUNNING			+ TODO it should be sent to S

	//							+ TODO S leave while service
	//							+ TODO S join while service
	//							+ TODO M.Stop while service

	// + TODO Client connects here ?

	// TODO S.Stop() or Scancel()
	// expect:
	// M.nodeTab -= S
	// M.clusterState	<- RECOVERY
	// ...

	// TODO Scancel -> S down - test how M behaves

	// TODO test M.recovery starting back from verification/service
	// (M needs to resend to all storages recovery messages just from start)

	xwait(wg)
	Mcancel()	// XXX temp
	Scancel()	// XXX temp
}

// basic interaction between Client -- Storage
func _TestClientStorage(t *testing.T) {
	// XXX temp disabled
	return

/*
	Cnl, Snl := NodeLinkPipe()
	wg := &xsync.WorkGroup{}

	Sctx, Scancel := context.WithCancel(context.Background())

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
