// Copyright (C) 2017-2020  Nexedi SA and Contributors.
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

import (
	"bytes"
	"context"
	"crypto/sha1"
	"io"
	"reflect"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/kylelemons/godebug/pretty"

	"lab.nexedi.com/kirr/neo/go/neo/proto"

	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/neo/go/internal/xtracing/tracetest"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/tracing"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xnet/pipenet"
	"lab.nexedi.com/kirr/go123/xsync"

	"time"
)


// M drives cluster with 1 S & C through recovery -> verification -> service -> shutdown
func TestMasterStorage(t0 *testing.T) {
	X := exc.Raiseif

	t := NewTestCluster(t0, "abc1")
	defer t.Stop()

	M := t.NewMaster("m")
	zstor := xfs1stor("../zodb/storage/fs1/testdata/1.fs")
	zback := xfs1back("../zodb/storage/fs1/testdata/1.fs")
	S := t.NewStorage("s", "m:1", zback)	// XXX do we need to provide Mlist here?
	C := t.NewClient("c", "m:1")

	lastTid, err := zstor.Sync(bg);  X(err)

	// start nodes		XXX move starting to TestCluster?
	gwg := xsync.NewWorkGroup(bg)
	//defer xwait(gwg)	XXX not yet correctly stopped on context cancel

	gwg.Go(func(ctx context.Context) error {
		return M.Run(ctx)
	})
	gwg.Go(func(ctx context.Context) error {
		return S.Run(ctx)
	})
	gwg.Go(func(ctx context.Context) error {
		return C.Run(ctx)
	})

	tM  := t.Checker("m")
	tS  := t.Checker("s")
	tC  := t.Checker("c")
	tMS := t.Checker("m-s")
	tSM := t.Checker("s-m")
	tCM := t.Checker("c-m")
	tMC := t.Checker("m-c")
	tCS := t.Checker("c-s")

	// ----------------------------------------

	// M starts listening
	tM.Expect(netlisten("m:1"))
	tM.Expect(δnode("m", "m:1", proto.MASTER, 1, proto.RUNNING, proto.IdTimeNone))
	tM.Expect(clusterState("m", proto.ClusterRecovering))

	// TODO create C; C tries connect to master - rejected ("not yet operational")

	// S starts listening
	tS.Expect(netlisten("s:1"))


	// S connects M
	tSM.Expect(netdial("s", "m:1"))
	tSM.Expect(netconnect("s:2", "m:2",  "m:1"))
	tSM.Expect(conntx("s:2", "m:2", 1, &proto.RequestIdentification{
		NodeType:	proto.STORAGE,
		UUID:		0,
		Address:	xnaddr("s:1"),
		ClusterName:	"abc1",
		IdTime:		proto.IdTimeNone,
	}))


	tM.Expect(δnode("m", "s:1", proto.STORAGE, 1, proto.PENDING, 0.01))

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
		err := M.Start();  X(err)
	})

	// trace

	tM.Expect(δnode("m", "s:1", proto.STORAGE, 1, proto.RUNNING, 0.01))
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

	lastOid, err := zstor.LastOid(bg);  X(err)
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

	// trace of client start

	// C connects M
	tCM.Expect(netdial("c", "m:1"))
	tCM.Expect(netconnect("c:1", "m:3",  "m:1"))
	tCM.Expect(conntx("c:1", "m:3", 1, &proto.RequestIdentification{
		NodeType:	proto.CLIENT,
		UUID:		0,
		Address:	xnaddr(""),
		ClusterName:	"abc1",
		IdTime:		proto.IdTimeNone,
	}))

	tM.Expect(δnode("m", "", proto.CLIENT, 1, proto.RUNNING, 0.02))

	tCM.Expect(conntx("m:3", "c:1", 1, &proto.AcceptIdentification{
		NodeType:	proto.MASTER,
		MyUUID:		proto.UUID(proto.MASTER, 1),
		NumPartitions:	1,
		NumReplicas:	0,
		YourUUID:	proto.UUID(proto.CLIENT, 1),
	}))

	// C asks M about PT and last_tid
	// NOTE this might come in parallel with vvv "C <- M NotifyNodeInformation C1,M1,S1"
	tCM.Expect(conntx("c:1", "m:3", 3, &proto.AskPartitionTable{}))
	tCM.Expect(conntx("m:3", "c:1", 3, &proto.AnswerPartitionTable{
		PTid:		1,
		RowList:	[]proto.RowInfo{
			{0, []proto.CellInfo{{proto.UUID(proto.STORAGE, 1), proto.UP_TO_DATE}}},
		},
	}))
	tCM.Expect(conntx("c:1", "m:3", 5, &proto.LastTransaction{}))
	tCM.Expect(conntx("m:3", "c:1", 5, &proto.AnswerLastTransaction{lastTid}))

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

	tC.Expect(δnode("c", "m:1", proto.MASTER,  1, proto.RUNNING, proto.IdTimeNone))
	tC.Expect(δnode("c", "s:1", proto.STORAGE, 1, proto.RUNNING, 0.01))
	tC.Expect(δnode("c", "",    proto.CLIENT,  1, proto.RUNNING, 0.02))

	// ----------------------------------------

	// C asks M about last tid	XXX better master sends it itself on new client connected
	wg = &errgroup.Group{}
	gox(wg, func() {
		cLastTid, err := C.Sync(bg);  X(err)

		if cLastTid != lastTid {
			exc.Raisef("C.LastTid -> %v  ; want %v", cLastTid, lastTid)
		}
	})

	tCM.Expect(conntx("c:1", "m:3", 7, &proto.LastTransaction{}))
	tCM.Expect(conntx("m:3", "c:1", 7, &proto.AnswerLastTransaction{
		Tid: lastTid,
	}))

	xwait(wg)


	// ----------------------------------------

	// C starts loading first object ...
	wg = &errgroup.Group{}
	xid1 := zodb.Xid{Oid: 1, At: zodb.TidMax}
	buf1, serial1, err := zstor.Load(bg, xid1);  X(err)
	gox(wg, func() {
		buf, serial, err := C.Load(bg, xid1);  X(err)

		if !(bytes.Equal(buf.Data, buf1.Data) && serial==serial1) {
			exc.Raisef("C.Load(%v) ->\ndata:\n%s\nserial:\n%s\n", xid1,
				pretty.Compare(buf1.Data, buf.Data), pretty.Compare(serial1, serial))
		}
	})

	// trace

	// ... -> connects to S
	tCS.Expect(netdial("c", "s:1"))
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
	buf1prev, serial1prev, err := zstor.Load(bg, xid1prev);  X(err)
	gox(wg, func() {
		buf, serial, err := C.Load(bg, xid1prev);  X(err)

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
	t.gotracer.Detach()

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

	// determining M serving address	XXX better with M api
	ev := cG.Recv()
	mnode, ok := ev.Event.(*eventNodeTab)
	if !ok {
		b.Fatalf("after M start: got %T  ; want eventNodeTab", ev.Event)
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
