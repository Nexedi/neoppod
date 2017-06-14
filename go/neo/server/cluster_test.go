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

package server
// test interaction between nodes

import (
	//"bytes"
	"context"
	//"io"
	//"reflect"
	"testing"

	//"../../neo/client"

	//"../../zodb"
	"../../zodb/storage/fs1"

	"../../xcommon/xnet"
	"../../xcommon/xnet/pipenet"
	"../../xcommon/xsync"
	"../../xcommon/xtesting"

	"lab.nexedi.com/kirr/go123/exc"

	"fmt"
)

var _ = fmt.Print

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

func (t *MyTracer) TraceNetDial(ev *xnet.TraceDial)	{ t.Trace1(ev) }
func (t *MyTracer) TraceNetListen(ev *xnet.TraceListen)	{ t.Trace1(ev) }
func (t *MyTracer) TraceNetTx(ev *xnet.TraceTx)		{ t.Trace1(ev) }


/*
func (tc *TraceChecker) ExpectNetDial(dst string) {
	tc.t.Helper()

	var ev *xnet.TraceDial
	msg := tc.xget1(&ev)

	if ev.Dst != dst {
		tc.t.Fatalf("net dial: have %v;  want: %v", ev.Dst, dst)
	}

	close(msg.ack)
}

func (tc *TraceChecker) ExpectNetListen(laddr string) {
	tc.t.Helper()

	var ev *xnet.TraceListen
	msg := tc.xget1(&ev)

	if ev.Laddr != laddr {
		tc.t.Fatalf("net listen: have %v;  want %v", ev.Laddr, laddr)
	}

	close(msg.ack)
}

func (tc *TraceChecker) ExpectNetTx(src, dst string, pkt string) {
	tc.t.Helper()

	var ev *xnet.TraceTx
	msg := tc.xget1(&ev)

	pktb := []byte(pkt)
	if !(ev.Src.String() == src &&
	     ev.Dst.String() == dst &&
	     bytes.Equal(ev.Pkt, pktb)) {
		     // TODO also print all (?) previous events
		     tc.t.Fatalf("expect:\nhave: %s -> %s  %v\nwant: %s -> %s  %v",
			ev.Src, ev.Dst, ev.Pkt, src, dst, pktb)
	}

	close(msg.ack)
}
*/


// M drives cluster with 1 S through recovery -> verification -> service -> shutdown
func TestMasterStorage(t *testing.T) {
	tracer := &MyTracer{xtesting.NewSyncTracer()}
	tc := xtesting.NewTraceChecker(t, tracer.SyncTracer)

	//net := xnet.NetTrace(pipenet.New(""), tracer)	// test network
	net := pipenet.New("testnet")	// test network

	Mhost := xnet.NetTrace(net.Host("m"), tracer)
	Shost := xnet.NetTrace(net.Host("s"), tracer)

	Maddr := "0"
	Saddr := "1"

	wg := &xsync.WorkGroup{}

	// start master
	M := NewMaster("abc1", Maddr, Mhost)
	Mctx, Mcancel := context.WithCancel(context.Background())
	wg.Gox(func() {
		err := M.Run(Mctx)
		_ = err // XXX
	})

	// expect:
	//tc.ExpectNetListen("0")
	tc.Expect(&xnet.TraceListen{Laddr: "0"})

	// M.clusterState	<- RECOVERY
	// M.nodeTab		<- Node(M)

	// start storage
	zstor := xfs1stor("../../zodb/storage/fs1/testdata/1.fs")
	S := NewStorage("abc1", Maddr, Saddr, net, zstor)
	Sctx, Scancel := context.WithCancel(context.Background())
	wg.Gox(func() {
		err := S.Run(Sctx)
		_ = err	// XXX
	})

	// expect:
	//tc.ExpectNetListen("1")
	tc.Expect(&xnet.TraceListen{Laddr: "1"})
	tc.Expect(&xnet.TraceDial{Dst: "0"})
	//tc.ExpectNetDial("0")

	//tc.Expect(xnet.NetTx{Src: "2c", Dst: "2s", Pkt: []byte("\x00\x00\x00\x01")})
	//tc.Expect(nettx("2c", "2s", "\x00\x00\x00\x01"))

	// handshake
	tc.Expect(nettx("s:1", "m:1", "\x00\x00\x00\x01"))
	tc.Expect(nettx("m:1", "s:1", "\x00\x00\x00\x01"))

	//tc.ExpectNetTx("2c", "2s", "\x00\x00\x00\x01")	// handshake
	//tc.ExpectNetTx("2s", "2c", "\x00\x00\x00\x01")
	tc.ExpectPar(
		//&xnet.TraceTx{Src: "2c", Dst: "2s", Pkt: []byte("\x00\x00\x00\x01")},
		//&xnet.TraceTx{Src: "2s", Dst: "2c", Pkt: []byte("\x00\x00\x00\x01")},)
		&xnet.TraceTx{Src: &pipenet.Addr{Net: "pipe", Port: 2, Endpoint: 0}, Dst: &pipenet.Addr{Net: "pipe", Port: 2, Endpoint: 1}, Pkt: []byte("\x00\x00\x00\x01")},
	)
		//&xnet.TraceTx{Src: "2s", Dst: "2c", Pkt: []byte("\x00\x00\x00\x01")},)


	// XXX temp
	return

	// M <- S	.? RequestIdentification{...}		+ TODO test ID rejects
	// M -> S	.? AcceptIdentification{...}
	// M.nodeTab	<- Node(S)	XXX order can be racy?
	// S.nodeTab	<- Node(M)	XXX order can be racy?
	//
	// ; storCtlRecovery
	// M -> S	.? Recovery
	// S <- M	.? AnswerRecovery
	//
	// M -> S	.? AskPartitionTable
	// S <- M	.? AnswerPartitionTable
	// M.partTab	<- ...	XXX
	// XXX updated something cluster currently can be operational

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
func TestClientStorage(t *testing.T) {
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
