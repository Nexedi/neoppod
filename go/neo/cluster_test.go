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

package neo_test
// test interaction between nodes

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	. "../neo"
	"../neo/client"
	"../neo/server"

	"../zodb"
	"../zodb/storage/fs1"

	"lab.nexedi.com/kirr/go123/exc"
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

// M drives cluster with 1 S through recovery -> verification -> service -> shutdown
func TestMasterStorage(t *testing.T) {
	net := NetPipe("")	// test network
	Maddr := "0"
	Saddr := "1"

	M := server.NewMaster("abc1", Maddr, net)

	zstor := xfs1stor("../zodb/storage/fs1/testdata/1.fs")
	S := server.NewStorage("abc1", Maddr, Saddr, net, zstor)

	Mctx, Mcancel := context.WithCancel(context.Background())
	Sctx, Scancel := context.WithCancel(context.Background())
	_ = Scancel;

	//Mev := M.subscribe(...)

	wg := WorkGroup()
	wg.Gox(func() {
		err := M.Run(Mctx)
		_ = err // XXX
	})

	//ev <- Mev
	//assert ev == ClusterInformation{State: RECOVERY}

	if false {
		err := S.Run(Sctx)	// XXX go
		_ = err
	}


	xwait(wg)
	Mcancel()	// XXX temp
}

// basic interaction between Client -- Storage
func TestClientStorage(t *testing.T) {
	// XXX temp disabled
	return

	Cnl, Snl := NodeLinkPipe()
	wg := WorkGroup()

	Sctx, Scancel := context.WithCancel(context.Background())

	net := NetPipe("")	// XXX here? (or a bit above?)
	zstor := xfs1stor("../zodb/storage/fs1/testdata/1.fs")	// XXX +readonly
	S := server.NewStorage("cluster", "Maddr", "Saddr", net, zstor)
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
}
