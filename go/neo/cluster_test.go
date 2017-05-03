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

package neo
// test interaction between nodes

import (
	"context"
	"testing"

	"../zodb"
	"../zodb/storage/fs1"
)

// basic interaction between Client -- Storage
func TestClientStorage(t *testing.T) {
	Cnl, Snl := nodeLinkPipe()
	wg := WorkGroup()

	Sctx, Scancel := context.WithCancel(context.Background())

	// TODO +readonly ?
	zstor, err := fs1.Open(context.Background(), "../zodb/storage/fs1/testdata/1.fs")
	if err != nil {
		t.Fatalf("zstor: %v", err)	// XXX err ctx ?
	}

	S := NewStorage(zstor)
	wg.Gox(func() {
		S.ServeLink(Sctx, Snl)
		// XXX + test error return
	})

	C, err := NewClient(Cnl)
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
		txni, dataIter, err := ziter.NextTxn()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ziter.NextTxn: %v", err)
		}

		txni.Tid

		for {
			datai, err := dataIter.NextData()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ziter.NextData: %v", err)
			}

			datai.Oid
			datai.Tid
			.Data
			.DataTid

			// XXX .Data = nil means deleted

			zodb.Xid{zodb.XTid{datai.Tid, false}, datai.Oid} // =tid:oid
			// TODO check Load
		}

	}

	// shutdown storage
	// XXX wait for S to shutdown + verify shutdown error
	Scancel()

	xwait(wg)
}
