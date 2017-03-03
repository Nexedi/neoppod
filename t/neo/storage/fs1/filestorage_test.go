// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// FileStorage v1. Tests	XXX text
package fs1

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"../../zodb"

	"lab.nexedi.com/kirr/go123/exc"
)

// XXX -> testDbEntry ?

// one database transaction record
type dbEntry struct {
	Header	TxnHeader
	Entryv	[]txnEntry
}

// one entry inside transaction
type txnEntry struct {
	Header	 DataHeader
	rawData	 []byte		// what is on disk, e.g. it can be backpointer
	userData []byte		// data client should see on load; nil means same as RawData
	dataTid  zodb.Tid	// data tid client should see on iter; 0 means same as Header.Tid
}

// Data returns data a client should see
func (txe *txnEntry) Data() []byte {
	data := txe.userData
	if data == nil {
		data = txe.rawData
	}
	return data
}

// DataTid returns data tid a client should see
func (txe *txnEntry) DataTid() zodb.Tid {
	dataTid := txe.dataTid
	if dataTid == 0 {
		dataTid = txe.Header.Tid
	}
	return dataTid
}

// successfull result of load for an oid
type oidLoadedOk struct {
	tid	zodb.Tid
	data	[]byte
}

// checkLoad verifies that fs.Load(xid) returns expected result
func checkLoad(t *testing.T, fs *FileStorage, xid zodb.Xid, expect oidLoadedOk) {
	data, tid, err := fs.Load(xid)
	if err != nil {
		t.Errorf("load %v: %v", xid, err)
	}
	if tid != expect.tid {
		t.Errorf("load %v: returned tid unexpected: %v  ; want: %v", xid, tid, expect.tid)
	}
	if !bytes.Equal(data, expect.data) {
		t.Errorf("load %v: different data:\nhave: %q\nwant: %q", xid, data, expect.data)
	}
}

func xfsopen(t *testing.T, path string) *FileStorage {
	fs, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestLoad(t *testing.T) {
	fs := xfsopen(t, "testdata/1.fs")	// TODO open read-only
	defer exc.XRun(fs.Close)

	// current knowledge of what was "before" for an oid as we scan over
	// data base entries
	before := map[zodb.Oid]oidLoadedOk{}

	for _, dbe := range _1fs_dbEntryv {
		for _, txe := range dbe.Entryv {
			txh := txe.Header

			// XXX check Load finds data at correct .Pos / etc ?

			// loadSerial
			// TODO also test for getting error when not found
			xid := zodb.Xid{zodb.XTid{txh.Tid, false}, txh.Oid}
			checkLoad(t, fs, xid, oidLoadedOk{txh.Tid, txe.Data()})

			// loadBefore
			// TODO also test for getting error when not found
			xid = zodb.Xid{zodb.XTid{txh.Tid, true}, txh.Oid}
			expect, ok := before[txh.Oid]
			if ok {
				checkLoad(t, fs, xid, expect)
			}

			// loadBefore to get current record
			xid.Tid += 1
			checkLoad(t, fs, xid, oidLoadedOk{txh.Tid, txe.Data()})

			before[txh.Oid] = oidLoadedOk{txh.Tid, txe.Data()}

		}
	}

	// loadBefore with TidMax
	for oid, expect := range before {
		xid := zodb.Xid{zodb.XTid{zodb.TidMax, true}, oid}
		checkLoad(t, fs, xid, expect)
	}
}

// iterate tidMin..tidMax and expect db entries in expectv
func testIterate(t *testing.T, fs *FileStorage, tidMin, tidMax zodb.Tid, expectv []dbEntry) {
	iter := fs.Iterate(tidMin, tidMax)
	fsi := iter.(*iterator)

	for k := 0; ; k++ {
		txnErrorf := func(format string, a ...interface{}) {
			subj := fmt.Sprintf("iterating %v..%v: step %v#%v", tidMin, tidMax, k, len(expectv))
			msg  := fmt.Sprintf(format, a...)
			t.Errorf("%v: %v", subj, msg)
		}

		txni, dataIter, err := iter.NextTxn()
		if err != nil {
			if err == io.EOF {
				if k != len(expectv) {
					txnErrorf("steps underrun")
				}
				break
			}
			txnErrorf("%v", err)
		}

		if k >= len(expectv) {
			txnErrorf("steps overrun")
		}

		dbe := expectv[k]

		// assert txni points to where we expect - this will allow us
		// not only to check .TxnInfo but also .Pos, .LenPrev, .Len etc in
		// whole expected TxnHeader
		if txni != &fsi.txnIter.Txnh.TxnInfo {
			t.Fatal("unexpected txni pointer")
		}

		// compare transaction headers modulo .workMem
		// (workMem is not initialized in _1fs_dbEntryv)
		txnh1 := fsi.txnIter.Txnh
		txnh2 := dbe.Header
		txnh1.workMem = nil
		txnh2.workMem = nil
		if !reflect.DeepEqual(txnh1, txnh2) {
			txnErrorf("unexpected txn entry:\nhave: %q\nwant: %q", txnh1, txnh2)
		}

		for kdata := 0; ; kdata++ {
			dataErrorf := func(format string, a...interface{}) {
				dsubj := fmt.Sprintf("dstep %v#%v", kdata, len(dbe.Entryv))
				msg   := fmt.Sprintf(format, a...)
				txnErrorf("%v: %v", dsubj, msg)
			}

			datai, err := dataIter.NextData()
			if err != nil {
				if err == io.EOF {
					if kdata != len(dbe.Entryv) {
						dataErrorf("dsteps underrun")
					}
					break
				}
				dataErrorf("%v", err)
			}

			if kdata > len(dbe.Entryv) {
				dataErrorf("dsteps overrun")
			}

			txe := dbe.Entryv[kdata]
			dh  := txe.Header

			// assert datai pointes to where we expect - this will allow us
			// not only to check oid/tid/data but also to check whole data header.
			if datai != &fsi.dataIter.sri {
				t.Fatal("unexpected datai pointer")
			}

			if !reflect.DeepEqual(fsi.dataIter.Datah, dh) {
				dataErrorf("unexpected data entry:\nhave: %q\nwant: %q", fsi.dataIter.Datah, dh)
			}

			// check what was actually returned - since it is not in ^^^ data structure
			if datai.Oid != dh.Oid {
				dataErrorf("oid mismatch: have %v;  want %v", datai.Oid, dh.Oid)
			}
			if datai.Tid != dh.Tid {
				dataErrorf("tid mismatch: have %v;  want %v", datai.Tid, dh.Tid)
			}
			if !bytes.Equal(datai.Data, txe.Data()) {
				dataErrorf("data mismatch:\nhave %q\nwant %q", datai.Data, txe.Data())
			}

			if datai.DataTid != txe.DataTid() {
				dataErrorf("data tid mismatch: have %v;  want %v", datai.DataTid, txe.DataTid())
			}
		}
	}
}

func TestIterate(t *testing.T) {
	fs := xfsopen(t, "testdata/1.fs")	// TODO open ro
	defer exc.XRun(fs.Close)

	// all []tids in test database
	tidv := []zodb.Tid{}
	for _, dbe := range _1fs_dbEntryv {
		tidv = append(tidv, dbe.Header.Tid)
	}

	// check all i,j pairs in tidv
	// for every tid also check ±1 to test edge cases
	for i, tidMin := range tidv {
	for j, tidMax := range tidv {
		minv := []zodb.Tid{tidMin-1, tidMin, tidMin+1}
		maxv := []zodb.Tid{tidMax-1, tidMax, tidMax+1}

		for ii, tmin := range minv {
		for jj, tmax := range maxv {
			// expected number of txn iteration steps
			nsteps := j - i + 1
			nsteps -= ii / 2	// one less point for tidMin+1
			nsteps -= (2 - jj) / 2	// one less point for tidMax-1
			if nsteps < 0 {
				nsteps = 0	// j < i and j == i and ii/jj
			}

			//fmt.Printf("%d%+d .. %d%+d\t -> %d steps\n", i, ii-1, j, jj-1, nsteps)
			testIterate(t, fs, tmin, tmax, _1fs_dbEntryv[i + ii/2:][:nsteps])
		}}
	}}
}
