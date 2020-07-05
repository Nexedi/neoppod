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

package fs1

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"testing"

	"lab.nexedi.com/kirr/neo/go/internal/xtesting"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/go123/exc"
)

// one database transaction record
type dbEntry struct {
	Header TxnHeader
	Entryv []txnEntry
}

// one entry inside transaction
type txnEntry struct {
	Header      DataHeader
	rawData     []byte   // what is on disk, e.g. it can be backpointer
	userData    []byte   // data client should see on load; `sameAsRaw` means same as RawData
	DataTidHint zodb.Tid // data tid client should see on iter
}

var sameAsRaw = []byte{0}

// Data returns data a client should see
func (txe *txnEntry) Data() []byte {
	data := txe.userData
	if len(data) > 0 && &data[0] == &sameAsRaw[0] {
		data = txe.rawData
	}
	return data
}

func xfsopen(t testing.TB, path string) (*FileStorage, zodb.Tid) {
	t.Helper()
	return xfsopenopt(t, path, &zodb.DriverOptions{ReadOnly: true})
}

func xfsopenopt(t testing.TB, path string, opt *zodb.DriverOptions) (*FileStorage, zodb.Tid) {
	t.Helper()
	fs, at0, err := Open(context.Background(), path, opt)
	if err != nil {
		t.Fatal(err)
	}
	return fs, at0
}

func TestLoad(t *testing.T) {
	fs, _ := xfsopen(t, "testdata/1.fs")
	defer exc.XRun(fs.Close)

	txnv := []xtesting.Txn{}
	for _, dbe := range _1fs_dbEntryv {
		txn := xtesting.Txn{Header: &zodb.TxnInfo{
			Tid: dbe.Header.Tid,

		}}
		for _, txe := range dbe.Entryv {
			data := &zodb.DataInfo{
				Oid:         txe.Header.Oid,
				Tid:         txn.Header.Tid,
				Data:        txe.Data(),
				DataTidHint: txe.DataTidHint,
			}
			txn.Data = append(txn.Data, data)
		}
		txnv = append(txnv, txn)
	}

	xtesting.DrvTestLoad(t, fs, txnv)
}

// iterate tidMin..tidMax and expect db entries in expectv
// TODO -> xtesting
func testIterate(t *testing.T, fs *FileStorage, tidMin, tidMax zodb.Tid, expectv []dbEntry) {
	ctx := context.Background()
	iter := fs.Iterate(ctx, tidMin, tidMax)
	fsi, ok := iter.(*zIter)
	if !ok {
		_, _, err := iter.NextTxn(ctx)
		t.Errorf("iterating %v..%v: iter type is %T  ; want zIter\nNextTxn gives: _, _, %v", tidMin, tidMax, iter, err)
		return
	}

	for k := 0; ; k++ {
		txnErrorf := func(format string, a ...interface{}) {
			t.Helper()
			subj := fmt.Sprintf("iterating %v..%v: step %v#%v", tidMin, tidMax, k, len(expectv))
			msg  := fmt.Sprintf(format, a...)
			t.Errorf("%v: %v", subj, msg)
		}

		txni, dataIter, err := iter.NextTxn(ctx)
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
		if txni != &fsi.iter.Txnh.TxnInfo {
			t.Fatal("unexpected txni pointer")
		}

		// compare transaction headers modulo .workMem
		// (workMem is not initialized in _1fs_dbEntryv)
		txnh1 := fsi.iter.Txnh
		txnh2 := dbe.Header
		txnh1.workMem = nil
		txnh2.workMem = nil
		if !reflect.DeepEqual(txnh1, txnh2) {
			txnErrorf("unexpected txn entry:\nhave: %q\nwant: %q", txnh1, txnh2)
		}

		for kdata := 0; ; kdata++ {
			dataErrorf := func(format string, a ...interface{}) {
				t.Helper()
				dsubj := fmt.Sprintf("dstep %v#%v", kdata, len(dbe.Entryv))
				msg   := fmt.Sprintf(format, a...)
				txnErrorf("%v: %v", dsubj, msg)
			}

			datai, err := dataIter.NextData(ctx)
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

			// assert datai points to where we expect - this will allow us
			// not only to check oid/tid/data but also to check whole data header.
			if datai != &fsi.datai {
				t.Fatal("unexpected datai pointer")
			}

			// compare data headers modulo .workMem
			// (workMem is not initialized in _1fs_dbEntryv)
			fsi.iter.Datah.workMem = dh.workMem

			if !reflect.DeepEqual(fsi.iter.Datah, dh) {
				dataErrorf("unexpected data entry:\nhave: %q\nwant: %q", fsi.iter.Datah, dh)
			}

			// check what was actually returned - since it is not in ^^^ data structure
			if datai.Oid != dh.Oid {
				dataErrorf("oid mismatch: have %v;  want %v", datai.Oid, dh.Oid)
			}
			if datai.Tid != dh.Tid {
				dataErrorf("tid mismatch: have %v;  want %v", datai.Tid, dh.Tid)
			}
			if !reflect.DeepEqual(datai.Data, txe.Data()) { // NOTE reflect to catch nil != ""
				dataErrorf("data mismatch:\nhave %q\nwant %q", datai.Data, txe.Data())
			}

			if datai.DataTidHint != txe.DataTidHint {
				dataErrorf("data tid hint mismatch: have %v;  want %v", datai.DataTidHint, txe.DataTidHint)
			}
		}
	}
}

// TODO -> xtesting
func TestIterate(t *testing.T) {
	fs, _ := xfsopen(t, "testdata/1.fs")
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

	// also check 0..tidMax
	testIterate(t, fs, 0, zodb.TidMax, _1fs_dbEntryv[:])
}

// TODO -> xtesting
func BenchmarkIterate(b *testing.B) {
	fs, _ := xfsopen(b, "testdata/1.fs")
	defer exc.XRun(fs.Close)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := fs.Iterate(ctx, zodb.Tid(0), zodb.TidMax)

		for {
			txni, dataIter, err := iter.NextTxn(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatal(err)
			}

			// use txni
			_ = txni.Tid

			for {
				datai, err := dataIter.NextData(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					b.Fatal(err)
				}

				// use datai
				_ = datai.Data
			}

		}
	}

	b.StopTimer()
}

// TestWatch verifies that watcher can observe commits done from outside.
func TestWatch(t *testing.T) {
	workdir := xworkdir(t)
	xtesting.DrvTestWatch(t, workdir + "/t.fs", openByURL)
}

// TestOpenRecovery verifies how Open handles data file with not-finished voted
// transaction in the end.
func TestOpenRecovery(t *testing.T) {
	X := exc.Raiseif
	main, err := ioutil.ReadFile("testdata/1.fs"); X(err)
	index, err := ioutil.ReadFile("testdata/1.fs.index"); X(err)
	headOk := _1fs_dbEntryv[len(_1fs_dbEntryv)-1].Header.Tid
	topPos := int64(_1fs_indexTopPos)
	voteTail, err := ioutil.ReadFile("testdata/1voted.tail"); X(err)

	workdir := xworkdir(t)
	ctx := context.Background()

	// checkL runs f on main + voteTail[:l] . Two cases are verified:
	// 1) when index is already present, and
	// 2) when initially there is no index.
	checkL := func(t *testing.T, l int, f func(t *testing.T, tfs string)) {
		tfs := fmt.Sprintf("%s/1+vote%d.fs", workdir, l)
		t.Run(fmt.Sprintf("oldindex=n/tail=+vote%d", l), func(t *testing.T) {
			err := ioutil.WriteFile(tfs, append(main, voteTail[:l]...), 0600); X(err)
			f(t, tfs)
		})
		t.Run(fmt.Sprintf("oldindex=y/tail=+vote%d", l), func(t *testing.T) {
			err := ioutil.WriteFile(tfs+".index", index, 0600); X(err)
			f(t, tfs)
		})
	}

	// if txn header can be fully read - it should be all ok
	lok := []int{0}
	for l := len(voteTail); l >= TxnHeaderFixSize; l-- {
		lok = append(lok, l)
	}
	for _, l := range lok {
		checkL(t, l, func(t *testing.T, tfs string) {
			fs, at0 := xfsopen(t, tfs)
			defer func() {
				err = fs.Close(); X(err)
			}()
			if at0 != headOk {
				t.Fatalf("at0: %s  ; expected: %s", at0, headOk)
			}
			head, err := fs.Sync(ctx); X(err)
			if head != headOk {
				t.Fatalf("head: %s  ; expected: %s", head, headOk)
			}
		})
	}

	// if txn header is stably incomplete - open should fail
	// XXX better check 0..sizeof(txnh)-1 but in this range each check is slow.
	for _, l := range []int{TxnHeaderFixSize-1,1} {
		checkL(t, l, func(t *testing.T, tfs string) {
			_, _, err := Open(ctx, tfs, &zodb.DriverOptions{ReadOnly: true})
			estr := ""
			if err != nil {
				estr = err.Error()
			}
			ewant := fmt.Sprintf("open %s: checking whether it is garbage @%d: %s",
				tfs, topPos, &RecordError{tfs, "transaction record", topPos, "read", io.ErrUnexpectedEOF})
			if estr != ewant {
				t.Fatalf("unexpected error:\nhave: %q\nwant: %q", estr, ewant)
			}
		})
	}
}
