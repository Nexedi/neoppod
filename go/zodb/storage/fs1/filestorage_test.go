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

package fs1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"
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

// state of an object in the database for some particular revision
type objState struct {
	tid  zodb.Tid
	data []byte // nil if obj was deleted
}


// checkLoad verifies that fs.Load(xid) returns expected result
func checkLoad(t *testing.T, fs *FileStorage, xid zodb.Xid, expect objState) {
	t.Helper()
	buf, tid, err := fs.Load(context.Background(), xid)

	// deleted obj - it should load with "no data"
	if expect.data == nil {
		errOk := &zodb.OpError{
			URL:  fs.URL(),
			Op:   "load",
			Args: xid,
			Err:  &zodb.NoDataError{Oid: xid.Oid, DeletedAt: expect.tid},
		}
		if !reflect.DeepEqual(err, errOk) {
			t.Errorf("load %v: returned err unexpected: %v  ; want: %v", xid, err, errOk)
		}

		if tid != 0 {
			t.Errorf("load %v: returned tid unexpected: %v  ; want: %v", xid, tid, expect.tid)
		}

		if buf != nil {
			t.Errorf("load %v: returned buf != nil", xid)
		}

	// regular load
	} else {
		if err != nil {
			t.Errorf("load %v: returned err unexpected: %v  ; want: nil", xid, err)
		}

		if tid != expect.tid {
			t.Errorf("load %v: returned tid unexpected: %v  ; want: %v", xid, tid, expect.tid)
		}

		switch {
		case buf == nil:
			t.Errorf("load %v: returned buf = nil", xid)

		case !reflect.DeepEqual(buf.Data, expect.data): // NOTE reflect to catch nil != ""
			t.Errorf("load %v: different data:\nhave: %q\nwant: %q", xid, buf.Data, expect.data)
		}
	}
}

func xfsopen(t testing.TB, path string) *FileStorage {
	t.Helper()
	return xfsopenopt(t, path, &zodb.DriverOptions{ReadOnly: true})
}

func xfsopenopt(t testing.TB, path string, opt *zodb.DriverOptions) *FileStorage {
	t.Helper()
	fs, err := Open(context.Background(), path, opt)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestLoad(t *testing.T) {
	fs := xfsopen(t, "testdata/1.fs")
	defer exc.XRun(fs.Close)

	// current knowledge of what was "before" for an oid as we scan over
	// data base entries
	before := map[zodb.Oid]objState{}

	for _, dbe := range _1fs_dbEntryv {
		for _, txe := range dbe.Entryv {
			txh := txe.Header

			// XXX check Load finds data at correct .Pos / etc ?

			// ~ loadSerial
			xid := zodb.Xid{txh.Tid, txh.Oid}
			checkLoad(t, fs, xid, objState{txh.Tid, txe.Data()})

			// ~ loadBefore
			xid = zodb.Xid{txh.Tid - 1, txh.Oid}
			expect, ok := before[txh.Oid]
			if ok {
				checkLoad(t, fs, xid, expect)
			}

			before[txh.Oid] = objState{txh.Tid, txe.Data()}

		}
	}

	// load at ∞ with TidMax
	// XXX should we get "no such transaction" with at > head?
	for oid, expect := range before {
		xid := zodb.Xid{zodb.TidMax, oid}
		checkLoad(t, fs, xid, expect)
	}
}

// iterate tidMin..tidMax and expect db entries in expectv
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

func TestIterate(t *testing.T) {
	fs := xfsopen(t, "testdata/1.fs")
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

func BenchmarkIterate(b *testing.B) {
	fs := xfsopen(b, "testdata/1.fs")
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

// // XXX kill
// var tracef = func(format string, argv ...interface{}) {
// 	log.Printf("W  " + format, argv...)
// }
// 
// func init() {
// 	log.SetFlags(log.Lmicroseconds)
// }



func TestWatch(t *testing.T) {
	X := exc.Raiseif

	//xtesting.NeedPy(t, "zodbtools")
	needZODBPy(t)
	workdir := xworkdir(t)
	tfs := workdir + "/t.fs"

	// Object represents object state to be committed.
	type Object struct {
		oid  zodb.Oid
		data string
	}

	// zcommit commits new transaction into tfs with data specified by objv.
	zcommit := func(at zodb.Tid, objv ...Object) (_ zodb.Tid, err error) {
		defer xerr.Contextf(&err, "zcommit @%s", at)

		// prepare text input for `zodb commit`
		zin := &bytes.Buffer{}
		fmt.Fprintf(zin, "user %q\n", "author")
		fmt.Fprintf(zin, "description %q\n", fmt.Sprintf("test commit; at=%s", at))
		fmt.Fprintf(zin, "extension %q\n", "")
		for _, obj := range objv {
			fmt.Fprintf(zin, "obj %s %d null:00\n", obj.oid, len(obj.data))
			zin.WriteString(obj.data)
			zin.WriteString("\n")
		}
		zin.WriteString("\n")

		// run py `zodb commit`
		cmd:= exec.Command("python2", "-m", "zodbtools.zodb", "commit", tfs, at.String())
		cmd.Stdin  = zin
		cmd.Stderr = os.Stderr
		out, err := cmd.Output(); X(err)

		out = bytes.TrimSuffix(out, []byte("\n"))
		tid, err := zodb.ParseTid(string(out))
		if err != nil {
			return zodb.InvalidTid, fmt.Errorf("committed, but invalid output: %s", err)
		}

		return tid, nil
	}

	xcommit := func(at zodb.Tid, objv ...Object) zodb.Tid {
		//tracef("-> xcommit %s", at)
		//defer tracef("<- xcommit")
		t.Helper()
		tid, err := zcommit(at, objv...); X(err)
		return tid
	}

	// force tfs creation & open tfs at go side
	at := xcommit(0, Object{0, "data0"})

	watchq := make(chan zodb.WatchEvent)
	fs := xfsopenopt(t, tfs, &zodb.DriverOptions{ReadOnly: true, Watchq: watchq})
	ctx := context.Background()

	checkLastTid := func(lastOk zodb.Tid) {
		t.Helper()
		head, err := fs.LastTid(ctx); X(err)
		if head != lastOk {
			t.Fatalf("check last_tid: got %s;  want %s", head, lastOk)
		}
	}

	checkLastTid(at)

	// commit -> check watcher observes what we committed.
	//
	// XXX python `import pkg_resources` takes ~ 300ms.
	// https://github.com/pypa/setuptools/issues/510
	//
	// Since pkg_resources are used everywhere (e.g. in zodburi to find all
	// uri resolvers) this import slowness becomes the major component to
	// run py `zodb commit`.
	//
	// if one day it is either fixed, or worked around, we could ↑ 10 to 100.
	for i := zodb.Oid(1); i <= 10; i++ {
		at = xcommit(at,
			Object{0, fmt.Sprintf("data0.%d", i)},
			Object{i, fmt.Sprintf("data%d", i)})

		e := <-watchq	// XXX err?

		if objvWant := []zodb.Oid{0, i}; !(e.Tid == at && reflect.DeepEqual(e.Changev, objvWant)) {
			t.Fatalf("watch:\nhave: %s %s\nwant: %s %s", e.Tid, e.Changev, at, objvWant)
		}

		checkLastTid(at)
	}

	err := fs.Close(); X(err)

	e, ok := <-watchq
	if ok {
		t.Fatalf("watch after close -> %v;  want closed", e)
	}
	_ = errors.Cause(nil)
	// XXX e.Err == ErrClosed

	//_, _, err = fs.Watch(ctx)
	//if e, eWant := errors.Cause(err), os.ErrClosed; e != eWant {
	//	t.Fatalf("watch after close -> %v;  want: cause %v", err, eWant)
	//}
}

// TestOpenRecovery verifies how Open handles data file with not-finished voted
// transaction in the end.
func TestOpenRecovery(t *testing.T) {
	X := exc.Raiseif
	main, err := ioutil.ReadFile("testdata/1.fs"); X(err)
	index, err := ioutil.ReadFile("testdata/1.fs.index"); X(err)
	lastTidOk := _1fs_dbEntryv[len(_1fs_dbEntryv)-1].Header.Tid
	voteTail, err := ioutil.ReadFile("testdata/1voted.tail"); X(err)

	workdir := xworkdir(t)
	ctx := context.Background()

	for l := len(voteTail); l >= 0; l-- {
		t.Run(fmt.Sprintf("tail=+vote%d", l), func(t *testing.T) {
			tfs := fmt.Sprintf("%s/1+vote%d.fs", workdir, l)
			err := ioutil.WriteFile(tfs, append(main, voteTail[:l]...), 0600); X(err)
			err = ioutil.WriteFile(tfs+".index", index, 0600); X(err)

			fs := xfsopen(t, tfs)
			head, err := fs.LastTid(ctx); X(err)
			if head != lastTidOk {
				t.Fatalf("last_tid: %s  ; expected: %s", head, lastTidOk)
			}

			err = fs.Close(); X(err)
		})
	}
}
