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

// Package xtesting provides infrastructure for ZODB testing.
package xtesting

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

var (
	pyMu   sync.Mutex
	pyHave = map[string]bool{} // {} pymod -> y/n  ; ".python" indicates python presence
)

// NeedPy skips current test if python and specified modules are not available.
//
// For example
//
//	xtesting.NeedPy(t)
//
// would check for python presence, and
//
//	xtesting.NeedPy(t, "ZODB", "golang.strconv")
//
// would check if all python and ZODB and golang.strconv python modules are available.
func NeedPy(t testing.TB, modules ...string) {
	pyMu.Lock()
	defer pyMu.Unlock()

	// verify if python is present
	havePy, know := pyHave[".python"]
	if !know {
		cmd := exec.Command("python2", "-c", "0")
		err := cmd.Run()
		havePy = (err == nil)
		pyHave[".python"] = havePy
	}

	if !havePy {
		t.Skipf("skipping: python is not availble")
	}

	var donthave []string
	for _, pymod := range modules {
		have, know := pyHave[pymod]
		if !know {
			cmd := exec.Command("python2", "-c", "import "+pymod)
			err := cmd.Run()
			have = (err == nil)
			pyHave[pymod] = have
		}
		if !have {
			donthave = append(donthave, pymod)
		}
	}

	if len(donthave) != 0 {
		t.Skipf("skipping: the following python modules are not available: %s", donthave)
	}

	// we verified everything - now it is ok not to skip.
	return
}

// ZRawObject represents raw ZODB object state.
type ZRawObject struct {
	Oid  zodb.Oid
	Data []byte // raw serialized zodb data
}

// ZPyCommitRaw commits new transaction into database @ zurl with raw data specified by objv.
//
// The commit is performed via zodbtools/py.
func ZPyCommitRaw(zurl string, at zodb.Tid, objv ...ZRawObject) (_ zodb.Tid, err error) {
	defer xerr.Contextf(&err, "%s: zpycommit @%s", zurl, at)

	// prepare text input for `zodb commit`
	zin := &bytes.Buffer{}
	fmt.Fprintf(zin, "user %q\n", "author")
	fmt.Fprintf(zin, "description %q\n", fmt.Sprintf("test commit; at=%s", at))
	fmt.Fprintf(zin, "extension %q\n", "")
	for _, obj := range objv {
		fmt.Fprintf(zin, "obj %s %d null:00\n", obj.Oid, len(obj.Data))
		zin.Write(obj.Data)
		zin.WriteString("\n")
	}
	zin.WriteString("\n")

	// run py `zodb commit`
	cmd:= exec.Command("python2", "-m", "zodbtools.zodb", "commit", zurl, at.String())
	cmd.Stdin  = zin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return zodb.InvalidTid, err
	}

	out = bytes.TrimSuffix(out, []byte("\n"))
	tid, err := zodb.ParseTid(string(out))
	if err != nil {
		return zodb.InvalidTid, fmt.Errorf("committed, but invalid output: %s", err)
	}

	return tid, nil
}


// ---- tests for storage drivers ----

// state of an object in the database for some particular revision
type objState struct {
	tid  zodb.Tid
	data []byte // nil if obj was deleted
}

// Txn represents one transaction.
type Txn struct {
	Header *zodb.TxnInfo
	Data   []*zodb.DataInfo
}

// checkLoad verifies that zdrv.Load(xid) returns expected result.
func checkLoad(t *testing.T, zdrv zodb.IStorageDriver, xid zodb.Xid, expect objState) {
	t.Helper()
	buf, tid, err := zdrv.Load(context.Background(), xid)

	// deleted obj - it should load with "no data"
	if expect.data == nil {
		errOk := &zodb.OpError{
			URL:  zdrv.URL(),
			Op:   "load",
			Args: xid,
			Err:  &zodb.NoDataError{Oid: xid.Oid, DeletedAt: expect.tid},
		}
		if !reflect.DeepEqual(err, errOk) {
			t.Errorf("load %v: returned err unexpected:\nhave: %v\nwant: %v", xid, err, errOk)
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
			t.Errorf("load %v: returned err unexpected:\nhave: %v\nwant: nil", xid, err)
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

// DrvTestLoad verifies that zdrv implements Load correctly.
//
// txnvOk is what data to expect to be in the database.
func DrvTestLoad(t *testing.T, zdrv zodb.IStorageDriver, txnvOk []Txn) {
	// current knowledge of what was "before" for an oid as we scan over
	// data base entries
	before := map[zodb.Oid]objState{}

	for _, txn := range txnvOk {
		for _, obj := range txn.Data {
			txh := txn.Header

			// XXX check Load finds data at correct .Pos / etc ?

			// ~ loadSerial
			xid := zodb.Xid{txh.Tid, obj.Oid}
			checkLoad(t, zdrv, xid, objState{txh.Tid, obj.Data})

			// ~ loadBefore
			xid = zodb.Xid{txh.Tid - 1, obj.Oid}
			expect, ok := before[obj.Oid]
			if ok {
				checkLoad(t, zdrv, xid, expect)
			}

			before[obj.Oid] = objState{txh.Tid, obj.Data}

		}
	}

	// load at ∞ with TidMax
	// XXX should we get "no such transaction" with at > head? - yes
	for oid, expect := range before {
		xid := zodb.Xid{zodb.TidMax, oid}
		checkLoad(t, zdrv, xid, expect)
	}
}

// DrvTestWatch verifies that storage driver watcher can observe commits done from outside.
func DrvTestWatch(t *testing.T, zurl string, zdrvOpen zodb.DriverOpener) {
	X := FatalIf(t)

	NeedPy(t, "zodbtools")

	u, err := url.Parse(zurl); X(err)

	// xcommit commits new transaction into zurl with raw data specified by objv.
	xcommit := func(at zodb.Tid, objv ...ZRawObject) zodb.Tid {
		t.Helper()
		tid, err := ZPyCommitRaw(zurl, at, objv...); X(err)
		return tid
	}

	// force database creation & open zurl at go side
	at := xcommit(0, ZRawObject{0, b("data0")})

	ctx := context.Background()
	watchq := make(chan zodb.Event)
	zdrv, at0, err := zdrvOpen(ctx, u, &zodb.DriverOptions{ReadOnly: true, Watchq: watchq})
	if at0 != at {
		t.Fatalf("opened @ %s  ; want %s", at0, at)
	}

	checkHead := func(headOk zodb.Tid) {
		t.Helper()
		head, err := zdrv.Sync(ctx); X(err)
		if head != headOk {
			t.Fatalf("check head: got %s;  want %s", head, headOk)
		}
	}

	checkHead(at)

	checkLoad := func(at zodb.Tid, oid zodb.Oid, dataOk string, serialOk zodb.Tid) {
		t.Helper()
		xid := zodb.Xid{at, oid}
		buf, serial, err := zdrv.Load(ctx, xid); X(err)
		data := string(buf.XData())
		if !(data == dataOk && serial == serialOk) {
			t.Fatalf("check load %s:\nhave: %q %s\nwant: %q %s",
				xid, data, serial, dataOk, serialOk)
		}
	}

	// commit -> check watcher observes what we committed.
	//
	// XXX python `import pkg_resources` takes ~ 300ms.
	// https://github.com/pypa/setuptools/issues/510
	//
	// Since pkg_resources are used everywhere (e.g. in zodburi to find all
	// uri resolvers) this import slowness becomes the major part of time to
	// run py `zodb commit`.
	//
	// if one day it is either fixed, or worked around, we could ↑ 10 to 100.
	for i := zodb.Oid(1); i <= 10; i++ {
		data0 := fmt.Sprintf("data0.%d", i)
		datai := fmt.Sprintf("data%d", i)
		at = xcommit(at,
			ZRawObject{0, b(data0)},
			ZRawObject{i, b(datai)})

		// TODO also test for watcher errors
		event := <-watchq

		var δ *zodb.EventCommit
		switch event := event.(type) {
		default:
			panic(fmt.Sprintf("unexpected event: %T", event))

		case *zodb.EventError:
			t.Fatal(event.Err)

		case *zodb.EventCommit:
			δ = event
		}

		if objvWant := []zodb.Oid{0, i}; !(δ.Tid == at && reflect.DeepEqual(δ.Changev, objvWant)) {
			t.Fatalf("watch:\nhave: %s %s\nwant: %s %s", δ.Tid, δ.Changev, at, objvWant)
		}

		checkHead(at)

		// make sure we can load what was committed.
		checkLoad(at, 0, data0, at)
		checkLoad(at, i, datai, at)
	}

	err = zdrv.Close(); X(err)

	e, ok := <-watchq
	if ok {
		t.Fatalf("watch after close -> %v;  want closed", e)
	}
}


// FatalIf(t) returns function f(err), which call t.Fatal if err != nil.
func FatalIf(t *testing.T) func(error) {
	return func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
}


// b is syntactic sugar for byte literals.
//
// e.g.
//
//	b("hello")
func b(data string) []byte {
	return []byte(data)
}
