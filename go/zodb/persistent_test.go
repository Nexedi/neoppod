// Copyright (C) 2018-2019  Nexedi SA and Contributors.
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

package zodb

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"lab.nexedi.com/kirr/neo/go/transaction"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/mem"
	"github.com/stretchr/testify/require"
)

// test Persistent type.
type MyObject struct {
	Persistent

	value string
}

func NewMyObject(jar *Connection) *MyObject {
	return NewPersistent(reflect.TypeOf(MyObject{}), jar).(*MyObject)
}

type myObjectState MyObject

func (o *myObjectState) DropState() {
	o.value = ""
}

func (o *myObjectState) PySetState(pystate interface{}) error {
	s, ok := pystate.(string)
	if !ok {
		return fmt.Errorf("myobject: setstate: want str; got %T", pystate)
	}

	o.value = s
	return nil
}

func (o *myObjectState) PyGetState() interface{} {
	return o.value
}

// Peristent that is not registered to ZODB.
type Unregistered struct {
	Persistent
}

func init() {
	t := reflect.TypeOf
	RegisterClass("t.zodb.MyObject", t(MyObject{}), t(myObjectState{}))
	RegisterClassAlias("t.zodb.MyOldObject", "t.zodb.MyObject")
}

// _checkObj verifies current state of persistent object.
//
// one can bind _checkObj to t via tCheckObj.
func _checkObj(t testing.TB, obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32, loading *loadState) {
	t.Helper()
	xbase := reflect.ValueOf(obj).Elem().FieldByName("Persistent")
	pbase := xbase.Addr().Interface().(*Persistent)

	var badv []string
	badf := func(format string, argv ...interface{}) {
		badv = append(badv, fmt.Sprintf(format, argv...))
	}

	zc := pbase.zclass
	//zc.class
	if typ := reflect.TypeOf(obj).Elem(); typ != zc.typ {
		badf("invalid zclass: .typ = %s  ; want %s", zc.typ, typ)
	}
	//zc.stateType

	if pbase.jar != jar {
		badf("invalid jar")
	}
	if pbase.oid != oid {
		badf("invalid oid: %s  ; want %s", pbase.oid, oid)
	}
	if pbase.serial != serial {
		badf("invalid serial: %s  ; want %s", pbase.serial, serial)
	}
	if pbase.state != state {
		badf("invalid state: %s  ; want %s", pbase.state, state)
	}
	if pbase.refcnt != refcnt {
		badf("invalid refcnt: %s  ; want %s", pbase.refcnt, refcnt)
	}
	if pbase.instance != obj {
		badf("base.instance != obj")
	}
	// XXX loading too?

	if len(badv) != 0 {
		msg := fmt.Sprintf("%#v:\n", obj)
		for _, bad := range badv {
			msg += fmt.Sprintf("\t- %s\n", bad)
		}
		t.Fatal(msg)
	}
}

func tCheckObj(t testing.TB) func(IPersistent, *Connection, Oid, Tid, ObjectState, int32, *loadState) {
	return func(obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32, loading *loadState) {
		t.Helper()
		_checkObj(t, obj, jar, oid, serial, state, refcnt, loading)
	}
}

// basic Persistent tests without storage.
func TestPersistentBasic(t *testing.T) {
	assert := require.New(t)
	checkObj := tCheckObj(t)

	// unknown type -> Broken
	xobj := newGhost("t.unknown", 10, nil)
	b, ok := xobj.(*Broken)
	if !ok {
		t.Fatalf("unknown -> %T;  want Broken", xobj)
	}

	checkObj(b, nil, 10, InvalidTid, GHOST, 0, nil)
	assert.Equal(b.class, "t.unknown")
	assert.Equal(b.state, (*mem.Buf)(nil))


	// t.zodb.MyObject -> *MyObject
	xobj = newGhost("t.zodb.MyObject", 11, nil)
	obj, ok := xobj.(*MyObject)
	if !ok {
		t.Fatalf("t.zodb.MyObject -> %T;  want MyObject", xobj)
	}

	checkObj(obj, nil, 11, InvalidTid, GHOST, 0, nil)
	assert.Equal(ClassOf(obj), "t.zodb.MyObject")

	// t.zodb.MyOldObject -> *MyObject
	xobj = newGhost("t.zodb.MyOldObject", 12, nil)
	obj, ok = xobj.(*MyObject)
	if !ok {
		t.Fatalf("t.zodb.MyOldObject -> %T;  want MyObject", xobj)
	}

	checkObj(obj, nil, 12, InvalidTid, GHOST, 0, nil)
	assert.Equal(ClassOf(obj), "t.zodb.MyObject")

	// ClassOf(unregistered-obj)
	obj2 := &Unregistered{}
	assert.Equal(ClassOf(obj2), `ZODB.Go("lab.nexedi.com/kirr/neo/go/zodb.Unregistered")`)

	// XXX deactivate refcnt < 0  - check error message (this verifies badf fix)
}

// zcacheControl is simple live cache control that prevents specified objects
// to be evicted from live cache.
type zcacheControl struct {
	keep []Oid // objects that must not be evicted
}

func (cc *zcacheControl) WantEvict(obj IPersistent) bool {
	for _, oid := range cc.keep {
		if obj.POid() == oid {
			return false
		}
	}
	return true
}

/*
type testenv struct {
	*testing.T

	ctx
	txn

	obj1
	obj2

	// XXX + at here?
}

testopen := func(opt *ConnOptions) *testenv {
	t0.Helper()
	// XXX create txn,ctx
	// XXX db.Open with opt
	// assert conn.At == ?
	// conn.db == db
	// conn.txn == txn
	return &testenv{
		T: t0,
		...
	}
}
*/


// Persistent tests with storage.
//
// this test covers everything at application-level: Persistent, DB, Connection, LiveCache.
//
// XXX test for cache=y/n (raw data cache)
// XXX test both txn.Abort() and conn.Resync()
func TestPersistentDB(t *testing.T) {
	X := exc.Raiseif
	assert := require.New(t)
	checkObj := tCheckObj(t)

	work, err := ioutil.TempDir("", "t-persistent"); X(err)
	defer func() {
		err := os.RemoveAll(work); X(err)
	}()

	zurl := work + "/1.fs"

	// create test db via py with 2 objects
	// XXX hack as _objX go without jar.
	_obj1 := NewMyObject(nil); _obj1.oid = 101; _obj1.value = "hello"
	_obj2 := NewMyObject(nil); _obj2.oid = 102; _obj2.value = "world"
	at1, err := ZPyCommit(zurl, 0, _obj1, _obj2); X(err)

	// open connection to it via zodb/go
	ctx := context.Background()
	stor, err := OpenStorage(ctx, zurl, &OpenOptions{ReadOnly: true}); X(err)
	db := NewDB(stor)

/*
	t := testopen(&ConnOptions)
	t.conn.Cache().SetControl(...)
	t.get() // .conn.Get() + assert type + assign .objX
	t.check("ghost", "ghost") // ?
	t.checkObj(1, InvalidTid, GHOST, 0, nil)
	t.checkObj(2, InvalidTid, GHOST, 0, nil)

	t.pactivate()
	t.check(at1, at1, "hello:1", "world:1")

	t.pactivate()
	t.check(at1, at1, "hello:2", "world:2")

	t.pdeactivate()
	t.check(at1, at1, "hello:1", "world:1")

	// deactivate:		state dropped for obj1, obj2 stays in live cache
	t.pdeactivate()
	t.check(ø, at1, "GHOST:0", "world:0")

	t2 := testopen(&ConnOptions{})
	assert.Equal(db.pool, []*Connection(nil))
	t2.get()
	t2.check(ø, ø, "ghost:0", "ghost:0")

	t2.pactivate()
	t2.check(at1, at2, "hello", "kitty")
	t2.pdeactivate()

	// conn1 stays at older view for now
	t.check(ø, ø, "ghost:0", "ghost:0")
	t.pactivate()
	t.check(at1, at1, "hello:1", "world:1")

	// conn1 deactivate:	obj2 stays in conn1 live cache with old state
	t.pdeactivate()
	t.check(ø, at1, "ghost:0", "world:0")

	t.abort() // t.conn.txn == t.txn; t.txn.Abort(); t.conn.txn == nil
	assert.Equal(db.pool, []*Connection{t.conn})

	checkObj(obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE,    0, nil)
*/

	txn1, ctx1 := transaction.New(ctx)
	conn1, err := db.Open(ctx1, &ConnOptions{}); X(err)
	assert.Equal(conn1.At(), at1)
	assert.Equal(db.pool, []*Connection(nil))
	assert.Equal(conn1.db,  db)
	assert.Equal(conn1.txn, txn1)

	// do not evict obj2 from live cache. obj1 is ok to be evicted.
	zcache1 := conn1.Cache()
	zcache1.SetControl(&zcacheControl{[]Oid{_obj2.oid}})
	// FIXME test that live cache keeps objects live even if we drop all
	// regular pointers to it and do GC.

	// get objects and assert their type
	xobj1, err := conn1.Get(ctx1, 101); X(err)
	xobj2, err := conn1.Get(ctx1, 102); X(err)

	assert.Equal(ClassOf(xobj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xobj2), "t.zodb.MyObject")

	// XXX objX -> c1objX ?

	obj1 := xobj1.(*MyObject)
	obj2 := xobj2.(*MyObject)
	checkObj(obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST, 0, nil)

	// activate:		jar has to load, state changes -> uptodate
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, 101, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE, 1, nil)
	assert.Equal(obj1.value, "hello")
	assert.Equal(obj2.value, "world")

	// activate again:	refcnt++
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, 101, at1, UPTODATE, 2, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE, 2, nil)

	// deactivate:		refcnt--
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, 101, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE, 1, nil)

	// deactivate:		state dropped for obj1, obj2 stays in live cache
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE,    0, nil)

	// invalidate:		obj2 state dropped
	obj1.PInvalidate()
	obj2.PInvalidate()
	checkObj(obj1, conn1, 101, InvalidTid, GHOST,    0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST,    0, nil)


	// commit change to obj2 from external process
	_obj2.value = "kitty"
	at2, err := ZPyCommit(zurl, at1, _obj2); X(err)

	// new db connection should see the change
	// XXX currently there is a race because db.Open does not do proper Sync
	txn2, ctx2 := transaction.New(ctx)
	conn2, err := db.Open(ctx2, &ConnOptions{}); X(err)
	assert.Equal(conn2.At(), at2)
	assert.Equal(db.pool, []*Connection(nil))
	assert.Equal(conn2.db,  db)
	assert.Equal(conn2.txn, txn2)

	xc2obj1, err := conn2.Get(ctx2, 101); X(err)
	xc2obj2, err := conn2.Get(ctx2, 102); X(err)

	assert.Equal(ClassOf(xc2obj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xc2obj2), "t.zodb.MyObject")

	c2obj1 := xc2obj1.(*MyObject)
	c2obj2 := xc2obj2.(*MyObject)
	checkObj(c2obj1, conn2, 101, InvalidTid, GHOST, 0, nil)
	checkObj(c2obj2, conn2, 102, InvalidTid, GHOST, 0, nil)

	err = c2obj1.PActivate(ctx2); X(err)
	err = c2obj2.PActivate(ctx2); X(err)
	checkObj(c2obj1, conn2, 101, at1, UPTODATE, 1, nil)
	checkObj(c2obj2, conn2, 102, at2, UPTODATE, 1, nil)
	assert.Equal(c2obj1.value, "hello")
	assert.Equal(c2obj2.value, "kitty")
	c2obj1.PDeactivate()
	c2obj2.PDeactivate()


	// conn1 stays at older view for now
	checkObj(obj1, conn1, 101, InvalidTid, GHOST,    0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST,    0, nil)
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, 101, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE, 1, nil)
	assert.Equal(obj1.value, "hello")
	assert.Equal(obj2.value, "world")

	// conn1 deactivate:	obj2 stays in conn1 live cache with old state
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, 102, at1, UPTODATE,    0, nil)

	// txn1 completes - conn1 goes back to db pool
	assert.Equal(conn1.txn, txn1)
	txn1.Abort()
	assert.Equal(conn1.txn, nil)
	assert.Equal(db.pool, []*Connection{conn1})


	// open new connection - it should be conn1 but at updated database view
	txn3, ctx3 := transaction.New(ctx)
	assert.NotEqual(txn3, txn1)
	conn3, err := db.Open(ctx3, &ConnOptions{}); X(err)
	assert.Equal(conn3, conn1)	// XXX is
	assert.Equal(conn1.At(), at2)
	assert.Equal(conn1.db,  db)
	assert.Equal(conn1.txn, txn3)
	assert.Equal(db.pool, []*Connection{})
	ctx1 = ctx3 // not to use ctx3 below
	ctx3 = nil

	// obj2 should be invalidated
	assert.Equal(conn1.Cache().Get(101), obj1)	// XXX is
	assert.Equal(conn1.Cache().Get(102), obj2)	// XXX is
	checkObj(obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST, 0, nil)

	// obj2 data should be new
	xobj1, err = conn1.Get(ctx1, 101); X(err)
	xobj2, err = conn1.Get(ctx1, 102); X(err)
	assert.Exactly(obj1, xobj1)	// XXX is
	assert.Exactly(obj2, xobj2)	// XXX is
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, 101, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, 102, at2, UPTODATE, 1, nil)
	assert.Equal(obj1.value, "hello")
	assert.Equal(obj2.value, "kitty")
	// XXX deactivate

	// finish tnx3 and txn2 - conn1 and conn2 go back to db pool
	txn3.Abort()
	txn2.Abort()
	assert.Equal(conn1.txn, nil)
	assert.Equal(conn2.txn, nil)
	assert.Equal(db.pool, []*Connection{conn1, conn2})


	// open new connection in nopool mode to verify resync
	txn4, ctx4 := transaction.New(ctx)
	rconn, err := db.Open(ctx4, &ConnOptions{NoPool: true}); X(err)
	assert.Equal(rconn.At(), at2)
	assert.Equal(db.pool, []*Connection{conn1, conn2})
	assert.Equal(rconn.db,  db)
	assert.Equal(rconn.txn, txn4)

	// pin obj2 into live cache, similarly to conn1
	rzcache := rconn.Cache()
	rzcache.SetControl(&zcacheControl{[]Oid{_obj2.oid}})

	// it should see latest data
	xrobj1, err := rconn.Get(ctx4, 101); X(err)
	xrobj2, err := rconn.Get(ctx4, 102); X(err)

	assert.Equal(ClassOf(xrobj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xrobj2), "t.zodb.MyObject")

	robj1 := xrobj1.(*MyObject)
	robj2 := xrobj2.(*MyObject)
	checkObj(robj1, rconn, 101, InvalidTid, GHOST, 0, nil)
	checkObj(robj2, rconn, 102, InvalidTid, GHOST, 0, nil)

	err = robj1.PActivate(ctx4); X(err)
	err = robj2.PActivate(ctx4); X(err)
	checkObj(robj1, rconn, 101, at1, UPTODATE, 1, nil)
	checkObj(robj2, rconn, 102, at2, UPTODATE, 1, nil)
	assert.Equal(robj1.value, "hello")
	assert.Equal(robj2.value, "kitty")

	// obj2 stays in live cache
	robj1.PDeactivate()
	robj2.PDeactivate()
	checkObj(robj1, rconn, 101, InvalidTid, GHOST, 0, nil)
	checkObj(robj2, rconn, 102, at2, UPTODATE,    0, nil)

	// txn4 completes, but rconn stays out of db pool
	assert.Equal(rconn.txn, txn4)
	txn4.Abort()
	assert.Equal(rconn.txn, nil)
	assert.Equal(db.pool, []*Connection{conn1, conn2})

	// Resync ↓ (at2 -> at1; within δtail coverage)
	txn5, ctx5 := transaction.New(ctx)
	rconn.Resync(txn5, at1)

	assert.Equal(rconn.At(), at1)	// XXX -> tt
	assert.Equal(db.pool, []*Connection{conn1, conn2})
	assert.Equal(rconn.db,  db)	// XXX -> tt
	assert.Equal(rconn.txn, txn5)	// XXX -> tt

	// obj2 should be invalidated
	assert.Equal(rconn.Cache().Get(101), robj1)	// XXX is
	assert.Equal(rconn.Cache().Get(102), robj2)	// XXX is
	checkObj(robj1, rconn, 101, InvalidTid, GHOST, 0, nil)
	checkObj(robj2, rconn, 102, InvalidTid, GHOST, 0, nil)

	// obj2 data should be old
	//tt.checkData(at1, at2, "hello", "world")

	xrobj1, err = rconn.Get(ctx5, 101); X(err)
	xrobj2, err = rconn.Get(ctx5, 102); X(err)
	assert.Exactly(robj1, xrobj1)	// XXX is
	assert.Exactly(robj2, xrobj2)	// XXX is
	err = robj1.PActivate(ctx5); X(err)
	err = robj2.PActivate(ctx5); X(err)
	checkObj(robj1, rconn, 101, at1, UPTODATE, 1, nil)
	checkObj(robj2, rconn, 102, at1, UPTODATE, 1, nil)
	assert.Equal(robj1.value, "hello")
	assert.Equal(robj2.value, "world")



	// XXX DB.Open with at on and +-1 δtail edges

	// TODO Resync ↑ (with δtail coverage)
	// TODO Resync   (without δtail coverage)

	// XXX cache dropping entries after GC

	// XXX Get(txn = different) -> panic
}



// TODO PyGetState vs PySetState tests (general - for any type):
//
// db1: produced by zodb/py
// go: load db1
// go: commit -> db2 (resave)
// go: load db2
// go: check (loaded from db2) == (loaded from db1)
