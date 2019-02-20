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
//	"runtime"
	"testing"

	"lab.nexedi.com/kirr/neo/go/transaction"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/mem"
	assert "github.com/stretchr/testify/require"
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

// checkObj verifies current state of persistent object.
//
// one can bind checkObj to t via tCheckObj.
func checkObj(t testing.TB, obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32) {
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
		badf("invalid refcnt: %d  ; want %d", pbase.refcnt, refcnt)
	}
	if pbase.instance != obj {
		badf("base.instance != obj")
	}

	if len(badv) != 0 {
		msg := fmt.Sprintf("%#v:\n", obj)
		for _, bad := range badv {
			msg += fmt.Sprintf("\t- %s\n", bad)
		}
		t.Fatal(msg)
	}
}

func tCheckObj(t testing.TB) func(IPersistent, *Connection, Oid, Tid, ObjectState, int32) {
	return func(obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32) {
		t.Helper()
		checkObj(t, obj, jar, oid, serial, state, refcnt)
	}
}

// basic Persistent tests without storage.
func TestPersistentBasic(t *testing.T) {
	assert := assert.New(t)
	checkObj := tCheckObj(t)

	// unknown type -> Broken
	xobj := newGhost("t.unknown", 10, nil)
	b, ok := xobj.(*Broken)
	if !ok {
		t.Fatalf("unknown -> %T;  want Broken", xobj)
	}

	checkObj(b, nil, 10, InvalidTid, GHOST, 0)
	assert.Equal(b.class, "t.unknown")
	assert.Equal(b.state, (*mem.Buf)(nil))


	// t.zodb.MyObject -> *MyObject
	xobj = newGhost("t.zodb.MyObject", 11, nil)
	obj, ok := xobj.(*MyObject)
	if !ok {
		t.Fatalf("t.zodb.MyObject -> %T;  want MyObject", xobj)
	}

	checkObj(obj, nil, 11, InvalidTid, GHOST, 0)
	assert.Equal(ClassOf(obj), "t.zodb.MyObject")

	// t.zodb.MyOldObject -> *MyObject
	xobj = newGhost("t.zodb.MyOldObject", 12, nil)
	obj, ok = xobj.(*MyObject)
	if !ok {
		t.Fatalf("t.zodb.MyOldObject -> %T;  want MyObject", xobj)
	}

	checkObj(obj, nil, 12, InvalidTid, GHOST, 0)
	assert.Equal(ClassOf(obj), "t.zodb.MyObject")

	// ClassOf(unregistered-obj)
	obj2 := &Unregistered{}
	assert.Equal(ClassOf(obj2), `ZODB.Go("lab.nexedi.com/kirr/neo/go/zodb.Unregistered")`)

	// deactivate refcnt < 0  -> panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("deactivate refcnt < 0: not panicked")
			}
			ehave := fmt.Sprintf("%s", r)
			ewant := fmt.Sprintf("t.zodb.MyObject(%s): deactivate: refcnt < 0  (= -1)", Oid(12))
			if ehave != ewant {
				t.Fatalf("deactivate refcnt < 0: panic error unexpected:\nhave: %q\nwant: %q", ehave, ewant)
			}
		}()

		obj.PDeactivate()
	}()
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

// ---- TestPersistentDB ----

// tPersistentDB represents one testing environment inside TestPersistentDB.
type tPersistentDB struct {
	*testing.T

	// a transaction and DB connection opened under it
	txn   transaction.Transaction
	ctx   context.Context
	conn  *Connection
}

// Get gets oid from t.conn and asserts it type.
func (t *tPersistentDB) Get(oid Oid) *MyObject {
	t.Helper()
	xobj, err := t.conn.Get(t.ctx, oid)
	if err != nil {
		t.Fatal(err)
	}

	zclass := ClassOf(xobj)
	zmy    := "t.zodb.MyObject"
	if zclass != zmy {
		t.Fatalf("get %d: got %s;  want %s", oid, zclass, zmy)
	}

	return xobj.(*MyObject)
}

// PActivate activates obj in t environment.
func (t *tPersistentDB) PActivate(obj IPersistent) {
	t.Helper()
	err := obj.PActivate(t.ctx)
	if err != nil {
		t.Fatal(err)
	}
}

// checkObj checks state of obj and that obj ∈ t.conn.
//
// if object is !GHOST - it also verifies its value.
func (t *tPersistentDB) checkObj(obj *MyObject, oid Oid, serial Tid, state ObjectState, refcnt int32, valueOk ...string) {
	t.Helper()

	// any object with live pointer to it must be also in conn's cache.
	connObj := t.conn.Cache().Get(oid)
	if obj != connObj {
		t.Fatalf("cache.get %s -> not same object:\nhave: %#v\nwant: %#v", oid, connObj, oid)
	}

	// and conn.Get must return exactly obj.
	connObj, err := t.conn.Get(t.ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	if obj != connObj {
		t.Fatalf("conn.get %s -> not same object:\nhave: %#v\nwant: %#v", oid, connObj, oid)
	}

	checkObj(t.T, obj, t.conn, oid, serial, state, refcnt)

	if state == GHOST {
		if len(valueOk) != 0 {
			panic("t.checkObj(GHOST) must come without value")
		}
		return
	}

	if len(valueOk) != 1 {
		panic("t.checkObj(!GHOST) must come with one value")
	}
	value := valueOk[0]
	if obj.value != value {
		t.Fatalf("obj.value mismatch: have %q;  want %q", obj.value, value)
	}
}

// Resync resyncs t to new transaction @at.
func (t *tPersistentDB) Resync(at Tid) {
	t.Helper()
	db := t.conn.db // XXX -> t.db ?

	txn, ctx := transaction.New(context.Background())
	t.conn.Resync(txn, at)

	t.txn = txn
	t.ctx = ctx

	assert.Equal(t, t.conn.db,  db)
	assert.Equal(t, t.conn.txn, t.txn)
	assert.Equal(t, t.conn.At(), at)
}

// Abort aborts t's connection and verifies it becomes !live.
func (t *tPersistentDB) Abort() {
	t.Helper()
	assert.Equal(t, t.conn.txn, t.txn)
	t.txn.Abort()
	assert.Equal(t, t.conn.txn, nil)
}


// Persistent tests with storage.
//
// this test covers everything at application-level: Persistent, DB, Connection, LiveCache.
//
// XXX test for cache=y/n (raw data cache)
func TestPersistentDB(t0 *testing.T) {
	X := exc.Raiseif
	assert := assert.New(t0)

	work, err := ioutil.TempDir("", "t-persistent"); X(err)
	defer func() {
		err := os.RemoveAll(work); X(err)
	}()

	zurl := work + "/1.fs"

	// create test db via py with 2 objects
	// XXX hack as _objX go without jar.
	_obj1 := NewMyObject(nil); _obj1.oid = 101; _obj1.value = "init"
	_obj2 := NewMyObject(nil); _obj2.oid = 102; _obj2.value = "db"
	at0, err := ZPyCommit(zurl, 0, _obj1, _obj2); X(err)

	_obj1.value = "hello"
	_obj2.value = "world"
	at1, err := ZPyCommit(zurl, at0, _obj1, _obj2); X(err)

	// open connection to it via zodb/go
	ctx := context.Background()
	stor, err := OpenStorage(ctx, zurl, &OpenOptions{ReadOnly: true}); X(err)
	db := NewDB(stor)

	testopen := func(opt *ConnOptions) *tPersistentDB {
		t0.Helper()

		txn, ctx := transaction.New(context.Background())
		conn, err := db.Open(ctx, opt); X(err)

		assert.Equal(conn.db, db)
		assert.Equal(conn.txn, txn)

		return &tPersistentDB{
			T:    t0,
			txn:  txn,
			ctx:  ctx,
			conn: conn,
		}
	}

	t1 := testopen(&ConnOptions{})
	t := t1
	assert.Equal(t.conn.At(), at1)
	assert.Equal(db.pool, []*Connection(nil))

	// δtail coverage is (at1, at1]  (at0 not included)
	assert.Equal(db.δtail.Tail(), at1)
	assert.Equal(db.δtail.Head(), at1)

	// do not evict obj2 from live cache. obj1 is ok to be evicted.
	zcache1 := t.conn.Cache()
	zcache1.SetControl(&zcacheControl{[]Oid{_obj2.oid}})
	// FIXME test that live cache keeps objects live even if we drop all
	// regular pointers to it and do GC.

	// get objects
	obj1 := t.Get(101)
	obj2 := t.Get(102)
	t.checkObj(obj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(obj2, 102, InvalidTid, GHOST, 0)

	// activate:		jar has to load, state changes -> uptodate
	t.PActivate(obj1)
	t.PActivate(obj2)
	t.checkObj(obj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(obj2, 102, at1, UPTODATE, 1, "world")

	// activate again:	refcnt++
	t.PActivate(obj1)
	t.PActivate(obj2)
	t.checkObj(obj1, 101, at1, UPTODATE, 2, "hello")
	t.checkObj(obj2, 102, at1, UPTODATE, 2, "world")

	// deactivate:		refcnt--
	obj1.PDeactivate()
	obj2.PDeactivate()
	t.checkObj(obj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(obj2, 102, at1, UPTODATE, 1, "world")

	// deactivate:		state dropped for obj1, obj2 stays in live cache
	obj1.PDeactivate()
	obj2.PDeactivate()
	t.checkObj(obj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(obj2, 102, at1, UPTODATE,    0, "world")

	// invalidate:		obj2 state dropped
	obj1.PInvalidate()
	obj2.PInvalidate()
	t.checkObj(obj1, 101, InvalidTid, GHOST,    0)
	t.checkObj(obj2, 102, InvalidTid, GHOST,    0)

	// commit change to obj2 from external process
	_obj2.value = "kitty"
	at2, err := ZPyCommit(zurl, at1, _obj2); X(err)

	// new db connection should see the change
	// XXX currently there is a race because db.Open does not do proper Sync
	t2 := testopen(&ConnOptions{})
	assert.Equal(t2.conn.At(), at2)
	assert.Equal(db.pool, []*Connection(nil))

	// δtail coverage is (at1, at2]
	assert.Equal(db.δtail.Tail(), at1)
	assert.Equal(db.δtail.Head(), at2)

	c2obj1 := t2.Get(101)
	c2obj2 := t2.Get(102)
	t2.checkObj(c2obj1, 101, InvalidTid, GHOST, 0)
	t2.checkObj(c2obj2, 102, InvalidTid, GHOST, 0)

	t2.PActivate(c2obj1)
	t2.PActivate(c2obj2)
	t2.checkObj(c2obj1, 101, at1, UPTODATE, 1, "hello")
	t2.checkObj(c2obj2, 102, at2, UPTODATE, 1, "kitty")
	c2obj1.PDeactivate()
	c2obj2.PDeactivate()


	// conn1 stays at older view for now
	t1.checkObj(obj1, 101, InvalidTid, GHOST,    0)
	t1.checkObj(obj2, 102, InvalidTid, GHOST,    0)
	t1.PActivate(obj1)
	t1.PActivate(obj2)
	t1.checkObj(obj1, 101, at1, UPTODATE, 1, "hello")
	t1.checkObj(obj2, 102, at1, UPTODATE, 1, "world")

	// conn1 deactivate:	obj2 stays in conn1 live cache with old state
	obj1.PDeactivate()
	obj2.PDeactivate()
	t1.checkObj(obj1, 101, InvalidTid, GHOST, 0)
	t1.checkObj(obj2, 102, at1, UPTODATE,    0, "world")

	// txn1 completes - conn1 goes back to db pool
	t1.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn})


	// open new connection - it should be conn1 but at updated database view
	t3 := testopen(&ConnOptions{})
	assert.Equal(t3.conn, t1.conn)	// XXX is
	t = t3
	assert.Equal(t.conn.At(), at2)
	assert.Equal(db.pool, []*Connection{})

	// obj2 should be invalidated
	t.checkObj(obj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(obj2, 102, InvalidTid, GHOST, 0)

	// obj2 data should be new
	t.PActivate(obj1);
	t.PActivate(obj2);
	t.checkObj(obj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(obj2, 102, at2, UPTODATE, 1, "kitty")

	obj1.PDeactivate()
	obj2.PDeactivate()
	t.checkObj(obj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(obj2, 102, at2, UPTODATE,    0, "kitty")

	// TODO live cache must not drop pinned entries after GC
/*
	obj1 = nil
	obj2 = nil
	for i := 0; i < 10; i++ {
		runtime.GC() // need only 2 runs since cache uses finalizers
	}

	xobj1 := t.conn.Cache().Get(101)
	xobj2 := t.conn.Cache().Get(102)
	assert.Equal(xobj1, nil)
	assert.NotEqual(xobj2, nil)
	obj2 = xobj2.(*MyObject)
	t.checkObj(obj2, 102, at2, UPTODATE,    0, "kitty")
*/


	// finish tnx3 and txn2 - conn1 and conn2 go back to db pool
	t.Abort()
	t2.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// open new connection in nopool mode to verify resync
	t4 := testopen(&ConnOptions{NoPool: true})
	t = t4
	assert.Equal(t.conn.At(), at2)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// pin obj2 into live cache, similarly to conn1
	rzcache := t.conn.Cache()
	rzcache.SetControl(&zcacheControl{[]Oid{_obj2.oid}})

	// it should see latest data
	robj1 := t.Get(101)
	robj2 := t.Get(102)
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, InvalidTid, GHOST, 0)

	t.PActivate(robj1)
	t.PActivate(robj2)
	t.checkObj(robj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(robj2, 102, at2, UPTODATE, 1, "kitty")

	// obj2 stays in live cache
	robj1.PDeactivate()
	robj2.PDeactivate()
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, at2, UPTODATE,    0, "kitty")

	// txn4 completes, but its conn stays out of db pool
	t.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// Resync ↓ (at2 -> at1; within δtail coverage)
	t.Resync(at1)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// obj2 should be invalidated
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, InvalidTid, GHOST, 0)

	// obj2 data should be old
	t.PActivate(robj1)
	t.PActivate(robj2)
	t.checkObj(robj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(robj2, 102, at1, UPTODATE, 1, "world")

	robj1.PDeactivate()
	robj2.PDeactivate()
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, at1, UPTODATE, 0, "world")

	// Resync ↑ (at1 -> at2; within δtail coverage)
	t.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})
	t.Resync(at2)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// obj2 should be invalidated
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, InvalidTid, GHOST, 0)

	t.PActivate(robj1)
	t.PActivate(robj2)
	t.checkObj(robj1, 101, at1, UPTODATE, 1, "hello")
	t.checkObj(robj2, 102, at2, UPTODATE, 1, "kitty")

	robj1.PDeactivate()
	robj2.PDeactivate()
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, at2, UPTODATE, 0, "kitty")

	// Resync ↓ (at1 -> at0; to outside δtail coverage)
	t.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})
	t.Resync(at0)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// obj2 should be invalidated
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, InvalidTid, GHOST, 0)

	t.PActivate(robj1)
	t.PActivate(robj2)
	t.checkObj(robj1, 101, at0, UPTODATE, 1, "init")
	t.checkObj(robj2, 102, at0, UPTODATE, 1, "db")

	robj1.PDeactivate()
	robj2.PDeactivate()
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, at0, UPTODATE, 0, "db")

	// Resync ↑ (at0 -> at2; from outside δtail coverage)
	t.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})
	t.Resync(at2)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// obj2 should be invalidated
	t.checkObj(robj1, 101, InvalidTid, GHOST, 0)
	t.checkObj(robj2, 102, InvalidTid, GHOST, 0)



	// XXX DB.Open with at on and +-1 δtail edges


	// XXX Get(txn = different) -> panic
}

// TODO Map & List tests.


// TODO PyGetState vs PySetState tests (general - for any type):
//
// db1: produced by zodb/py
// go: load db1
// go: commit -> db2 (resave)
// go: load db2
// go: check (loaded from db2) == (loaded from db1)
