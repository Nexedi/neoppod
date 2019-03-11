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
	"runtime"
	"testing"

	"lab.nexedi.com/kirr/neo/go/transaction"

	"lab.nexedi.com/kirr/go123/mem"
	assert "github.com/stretchr/testify/require"
)

// test Persistent type.
type MyObject struct {
	Persistent

	value    string // persistent state
	_v_value string // volatile in-RAM only state; not managed by persistency layer
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

// Persistent that is not registered to ZODB.
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

// tCheckObj binds checkObj to t.
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

// ---- TestPersistentDB ----

// zcacheControl is simple live cache control that is organized as `{} oid ->
// PCachePolicy` table.
type zcacheControl struct {
	pCachePolicy map[Oid]PCachePolicy
}

func (cc *zcacheControl) PCacheClassify(obj IPersistent) PCachePolicy {
	return cc.pCachePolicy[obj.POid()] // default -> 0
}

// tPersistentDB represents testing database.	XXX -> tDB ?
type tPersistentDB struct {
	*testing.T

	work string // working directory
	zurl string // zurl for database under work

	// zodb/go stor/db handle for the database
	stor IStorage
	db   *DB

	head    Tid           // last committed transaction
	commitq []IPersistent // queue to be committed
}

// tPersistentConn represents testing Connection.	XXX -> tConn ?
type tPersistentConn struct {
	*testing.T

	// a transaction and DB connection opened under it
	txn   transaction.Transaction
	ctx   context.Context
	conn  *Connection
}

// testdb creates and initializes new test database.
func testdb(t0 *testing.T, rawcache bool) *tPersistentDB {
	t0.Helper()
	t := &tPersistentDB{T: t0}
	X := t.fatalif

	work, err := ioutil.TempDir("", "t-persistent"); X(err)
	t.work = work
	t.zurl = work + "/1.fs"

	finishok := false
	defer func() {
		if !finishok {
			err := os.RemoveAll(t.work); X(err)
		}
	}()

	// create test db via py with 2 objects
	t.Add(11, "init")
	t.Add(12, "db")
	t.Commit()

	// open the db via zodb/go
	stor, err := Open(context.Background(), t.zurl, &OpenOptions{ReadOnly: true, NoCache: !rawcache}); X(err)
	db := NewDB(stor)
	t.stor = stor
	t.db = db

	finishok = true
	return t
}

// Close release resources associated with test database.
func (t *tPersistentDB) Close() {
	t.Helper()
	X := t.fatalif

	err := t.db.Close(); X(err)
	err = t.stor.Close(); X(err)
	err = os.RemoveAll(t.work); X(err)
}

// Add marks object with oid as modified and queues it to be committed as
// MyObject(value).
//
// The commit is performed by Commit.
func (t *tPersistentDB) Add(oid Oid, value string) {
	obj := NewMyObject(nil) // XXX hack - goes without jar
	obj.oid = oid
	obj.value = value
	t.commitq = append(t.commitq, obj)
}

// Commit commits objects queued by Add.
func (t *tPersistentDB) Commit() {
	t.Helper()

	head, err := ZPyCommit(t.zurl, t.head, t.commitq...)
	if err != nil {
		t.Fatal(err)
	}
	t.head = head
	t.commitq = nil
}

// Open opens new test transaction/connection.
func (t *tPersistentDB) Open(opt *ConnOptions) *tPersistentConn {
	t.Helper()
	X := t.fatalif

	txn, ctx := transaction.New(context.Background())
	conn, err := t.db.Open(ctx, opt); X(err)

	assert.Same(t, conn.db, t.db)
	assert.Same(t, conn.txn, txn)

	return &tPersistentConn{
		T:    t.T,
		txn:  txn,
		ctx:  ctx,
		conn: conn,
	}
}

// Get gets oid from t.conn and asserts its type.
func (t *tPersistentConn) Get(oid Oid) *MyObject {
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
func (t *tPersistentConn) PActivate(obj IPersistent) {
	t.Helper()
	err := obj.PActivate(t.ctx)
	if err != nil {
		t.Fatal(err)
	}
}

// checkObj checks state of obj and that obj ∈ t.conn.
//
// if object is !GHOST - it also verifies its value.
func (t *tPersistentConn) checkObj(obj *MyObject, oid Oid, serial Tid, state ObjectState, refcnt int32, valueOk ...string) {
	t.Helper()

	// any object with live pointer to it must be also in conn's cache.
	cache := t.conn.Cache()
	cache.Lock()
	connObj := cache.Get(oid)
	cache.Unlock()
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
func (t *tPersistentConn) Resync(at Tid) {
	t.Helper()
	db := t.conn.db

	txn, ctx := transaction.New(context.Background())
	err := t.conn.Resync(ctx, at)
	if err != nil {
		t.Fatalf("resync %s -> %s", at, err)
	}

	t.txn = txn
	t.ctx = ctx

	assert.Same(t, t.conn.db,  db)
	assert.Same(t, t.conn.txn, t.txn)
	assert.Equal(t, t.conn.At(), at)
}

// Abort aborts t's connection and verifies it becomes !live.
func (t *tPersistentConn) Abort() {
	t.Helper()
	assert.Same(t, t.conn.txn, t.txn)
	t.txn.Abort()
	assert.Equal(t, t.conn.txn, nil)
}

func (t *tPersistentDB) fatalif(err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func (t *tPersistentConn) fatalif(err error) {
	if err != nil {
		t.Fatal(err)
	}
}

// Persistent tests with storage.
//
// this test covers everything at application-level: Persistent, DB, Connection, LiveCache.
func TestPersistentDB(t *testing.T) {
	// perform tests without and with raw data cache.
	// (rawcache=y verifies how raw cache handles invalidations)
	t.Run("rawcache=n", func(t *testing.T) { testPersistentDB(t, false) })
	t.Run("rawcache=y", func(t *testing.T) { testPersistentDB(t, true) })
}

func testPersistentDB(t0 *testing.T, rawcache bool) {
	assert := assert.New(t0)

	tdb := testdb(t0, rawcache)
	defer tdb.Close()
	at0 := tdb.head

	tdb.Add(101, "hello")
	tdb.Add(102, "world")
	tdb.Commit()
	at1 := tdb.head

	db := tdb.db

	t1 := tdb.Open(&ConnOptions{})
	t := t1
	assert.Equal(t.conn.At(), at1)
	assert.Equal(db.pool, []*Connection(nil))

	// δtail coverage is (at1, at1]  (at0 not included)
	assert.Equal(db.δtail.Tail(), at1)
	assert.Equal(db.δtail.Head(), at1)

	// do not evict obj2 from live cache. obj1 is ok to be evicted.
	zcc := &zcacheControl{map[Oid]PCachePolicy{
		102: PCachePinObject | PCacheKeepState,
	}}

	zcache1 := t.conn.Cache()
	zcache1.Lock()
	zcache1.SetControl(zcc)
	zcache1.Unlock()

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
	tdb.Add(102, "kitty")
	tdb.Commit()
	at2 := tdb.head

	// new db connection should see the change
	t2 := tdb.Open(&ConnOptions{})
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
	t3 := tdb.Open(&ConnOptions{})
	assert.Same(t3.conn, t1.conn)
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

	// live cache keeps pinned object live even if we drop
	// all regular pointers to it and do GC.
	// XXX also PCachePinObject without PCacheKeepState
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


	// finish tnx3 and txn2 - conn1 and conn2 go back to db pool
	t.Abort()
	t2.Abort()
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})


	// ---- Resync ----

	// open new connection in nopool mode to verify resync
	t4 := tdb.Open(&ConnOptions{NoPool: true})
	t = t4
	assert.Equal(t.conn.At(), at2)
	assert.Equal(db.pool, []*Connection{t1.conn, t2.conn})

	// pin obj2 into live cache, similarly to conn1
	rzcache := t.conn.Cache()
	rzcache.Lock()
	rzcache.SetControl(zcc)
	rzcache.Unlock()

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
}

// TODO Map & List tests.


// TODO PyGetState vs PySetState tests (general - for any type):
//
// db1: produced by zodb/py
// go: load db1
// go: commit -> db2 (resave)
// go: load db2
// go: check (loaded from db2) == (loaded from db1)
