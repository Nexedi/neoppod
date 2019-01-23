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

	badf := func(format string, argv ...interface{}) {
		t.Helper()
		msg := fmt.Sprintf(format, argv...)
		t.Fatalf("%#v: %s", obj, msg)
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

// Persistent tests with storage.
//
// this test covers everything at application-level: Persistent, DB, Connection, LiveCache.
func TestPersistentDB(t *testing.T) {
	X := exc.Raiseif
	assert := require.New(t)
	checkObj := tCheckObj(t)

	work, err := ioutil.TempDir("", "t-persistent"); X(err)
	defer func() {
		//return
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

	txn1, ctx1 := transaction.New(ctx)
	conn1, err := db.Open(ctx1, &ConnOptions{}); X(err)

	// do not evict obj1 from live cache. obj2 is ok to be evicted.
	zcache1 := conn1.Cache()
	zcache1.SetControl(&zcacheControl{[]Oid{_obj1.oid}})

	// get objects and assert their type
	assert.Equal(conn1.At(), at1)
	xobj1, err := conn1.Get(ctx1, 101); X(err)
	xobj2, err := conn1.Get(ctx1, 102); X(err)

	assert.Equal(ClassOf(xobj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xobj2), "t.zodb.MyObject")

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

	// deactivate:		state dropped for obj2, obj1 stays in live cache
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, 101, at1, UPTODATE, 0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST,    0, nil)

	// invalidate:		obj1 state dropped
	obj1.PInvalidate()
	obj2.PInvalidate()
	checkObj(obj1, conn1, 101, InvalidTid, GHOST,    0, nil)
	checkObj(obj2, conn1, 102, InvalidTid, GHOST,    0, nil)


	// commit change to obj2 from external process
	_obj2.value = "kitty"
	at2, err := ZPyCommit(zurl, at1, _obj2); X(err)

	// new db connection should see the change
	txn2, ctx2 := transaction.New(ctx)
	conn2, err := db.Open(ctx2, &ConnOptions{}); X(err)

	assert.Equal(conn2.At(), at1)
	xc2obj1, err := conn2.Get(ctx2, 101); X(err)
	xc2obj2, err := conn2.Get(ctx2, 102); X(err)

	assert.Equal(ClassOf(xc2obj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xc2obj2), "t.zodb.MyObject")

	c2obj1 := xc2obj1.(*MyObject)
	c2obj2 := xc2obj2.(*MyObject)
	checkObj(c2obj1, conn1, 101, InvalidTid, GHOST, 0, nil)
	checkObj(c2obj2, conn1, 102, InvalidTid, GHOST, 0, nil)

	err = c2obj1.PActivate(ctx1); X(err)
	err = c2obj2.PActivate(ctx1); X(err)
	checkObj(c2obj1, conn1, 101, at1, UPTODATE, 1, nil)
	checkObj(c2obj2, conn1, 102, at2, UPTODATE, 1, nil)
	assert.Equal(c2obj1.value, "hello")
	assert.Equal(c2obj2.value, "kitty")


	// XXX c1 stays at older view


	// XXX
	txn1.Abort()
	txn2.Abort()
}
