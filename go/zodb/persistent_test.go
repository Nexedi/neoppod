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

var ZPyCommit func(string, Tid, ...interface{}) (Tid, error)	// XXX ZObject

// Persistent tests with storage.
func TestPersistentDB(t *testing.T) {
	X := exc.Raiseif
	assert := require.New(t)
	checkObj := tCheckObj(t)

	work, err := ioutil.TempDir("", "t-persistent"); X(err)
	defer func() {
		err := os.RemoveAll(work); X(err)
	}()

	zurl := work + "/1.fs"

	const oid1 = 1	// XXX
	const oid2 = 2

	// XXX create test db via py (change both oid1 & oid2)
	// XXX commit1 to test db
	at1, err := ZPyCommit(zurl, 0); X(err)	// XXX data
	fmt.Printf("AAA %s\n", at1)

	ctx := context.Background()
	stor, err := OpenStorage(ctx, zurl, &OpenOptions{ReadOnly: true}); X(err)
	aa, err := stor.LastTid(ctx); X(err)
	fmt.Printf("BBB %s\n", aa)
	db := NewDB(stor)

	txn1, ctx1 := transaction.New(ctx)
	conn1, err := db.Open(ctx1, &ConnOptions{}); X(err)

	// do not evict oid1 from live cache. oid2 is ok to be evicted.
	zcache1 := conn1.Cache()
	zcache1.SetControl(&zcacheControl{[]Oid{oid1}})

	assert.Equal(conn1.At(), at1)
	xobj1, err := conn1.Get(ctx1, oid1); X(err)
	xobj2, err := conn1.Get(ctx1, oid2); X(err)

	assert.Equal(ClassOf(xobj1), "t.zodb.MyObject")
	assert.Equal(ClassOf(xobj2), "t.zodb.MyObject")

	obj1 := xobj1.(*MyObject)
	obj2 := xobj2.(*MyObject)
	checkObj(obj1, conn1, oid1, InvalidTid, GHOST, 0, nil)
	checkObj(obj2, conn1, oid2, InvalidTid, GHOST, 0, nil)

	// activate:		jar has to load, state changes -> uptodate
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, oid1, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, oid2, at1, UPTODATE, 1, nil)

	// activate again:	refcnt++
	err = obj1.PActivate(ctx1); X(err)
	err = obj2.PActivate(ctx1); X(err)
	checkObj(obj1, conn1, oid1, at1, UPTODATE, 2, nil)
	checkObj(obj2, conn1, oid2, at1, UPTODATE, 2, nil)

	// deactivate:		refcnt--
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, oid1, at1, UPTODATE, 1, nil)
	checkObj(obj2, conn1, oid2, at1, UPTODATE, 1, nil)

	// deactivate:		state dropped for obj2, obj1 stays in live cache
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, oid1, at1, UPTODATE, 0, nil)
	checkObj(obj2, conn1, oid2, InvalidTid, GHOST,    0, nil)

	// invalidate:		obj1 state dropped
	obj1.PDeactivate()
	obj2.PDeactivate()
	checkObj(obj1, conn1, oid1, InvalidTid, GHOST,    0, nil)
	checkObj(obj2, conn1, oid2, InvalidTid, GHOST,    0, nil)


	// XXX
	txn1.Abort()
}
