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
	"fmt"
	"reflect"
	"testing"

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

func TestPersistent(t *testing.T) {
	assert := require.New(t)

	// checkObj verifies current state of persistent object.
	checkObj := func(obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32, loading *loadState) {
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


	// TODO activate	- jar has to load, state changes
	// TODO activate again	- refcnt++
	// TODO deactivate	- refcnt--
	// TODO deactivate	- state dropped
}
