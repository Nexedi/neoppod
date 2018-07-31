// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

func init() {
	t := reflect.TypeOf
	RegisterClass("t.zodb.MyObject", t(MyObject{}), t(myObjectState{}))
}

func TestPersistent(t *testing.T) {
	checkObj := func(obj IPersistent, jar *Connection, oid Oid, serial Tid, state ObjectState, refcnt int32, loading *loadState) {
		xbase := reflect.ValueOf(obj).Elem().FieldByName("Persistent")
		pbase := xbase.Addr().Interface().(*Persistent)

		badf := func(format string, argv ...interface{}) {
			msg := fmt.Sprintf(format, argv...)
			t.Fatalf("%#v: %s", obj, msg)
		}

		// XXX .zclass ?
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

	xobj := newGhost("t.zodb.MyObject", 11, nil)
	obj, ok := xobj.(*MyObject)
	if !ok {
		t.Fatalf("unknown -> %T;  want Broken", xobj)
	}

	checkObj(obj, nil, 11, 0, GHOST, 0, nil)


	// TODO activate	- jar has to load, state changes
	// TODO activate again	- refcnt++
	// TODO deactivate	- refcnt--
	// TODO deactivate	- state dropped
}

// XXX reenable
func _TestBroken(t *testing.T) {
	assert := require.New(t)

	// unknown type -> Broken
	xobj := newGhost("t.unknown", 11, nil)

	obj, ok := xobj.(*Broken)
	if !ok {
		t.Fatalf("unknown -> %T;  want Broken", xobj)
	}

	// XXX .zclass ?
	assert.Equal(obj.class, "t.unknown")
	assert.Equal(obj.state, nil)

	assert.Equal(obj.jar, (*Connection)(nil))
	assert.Equal(obj.oid, 11)
	assert.Equal(obj.serial, 0)
	assert.Equal(obj.state, GHOST)
	assert.Equal(obj.refcnt, 0)
	assert.Equal(obj.instance, obj)
	assert.Equal(obj.loading, nil)
}
