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
	assert := require.New(t)

	xobj := newGhost("t.zodb.MyObject", 11, nil)

	obj, ok := xobj.(*MyObject)
	if !ok {
		t.Fatalf("unknown -> %T;  want Broken", xobj)
	}

	// XXX .zclass ?
	assert.Equal(obj.jar, nil)
	assert.Equal(obj.oid, 11)
	assert.Equal(obj.serial, 0)
	assert.Equal(obj.state, GHOST)
	assert.Equal(obj.refcnt, 0)
	assert.Equal(obj.instance, obj)
	assert.Equal(obj.loading, nil)
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

	assert.Equal(obj.jar, nil)
	assert.Equal(obj.oid, 11)
	assert.Equal(obj.serial, 0)
	assert.Equal(obj.state, GHOST)
	assert.Equal(obj.refcnt, 0)
	assert.Equal(obj.instance, obj)
	assert.Equal(obj.loading, nil)
}
