// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE

package zodb
// Support for python objects/data in ZODB.

// XXX + PyData.referencesf ?

import (
	"context"

	"lab.nexedi.com/kirr/go123/mem"

	pickle "github.com/kisielk/og-rek"
)

// PyPersistent is common base implementation for in-RAM representation of ZODB Python objects.
type PyPersistent struct {
	*Persistent		// XXX remove ptr
}

//func (pyobj *PyPersistent) PyClass() pickle.Class	{ return pyobj.pyclass	}
//func (pyobj *PyPersistent) PyState() interface{}	{ return pyobj.pystate	}

// PyStateful is the interface describing in-RAM object whose data state can be
// exchanged as Python data.
type PyStateful interface {
	// PySetState should set state of the in-RAM object from Python data.
	// Analog of __setstate__() in Python.
	PySetState(pystate interface{}) error

	// PyGetState should return state of the in-RAM object as Python data.
	// Analog of __getstate__() in Python.
	//PyGetState() interface{}	TODO
}

// ---- PyPersistent <-> Persistent state exchange ----

// pyinstance returns .instance upcasted to IPyPersistent.
//
// this should be always safe because we always create pyObjects via
// newGhost which passes IPyPersistent as instance to IPersistent.	XXX no longer true
//func (pyobj *PyPersistent) pyinstance() IPyPersistent {
func (pyobj *PyPersistent) pyinstance() interface {IPersistent; Ghostable; PyStateful} {
	//return pyobj.instance.(IPyPersistent)
	return pyobj.instance.(interface {IPersistent; Ghostable; PyStateful})
}

func (pyobj *PyPersistent) SetState(state *mem.Buf) error {
	pyclass, pystate, err := PyData(state.Data).Decode()
	if err != nil {
		return err	// XXX err ctx
	}

	class := pyclassPath(pyclass)
	obj   := pyobj.pyinstance()

	if objClass := zclassOf(obj); class != objClass {
		// complain that pyclass changed
		// (both ref and object data use pyclass so it indeed can be different)
		return &wrongClassError{want: objClass, have: class} // XXX + err ctx
	}

	return obj.PySetState(pystate)	// XXX err ctx = ok?
}

// TODO PyPersistent.GetState



// ----------------------------------------

// pyclassPath returns full path for a python class.
//
// for example class "ABC" in module "wendelin.lib" has its full path as "wendelin.lib.ABC".
func pyclassPath(pyclass pickle.Class) string {
	return pyclass.Module + "." + pyclass.Name
}

// loadpy loads object specified by oid and decodes it as a ZODB Python object.
//
// loadpy does not create any in-RAM object associated with Connection.
// It only returns decoded database data.
func (conn *Connection) loadpy(ctx context.Context, oid Oid) (class string, pystate interface{}, serial Tid, _ error) {
	buf, serial, err := conn.stor.Load(ctx, Xid{Oid: oid, At: conn.at})
	if err != nil {
		return "", nil, 0, err
	}

	defer buf.Release()

	pyclass, pystate, err := PyData(buf.Data).Decode()
	if err != nil {
		return "", nil, 0, err	// XXX err ctx
	}

	return pyclassPath(pyclass), pystate, serial, nil
}
