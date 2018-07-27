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

// IPyPersistent is the interface that every in-RAM object representing Python ZODB object implements.
type IPyPersistent interface {
	IPersistent

	//PyClass() pickle.Class // python class of this object
//	PyState() interface{}  // object state. python passes this to pyclass.__new__().__setstate__()

	// IPyPersistent must be stateful for persistency to work
	// XXX try to move out of IPyPersistent? Rationale: we do not want e.g. PySetState to
	// be available to user who holds IPyPersistent interface: it is confusing to have
	// both PActivate and PySetState at the same time.
	PyStateful
}

// PyPersistent is common base implementation for in-RAM representation of ZODB Python objects.
type PyPersistent struct {
	Persistent
	pyclass pickle.Class
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
// newGhost which passes IPyPersistent as instance to IPersistent.
func (pyobj *PyPersistent) pyinstance() IPyPersistent {
	return pyobj.instance.(IPyPersistent)
}

func (pyobj *PyPersistent) SetState(state *mem.Buf) error {
	pyclass, pystate, err := PyData(state.Data).Decode()
	if err != nil {
		return err	// XXX err ctx
	}

	class := pyclassPath(pyclass)
	if class != pyobj.class {
		// complain that pyclass changed
		// (both ref and object data use pyclass so it indeed can be different)
		return &wrongClassError{want: pyobj.class, have: class} // XXX + err ctx
	}

	return pyobj.pyinstance().PySetState(pystate)	// XXX err ctx = ok?
}

// TODO PyPersistent.GetState

/*
// ---- pyclass -> new ghost ----

// function representing new of a class.
type pyClassNewFunc func(base *PyPersistent) IPyPersistent

// path(pyclass) -> new(pyobj)
var pyClassTab = make(map[string]pyClassNewFunc)

// PyRegisterClass registers python class to be transformed to Go instance
// created via classNew.
//
// must be called from global init().
func PyRegisterClass(pyClassPath string, classNew pyClassNewFunc) {
	pyClassTab[pyClassPath] = classNew
	// XXX + register so that PyData decode handles pyClassPath
}

// newGhost creates new ghost object corresponding to pyclass and oid.
func (conn *Connection) newGhost(pyclass pickle.Class, oid Oid) IPyPersistent {
	pyobj := &PyPersistent{
		Persistent:  Persistent{jar: conn, oid: oid, serial: 0, state: GHOST},
		pyclass: pyclass,
	}

	// switch on pyclass and transform e.g. "zodb.BTree.Bucket" -> *ZBucket
	classNew := pyClassTab[pyclass.Module + "." + pyclass.Name]
	var instance IPyPersistent
	if classNew != nil {
		instance = classNew(pyobj)
	} else {
		instance = &dummyPyInstance{PyPersistent: pyobj}
	}

	pyobj.instance = instance
	return instance
}

// dummyPyInstance is used for python classes that were not registered.
type dummyPyInstance struct {
	*PyPersistent
	pystate interface{}
}

func (d *dummyPyInstance) DropState() {
	d.pystate = nil
}

func (d *dummyPyInstance) PySetState(pystate interface{}) error	{
	d.pystate = pystate
	return nil
}
*/


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
