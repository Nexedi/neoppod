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

import (
	"context"
	"fmt"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/weak"

	pickle "github.com/kisielk/og-rek"
)

// IPyPersistent is the interface that every in-RAM object representing Python ZODB object implements.
type IPyPersistent interface {
	IPersistent

	PyClass() pickle.Class // python class of this object
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

func (pyobj *PyPersistent) PyClass() pickle.Class	{ return pyobj.pyclass	}
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

	if pyclass != pyobj.pyclass {
		// complain that pyclass changed
		// (both ref and object data use pyclass so it indeed can be different)
		return &wrongClassError{want: pyobj.pyclass, have: pyclass} // XXX + err ctx
	}

	return pyobj.pyinstance().PySetState(pystate)	// XXX err ctx = ok?
}

// TODO PyPersistent.GetState

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


// ----------------------------------------


// Get returns in-RAM object corresponding to specified ZODB object according to current database view.
//
// If there is already in-RAM object that corresponds to oid, that in-RAM object is returned.
// Otherwise new in-RAM object is created and filled with object's class loaded from the database.
//
// The scope of the object returned is the Connection.	XXX ok?
//
// The object's data is not necessarily loaded after Get returns. Use
// PActivate to make sure the object is fully loaded.
func (conn *Connection) Get(ctx context.Context, oid Oid) (IPyPersistent, error) {
	conn.objmu.Lock()		// XXX -> rlock
	wobj := conn.objtab[oid]
	var xobj interface{}
	if wobj != nil {
		xobj = wobj.Get()
	}
	conn.objmu.Unlock()

	// object was already there in objtab.
	if xobj != nil {
		return xobj.(IPyPersistent), nil
	}

	// object is not there in objtab - raw load it, get its class -> get(pyclass, oid)
	pyclass, pystate, serial, err := conn.loadpy(ctx, oid)
	if err != nil {
		return nil, err		// XXX errctx
	}

	obj, err := conn.get(pyclass, oid)
	if err != nil {
		return nil, err
	}

	// XXX we are dropping just loaded pystate. Usually Get should be used
	// to only load root object, so maybe that is ok.
	//
	// TODO -> use (pystate, serial) to activate.
	_, _ = pystate, serial
	return obj, nil
}

// wrongClassError is the error cause returned when python object's class is not what was expected.
type wrongClassError struct {
	want, have pickle.Class
}

func (e *wrongClassError) Error() string {
	return fmt.Sprintf("wrong class: want %q; have %q", e.want, e.have)
}

// get returns in-RAM object corresponding to specified ZODB object according to current database view.
//
// If there is already in-RAM object that corresponds to oid, that in-RAM object is returned.
// Otherwise new in-RAM object is created according to specified class.
//
// The object's data is not necessarily loaded after get returns. Use
// PActivate to make sure the object is fully loaded.
//
// XXX object scope.
//
// Use-case: in ZODB references are (pyclass, oid), so new ghost is created
// without further loading anything.
func (conn *Connection) get(pyclass pickle.Class, oid Oid) (IPyPersistent, error) {
	conn.objmu.Lock()		// XXX -> rlock
	wobj := conn.objtab[oid]
	var pyobj IPyPersistent
	checkClass := false
	if wobj != nil {
		if xobj := wobj.Get(); xobj != nil {
			pyobj = xobj.(IPyPersistent)
		}
	}
	if pyobj == nil {
		pyobj = conn.newGhost(pyclass, oid)
		conn.objtab[oid] = weak.NewRef(pyobj)
	} else {
		checkClass = true
	}
	conn.objmu.Unlock()

	if checkClass {
		if cls := pyobj.PyClass(); pyclass != cls {
			return nil, &OpError{
				URL:  conn.stor.URL(),
				Op:   fmt.Sprintf("@%s: get", conn.at), // XXX abuse
				Args: oid,
				Err:  &wrongClassError{pyclass, cls},
			}
		}
	}

	return pyobj, nil
}

// loadpy loads object specified by oid and decodes it as a ZODB Python object.
//
// loadpy does not create any in-RAM object associated with Connection.
// It only returns decoded database data.
func (conn *Connection) loadpy(ctx context.Context, oid Oid) (pyclass pickle.Class, pystate interface{}, serial Tid, _ error) {
	buf, serial, err := conn.stor.Load(ctx, Xid{Oid: oid, At: conn.at})
	if err != nil {
		return pickle.Class{}, nil, 0, err
	}

	defer buf.Release()

	pyclass, pystate, err = PyData(buf.Data).Decode()
	if err != nil {
		return pickle.Class{}, nil, 0, err	// XXX err ctx
	}

	return pyclass, pystate, serial, nil
}
