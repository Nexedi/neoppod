// Copyright (C) 2016-2018  Nexedi SA and Contributors.
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
// serialization compatibility with ZODB/py

import (
	"bytes"
	"errors"
	"fmt"

	pickle "github.com/kisielk/og-rek"
)

/*
// XXX move out of py
// Object is in-process representaion of a ZODB object.
type Object interface {
	Jar()		*Connection
	Oid()		Oid
	Serial()	Tid

	Data()	*mem.Buf	// XXX for raw, py -> PyObject

	// XXX register(jar, oid) - register to database ? (for Connection.Add)

	// PActivate
	// PDeactivate
	// PInvalidate
}

// XXX
type DataManager interface {
	//LoadObject(obj Object)
	//LoadObject(oid Oid)
	LoadState(obj)

	// XXX oldstate(oid, tid)
	// XXX register(obj)
}



// Connection represent client-visible part of a connection to ZODB database.
//
// XXX consitent view for some `at`.
// XXX not needed? (just DB.Open() -> root object and connection not be visible)?
type Connection interface {
	// Get returns object by oid.
	// XXX multiple calls to Get with same oid return the same object.
	// XXX root object has oid=0.
	Get(ctx context.Context, oid Oid) (Object, error)

	// XXX Add(obj Object) -> add obj to database and assign it an oid
	// obj must be not yet added

	// XXX ReadCurrent(obj)

	// XXX Close() error
}
*/











// PyData represents raw data stored into ZODB by Python applications.
//
// The format is based on python pickles. Basically every serialized object has
// two parts: pickle with class description and pickle with object state. See
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/serialize.py
//
// for format description.
//
// PyData can be decoded into PyObject.
type PyData []byte

//type PyClass struct {
//	Module string
//	Name   string
//}
// XXX + String = Module + "." + Name

/*
// PyObject represents persistent Python object.
//
// PyObject can be decoded from PyData.		XXX
type PyObject struct {
	//Jar     *Connection	// XXX -> ro
	Oid	Oid		// XXX -> ro
	Serial	Tid		// XXX -> ro

	// XXX + Oid, Serial ?	(_p_oid, _p_serial)
	PyClass pickle.Class // python class of this object	XXX -> ro	?
	PyState interface{}  // object state. python passes this to pyclass.__new__().__setstate__()
}


// PyLoader is like Loader by returns decoded Python objects instead of raw data.
type PyLoader interface {
	// XXX returned pyobject, contrary to Loader, can be modified, because
	// it is not shared. right?
	Load(ctx, xid Xid) (*PyObject, error)
}
*/


// Decode decodes raw ZODB python data into Python class and state.
func (d PyData) Decode() (pyclass pickle.Class, pystate interface{}, _ error) {
	p := pickle.NewDecoder(bytes.NewReader([]byte(d)))
	xklass, err := p.Decode()
	if err != nil {
		return pickle.Class{}, nil, fmt.Errorf("pydata: decode: class description: %s", err)
	}

	klass, err := normPyClass(xklass)
	if err != nil {
		return pickle.Class{}, nil, fmt.Errorf("pydata: decode: class description: %s", err)
	}

	state, err := p.Decode()
	if err != nil {
		return pickle.Class{}, nil, fmt.Errorf("pydata: decode: object state: %s", err)
	}

	//return &PyObject{pyClass: klass, State: state}, nil
	return klass, state, nil
}


// ClassName returns fully-qualified python class name used for object type.
//
// The format is "module.class".
// If pickle decoding fails - "?.?" is returned.
func (d PyData) ClassName() string {
	// see ObjectReader.getClassName & get_pickle_metadata in zodb/py
	p := pickle.NewDecoder(bytes.NewReader([]byte(d)))
	xklass, err := p.Decode()
	if err != nil {
		return "?.?"
	}

	klass, err := normPyClass(xklass)
	if err != nil {
		return "?.?"
	}

	return klass.Module + "." + klass.Name
}

var errInvalidPyClass = errors.New("invalid py class description")

// normPyClass normalizes py class that has just been decoded from a serialized
// ZODB object or reference.
func normPyClass(xklass interface{}) (pickle.Class, error) {
	// class description:
	//
	//	- type(obj), or
	//	- (xklass, newargs|None)	; xklass = type(obj) | (modname, classname)

	if t, ok := xklass.(pickle.Tuple); ok {
		// t = (xklass, newargs|None)
		if len(t) != 2 {
			return pickle.Class{}, errInvalidPyClass
		}
		// XXX newargs is ignored (zodb/py uses it only for persistent classes)
		xklass = t[0]
		if t, ok := xklass.(pickle.Tuple); ok {
			// t = (modname, classname)
			if len(t) != 2 {
				return pickle.Class{}, errInvalidPyClass
			}
			modname, ok1 := t[0].(string)
			classname, ok2 := t[1].(string)
			if !(ok1 && ok2) {
				return pickle.Class{}, errInvalidPyClass
			}

			return pickle.Class{Module: modname, Name: classname}, nil
		}
	}

	if klass, ok := xklass.(pickle.Class); ok {
		// klass = type(obj)
		return klass, nil
	}

	return pickle.Class{}, errInvalidPyClass
}

// // PyClass returns Python class of the object.
// func (pyobj *PyObject) PyClass() pickle.Class {
// 	return pyobj.pyClass
// }
