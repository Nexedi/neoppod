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
	"fmt"

	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/go123/xerr"
)

// PyData represents raw data stored into ZODB by Python applications.
//
// The format is based on python pickles. Basically every serialized object has
// two parts: pickle with class description and pickle with object state. See
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/serialize.py
//
// for format description.
type PyData []byte

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

	klass, err := xpyclass(xklass)
	if err != nil {
		return "?.?"
	}

	return pyclassPath(klass)
}

// xpyclass verifies and extracts py class from unpickled value.
//
// it normalizes py class that has just been decoded from a serialized ZODB
// object or reference.
//
// class description:
//
//	- type(obj), or
//	- (xklass, newargs|None)	; xklass = type(obj) | (modname, classname)
func xpyclass(xklass interface{}) (_ pickle.Class, err error) {
	defer xerr.Context(&err, "class")

	if t, ok := xklass.(pickle.Tuple); ok {
		// t = (xklass, newargs|None)
		if len(t) != 2 {
			return pickle.Class{}, fmt.Errorf("top: expect [2](); got [%d]()", len(t))
		}
		// XXX newargs is ignored (zodb/py uses it only for persistent classes)
		xklass = t[0]
		if t, ok := xklass.(pickle.Tuple); ok {
			// t = (modname, classname)
			if len(t) != 2 {
				return pickle.Class{}, fmt.Errorf("xklass: expect [2](); got [%d]()", len(t))
			}
			modname, ok1 := t[0].(string)
			classname, ok2 := t[1].(string)
			if !(ok1 && ok2) {
				return pickle.Class{}, fmt.Errorf("xklass: expect (str, str); got (%T, %T)", t[0], t[1])
			}

			return pickle.Class{Module: modname, Name: classname}, nil
		}
	}

	if klass, ok := xklass.(pickle.Class); ok {
		// klass = type(obj)
		return klass, nil
	}

	return pickle.Class{}, fmt.Errorf("expect type; got %T", xklass)
}

// pyclassPath returns full path for a python class.
//
// for example class "ABC" in module "wendelin.lib" has its full path as "wendelin.lib.ABC".
func pyclassPath(pyclass pickle.Class) string {
	return pyclass.Module + "." + pyclass.Name
}
