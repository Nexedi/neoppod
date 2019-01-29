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
// Support for python objects/data in ZODB.

import (
	"context"

	"lab.nexedi.com/kirr/go123/mem"
)

// PyStateful is the interface describing in-RAM object whose data state can be
// exchanged as Python data.
type PyStateful interface {
	// PySetState should set state of the in-RAM object from Python data.
	//
	// It is analog of __setstate__() in Python.
	//
	// The error returned does not need to have object/setstate prefix -
	// persistent machinery is adding such prefix automatically.
	PySetState(pystate interface{}) error

	// PyGetState should return state of the in-RAM object as Python data.
	// Analog of __getstate__() in Python.
	//PyGetState() interface{}	TODO
}

// pySetState decodes raw state as zodb/py serialized stream, and sets decoded
// state on PyStateful obj.
//
// It is an error if decoded state has python class not as specified.
// jar is used to resolve persistent references.
func pySetState(obj PyStateful, objClass string, state *mem.Buf, jar *Connection) error {
	pyclass, pystate, err := PyData(state.Data).decode(jar)
	if err != nil {
		return err
	}

	class := pyclassPath(pyclass)

	if class != objClass {
		// complain that pyclass changed
		// (both ref and object data use pyclass so it indeed can be different)
		return &wrongClassError{want: objClass, have: class}
	}

	return obj.PySetState(pystate)
}

// TODO pyGetState



// loadpy loads object specified by oid and decodes it as a ZODB Python object.
//
// loadpy does not create any in-RAM object associated with Connection.
// It only returns decoded database data.
func (conn *Connection) loadpy(ctx context.Context, oid Oid) (class string, pystate interface{}, serial Tid, _ error) {
	xid := Xid{Oid: oid, At: conn.at}
	buf, serial, err := conn.db.stor.Load(ctx, xid)
	if err != nil {
		return "", nil, 0, err
	}

	defer buf.Release()

	pyclass, pystate, err := PyData(buf.Data).decode(conn)
	if err != nil {
		err = &OpError{
			URL:  conn.db.stor.URL(),
			Op:   "loadpy",
			Args: xid,
			Err:  err,
		}
		return "", nil, 0, err
	}

	return pyclassPath(pyclass), pystate, serial, nil
}
