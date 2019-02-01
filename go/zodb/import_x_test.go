// Copyright (C) 2019  Nexedi SA and Contributors.
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

package zodb_test

import (
	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/internal/xtesting"

	// we need file:// support in ZODB tests.
	// wks or any other storage cannot be imported from zodb due to cycle.
	_ "lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
)

// import at runtime few things into zodb tests, that zodb cannot import itself
// due to cyclic dependency.

func init() {
	zodb.ZPyCommit = ZPyCommit
}

// ZPyCommit commits new transaction with specified objects.
//
// The objects need to be alive, but do not need to be marked as changed.
// The commit is performed via zodb/py.
func ZPyCommit(zurl string, at zodb.Tid, objv ...zodb.IPersistent) (zodb.Tid, error) {
	var rawobjv []xtesting.ZRawObject // raw zodb objects data to commit
	var bufv []*mem.Buf               // buffers to release
	defer func() {
		for _, buf := range bufv {
			buf.Release()
		}
	}()

	for _, obj := range objv {
		buf := zodb.PSerialize(obj)
		rawobj := xtesting.ZRawObject{
			Oid:  obj.POid(),
			Data: buf.Data,
		}
		rawobjv = append(rawobjv, rawobj)
		bufv = append(bufv, buf)
	}

	return xtesting.ZPyCommitRaw(zurl, at, rawobjv...)
}
