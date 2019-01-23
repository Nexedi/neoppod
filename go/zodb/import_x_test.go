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
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/internal/xtesting"

	// wks or any other storage cannot be imported from zodb due to cycle
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
)

// import at runtime few things into zodb, that zodb cannot import itself due to cyclic dependency.

func init() {
	zodb.ZPyCommit = ZPyCommit
}

func ZPyCommit(zurl string, at zodb.Tid, objv ...zodb.IPersistent) (zodb.Tid, error) {
	var rawobjv []xtesting.ZRawObject // raw zodb objects data to commit
	for _, obj := range objv {
		rawobj := xtesting.ZRawObject{
			Oid:  obj.POid(),
			Data: string(zodb.PSerialize(obj).XData()),
		}
		rawobjv = append(rawobjv, rawobj)
	}

	return xtesting.ZPyCommitRaw(zurl, at, rawobjv...)
}
