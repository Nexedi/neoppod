// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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

package proto
// NEO. test wire protocol compatibility with python

//go:generate ./py/pyneo-gen-testdata

import (
	"reflect"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

// verify that message codes are the same in between py and go
func TestMsgCodeVsPy(t *testing.T) {
	goMsgRegistry := map[uint16]string{} // code -> packet name
	for code, pktType := range msgTypeRegistry{
		goMsgRegistry[code] = pktType.Name()
	}

	if !reflect.DeepEqual(goMsgRegistry, pyMsgRegistry) {
		t.Fatalf("message registry: py vs go mismatch:\n%s\n",
			pretty.Compare(pyMsgRegistry, goMsgRegistry))
	}
}
