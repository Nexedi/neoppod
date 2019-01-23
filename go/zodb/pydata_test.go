// Copyright (C) 2016-2019  Nexedi SA and Contributors.
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

//go:generate ./py/pydata-gen-testdata

import (
	"testing"
)

type _PyDataClassName_TestEntry struct {
	pydata    string
	className string
}

// XXX + test with zodbpickle.binary (see 12ee41c4 in ZODB)
// NOTE zodbpickle.binary pickles as just bytes which ~> ogórek.Bytes

func TestPyClassName(t *testing.T) {
	for _, tt := range _PyData_ClassName_Testv {
		className := PyData(tt.pydata).ClassName()
		if className != tt.className {
			t.Errorf("class name for %q:\nhave: %q\nwant: %q",
				tt.pydata, className, tt.className)
		}
	}
}

func TestPyDecode(t *testing.T) {
	// XXX
}

func TestPyEncode(t *testing.T) {
	// XXX
}
