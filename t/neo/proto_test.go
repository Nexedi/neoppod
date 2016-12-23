// Copyright (C) 2016  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// NEO. Protocol definition. Tests

package neo

import (
	"testing"
	"unsafe"
)

func TestPktHeader(t *testing.T) {
	// make sure PktHeader is really packed
	if unsafe.Sizeof(PktHead{}) != 10 {
		t.Fatalf("sizeof(PktHead) = %v  ; want 10", unsafe.Sizeof(PktHead{}))
	}
}
