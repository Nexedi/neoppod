// Copyright (C) 2017  Nexedi SA and Contributors.
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

package zodb

import (
	"reflect"
	"testing"
	"unsafe"
)

func sliceDataPtr(b []byte) unsafe.Pointer {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data)
}

func TestBufAllocFree(t *testing.T) {
	for i := uint(0); i < 20; i++ {
		//if i < 7 { continue }
		size := 1<<i - 1
		xcap := 1<<i
		buf := BufAlloc(size)
		if i < order0 {
			xcap = 1<<order0
		}
		if int(i) >= order0 + len(bufPoolv) {
			xcap = size
		}

		if len(buf.Data) != size {
			t.Errorf("%v: len=%v  ; want %v", i, len(buf.Data), size)
		}
		if cap(buf.Data) != xcap {
			t.Errorf("%v: cap=%v  ; want %v", i, cap(buf.Data), xcap)
		}

		// this allocations are not from pool - memory won't be reused
		if int(i) >= order0 + len(bufPoolv) {
			continue
		}

		// free and allocate another buf -> it must be it
		data := buf.Data
		buf.Free()
		buf2 := BufAlloc(size)

		if !(buf2 == buf && sliceDataPtr(buf2.Data) == sliceDataPtr(data)) {
			t.Errorf("%v: buffer not reused on free/realloc", i)
		}
	}
}
