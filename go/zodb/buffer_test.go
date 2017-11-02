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

// As of go19 sync.Pool under race-detector randomly drops items on the floor
// https://github.com/golang/go/blob/ca360c39/src/sync/pool.go#L92
// so it is not possible to verify we will get what we've just put there.
// +build !race

package zodb

import (
	"reflect"
	"testing"
	"unsafe"
)

//go:linkname runtime_procPin runtime.procPin
//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procPin() int
func runtime_procUnpin()


func sliceDataPtr(b []byte) unsafe.Pointer {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data)
}

func TestBufAllocFree(t *testing.T) {
	// sync.Pool uses per-P free-lists. We check that after release we will
	// allocate released object. This works only if P is not changed underneath.
	runtime_procPin()
	defer runtime_procUnpin()

	for i := uint(0); i < 25; i++ {
		size := 1<<i - 1
		xcap := 1<<i
		buf := BufAlloc(size)
		if i < order0 {
			xcap = 1 << order0
		}
		if int(i) >= order0+len(bufPoolv) {
			xcap = size
		}

		if len(buf.Data) != size {
			t.Fatalf("%v: len=%v  ; want %v", i, len(buf.Data), size)
		}
		if cap(buf.Data) != xcap {
			t.Fatalf("%v: cap=%v  ; want %v", i, cap(buf.Data), xcap)
		}

		checkref := func(rc int32) {
			t.Helper()
			if buf.refcnt != rc {
				t.Fatalf("%v: refcnt=%v  ; want %v", i, buf.refcnt, rc)
			}
		}

		checkref(0)

		// free and allocate another buf -> it must be it
		data := buf.Data
		buf.Release()
		checkref(-1)
		buf2 := BufAlloc(size)

		// not from pool - memory won't be reused
		if int(i) >= order0+len(bufPoolv) {
			if buf2 == buf || sliceDataPtr(buf2.Data) == sliceDataPtr(data) {
				t.Fatalf("%v: buffer reused but should not", i)
			}
			continue
		}

		// from pool -> it must be the same
		if !(buf2 == buf && sliceDataPtr(buf2.Data) == sliceDataPtr(data)) {
			t.Fatalf("%v: buffer not reused on free/realloc", i)
		}
		checkref(0)

		// add more ref and release original buf - it must stay alive
		buf.Incref()
		checkref(1)
		buf.Release()
		checkref(0)

		// another alloc must be different
		buf2 = BufAlloc(size)
		checkref(0)

		if buf2 == buf || sliceDataPtr(buf2.Data) == sliceDataPtr(data) {
			t.Fatalf("%v: buffer reused but should not", i)
		}

		// release buf again -> should go to pool
		buf.Release()
		checkref(-1)
		buf2 = BufAlloc(size)
		if !(buf2 == buf && sliceDataPtr(buf2.Data) == sliceDataPtr(data)) {
			t.Fatalf("%v: buffer not reused on free/realloc", i)
		}
		checkref(0)
	}
}
