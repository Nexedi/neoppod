// Copyright (C) 2018  Nexedi SA and Contributors.
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

package weak

import (
	"runtime"
	"testing"
	"time"
	"unsafe"
)

// verify that interface <-> iface works ok.
func TestIface(t *testing.T) {
	var i interface{}
	var fi *iface

	isize := unsafe.Sizeof(i)
	fsize := unsafe.Sizeof(*fi)

	if isize != fsize {
		t.Fatalf("sizeof(interface{}) (%d)  !=   sizeof(iface) (%d)", isize, fsize)
	}

	i = 3
	var j interface{}
	if !(j == nil && i != j) {
		t.Fatalf("i == j ?  (i: %#v,  j: %#v}", i, j)
	}

	fi = (*iface)(unsafe.Pointer(&i))
	fj := (*iface)(unsafe.Pointer(&j))
	*fj = *fi

	if i != j {
		t.Fatalf("i (%#v)  !=  j (%#v)", i, j)
	}
}

func TestWeakRef(t *testing.T) {
	type T struct{ _ [8]int64 } // large enough not to go into tinyalloc

	p := new(T)
	w := NewRef(p)
	pptr := uintptr(unsafe.Pointer(p))

	assertEq := func(a, b interface{}) {
		t.Helper()
		if a != b {
			t.Fatalf("not equal: %#v  !=  %#v", a, b)
		}
	}

	// perform GC + give finalizers a chance to run.
	GC := func() {
		runtime.GC()

		// GC only queues finalizers, not runs them directly. Give it
		// some time so that finalizers could have been run.
		time.Sleep(10 * time.Millisecond) // XXX hack
	}

	assertEq(w.state, objLive)
	assertEq(w.Get(), p)
	assertEq(w.state, objGot)
	GC()
	assertEq(w.state, objGot) // fin has not been run at all (p is live)
	assertEq(w.Get(), p)
	assertEq(w.state, objGot)

	p = nil
	GC()
	assertEq(w.state, objLive) // fin ran and downgraded got -> live
	switch p_ := w.Get().(type) {
	default:
		t.Fatalf("Get after objGot -> objLive: %#v", p_)
	case *T:
		if uintptr(unsafe.Pointer(p_)) != pptr {
			t.Fatal("Get after objGot -> objLive: T, but ptr is not the same")
		}
	}
	assertEq(w.state, objGot)

	GC()
	assertEq(w.state, objLive) // fin ran again and again downgraded got -> live

	GC()
	assertEq(w.state, objReleased) // fin ran again and released the object
	assertEq(w.Get(), nil)
}
