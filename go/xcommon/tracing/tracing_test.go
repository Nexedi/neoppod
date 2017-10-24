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

package tracing

import (
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/kylelemons/godebug/pretty"
)

func TestAttachDetach(t *testing.T) {
	var traceX *Probe // list head of a tracing event

	// check that traceX probe list has such and such content and also that .prev
	// pointers in all elements are right
	checkX := func(probev ...*Probe) {
		t.Helper()
		var pv []*Probe

		pp := (*Probe)(unsafe.Pointer(&traceX))
		for p := traceX; p != nil; pp, p = p, p.next {
			if p.prev != pp {
				t.Fatalf("probe list: %#v: .prev is wrong", p)
			}
			pv = append(pv, p)
		}

		if !reflect.DeepEqual(pv, probev) {
			t.Fatalf("probe list:\n%s\n", pretty.Compare(probev, pv))
		}
	}

	checkX()

	// attach probe to traceX
	attachX := func(probe *Probe) {
		Lock()
		AttachProbe(nil, &traceX, probe)
		Unlock()
	}

	// detach probe
	detach := func(probe *Probe) {
		Lock()
		probe.Detach()
		Unlock()
	}

	p1 := &Probe{}
	attachX(p1)
	checkX(p1)

	detach(p1)
	checkX()
	detach(p1)
	checkX()
	attachX(p1)
	checkX(p1)

	p2 := &Probe{}
	attachX(p2)
	checkX(p1, p2)

	p3 := &Probe{}
	attachX(p3)
	checkX(p1, p2, p3)

	detach(p2)
	checkX(p1, p3)

	detach(p1)
	checkX(p3)

	detach(p3)
	checkX()
}

// Test use vs concurent detach.
//
// Detach works under tracing lock (= world stopped) - so changing a probe list
// should be ok, but since race detector does not know we stopped the world it
// could complain.
func TestUseDetach(t *testing.T) {
	var traceX *Probe // list head of a tracing event

	// attach probe to traceX
	probe := Probe{}
	Lock()
	AttachProbe(nil, &traceX, &probe)
	Unlock()

	// simulate traceX signalling and so probe usage and concurrent probe detach
	go func() {
		// delay a bit so that main goroutine first spins some time
		// with non-empty probe list
		time.Sleep(1 * time.Millisecond)

		Lock()
		probe.Detach()
		Unlock()
	}()

loop:
	for {
		np := 0
		for p := traceX; p != nil; p = p.Next() {
			np++
		}

		switch np {
		case 1:
			// ok - not yet detached
		case 0:
			// ok - detached
			break loop
		default:
			t.Fatalf("probe seen %d times; must be either 1 or 0", np)
		}

		// XXX as of go19 tight loops are not preemptible (golang.org/issues/10958)
		//     and Lock does stop-the-world -> make this loop explicitly preemtible.
		runtime.Gosched()
	}
}
