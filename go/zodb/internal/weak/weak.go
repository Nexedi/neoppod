// Copyright (C) 2018-2020  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// based on:
// https://groups.google.com/d/msg/golang-nuts/PYWxjT2v6ps/dL71oJk1mXEJ
// https://play.golang.org/p/f9HY6-z8Pp
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

// Package weak provides weak references for Go.
package weak

//go:generate gotrace gen .

import (
	"fmt"
	"runtime"
	"sync"
//	"time"
	"unsafe"
)

// iface is how Go runtime represents an interface.
//
// NOTE layout must be synchronized to Go runtime representation.
// NOTE correctness depends on non-moving property of Go GC.
type iface struct {
	typ  uintptr // type
	data uintptr // data
}

// weakRefState represents current state of an object Ref points to.
type weakRefState int32

const (
	objGot      weakRefState = +1 // Ref.Get returned !nil
	objLive     weakRefState =  0 // object is alive, Get did not run yet in this GC cycle
	objReleased weakRefState = -1 // the finalizer marked object as released
)


// Ref is a weak reference.
//
// Create one with NewRef and retrieve referenced object with Get.
//
// There must be no more than 1 weak reference to any object.
// Weak references must not be attached to an object on which runtime.SetFinalizer is also used.
// Weak references must not be copied.
type Ref struct {
	iface

	// XXX try to do without mutex and only with atomics
	mu    sync.Mutex
	state weakRefState
}

//trace:event traceRelease(w *Ref, released bool)

// NewRef creates new weak reference pointing to obj.
//
// TODO + onrelease callback?
func NewRef(obj interface{}) *Ref {
	// since starting from ~ Go1.4 the GC is precise, we can save interface
	// pointers to uintptr and that won't prevent GC from garbage
	// collecting the object.
	w := &Ref{
		iface: *(*iface)(unsafe.Pointer(&obj)),
		state: objLive,
	}

	var release func(interface{})
	release = func(obj interface{}) {
		// assert that the object was not moved
		iobj := *(*iface)(unsafe.Pointer(&obj))
		if w.iface != iobj {
			panic(fmt.Sprintf("weak: release: object moved:  w.iface=%x  obj=%x", w.iface, iobj))
		}

		// GC decided that the object is no longer reachable and
		// scheduled us to run as finalizer. During the time till we
		// actually run, Ref.Get might have been come to run and
		// "rematerializing" the object for use. Check if we do not
		// race with any Get in progress, and reschedule us to retry at
		// next GC if we do.
		w.mu.Lock()
		if w.state == objGot {
			w.state = objLive
			runtime.SetFinalizer(obj, release)
			traceRelease(w, false)
		} else {
			w.state = objReleased
			traceRelease(w, true)
		}
		w.mu.Unlock()

	}

	runtime.SetFinalizer(obj, release)
	return w
}

//trace:event traceGotPre(w *Ref)

// Get returns object pointed to by this weak reference.
//
// If original object is still alive - it is returned.
// If not - nil is returned.
func (w *Ref) Get() (obj interface{}) {
	w.mu.Lock()
	if w.state != objReleased {
		w.state = objGot

		traceGotPre(w)
		//time.Sleep(100*time.Nanosecond)
		//time.Sleep(10*time.Millisecond)
		//runtime.GC()
		//runtime.GC()

		// recreate interface{} from saved words.
		// XXX do writes as pointers so that compiler emits write barriers to notify GC?
		i := (*iface)(unsafe.Pointer(&obj))
		*i = w.iface
	}
	w.mu.Unlock()
	return obj
}
