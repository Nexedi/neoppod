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

// Package tracing provides runtime and usage support for Go tracing facilities
// TODO describe how to define tracepoints
// TODO doc:
// - tracepoints
// - probes
// - probes can be attached/detached to/from tracepoints
//
// TODO document //trace:event & //trace:import
// TODO document `gotrace gen` + `gotrace list`
package tracing

import (
	"sync"
	"sync/atomic"
)

// big tracing lock
var traceMu     sync.Mutex
var traceLocked int32      // for cheap protective checks whether Lock is held

// Lock serializes modification access to tracepoints
//
// Under Lock it is safe to attach/detach probes to/from tracepoints:
// - no other goroutine is attaching or detaching probes from tracepoints,
// - a tracepoint readers won't be neither confused nor raced by such adjustments.
//
// Lock returns with the world stopped.
func Lock() {
	traceMu.Lock()
	runtime_stopTheWorld("tracing lock")
	atomic.StoreInt32(&traceLocked, 1)
}

// Unlock is the opposite to Lock and returns with the world resumed
func Unlock() {
	atomic.StoreInt32(&traceLocked, 0)
	runtime_startTheWorld()
	traceMu.Unlock()
}

// verifyLocked makes sure tracing is locked and panics otherwise
func verifyLocked() {
	if atomic.LoadInt32(&traceLocked) == 0 {
		panic("tracing must be locked")
	}
}

// verifyUnlocked makes sure tracing is not locked and panics otherwise
func verifyUnlocked() {
	if atomic.LoadInt32(&traceLocked) != 0 {
		panic("tracing must be unlocked")
	}
}


// Probe describes one probe attached to a tracepoint
type Probe struct {
	prev, next *Probe

	// implicitly:
	// probefunc  func(some arguments)
}

// Next returns next probe attached to the same tracepoint
// It is safe to iterate Next under any conditions.
func (p *Probe) Next() *Probe {
	return p.next
}

// AttachProbe attaches newly created Probe to the end of a probe list
// If group is non-nil the probe is also added to the group.
// Must be called under Lock.
// Probe must be newly created.
func AttachProbe(pg *ProbeGroup, listp **Probe, probe *Probe) {
	verifyLocked()

	if !(probe.prev == nil || probe.next == nil) {
		panic("attach probe: probe is not newly created")
	}

	var last *Probe
	for p := *listp; p != nil; p = p.next {
		last = p
	}

	if last != nil {
		last.next = probe
		probe.prev = last
	} else {
		*listp = probe
	}

	if pg != nil {
		pg.Add(probe)
	}
}

// Detach detaches probe from a tracepoint
// Must be called under Lock
func (p *Probe) Detach() {
	verifyLocked()

	// protection: already detached
	if p.prev == p {
		return
	}

	// we can safely change prev.next pointer:
	// - no reader is currently reading it
	// - either a reader already read prev.next, and will proceed with our probe entry, or
	// - it will read updated prev.next and will proceed with p.next probe entry
	if p.prev != nil {
		p.prev.next = p.next
	}

	// we can safely change next.prev pointer:
	// - readers only go through list forward
	// - there is no other updater because we are under Lock
	if p.next != nil {
		p.next.prev = p.prev
	}

	// mark us detached so that if Detach is erroneously called the second
	// time it does not do harm
	p.prev = p
}

// ProbeGroup is a group of probes attached to tracepoints
type ProbeGroup struct {
	probev []*Probe
}

// Add adds a probe to the group
// Must be called under Lock
func (pg *ProbeGroup) Add(p *Probe) {
	verifyLocked()
	pg.probev = append(pg.probev, p)
}

// Done detaches all probes registered to the group
// Must be called under normal conditions, not under Lock
func (pg *ProbeGroup) Done() {
	verifyUnlocked()
	Lock()
	defer Unlock()

	for _, p := range pg.probev {
		p.Detach()
	}
	pg.probev = nil
}
