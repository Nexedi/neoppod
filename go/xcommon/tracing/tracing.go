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

/*
Package tracing provides usage and runtime support for Go tracing facilities.

Trace events

A Go package can define several events of interest to trace via special
comments. With such definition a tracing event becomes associated with trace
function that is used to signal when the event happens. For example:

	package hello

	//trace:event traceHelloPre(who string)
	//trace:event traceHello(who string)

	func SayHello(who string) {
		traceHelloPre(who)
		fmt.Println("Hello, %s", who)
		traceHello(who)
	}

By default trace function does nothing and has very small overhead(*).


Probes

However it is possible to attach probing functions to events. A probe, once
attached, is called whenever event is signalled in the context which triggered
the event and pauses original code execution until the probe is finished. It is
possible to attach several probing functions to the same event and dynamically
detach/(re-)attach them at runtime. Attaching/detaching probes must be done
under tracing.Lock. For example:

	type saidHelloT struct {
		who  string
		when time.Time
	}
	saidHello := make(chan saidHelloT)

	tracing.Lock()
	p := traceHello_Attach(nil, func(who string) {
		saidHello <- saidHelloT{who, time.Now()}
	})
	tracing.Unlock()

	go func() {
		for hello := range saidHello {
			fmt.Printf("Said hello to %v @ %v\n", hello.who, hello.when)
		}
	}()

	SayHello("JP")
	SayHello("Kirr")
	SayHello("Varya")

	tracing.Lock()
	p.Detach()
	tracing.Unlock()

	close(saidHello)

For convenience it is possible to keep group of attached probes and detach them
all at once using ProbeGroup:

	pg := &tracing.ProbeGroup{}

	tracing.Lock()
	traceHelloPre_Attach(pg, func(who string) { ... })
	traceHello_Attach(pg, func(who string) { ... })
	tracing.Unlock()

	// some activity

	// when probes needs to be detached (no explicit tracing.Lock needed):
	pg.Done()

Probes is general mechanism which allows various kinds of trace events usage.
Three ways particularly are well-understood and handy:

	- recording events stream
	- profiling
	- synchronous tracing


Recording events stream

To get better understanding of what happens when it is possible to record
events into a stream and later either visualize or postprocess them.
This is similar to how Go execution tracer works:

	https://golang.org/s/go15trace
	https://golang.org/pkg/runtime/trace
	https://golang.org/cmd/trace

though there it records only predefined set of events related to Go runtime.

TODO tracing should provide infrastructure to write events out in format
understood by chromium trace-viewer: https://github.com/catapult-project/catapult/tree/master/tracing

NOTE there is also talk/work to implement user events for runtime/trace: https://golang.org/issues/16619.

Profiling

A profile is aggregate summary of collection of stack traces showing the call sequences that led
to instances of a particular event. One could create runtime/pprof.Profile and
use Profile.Add in a probe attached to particular trace event. The profile can
be later analyzed and visualised with Profile.WriteTo and `go tool pprof`.

Please refer to runtime/pprof package documentation for details.

XXX Profile.Add needs unique value for each invocation - how do we do? Provide NaN every time?

XXX should tracing provide more tight integration with runtime/pprof.Profile?


Synchronous tracing

For testing purposes it is sometimes practical to leverage the property that
probes pause original code execution until the probe run is finished. That
means while the probe is running original goroutine

- is paused at well-defined point (where trace function is called), thus
- it cannot mutate any state it is programmed to mutate.

Using this properties it is possible to attach testing probes and verify that
a set of goroutines in tested code in question

- produce events in correct order, and
- at every event associated internal state is correct.

TODO example.


Cross package tracing

Trace events are not part of exported package API with rationale that package's
regular API and internal trace events usually have different stability
commitments. However with tracing-specific importing mechanism it is possible
to get access to trace events another package provides:

	package another

	//trace:import "hello"

This will make _Attach functions for all tracing events from package hello be
available as regular functions prefixed with imported package name:

	tracing.Lock()
	hello_traceHello_Attach(nil, func(who string) {
		fmt.Printf("SayHello in package hello: %s", who)
	tracing.Unlock()

	...


Gotrace

The way //trace:event and //trace:import works is via additional code being
generated for them. Whenever a package uses any //trace: directive,
it has to organize to run `gotrace gen` on its sources for them to work,
usually with the help of //go:generate. For example:

	package hello

	//go:generate gotrace gen .

	//trace:event ...

Besides `gotrace gen` gotrace has other subcommands also related to tracing,
for example `gotrace list` lists trace events a package provides.

Please see TODO link for gotrace documentation.

--------

(*) conditionally checking whether a pointer != nil. After
https://golang.org/issues/19348 is implemented the call/return overhead will be
also gone.
*/
package tracing

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/xcommon/tracing/internal/xruntime"
)

// big tracing lock
var traceMu     sync.Mutex
var traceLocked int32      // for cheap protective checks whether Lock is held

// Lock serializes modification access to tracepoints.
//
// Under Lock it is safe to attach/detach probes to/from tracepoints:
// - no other goroutine is attaching or detaching probes from tracepoints,
// - a tracepoint readers won't be neither confused nor raced by such adjustments.
//
// Lock returns with the world stopped.
func Lock() {
	traceMu.Lock()
	xruntime.StopTheWorld("tracing lock")
	atomic.StoreInt32(&traceLocked, 1)
	// we synchronized with everyone via stopping the world - there is now
	// no other goroutines running to race with.
	xruntime.RaceIgnoreBegin()
}

// Unlock is the opposite to Lock and returns with the world resumed
func Unlock() {
	xruntime.RaceIgnoreEnd()
	atomic.StoreInt32(&traceLocked, 0)
	xruntime.StartTheWorld()
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
	// NOTE .next must come first as probe list header is only 1 word and
	// is treated as *Probe on probe attach/detach - accessing/modifying its .next
	next, prev *Probe

	// implicitly:
	// probefunc  func(some arguments)
}

// Next returns next probe attached to the same tracepoint.
//
// It is safe to iterate Next under any conditions.
func (p *Probe) Next() *Probe {
	return p.next
}

// AttachProbe attaches newly created Probe to the end of a probe list.
//
// If group is non-nil the probe is also added to the group.
// Must be called under Lock.
// Probe must be newly created.
func AttachProbe(pg *ProbeGroup, listp **Probe, probe *Probe) {
	verifyLocked()

	if !(probe.prev == nil || probe.next == nil) {
		panic("attach probe: probe is not newly created")
	}

	last := (*Probe)(unsafe.Pointer(listp))
	for p := *listp; p != nil; last, p = p, p.next {
	}

	last.next = probe
	probe.prev = last

	if pg != nil {
		pg.Add(probe)
	}
}

// Detach detaches probe from a tracepoint.
//
// Must be called under Lock.
func (p *Probe) Detach() {
	verifyLocked()

	// protection: already detached
	if p.prev == nil {
		return
	}

	// we can safely change prev.next pointer:
	// - no reader is currently reading it
	// - either a reader already read prev.next, and will proceed with our probe entry, or
	// - it will read updated prev.next and will proceed with p.next probe entry
	p.prev.next = p.next

	// we can safely change next.prev pointer:
	// - readers only go through list forward
	// - there is no other updater because we are under Lock
	if p.next != nil {
		p.next.prev = p.prev
	}

	// mark us detached so that if Detach is erroneously called the second
	// time it does not do harm
	p.prev = nil
	p.next = nil
}

// ProbeGroup is a group of probes attached to tracepoints.
type ProbeGroup struct {
	probev []*Probe
}

// Add adds a probe to the group.
//
// Must be called under Lock.
func (pg *ProbeGroup) Add(p *Probe) {
	verifyLocked()
	pg.probev = append(pg.probev, p)
}

// Done detaches all probes registered to the group.
//
// Must be called under normal conditions, not under Lock.
func (pg *ProbeGroup) Done() {
	verifyUnlocked()
	Lock()
	defer Unlock()

	for _, p := range pg.probev {
		p.Detach()
	}
	pg.probev = nil
}
