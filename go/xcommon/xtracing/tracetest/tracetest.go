// Copyright (C) 2017-2018  Nexedi SA and Contributors.
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

// Package tracetest provides infrastructure for testing concurrent systems
// based on synchronous event tracing.
//
// A serial system can be verified by checking that its execution produces
// expected serial stream of events. But concurrent systems cannot be verified
// by exactly this way because events are only partly-ordered with respect to
// each other by causality or so called happens-before relation.
//
// However in a concurrent system one can decompose all events into serial
// streams in which events are strictly ordered by causality with respect to
// each other. This decomposition in turn allows to verify that in every stream
// events happenned as expected.
//
// Verification of events for all streams can be done by one *sequential*
// process:
//
//	- if events A and B in different streams are unrelated to each other by
// 	  causality, the sequence of checks models a particular possible flow of
// 	  time. Notably since events are delivered synchronously and sender is
// 	  blocked until receiver/checker explicitly confirms event has been
// 	  processed, by checking either A then B, or B then A allows to check
// 	  for a particular race-condition.
//
// 	- if events A and B in different streams are related to each other by
// 	  causality (i.e. there is some happens-before relation for them) the
// 	  sequence of checking should represent that ordering relation.
//
// The package should be used as follows:
//
//	- implement tracer that will be synchronously collecting events from
//	  execution of your program. This can be done with package
//	  lab.nexedi.com/kirr/go123/tracing or by other similar means.
//
//	  the tracer have to output events to dispatcher (see below).
//
//	- implement router that will be making decisions specific to your
//	  particular testing scenario on to which stream an event should belong.
//
//	  the router will be consulted by dispatcher (see below) for its working.
//
//	- create Dispatcher. This is the central place where events are
//	  delivered from tracer and are further delivered in accordance to what
//	  router says.
//
//	- for every serial stream of events create synchronous delivery channel
//	  (SyncChan) and event Checker. XXX
//
//
// XXX more text describing how to use the package.
// XXX link to example.
//
//
// XXX say that checker will detect deadlock if there is no event or it or
// another comes at different trace channel.
//
// XXX (if tested system is serial only there is no need to use Dispatcher and
// routing - the collector can send output directly to the only SyncChan with
// only one EventChecker connected to it).
package tracetest

import (
	"fmt"
	"sort"
	"strings"
	"reflect"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

// SyncChan provides synchronous channel with additional property that send
// blocks until receiving side explicitly acknowledges message was received and
// processed.
//
// New channels must be created via NewSyncChan.
//
// It is safe to use SyncChan from multiple goroutines simultaneously.
type SyncChan struct {
	msgq chan *SyncMsg
	name string
}

// Send sends event to a consumer and waits for ack.
//
// if main testing goroutine detects any problem Send panics.	XXX
func (ch *SyncChan) Send(event interface{}) {
	ack := make(chan bool)
	ch.msgq <- &SyncMsg{event, ack}
	ok := <-ack
	if !ok {
		panic(fmt.Sprintf("%s: send: deadlock", ch.name))
	}
}

// Recv receives message from a producer.
//
// The consumer, after dealing with the message, must send back an ack.
func (ch *SyncChan) Recv() *SyncMsg {
	msg := <-ch.msgq
	return msg
}

// SyncMsg represents message with 1 event sent over SyncChan.
//
// The goroutine which sent the message will wait for Ack before continue.
type SyncMsg struct {
	Event interface {}
	ack   chan<- bool
}

// Ack acknowledges the event was processed and unblocks producer goroutine.
func (m *SyncMsg) Ack() {
	m.ack <- true
}

// NewSyncChan creates new SyncChan channel.
func NewSyncChan(name string) *SyncChan {
	// XXX somehow avoid channels with duplicate names
	//     (only allow to create named channels from under dispatcher?)
	return &SyncChan{msgq: make(chan *SyncMsg), name: name}
}


// ----------------------------------------


// EventChecker is testing utility to verify that sequence of events coming
// from a single SyncChan is as expected.
type EventChecker struct {
	t  testing.TB
	in *SyncChan
	dispatch *EventDispatcher
}

// NewEventChecker constructs new EventChecker that will retrieve events from
// `in` and use `t` for tests reporting.
//
// XXX -> dispatch.NewChecker() ?
func NewEventChecker(t testing.TB, dispatch *EventDispatcher, in *SyncChan) *EventChecker {
	return &EventChecker{t: t, in: in, dispatch: dispatch}
}

// get1 gets 1 event in place and checks it has expected type
//
// if checks do not pass - fatal testing error is raised
// XXX why eventp, not just event here?
func (evc *EventChecker) xget1(eventp interface{}) *SyncMsg {
	evc.t.Helper()
	var msg *SyncMsg

	select {
	case msg = <-evc.in.msgq:	// unwrapped Recv
		// ok

	case <-time.After(2*time.Second):	// XXX timeout hardcoded
		evc.deadlock(eventp)
	}

	reventp := reflect.ValueOf(eventp)
	if reventp.Type().Elem() != reflect.TypeOf(msg.Event) {
		evc.t.Fatalf("%s: expect: %s:  got %#v", evc.in.name, reventp.Elem().Type(), msg.Event)
	}

	// *eventp = msg.Event
	reventp.Elem().Set(reflect.ValueOf(msg.Event))

	return msg
}

// expect1 asks checker to expect next event to be eventExpect (both type and value)
//
// if checks do not pass - fatal testing error is raised.
func (evc *EventChecker) expect1(eventExpect interface{}) *SyncMsg {
	evc.t.Helper()

	reventExpect := reflect.ValueOf(eventExpect)

	reventp := reflect.New(reventExpect.Type())
	msg := evc.xget1(reventp.Interface())
	revent := reventp.Elem()

	if !reflect.DeepEqual(revent.Interface(), reventExpect.Interface()) {
		evc.t.Fatalf("%s: expect: %s:\nwant: %v\nhave: %v\ndiff: %s",
			evc.in.name,
			reventExpect.Type(), reventExpect, revent,
			pretty.Compare(reventExpect.Interface(), revent.Interface()))
	}

	return msg
}

// Expect asks checker to receive next event and verify it to be equal to expected.
//
// If check is successful ACK is sent back to event producer.
// If check does not pass - fatal testing error is raised.
func (evc *EventChecker) Expect(expected interface{}) {
	evc.t.Helper()

	msg := evc.expect1(expected)
	msg.Ack()
}

// ExpectNoACK asks checker to receive next event and verify it to be equal to
// expected without sending back ACK.
//
// No ACK is sent back to event producer - the caller becomes responsible to
// send ACK back by itself.
//
// If check does not pass - fatal testing error is raised.
func (evc *EventChecker) ExpectNoACK(expected interface{}) *SyncMsg {
	evc.t.Helper()

	msg := evc.expect1(expected)
	return msg
}


// deadlock reports diagnostic when retrieving event from .in timed out.
//
// timing out on recv means there is a deadlock either if no event was sent at
// all, or some other event was sent to another channel/checker.
//
// report the full picture - what was expected and what was sent where.
func (evc *EventChecker) deadlock(eventp interface{}) {
	evc.t.Helper()

	rt := evc.dispatch.rt
	dstv := rt.AllRoutes()

	bad := fmt.Sprintf("%s: deadlock waiting for %T\n", evc.in.name, eventp)
	type sendInfo struct{dst *SyncChan; event interface{}}
	var sendv []sendInfo
	for _, dst := range dstv {
		// check whether someone is sending on a dst without blocking.
		// if yes - report to sender there is a problem - so it can cancel its task.
		select {
		case msg := <-dst.msgq:
			sendv = append(sendv, sendInfo{dst, msg.Event})
			//msg.ack <- false

		default:
		}

		// XXX panic triggering disabled because if sender panics we have no chance to continue
		// TODO retest this

		// in any case close channel where futer Sends may arrive so that will panic too.
		//close(dst.msgq)
	}

	// order channels by name
	sort.Slice(sendv, func(i, j int) bool {
		return strings.Compare(sendv[i].dst.name, sendv[j].dst.name) < 0
	})

	if len(sendv) == 0 {
		bad += fmt.Sprintf("noone is sending\n")
	} else {
		bad += fmt.Sprintf("there are %d sender(s) on other channel(s):\n", len(sendv))
		for _, __ := range sendv {
			bad += fmt.Sprintf("%s:\t%T %v\n", __.dst.name, __.event, __.event)
		}
	}

	evc.t.Fatal(bad)
}

// XXX goes away? (if there is no happens-before for events - just check them one by one in dedicated goroutines ?)
/*
// ExpectPar asks checker to expect next series of events to be from eventExpectV in no particular order
// XXX naming
func (tc *TraceChecker) ExpectPar(eventExpectV ...interface{}) {
	tc.t.Helper()

loop:
	for len(eventExpectV) > 0 {
		msg := tc.st.Get1()

		for i, eventExpect := range eventExpectV {
			if !reflect.DeepEqual(msg.Event, eventExpect) {
				continue
			}

			// found matching event - good
			eventExpectV = append(eventExpectV[:i], eventExpectV[i+1:]...)
			msg.Ack()	// XXX -> send ack for all only when all collected?
			continue loop
		}

		// matching event not found - bad
		strv := []string{}
		for _, e := range eventExpectV {
			strv = append(strv, fmt.Sprintf("%T %v", e, e))
		}
		tc.t.Fatalf("expect:\nhave: %T %v\nwant: [%v]", msg.Event, msg.Event, strings.Join(strv, " | "))
	}
}
*/


// ----------------------------------------

// EventRouter is the interface used for routing events to appropriate output SyncChan.
//
// It should be safe to use EventRouter from multiple goroutines simultaneously.
type EventRouter interface {
	// Route should return appropriate destination for event.
	Route(event interface{}) *SyncChan

	// AllRoutes should return all routing destinations.
	AllRoutes() []*SyncChan
}

// EventDispatcher dispatches events to appropriate SyncChan for checking
// according to provided router.
type EventDispatcher struct {
	rt EventRouter
}

// NewEventDispatcher creates new dispatcher and provides router to it.
func NewEventDispatcher(router EventRouter) *EventDispatcher {
	return &EventDispatcher{rt: router}
}

// Dispatch dispatches event to appropriate output channel.
//
// It is safe to use Dispatch from multiple goroutines simultaneously.
func (d *EventDispatcher) Dispatch(event interface{}) {
	outch := d.rt.Route(event)
	// XXX if nil?

	// TODO it is possible to emperically detect here if a test incorrectly
	// decomposed its system into serial streams: consider unrelated to each
	// other events A and B are incorrectly routed to the same channel. It
	// could be so happenning that the order of checks on the test side is
	// almost always correct and so the error is not visible. However
	//
	//	if we add delays to delivery of either A or B
	//	and test both combinations
	//
	// we will for sure detect the error as, if A and B are indeed
	// unrelated, one of the delay combination will result in events
	// delivered to test in different to what it expects order.
	//
	// the time for delay could be taken as follows:
	//
	//	- run the test without delay; collect δt between events on particular stream
	//	- take delay = max(δt)·10
	//
	// to make sure there is indeed no different orderings possible on the
	// stream, rerun the test N(event-on-stream) times, and during i'th run
	// delay i'th event.


	// TODO timeout: deadlock? (print all-in-flight events on timout)
	// XXX  or better ^^^ to do on receiver side?
	//
	// XXX -> if deadlock detection is done on receiver side (so in
	// EventChecker) -> we don't need EventDispatcher at all?
	outch.Send(event)
}
