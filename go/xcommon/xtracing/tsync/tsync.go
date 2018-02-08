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

// Package tsync provides infrastructure for synchronous testing based on program tracing.
// XXX naming -> ttest?
//
// A serial system can be verified by checking that its execution produces
// expected serial stream of events. But concurrent systems cannot be verified
// by exactly this way because events are only partly-ordered with respect to
// each other by causality or so called happens-before relation.
//
// However in a concurrent system one can decompose all events into serial
// streams in which events are strictly ordered by causality with respect to
// each other. This decomposition in turn allows to verify that in every stream
// events were as expected.
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
// XXX more text describing how to use the package.
//
// XXX (if tested system is serial only there is no need to use Dispatcher and
// routing - the collector can send output directly to the only SyncChan with
// only one EventChecker connected to it).
package tsync

import (
	"reflect"
	"testing"

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
}

// Send sends event to a consumer and waits for ack.
func (ch *SyncChan) Send(event interface{}) {
	ack := make(chan struct{})
	ch.msgq <- &SyncMsg{event, ack}
	<-ack
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
	ack   chan<- struct{}
}

// Ack acknowledges the event was processed and unblocks producer goroutine.
func (m *SyncMsg) Ack() {
	close(m.ack)
}

// NewSyncChan creates new SyncChan channel.
func NewSyncChan() *SyncChan {
	return &SyncChan{msgq: make(chan *SyncMsg)}
}


// ----------------------------------------


// EventChecker is testing utility to verify that sequence of events coming
// from a single SyncChan are as expected.
type EventChecker struct {
	t  testing.TB
	in *SyncChan
}

// NewEventChecker constructs new EventChecker that will retrieve events from
// `in` and use `t` for tests reporting.
func NewEventChecker(t testing.TB, in *SyncChan) *EventChecker {
	return &EventChecker{t: t, in: in}
}

// get1 gets 1 event in place and checks it has expected type
//
// if checks do not pass - fatal testing error is raised
func (evc *EventChecker) xget1(eventp interface{}) *SyncMsg {
	evc.t.Helper()
	// XXX handle deadlock timeout
	msg := evc.in.Recv()

	reventp := reflect.ValueOf(eventp)
	if reventp.Type().Elem() != reflect.TypeOf(msg.Event) {
		evc.t.Fatalf("expect: %s:  got %#v", reventp.Elem().Type(), msg.Event)
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
		evc.t.Fatalf("expect: %s:\nwant: %v\nhave: %v\ndiff: %s",
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
type EventRouter interface {
	// Route should return appropriate destination for event.
	//
	// If nil is returned default destination is used.	// XXX ok?
	//
	// It should be safe to call Route from multiple goroutines simultaneously.
	Route(event interface{}) *SyncChan

	// AllDst() []*SyncChan
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

	// TODO timeout: deadlock? (print all-in-flight events on timout)
	// XXX  or better ^^^ to do on receiver side?
	//
	// XXX -> if deadlock detection is done on receiver side (so in
	// EventChecker) -> we don't need EventDispatcher at all?
	outch.Send(event)
}