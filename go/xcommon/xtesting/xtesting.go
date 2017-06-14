// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// Package xtesting provides addons to std package testing
package xtesting

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TODO tests for this

// XXX Tracer interface {Trace1} ?

// SyncTracer provides base infrastructure for synchronous tracing
//
// Tracing events from several sources could be collected and sent for consumption via 1 channel.
// For each event the goroutine which produced it will wait for ack before continue.
type SyncTracer struct {
	tracech chan *SyncTraceMsg
}

// SyncTraceMsg represents message with 1 synchronous tracing communication
// the goroutine which produced the message will wait for send on Ack before continue.
type SyncTraceMsg struct {
	Event interface {}
	Ack   chan<- struct{}
}

// XXX doc
func NewSyncTracer() *SyncTracer {
	return &SyncTracer{tracech: make(chan *SyncTraceMsg)}
}

// Trace1 sends message with 1 tracing event to a consumer and waits for ack
func (st *SyncTracer) Trace1(event interface{}) {
	ack := make(chan struct{})
	st.tracech <- &SyncTraceMsg{event, ack}
	<-ack
}

// Get1 receives message with 1 tracing event from a producer
// The consumer, after dealing with the message, must send back an ack.
func (st *SyncTracer) Get1() *SyncTraceMsg {
	return <-st.tracech
	//msg := <-st.tracech
	//fmt.Printf("trace: get1: %T %v\n", msg.Event, msg.Event)
	//return msg
}





// TraceChecker synchronously collects and checks tracing events from a SyncTracer
type TraceChecker struct {
	t  *testing.T
	st *SyncTracer
}

// XXX doc
func NewTraceChecker(t *testing.T, st *SyncTracer) *TraceChecker {
	return &TraceChecker{t: t, st: st}
}

// get1 gets 1 event in place and checks it has expected type
// if checks do not pass - fatal testing error is raised
// XXX merge back to expect1 ?
func (tc *TraceChecker) xget1(eventp interface{}) *SyncTraceMsg {
	tc.t.Helper()
	msg := tc.st.Get1()

	reventp := reflect.ValueOf(eventp)
	if reventp.Type().Elem() != reflect.TypeOf(msg.Event) {
		tc.t.Fatalf("expect: %s:  got %#v", reventp.Elem().Type(), msg.Event)
	}

	// *eventp = msg.Event
	reventp.Elem().Set(reflect.ValueOf(msg.Event))

	return msg
}

// expect1 asks checker to expect next event to be eventExpect (both type and value)
// if checks do not pass - fatal testing error is raised
// XXX merge back to expect?
func (tc *TraceChecker) expect1(eventExpect interface{}) {
	tc.t.Helper()

	reventExpect := reflect.ValueOf(eventExpect)

	reventp := reflect.New(reventExpect.Type())
	msg := tc.xget1(reventp.Interface())
	revent := reventp.Elem()

	if !reflect.DeepEqual(revent.Interface(), reventExpect.Interface()) {
		tc.t.Fatalf("expect: %s:\nhave: %v\nwant: %v", reventExpect.Type(), revent, reventExpect)
	}

	close(msg.Ack)
}

// Expect asks checker to expect next series of events to be from eventExpectV in specified order
func (tc *TraceChecker) Expect(eventExpectV ...interface{}) {
	tc.t.Helper()

	for _, eventExpect := range eventExpectV {
		tc.expect1(eventExpect)
	}
}

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
			close(msg.Ack)	// XXX -> send ack for all only when all collected?
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
