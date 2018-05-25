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

// Package xcontext provides addons to std package context.
//
// Merging contexts
//
// Merge and MergeChan could be handy in situations where spawned job needs to
// be canceled whenever any of 2 contexts becomes done. This frequently arises
// with service methods that accept context as argument, and the service
// itself, on another control line, could be instructed to become
// non-operational. For example:
//
//	func (srv *Service) DoSomething(ctx context.Context) error {
//		// srv.down is chan struct{} that becomes ready when service is closed.
//		ctxDown, cancel := xcontext.MergeChan(ctx, srv.down)
//		defer cancel()
//
//		err := doJob(ctxDown)
//		if ctxDown.Err() != nil && ctx.Err() == nil {
//			err = ErrDueToServiceDown
//		}
//
//		...
//	}
//
//
//
// XXX docs:
//	- Canceled
//	- Merge
//
//	- WhenDone
package xcontext

import (
	"context"
	"sync"
	"time"
)

// mergeCtx represents 2 context merged into 1.
type mergeCtx struct {
	ctx1, ctx2 context.Context

	done    chan struct{}
	doneErr error

	cancelCh   chan struct{}
	cancelOnce sync.Once
}

// Merge merges 2 contexts into 1.
//
// The result context:
//
//	- is done when ctx1 or ctx2 is done, or cancel called, whichever happens first,
//	- has deadline = min(ctx1.Deadline, ctx2.Deadline),
//	- has associated values merged from ctx1 and ctx2, with ctx1 taking precedence.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func Merge(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	mc := &mergeCtx{
		ctx1:     ctx1,
		ctx2:     ctx2,
		done:     make(chan struct{}),
		cancelCh: make(chan struct{}),
	}

	// if src ctx is already cancelled - make mc cancelled right after creation
	//
	// this saves goroutine spawn and makes
	//
	//	ctx = Merge(ctx1, ctx2); ctx.Err != nil
	//
	// check possible.
	select {
	case <-ctx1.Done():
		close(mc.done)
		mc.doneErr = ctx1.Err()

	case <-ctx2.Done():
		close(mc.done)
		mc.doneErr = ctx2.Err()

	default:
		// src ctx not canceled - spawn ctx{1,2}.done merger.
		go mc.wait()
	}

	return mc, mc.cancel
}

// wait waits when .ctx1 or .ctx2 is done and then mark mergeCtx as done
func (mc *mergeCtx) wait() {
	select {
	case <-mc.ctx1.Done():
		mc.doneErr = mc.ctx1.Err()

	case <-mc.ctx2.Done():
		mc.doneErr = mc.ctx2.Err()

	case <-mc.cancelCh:
		mc.doneErr = context.Canceled
	}

	close(mc.done)
}

// cancel sends signal to wait to shutdown.
//
// cancel is the context.CancelFunc returned for mergeCtx by Merge.
func (mc *mergeCtx) cancel() {
	mc.cancelOnce.Do(func() {
		close(mc.cancelCh)
	})
}

// Done implements context.Context .
func (mc *mergeCtx) Done() <-chan struct{} {
	return mc.done
}

// Err implements context.Context .
func (mc *mergeCtx) Err() error {
	// synchronize on .done to avoid .doneErr read races
	select {
	case <-mc.done:

	default:
		// done not yet closed
		return nil
	}

	// .done closed; .doneErr was set before - no race
	return mc.doneErr
}

// Deadline implements context.Context .
func (mc *mergeCtx) Deadline() (time.Time, bool) {
	d1, ok1 := mc.ctx1.Deadline()
	d2, ok2 := mc.ctx2.Deadline()
	switch {
	case !ok1:
		return d2, ok2
	case !ok2:
		return d1, ok1
	case d1.Before(d2):
		return d1, true
	default:
		return d2, true
	}
}

// Value implements context.Context .
func (mc *mergeCtx) Value(key interface{}) interface{} {
	v := mc.ctx1.Value(key)
	if v != nil {
		return v
	}
	return mc.ctx2.Value(key)
}

// ----------------------------------------

// chanCtx wraps channel into context.Context interface.
type chanCtx struct {
	done <-chan struct{}
}

// MergeChan merges context and channel into 1 context.
//
// MergeChan, similarly to Merge, provides resulting context which:
//
//	- is done when ctx1 is done or done2 is closed, or cancel called, whichever happens first,
//	- has the same deadline as ctx1,
//	- has the same associated values as ctx1.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func MergeChan(ctx1 context.Context, done2 <-chan struct{}) (context.Context, context.CancelFunc) {
	return Merge(ctx1, chanCtx{done2})
}

// Done implements context.Context .
func (c chanCtx) Done() <-chan struct{} {
	return c.done
}

// Err implements context.Context .
func (c chanCtx) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// Deadline implements context.Context .
func (c chanCtx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

// Value implements context.Context .
func (c chanCtx) Value(key interface{}) interface{} {
	return nil
}



// Cancelled reports whether an error is due to a canceled context.
//
// Since both cancellation ways - explicit and due to exceeding context
// deadline - result in the same signal to work being done under context,
// Canceled treats both context.Canceled and context.DeadlineExceeded as errors
// indicating context cancellation.
func Canceled(err error) bool {
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return true
	}

	return false
}


// WhenDone arranges for f to be called either when ctx is cancelled or
// surrounding function returns.
//
// To work as intended it should be called under defer like this:
//
//	func myfunc(ctx, ...) {
//		defer xcontext.WhenDone(ctx, func() { ... })()
func WhenDone(ctx context.Context, f func()) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// ok
		case <-done:
			// ok
		}

		f()
	}()

	return func() {
		close(done)
	}
}
