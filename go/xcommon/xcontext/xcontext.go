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

// Package xcontext provides addons to std package context.
package xcontext

import (
	"context"
	"sync"
	"time"
)

// Merge merges 2 contexts into 1.
//
// The result context:
// - is done when ctx1 or ctx2 is done, or cancel called, whichever happens first,
// - has deadline = min(ctx1.Deadline, ctx2.Deadline),
// - has associated values merged from ctx1 and ctx2.
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
	go mc.wait()
	return mc, mc.cancel
}

type mergeCtx struct {
	ctx1, ctx2 context.Context

	done    chan struct{}
	doneErr error

	cancelCh   chan struct{}
	cancelOnce sync.Once
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

// cancel sends signal to wait to shutdown
// cancel is the context.CancelFunc returned for mergeCtx by Merge
func (mc *mergeCtx) cancel() {
	mc.cancelOnce.Do(func() {
		close(mc.cancelCh)
	})
}

func (mc *mergeCtx) Done() <-chan struct{} {
	return mc.done
}

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

func (mc *mergeCtx) Value(key interface{}) interface{} {
	v := mc.ctx1.Value(key)
	if v != nil {
		return v
	}
	return mc.ctx2.Value(key)
}