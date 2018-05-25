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
// XXX docs:
//	- Canceled
//	- WhenDone
package xcontext

import (
	"context"
)

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
