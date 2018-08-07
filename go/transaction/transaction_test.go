// Copyright (C) 2018  Nexedi SA and Contributors.
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

package transaction

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()

	// Current(ø) -> panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Current(ø) -> no panic")
			}

			if want := "transaction: no current transaction"; r != want {
				t.Fatalf("Current(ø) -> %q;  want %q", r, want)
			}
		}()

		Current(ctx)
	}()

	// New
	txn, ctx := New(ctx)
	if txn_ := Current(ctx); txn_ != txn {
		t.Fatalf("New inconsistent with Current: txn = %#v;  txn_ = %#v", txn, txn_)
	}

	// New(!ø) -> panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("New(!ø) -> no panic")
			}

			if want := "transaction: new: nested transactions not supported"; r != want {
				t.Fatalf("New(!ø) -> %q;  want %q", r, want)
			}
		}()

		_, _ = New(ctx)
	}()
}

// DataManager that verifies abort path.
type dmAbortOnly struct {
	t      *testing.T
	txn    Transaction
	nabort int32
}

func (d *dmAbortOnly) Modify() {
	d.txn.Join(d)
}

func (d *dmAbortOnly) Abort(txn Transaction) {
	if txn != d.txn {
		d.t.Fatalf("abort: txn is different")
	}
	atomic.AddInt32(&d.nabort, +1)
}

func (d *dmAbortOnly) bug()                                             { d.t.Fatal("must not be called on abort") }
func (d *dmAbortOnly) TPCBegin(_ Transaction)                           { d.bug(); panic(0) }
func (d *dmAbortOnly) Commit(_ context.Context, _ Transaction) error    { d.bug(); panic(0) }
func (d *dmAbortOnly) TPCVote(_ context.Context, _ Transaction) error   { d.bug(); panic(0) }
func (d *dmAbortOnly) TPCFinish(_ context.Context, _ Transaction) error { d.bug(); panic(0) }
func (d *dmAbortOnly) TPCAbort(_ context.Context, _ Transaction)        { d.bug(); panic(0) }

func TestAbort(t *testing.T) {
	txn, ctx := New(context.Background())
	dm := &dmAbortOnly{t: t, txn: Current(ctx)}
	dm.Modify()

	// XXX +sync

	txn.Abort()
	if !(dm.nabort == 1 && txn.Status() == Aborted) {
		t.Fatalf("abort: nabort=%d; txn.Status=%v", dm.nabort, txn.Status())
	}

	// Abort 2nd time -> panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Abort2 -> no panic")
			}
			if want := "transaction: abort: transaction completion already began"; r != want {
				t.Fatalf("Abort2 -> %q;  want %q", r, want)
			}
		}()

		txn.Abort()
	}()
}
