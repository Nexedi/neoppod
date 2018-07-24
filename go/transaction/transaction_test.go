// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE.

package transaction

import (
	"context"
	"testing"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()

	// Current(ø) -> panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Current(ø) -> not paniced")
			}

			if want := "transaction: no current transaction"; r != want {
				t.Fatalf("Current(ø) -> %q;  want %q", r, want)
			}
		}()

		Current(ctx)
	}()


	txn, ctx := New(ctx)
	if txn_ := Current(ctx); txn_ != txn {
		t.Fatalf("New inconsistent with Current: txn = %#v;  txn_ = %#v", txn, txn_)
	}

	// subtransactions not allowed
	func () {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("New(!ø) -> not paniced")
			}

			if want := "transaction: new: nested transactions not supported"; r != want {
				t.Fatalf("New(!ø) -> %q;  want %q", r, want)
			}
		}()

		_, _ = New(ctx)
	}()
}
