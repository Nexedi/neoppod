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
package xcontext

import (
	"context"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	bg := context.Background()
	ctx1, cancel1 := context.WithCancel(bg)
	ctx2, cancel2 := context.WithCancel(bg)

	ctx1 = context.WithValue(ctx1, 1, "hello")
	ctx2 = context.WithValue(ctx2, 2, "world")

	mc, _ := Merge(ctx1, ctx2)

	assertEq := func(a, b interface{}) {
		t.Helper()
		if a != b {
			t.Fatalf("%v != %v", a, b)
		}
	}

	assertEq(mc.Value(1), "hello")
	assertEq(mc.Value(2), "world")
	assertEq(mc.Value(3), nil)

	t0 := time.Time{}

	d, ok := mc.Deadline()
	if !(d == t0 && ok == false) {
		t.Fatal("deadline must be unset")
	}

	assertEq(mc.Err(), nil)

	select {
	case <-mc.Done():
		t.Fatal("done before any parent done")
	default:
	}

	cancel2()
	<-mc.Done()
	assertEq(mc.Err(), context.Canceled)

	////////
	mc, _ = Merge(ctx1, bg)
	assertEq(mc.Value(1), "hello")
	assertEq(mc.Value(2), nil)
	assertEq(mc.Value(3), nil)

	d, ok = mc.Deadline()
	if !(d == t0 && ok == false) {
		t.Fatal("deadline must be unset")
	}

	assertEq(mc.Err(), nil)

	select {
	case <-mc.Done():
		t.Fatal("done before any parent done")
	default:
	}

	cancel1()
	<-mc.Done()
	assertEq(mc.Err(), context.Canceled)

	////////
	t1 := t0.AddDate(7777, 1, 1)
	t2 := t0.AddDate(9999, 1, 1)
	ctx1, _ = context.WithDeadline(bg, t1)
	ctx2, _ = context.WithDeadline(bg, t2)

	checkDeadline := func(a, b context.Context, tt time.Time) {
		t.Helper()
		m, _ := Merge(a, b)
		d, ok := m.Deadline()
		if !ok {
			t.Fatal("no deadline returned")
		}
		if d != tt {
			t.Fatalf("incorrect deadline: %v  ; want %v", d, tt)
		}
	}

	checkDeadline(ctx1, bg, t1)
	checkDeadline(bg, ctx2, t2)
	checkDeadline(ctx1, ctx2, t1)
	checkDeadline(ctx2, ctx1, t1)

	////////
	mc, mcancel := Merge(bg, bg)

	select {
	case <-mc.Done():
		t.Fatal("done before any parent done")
	default:
	}

	mcancel()
	mcancel()
	<-mc.Done()
	assertEq(mc.Err(), context.Canceled)
}
