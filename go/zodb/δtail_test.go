// Copyright (C) 2018-2019  Nexedi SA and Contributors.
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

package zodb

import (
	"fmt"
	"reflect"
	"testing"
)

func TestΔTail(t *testing.T) {
	δtail := NewΔTail()

	// R is syntactic sugar to create 1 δRevEntry
	R := func(rev Tid, changev ...Oid) δRevEntry {
		return δRevEntry{rev, changev}
	}

	// δAppend is syntactic sugar for δtail.Append
	δAppend := func(δ δRevEntry) {
		δtail.Append(δ.rev, δ.changev)
	}

	// δCheck verifies that δtail state corresponds to provided tailv
	δCheck := func(head Tid, tailv ...δRevEntry) {
		t.Helper()

		for i := 1; i < len(tailv); i++ {
			if !(tailv[i-1].rev < tailv[i].rev) {
				panic("test tailv: rev not ↑")
			}
		}

		// Len/Head/Tail
		if l := δtail.Len(); l != len(tailv) {
			t.Fatalf("Len() -> %d  ; want %d", l, len(tailv))
		}

		if h := δtail.Head(); h != head {
			t.Fatalf("Head() -> %s  ; want %s", h, head)
		}

		tail := head
		if len(tailv) > 0 {
			tail = tailv[0].rev-1
		}
		if tt := δtail.Tail(); tt != tail {
			t.Fatalf("Tail() -> %s  ; want %s", tt, tail)
		}

		if !tailvEqual(δtail.tailv, tailv) {
			t.Fatalf("tailv:\nhave: %v\nwant: %v", δtail.tailv, tailv)
		}

		// SliceByRev

		// check that δtail.SliceByRev(rlo, rhi) == tailv[ilo:ihi).
		//fmt.Printf("\nwhole: (%s, %s]  %v\n", δtail.Tail(), δtail.Head(), tailv)
		sliceByRev := func(rlo, rhi Tid, ilo, ihi int) {
			t.Helper()
			//fmt.Printf("(%s, %s] -> [%d:%d)\n", rlo, rhi, ilo, ihi)
			have := δtail.SliceByRev(rlo, rhi)
			want := tailv[ilo:ihi]
			if !tailvEqual(have, want) {
				t.Fatalf("SliceByRev(%s, %s) -> %v  ; want %v", rlo, rhi, have, want)
			}

			if len(have) == 0 {
				return
			}

			// make sure returned region is indeed correct
			tbefore := Tid(0)
			if ilo-1 >= 0 {
				tbefore = tailv[ilo-1].rev-1
			}
			tail := tailv[ilo].rev-1
			head := tailv[ihi-1].rev
			hafter := TidMax
			if ihi < len(tailv) {
				hafter = tailv[ihi].rev
			}

			if !(tbefore < rlo && rlo <= tail && head <= rhi && rhi < hafter) {
				t.Fatalf("SliceByRev(%s, %s) -> %v  ; edges do not match query:\n" +
					"%s (%s, %s] %s", rlo, rhi, have, tbefore, tail, head, hafter)
			}
		}

		for ilo := 0; ilo < len(tailv); ilo++ {
			for ihi := ilo; ihi < len(tailv); ihi++ {
				// [ilo, ihi)
				sliceByRev(
					tailv[ilo].rev - 1,
					tailv[ihi].rev - 1,
					ilo, ihi,
				)

				// [ilo, ihi]
				sliceByRev(
					tailv[ilo].rev - 1,
					tailv[ihi].rev,
					ilo, ihi+1,
				)

				// (ilo, ihi]
				if ilo+1 < len(tailv) {
					sliceByRev(
						tailv[ilo].rev,
						tailv[ihi].rev,
						ilo+1, ihi+1,
					)
				}

				// (ilo, ihi)
				if ilo+1 < len(tailv) && ilo+1 <= ihi {
					sliceByRev(
						tailv[ilo].rev,
						tailv[ihi].rev - 1,
						ilo+1, ihi,
					)
				}
			}
		}

		// verify lastRevOf query / index
		lastRevOf := make(map[Oid]Tid)
		for _, δ := range tailv {
			for _, id := range δ.changev {
				idRev, exact := δtail.LastRevOf(id, δ.rev)
				if !(idRev == δ.rev && exact) {
					t.Fatalf("LastRevOf(%v, at=%s) -> %s, %v  ; want %s, %v", id, δ.rev, idRev, exact, δ.rev, true)
				}

				lastRevOf[id] = δ.rev
			}
		}

		if !reflect.DeepEqual(δtail.lastRevOf, lastRevOf) {
			t.Fatalf("lastRevOf:\nhave: %v\nwant: %v", δtail.lastRevOf, lastRevOf)
		}

	}

	// δCheckLastUP verifies that δtail.LastRevOf(id, at) gives lastOk and exact=false.
	// (we don't need to check for exact=true as those cases are covered in δCheck)
	δCheckLastUP := func(id Oid, at, lastOk Tid) {
		t.Helper()

		last, exact := δtail.LastRevOf(id, at)
		if !(last == lastOk && exact == false) {
			t.Fatalf("LastRevOf(%v, at=%s) -> %s, %v  ; want %s, %v", id, at, last, exact, lastOk, false)
		}
	}


	δCheck(0)
	δCheckLastUP(4, 12, 12)	// δtail = ø

	δAppend(R(10, 3,5))
	δCheck(10, R(10, 3,5))

	δCheckLastUP(3,  9,  9)	// at < δtail
	δCheckLastUP(3, 12, 12)	// at > δtail
	δCheckLastUP(4, 10, 10)	// id ∉ δtail

	δAppend(R(11, 7))
	δCheck(11, R(10, 3,5), R(11, 7))

	δAppend(R(12, 7))
	δCheck(12, R(10, 3,5), R(11, 7), R(12, 7))

	δAppend(R(14, 3,8))
	δCheck(14, R(10, 3,5), R(11, 7), R(12, 7), R(14, 3,8))

	δCheckLastUP(8, 12, 10) // id ∈ δtail, but has no entry with rev ≤ at

	δtail.ForgetBefore(10)
	δCheck(14, R(10, 3,5), R(11, 7), R(12, 7), R(14, 3,8))

	δtail.ForgetBefore(11)
	δCheck(14, R(11, 7), R(12, 7), R(14, 3,8))

	δtail.ForgetBefore(13)
	δCheck(14, R(14, 3,8))

	δtail.ForgetBefore(15)
	δCheck(14)

	// Append panics on non-↑ rev

	// δAppendPanic verifies that Append(δ.rev = rev) panics.
	δAppendPanic := func(rev Tid) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("append(rev=%s) non-↑: not panicked", rev)
			}
			want := fmt.Sprintf("δtail.Append: rev not ↑: %s -> %s", δtail.head, rev)
			if r != want {
				t.Fatalf("append non-↑:\nhave: %q\nwant: %q", r, want)
			}
		}()

		δAppend(R(rev))
	}

	// on empty δtail
	δAppendPanic(14)
	δAppendPanic(13)
	δAppendPanic(12)

	// on !empty δtail
	δAppend(R(15, 1))
	δCheck(15, R(15, 1))
	δAppendPanic(15)
	δAppendPanic(14)


	// .tailv underlying storage is not kept after forget
	δtail.ForgetBefore(16)
	δCheck(15)

	const N = 1E3
	for rev, i := Tid(16), 0; i < N; i, rev = i+1, rev+1 {
		δAppend(R(rev, 1))
	}

	capN := cap(δtail.tailv)
	δtail.ForgetBefore(N)
	if c := cap(δtail.tailv); !(c < capN/10) {
		t.Fatalf("forget: tailv storage did not shrink: cap%v: %d -> cap: %d", N, capN, c)
	}

	// .tailv underlying storage does not grow indefinitely
	// XXX cannot test as the growth here goes to left and we cannot get
	// access to whole underlying array from a slice.
}

func tailvEqual(a, b []δRevEntry) bool {
	// for empty one can be nil and another !nil [] = reflect.DeepEqual
	// does not think those are equal.
	return (len(a) == 0 && len(b) == 0) ||
		reflect.DeepEqual(a, b)
}
