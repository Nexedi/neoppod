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

// XXX do we really need ΔTail to be exported from zodb?
// (other users are low level caches + maybe ZEO/NEO -> zplumbing? but then import cycle)

import (
	"fmt"
)

// ΔTail represents tail of revisional changes.
//
// It semantically consists of
//
//	[](rev↑, []id)		; rev ∈ [tail, head]
//
// and index
//
//	{} id -> max(rev: rev changed id)
//
// where
//
//	rev          - is ZODB revision, and
//	id           - is an identifier of what has been changed(*)
//	[tail, head] - is covered revision range
//
// It provides operations to
//
//	- append information to the tail about next revision,
//	- forget information in the tail past specified revision, and
//	- query the tail for len, head and tail.
//	- query the tail for slice with rev ∈ (lo, hi].
//	- query the tail about what is last revision that changed an id.
//
// ΔTail is safe to access for multiple-readers / single writer.
//
// (*) examples of id:
//
//	oid  - ZODB object identifier, when ΔTail represents changes to ZODB objects,
//	#blk - file block number, when ΔTail represents changes to a file.
type ΔTail struct {
	head  Tid
	tailv []δRevEntry

	lastRevOf map[Oid]Tid // index for LastRevOf queries
	// TODO also add either tailv idx <-> rev index, or lastRevOf -> tailv idx
	// (if linear back-scan of δRevEntry starts to eat cpu).
}

// δRevEntry represents information of what have been changed in one revision.
//
// XXX -> CommitEvent?
type δRevEntry struct {
	rev     Tid
	changev []Oid
}

// NewΔTail creates new ΔTail object.
func NewΔTail() *ΔTail {
	return &ΔTail{lastRevOf: make(map[Oid]Tid)}
}

// Len returns number of elements.
func (δtail *ΔTail) Len() int {
	return len(δtail.tailv)
}

// Head returns newest database state for which δtail has history coverage.
//
// For newly created ΔTail Head returns 0.
// Head is ↑, in particular it does not go back to 0 when δtail becomes empty.
func (δtail *ΔTail) Head() Tid {
	return δtail.head
}

// Tail returns oldest database state for which δtail has history coverage.
//
// For newly created ΔTail Tail returns 0.
// Tail is ↑, in particular it does not go back to 0 when δtail becomes empty.
func (δtail *ΔTail) Tail() Tid {
	if len(δtail.tailv) > 0 {
		return δtail.tailv[0].rev
	}
	return δtail.head
}

// SliceByRev returns δtail slice with .rev ∈ (low, high].
//
// it must be called with the following condition:
//
//	tail ≤ low ≤ high ≤ head
//
// the caller must not modify returned slice.
//
// Note: contrary to regular go slicing, low is exclisive while high is inclusive.
func (δtail *ΔTail) SliceByRev(low, high Tid) /*readonly*/ []δRevEntry {
	tail := δtail.Tail()
	head := δtail.head
	if !(tail <= low && low <= high && high <= head) {
		panic(fmt.Sprintf("δtail.Slice: (%s, %s] invalid; tail..head = %s..%s", low, high, tail, head))
	}

	tailv := δtail.tailv

	// ex (0,0] tail..head = 0..0
	if len(tailv) == 0 {
		return tailv
	}

	// find max j : [j].rev <= high		XXX linear scan
	j := len(tailv)-1
	for ; tailv[j].rev > high; j-- {}

	// find max i : [i].rev > low		XXX linear scan
	i := j
	for ; i >= 0 && tailv[i].rev > low; i-- {}
	i++

	return tailv[i:j]
}

// XXX add way to extend coverage without appending changed data? (i.e. if a
// txn did not change file at all) -> but then it is simply .Append(rev, nil)?

// Append appends to δtail information about what have been changed in next revision.
//
// rev must be ↑.
func (δtail *ΔTail) Append(rev Tid, changev []Oid) {
	// check rev↑
	if δtail.head >= rev {
		panic(fmt.Sprintf("δtail.Append: rev not ↑: %s -> %s", δtail.head, rev))
	}

	δtail.head = rev
	δtail.tailv = append(δtail.tailv, δRevEntry{rev, changev})
	for _, id := range changev {
		δtail.lastRevOf[id] = rev
	}
}

// ForgetBefore discards all δtail entries with rev < revCut.
func (δtail *ΔTail) ForgetBefore(revCut Tid) {
	icut := 0
	for i, δ := range δtail.tailv {
		rev := δ.rev
		if rev >= revCut {
			break
		}
		icut = i+1

		// if forgotten revision was last for id, we have to update lastRevOf index
		for _, id := range δ.changev {
			if δtail.lastRevOf[id] == rev {
				delete(δtail.lastRevOf, id)
			}
		}
	}

	// tailv = tailv[icut:]	  but without
	// 1) growing underlying storage array indefinitely
	// 2) keeping underlying storage after forget
	l := len(δtail.tailv)-icut
	tailv := make([]δRevEntry, l)
	copy(tailv, δtail.tailv[icut:])
	δtail.tailv = tailv
}

// LastRevOf tries to return what was the last revision that changed id as of at database state.
//
// Depending on current information in δtail it returns either exact result, or
// an upper-bound estimate for the last id revision, assuming id was changed ≤ at:
//
// 1) if δtail does not cover at, at is returned:
//
//	# at ∉ [min(rev ∈ δtail), max(rev ∈ δtail)]
//	LastRevOf(id, at) = at
//
// 2) if δtail has an entry corresponding to id change, it gives exactly the
// last revision that changed id:
//
//	# at ∈ [min(rev ∈ δtail), max(rev ∈ δtail)]
//	# ∃ rev ∈ δtail: rev changed id && rev ≤ at
//	LastRevOf(id, at) = max(rev: rev changed id && rev ≤ at)
//
// 3) if δtail does not contain appropriate record with id - it returns δtail's
// lower bound as the estimate for the upper bound of the last id revision:
//
//	# at ∈ [min(rev ∈ δtail), max(rev ∈ δtail)]
//	# ∄ rev ∈ δtail: rev changed id && rev ≤ at
//	LastRevOf(id, at) = min(rev ∈ δtail)
//
// On return exact indicates whether returned revision is exactly the last
// revision of id, or only an upper-bound estimate of it.
func (δtail *ΔTail) LastRevOf(id Oid, at Tid) (_ Tid, exact bool) {
	// check if we have no coverage at all
	l := len(δtail.tailv)
	if l == 0 {
		return at, false
	}
	revMin := δtail.tailv[0].rev
	revMax := δtail.tailv[l-1].rev
	if !(revMin <= at && at <= revMax) {
		return at, false
	}

	// we have the coverage
	rev, ok := δtail.lastRevOf[id]
	if !ok {
		return δtail.tailv[0].rev, false
	}

	if rev <= at {
		return rev, true
	}

	// what's in index is after at - scan tailv back to find appropriate entry
	// XXX linear scan
	for i := l - 1; i >= 0; i-- {
		δ := δtail.tailv[i]
		if δ.rev > at {
			continue
		}

		for _, δid := range δ.changev {
			if id == δid {
				return δ.rev, true
			}
		}
	}

	// nothing found
	return δtail.tailv[0].rev, false
}
