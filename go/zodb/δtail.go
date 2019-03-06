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
//	[](rev↑, []id)		; rev ∈ (tail, head]
//
// and index
//
//	{} id -> max(rev: rev changed id)
//
// where
//
//	rev          - is ZODB revision,
//	id           - is an identifier of what has been changed(*), and
//	(tail, head] - is covered revision range
//
// It provides operations to
//
//	- append information to the tail about next revision,
//	- forget information in the tail past specified revision,
//	- query the tail for slice with rev ∈ (lo, hi],
//	- query the tail about what is last revision that changed an id,
//	- query the tail for len and (tail, head].
//
// ΔTail is safe to access for multiple-readers / single writer.
//
// (*) examples of id:
//
//	oid  - ZODB object identifier, when ΔTail represents changes to ZODB objects,
//	#blk - file block number, when ΔTail represents changes to a file.
type ΔTail struct {
	head  Tid
	tail  Tid
	tailv []ΔRevEntry	// XXX -> revv ? δv? δvec? changev ?

	lastRevOf map[Oid]Tid // index for LastRevOf queries
	// XXX -> lastRevOf = {} oid -> []rev↑ if linear scan in LastRevOf starts to eat cpu
}

// ΔRevEntry represents information of what have been changed in one revision.
//
// XXX -> CommitEvent?
type ΔRevEntry struct {
	Rev     Tid
	Changev []Oid
}

// NewΔTail creates new ΔTail object.
//
// Initial coverage of created ΔTail is (at₀, at₀].
func NewΔTail(at0 Tid) *ΔTail {
	return &ΔTail{
		head:      at0,
		tail:      at0,
		lastRevOf: make(map[Oid]Tid),
	}
}

// Len returns number of revisions.
func (δtail *ΔTail) Len() int {
	return len(δtail.tailv)
}

// Head returns newest database state for which δtail has history coverage.
//
// Head is ↑ on Append, in particular it does not ↓ on Forget even if δtail becomes empty.
func (δtail *ΔTail) Head() Tid {
	return δtail.head
}

// Tail returns oldest database state for which δtail has history coverage.
//
// XXX not inclusive?
//
// Tail is ↑= on Forget, even if δtail becomes empty.
func (δtail *ΔTail) Tail() Tid {
	return δtail.tail
}

// SliceByRev returns δtail slice of elements with .rev ∈ (low, high].
//
// it must be called with the following condition:
//
//	tail ≤ low ≤ high ≤ head
//
// the caller must not modify returned slice.
//
// Note: contrary to regular go slicing, low is exclusive while high is inclusive.
func (δtail *ΔTail) SliceByRev(low, high Tid) /*readonly*/ []ΔRevEntry {
	tail := δtail.Tail()
	head := δtail.head
	if !(tail <= low && low <= high && high <= head) {
		panic(fmt.Sprintf("δtail.Slice: invalid query: (%s, %s];  (tail, head] = (%s, %s]", low, high, tail, head))
	}

	tailv := δtail.tailv

	// ex (0,0] tail..head = 0..0
	if len(tailv) == 0 {
		return tailv
	}

	// find max j : [j].rev ≤ high		XXX linear scan -> binary search
	j := len(tailv)-1
	for ; j >= 0 && tailv[j].Rev > high; j-- {}
	if j < 0 {
		return nil // ø
	}

	// find max i : [i].rev > low		XXX linear scan -> binary search
	i := j
	for ; i >= 0 && tailv[i].Rev > low; i-- {}
	i++

	return tailv[i:j+1]
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
	δtail.tailv = append(δtail.tailv, ΔRevEntry{rev, changev})
	for _, id := range changev {
		δtail.lastRevOf[id] = rev
	}
}

// ForgetPast discards all δtail entries with rev ≤ revCut.
func (δtail *ΔTail) ForgetPast(revCut Tid) {
	// revCut ≤ tail: nothing to do; don't let .tail go ↓
	if revCut <= δtail.tail {
		return
	}

	icut := 0
	for i, δ := range δtail.tailv {
		rev := δ.Rev
		if rev > revCut {
			break
		}
		icut = i+1

		// if forgotten revision was last for id, we have to update lastRevOf index
		for _, id := range δ.Changev {
			if δtail.lastRevOf[id] == rev {
				delete(δtail.lastRevOf, id)
			}
		}
	}

	// tailv = tailv[icut:]	  but without
	// 1) growing underlying storage array indefinitely
	// 2) keeping underlying storage after forget
	l := len(δtail.tailv)-icut
	tailv := make([]ΔRevEntry, l)
	copy(tailv, δtail.tailv[icut:])
	δtail.tailv = tailv

	δtail.tail = revCut
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
	revMin := δtail.tailv[0].Rev
	revMax := δtail.tailv[l-1].Rev
	if !(revMin <= at && at <= revMax) {
		return at, false
	}

	// we have the coverage
	rev, ok := δtail.lastRevOf[id]
	if !ok {
		return δtail.tailv[0].Rev, false
	}

	if rev <= at {
		return rev, true
	}

	// what's in index is after at - scan tailv back to find appropriate entry
	// XXX linear scan - see .lastRevOf comment.
	for i := l - 1; i >= 0; i-- {
		δ := δtail.tailv[i]
		if δ.Rev > at {
			continue
		}

		for _, δid := range δ.Changev {
			if id == δid {
				return δ.Rev, true
			}
		}
	}

	// nothing found
	return δtail.tailv[0].Rev, false
}
