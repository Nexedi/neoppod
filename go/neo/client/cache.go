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

package client
// cache management

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"

//	"github.com/kylelemons/godebug/pretty"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// storLoader represents loading part of a storage
// XXX -> zodb?
type storLoader interface {
	Load(xid zodb.Xid) (data []byte, serial zodb.Tid, err error)
}

// Cache adds RAM caching layer over a storage
type Cache struct {
	loader storLoader

	mu sync.RWMutex

	// cache is fully synchronized with storage for transactions with tid < before.
	// XXX clarify ^^^ (it means if revCacheEntry.before=∞ it is Cache.before)
	before	zodb.Tid

	entryMap map[zodb.Oid]*oidCacheEntry	// oid -> oid's cache entries

	// garbage collection:
	gcMu sync.Mutex
	lru  listHead	// revCacheEntries in LRU order
	size int	// cached data size in bytes

	sizeMax int	// cache is allowed to occupy not more than this
}

// oidCacheEntry maintains cached revisions for 1 oid
type oidCacheEntry struct {
	sync.Mutex

	// cached revisions in ascending order
	// [i].serial < [i].before <= [i+1].serial < [i+1].before
	//
	// NOTE ^^^ .serial = 0 while loading is in progress
	// NOTE ^^^ .serial = 0 if .err != nil
	//
	// XXX or?
	// cached revisions in descending order
	// .before > .serial >= next.before > next.serial ?
	rcev []*revCacheEntry
}

// revCacheEntry is information about 1 cached oid revision
type revCacheEntry struct {
	inLRU  listHead		// in Cache.lru
	parent *oidCacheEntry	// oidCacheEntry holding us

	// we know that loadBefore(oid, .before) will give this .serial:oid.
	//
	// this is only what we currently know - not necessarily covering
	// whole correct range - e.g. if oid revisions in db are 1 and 5 if we
	// query db with loadBefore(3) on return we'll get serial=1 and
	// remember .before as 3. But for loadBefore(4) we have to redo
	// database query again.
	//
	// if .before=∞ here, that actually means before is cache.before
	// ( this way we do not need to bump .before to next tid in many
	//   unchanged cache entries when a transaction invalidation comes )
	//
	// .before can be > cache.before and still finite - that represents a
	// case when loadBefore with tid > cache.before was called.
	before zodb.Tid

	// loading result: object (data, serial) or error
	data   []byte
	serial zodb.Tid
	err    error

	ready chan struct{} // closed when loading finished
}

// lock order: Cache.mu   > oidCacheEntry > (?) revCacheEntry
//             Cache.gcMu > ?

// XXX maintain nhit / nmiss?


// NewCache creates new cache backed up by loader
// XXX +sizeMax
func NewCache(loader storLoader) *Cache {
	c := &Cache{
		loader: loader,
		entryMap: make(map[zodb.Oid]*oidCacheEntry),
	}
	c.lru.Init()
	return c
}

// Load loads data from database via cache.
//
// If data is already in cache cached content is returned.
func (c *Cache) Load(xid zodb.Xid) (data []byte, serial zodb.Tid, err error) {
	rce, rceNew := c.lookupRCE(xid)

	// rce is already in cache - use it
	if !rceNew {
		<-rce.ready
		c.gcMu.Lock()
		rce.inLRU.MoveBefore(&c.lru)
		c.gcMu.Unlock()

	// rce is not in cache - this goroutine becomes responsible for loading it
	} else {
		// XXX use connection poll
		// XXX or it should be cared by loader?
		c.loadRCE(rce, xid.Oid)
	}

	if rce.err != nil {
		return nil, 0, rce.userErr(xid)
	}

	// for loadSerial - check we have exact hit - else "nodata"
	if !xid.TidBefore {
		if rce.serial != xid.Tid {
			return nil, 0, &zodb.ErrXidMissing{xid}
		}
	}

	return rce.data, rce.serial, nil
}

// Prefetch arranges for data to be eventually present in cache.
//
// If data is not yet in cache loading for it is started in the background.
// Prefetch is not blocking operation and does not wait for loading, if any was
// started, to complete.
func (c *Cache) Prefetch(xid zodb.Xid) {
	rce, rceNew := c.lookupRCE(xid)

	// !rceNew -> no need to adjust LRU - it will be adjusted by further actual data Load
	// XXX or it is better to adjust LRU here too?

	// spawn loading in the background if rce was not yet loaded
	if rceNew {
		// XXX use connection poll
		go c.loadRCE(rce, xid.Oid)
	}

}


// lookupRCE returns revCacheEntry corresponding to xid.
//
// If xid indicates loadSerial query (xid.TidBefore=false) then rce will be
// lookuped and eventually loaded as if it was queried with <(serial+1).
// It is caller responsibility to check loadSerial cases for exact hits after
// rce will become ready.
//
// rceNew indicates whether rce is new and so loading on it has not been
// initiated yet. If so rce should be loaded with loadRCE.
func (c *Cache) lookupRCE(xid zodb.Xid) (rce *revCacheEntry, rceNew bool) {
	// loadSerial(serial) -> loadBefore(serial+1)
	before := xid.Tid
	if !xid.TidBefore {
		before++ // XXX overflow
	}

	// oid -> oce (oidCacheEntry)  ; create new empty oce if not yet there
	// exit with oce locked and cache.before read consistently
	c.mu.RLock()

	oce := c.entryMap[xid.Oid]
	cacheBefore := c.before

	if oce != nil {
		oce.Lock()
		c.mu.RUnlock()
	} else {
		// relock cache in write mode to create oce
		c.mu.RUnlock()
		c.mu.Lock()
		oce = c.entryMap[xid.Oid]
		if oce == nil {
			oce = &oidCacheEntry{}
			c.entryMap[xid.Oid] = oce
		}
		cacheBefore = c.before // reload c.before because we relocked the cache
		oce.Lock()
		c.mu.Unlock()
	}

	// oce, before -> rce (revCacheEntry)
	l := len(oce.rcev)
	i := sort.Search(l, func(i int) bool {
		before_i := oce.rcev[i].before
		if before_i == zodb.TidMax {
			before_i = cacheBefore
		}
		return before <= before_i
	})

	switch {
	// not found - before > max(rcev.before) - insert new max entry
	case i == l:
		rce = oce.newRevEntry(i, before)
		if rce.before == cacheBefore {
			// FIXME better do this when the entry becomes loaded ?
			// XXX vs concurrent invalidations?
			rce.before = zodb.TidMax
		}
		rceNew = true

	// found:
	// before <= rcev[i].before
	// before >  rcev[i-1].before

	// exact match - we already have entry for this before
	case before == oce.rcev[i].before:
		rce = oce.rcev[i]

	// non-exact match:
	// - same entry if q(before) ∈ (serial, before]
	// - we can also reuse this entry if q(before) < before and err="nodata"
	case oce.rcev[i].loaded() && (
		(oce.rcev[i].err == nil && oce.rcev[i].serial < before) ||
		(isErrNoData(oce.rcev[i].err) && before < oce.rcev[i].before)):
		rce = oce.rcev[i]

	// otherwise - insert new entry
	default:
		rce = oce.newRevEntry(i, before)
		rceNew = true
	}

	oce.Unlock()
	return rce, rceNew
}

// loadRCE performs data loading from database into rce.
//
// rce must be new just created by lookupRCE() with returned rceNew=true.
// loading completion is signalled by closing rce.ready.
func (c *Cache) loadRCE(rce *revCacheEntry, oid zodb.Oid) {
	oce := rce.parent
	data, serial, err := c.loader.Load(zodb.Xid{
		Oid:  oid,
		XTid: zodb.XTid{Tid: rce.before, TidBefore: true},
	})

	// normalize data/serial if it was error
	if err != nil {
		data = nil
		serial = 0
	}
	rce.serial = serial
	rce.data = data
	rce.err = err
	// verify db gives serial < before
	if rce.serial >= rce.before {
		rce.errDB(oid, "load(<%v) -> %v", rce.before, serial)
	}

	close(rce.ready)
	δsize := len(rce.data)

	// merge rce with adjacent entries in parent
	// ( e.g. loadBefore(3) and loadBefore(4) results in the same data loaded if
	//   there are only revisions with serials 1 and 5 )
	oce.Lock()
	i := oce.find(rce)
	if i == -1 {
		// rce was already dropped by merge / evicted
		// (XXX recheck about evicted)
		oce.Unlock()
		return
	}

	// if rce & rceNext cover the same range -> drop rce
	//
	// if we drop rce - do not update c.lru as:
	// 1. new rce is not on lru list,
	// 2. rceNext (which becomes rce) might not be there on lru list.
	//
	// if rceNext is not yet there on lru list its loadRCE is in progress
	// and will update lru and cache size for it itself.
	rceDropped := false
	if i + 1 < len(oce.rcev) {
		rceNext := oce.rcev[i+1]
		if rceNext.loaded() && tryMerge(rce, rceNext, rce, oid) {
			// not δsize -= len(rce.data)
			// tryMerge can change rce.data if consistency is broken
			δsize = 0
			rce = rceNext
			rceDropped = true
		}
	}

	// if rcePrev & rce cover the same range -> drop rcePrev
	// (if we drop rcePrev we'll later remove it from c.lru when under c.gcMu)
	var rcePrevDropped *revCacheEntry
	if i > 0 {
		rcePrev := oce.rcev[i-1]
		if rcePrev.loaded() && tryMerge(rcePrev, rce, rce, oid) {
			rcePrevDropped = rcePrev
			// XXX not always right - e.g. if rcePrev did not yet took oce lock
			// XXX ^^^ test for this
			δsize -= len(rcePrev.data)
		}
	}

	oce.Unlock()

	// update lru & cache size
	c.gcMu.Lock()
	//xv1 := map[string]interface{}{"lru": &c.lru, "rce": &rce.inLRU}
	//fmt.Printf("aaa:\n%s\n", pretty.Sprint(xv1))
	if rcePrevDropped != nil {
		rcePrevDropped.inLRU.Delete()
	}
	if !rceDropped {
		rce.inLRU.MoveBefore(&c.lru)
	}
	//xv2 := map[string]interface{}{"lru": &c.lru, "rce": &rce.inLRU}
	//fmt.Printf("\n--------\n%s\n\n\n", pretty.Sprint(xv2))

	c.size += δsize
	if c.size > c.sizeMax {
		// XXX -> run gc
	}
	c.gcMu.Unlock()
}

// tryMerge tries to merge rce prev into next
//
// both prev and next must be already loaded.
// prev and next must come adjacent to each other in parent.rcev with
// prev.before < next.before .
//
// cur must be one of either prev or next and indicates which rce is current
// and so may be adjusted with consistency check error.
//
// return: true if merging done and thus prev was dropped from parent
//
// must be called with .parent locked
//
// XXX move oid from args to revCacheEntry?
func tryMerge(prev, next, cur *revCacheEntry, oid zodb.Oid) bool {

	//		  can merge if    consistent if
	//	                          (if merging)
	//
	//	Pok  Nok    Ns < Pb         Ps  = Ns
	//	Pe   Nok    Ns < Pb         Pe != "nodata"	(e.g. it was IO loading error for P)
	//	Pok  Ne       ---
	//	Ne   Pe     (Pe="nodata") && (Ne="nodata")	-> XXX vs deleteObject?
	//							-> let deleted object actually read
	//							-> as special non-error value
	//
	// b - before
	// s - serial
	// e - error

	if next.err == nil && next.serial < prev.before {
		// drop prev
		prev.parent.del(prev)

		// check consistency
		switch {
		case prev.err == nil && prev.serial != next.serial:
			cur.errDB(oid, "load(<%v) -> %v; load(<%v) -> %v",
				prev.before, prev.serial, next.before, next.serial)

		case prev.err != nil && !isErrNoData(prev.err):
			if cur.err == nil {
				cur.errDB(oid, "load(<%v) -> %v; load(<%v) -> %v",
					prev.before, prev.err, next.before, next.serial)
			}
		}

		return true
	}

	if isErrNoData(prev.err) && isErrNoData(next.err) {
		// drop prev
		prev.parent.del(prev)

		// not checking consistency - error is already there and
		// (Pe="nodata") && (Ne="nodata") already indicates prev & next are consistent.

		return true
	}

	return false
}

// ----------------------------------------

/*
func (c *cache) gc(...) {
	c.lruMu.Lock()
	revh := c.lru.next
	c.lruMu.Unlock()

	for ; revh != &lru; revh = revh.next {	// .next after .delete - ok ?
		rce := revh.rceFromInLRU()
		oce := rce.parent

		oce.Lock()
		oce.del(rce)
		oce.Unlock()

	}
}

// cleaner is the process that cleans cache by evicting less-needed entries.
func (c *cache) cleaner() {
	for {
		// cleaner is the only mutator/user of Cache.lru and revCacheEntry.inLRU
		select {
		case rce := <-c.used:
			rce.inLRU.MoveBefore(&c.lru)

		default:
			for rce := c.lru.next; rce != &c.lru; rce = rce.next {
			}
		}
	}
}
*/

// isErrNoData returns whether an error is due to "there is no such data in
// database", not e.g. some IO loading error
func isErrNoData(err error) bool {
	switch err.(type) {
	default:
		return false

	case *zodb.ErrOidMissing:
	case *zodb.ErrXidMissing:
	}
	return true
}

// newRevEntry creates new revCacheEntry with .before and inserts it into .rcev @i.
// (if i == len(oce.rcev) - entry is appended)
func (oce *oidCacheEntry) newRevEntry(i int, before zodb.Tid) *revCacheEntry {
	rce := &revCacheEntry{
		parent: oce,
		serial: 0,
		before: before,
		ready:  make(chan struct{}),
	}
	rce.inLRU.Init() // initially not on Cache.lru list

	oce.rcev = append(oce.rcev, nil)
	copy(oce.rcev[i+1:], oce.rcev[i:])
	oce.rcev[i] = rce

	return rce
}

// find finds rce in .rcev and returns its index
// not found -> -1.
func (oce *oidCacheEntry) find(rce *revCacheEntry) int {
	for i, r := range oce.rcev {
		if r == rce {
			return i
		}
	}
	return -1
}

// deli deletes .rcev[i]
func (oce *oidCacheEntry) deli(i int) {
	n := len(oce.rcev) - 1
	copy(oce.rcev[i:], oce.rcev[i+1:])
	// release ptr to revCacheEntry so it won't confusingly stay live when
	// its turn to be deleted come.
	oce.rcev[n] = nil
	oce.rcev = oce.rcev[:n]
}

// del delets rce from .rcev.
// it panics if rce is not there.
func (oce *oidCacheEntry) del(rce *revCacheEntry) {
	i := oce.find(rce)
	if i == -1 {
		panic("rce not found")
	}

	oce.deli(i)
}


// loaded reports whether rce was already loaded
func (rce *revCacheEntry) loaded() bool {
	select {
	case <-rce.ready:
		return true
	default:
		return false
	}
}

// userErr returns error that, if any, needs to be returned to user from Cache.Load
//
// ( ErrXidMissing contains xid for which it is missing. In cache we keep such
//   xid with max .before but users need to get ErrXidMissing with their own query )
func (rce *revCacheEntry) userErr(xid zodb.Xid) error {
	switch e := rce.err.(type) {
	case *zodb.ErrXidMissing:
		if e.Xid != xid {
			return &zodb.ErrXidMissing{xid}
		}
	}

	return rce.err
}

// revCacheEntry: .inLRU -> .
func (h *listHead) rceFromInLRU() (rce *revCacheEntry) {
	urce := unsafe.Pointer(uintptr(unsafe.Pointer(h)) - unsafe.Offsetof(rce.inLRU))
	return (*revCacheEntry)(urce)
}

// errDB returns error about database being inconsistent
func errDB(oid zodb.Oid, format string, argv ...interface{}) error {
	// XXX -> separate type?
	return fmt.Errorf("cache: database inconsistency: oid: %v: " + format,
		append([]interface{}{oid}, argv...)...)
}

// errDB marks rce with database inconsistency error
func (rce *revCacheEntry) errDB(oid zodb.Oid, format string, argv ...interface{}) {
	rce.err = errDB(oid, format, argv...)
	rce.data = nil
	rce.serial = 0
}
