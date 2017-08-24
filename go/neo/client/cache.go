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

	entryMap map[zodb.Oid]*oidCacheEntry	// oid -> cache entries for this oid

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
	// XXX ^^^ .serial = 0 while loading is in progress
	// XXX ^^^ .serial = 0 if err != nil
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
	// this is only what we currently know - not neccessarily covering
	// whole correct range - e.g. if oid revisions in db are 1 and 5 if we
	// query db with loadBefore(3) on return we'll get serial=1 and
	// remember .before as 3. But for loadBefore(4) we have to redo
	// database query again.
	//
	// if .before=∞ here, that actually means before is cache.before
	// ( this way we do not need to bump before to next tid in many
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

func NewCache(loader storLoader) *Cache {
	return &Cache{loader: loader, entryMap: make(map[zodb.Oid]*oidCacheEntry)}
}

// newReveEntry creates new revCacheEntry with .before and inserts it into .rcev @i
// (if i == len(oce.rcev) - entry is appended)
func (oce *oidCacheEntry) newRevEntry(i int, before zodb.Tid) *revCacheEntry {
	rce := &revCacheEntry{
		parent: oce,
		serial: 0,
		before: before,
		ready:  make(chan struct{}),
	}
	rce.inLRU.Init()

	oce.rcev = append(oce.rcev, nil)
	copy(oce.rcev[i+1:], oce.rcev[i:])
	oce.rcev[i] = rce

	return rce
}

// find finds rce under oce and returns its index in oce.rcev.
// not found -> -1.
func (oce *oidCacheEntry) find(rce *revCacheEntry) int {
	for i, r := range oce.rcev {
		if r == rce {
			return i
		}
	}
	return -1
}

func (oce *oidCacheEntry) deli(i int) {
	n := len(oce.rcev) - 1
	copy(oce.rcev[i:], oce.rcev[i+1:])
	// release ptr to revCacheEntry so it won't confusingly stay live when
	// its turn to be deleted come.
	oce.rcev[n] = nil
	oce.rcev = oce.rcev[:n]
}

// XXX doc; must be called with oce lock held
func (oce *oidCacheEntry) del(rce *revCacheEntry) {
	i := oce.find(rce)
	if i == -1 {
		panic("rce not found")
	}

	oce.deli(i)
}

// lock order: Cache.mu   > oidCacheEntry > (?) revCacheEntry
//             Cache.gcMu > ?

// XXX maintain nhit / nmiss?

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

func (c *Cache) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
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
		c.loadRCE(rce, xid)
	}

	return rce.data, rce.serial, rce.userErr(xid)
}

func (c *Cache) Prefetch(xid zodb.Xid) {
	rce, rceNew := c.lookupRCE(xid)

	// XXX!rceNew -> adjust LRU?

	// spawn prefetch in the background if rce was not yet loaded
	if rceNew {
		// XXX use connection poll
		go c.loadRCE(rce, xid)
	}

}

// lookupRCE returns revCacheEntry corresponding to xid.
//
// rceNew indicates whether rce is new and loading on it has not been initiated.
// rce should be loaded with loadRCE.
func (c *Cache) lookupRCE(xid zodb.Xid) (rce *revCacheEntry, rceNew bool) {
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
		cacheBefore = c.before // reload c.before becuase we relocked the cache
		oce.Lock()
		c.mu.Unlock()
	}

	// oce, before -> rce (revCacheEntry)
	if xid.TidBefore {
		l := len(oce.rcev)
		i := sort.Search(l, func(i int) bool {
			before := oce.rcev[i].before
			if before == zodb.TidMax {
				before = cacheBefore
			}
			return xid.Tid <= before
		})

		switch {
		// not found - tid > max(rcev.before) - insert new max entry
		case i == l:
			rce = oce.newRevEntry(i, xid.Tid)
			if rce.before == cacheBefore {
				// FIXME better do this when the entry becomes loaded ?
				// XXX vs concurrent invalidations?
				rce.before = zodb.TidMax
			}
			rceNew = true

		// found:
		// tid <= rcev[i].before
		// tid >  rcev[i-1].before

		// exact match - we already have entry for this before
		case xid.Tid == oce.rcev[i].before:
			rce = oce.rcev[i]

		// non-exact match:
		// - same entry if q(before) ∈ (serial, before]
		// - we can also reuse this entry if q(before) < before and err="nodata"
		case oce.rcev[i].loaded() && (
			(oce.rcev[i].err == nil && oce.rcev[i].serial < xid.Tid) ||
			(isErrNoData(oce.rcev[i].err) && xid.Tid < oce.rcev[i].before)):
			rce = oce.rcev[i]

		// otherwise - insert new entry
		default:
			rce = oce.newRevEntry(i, xid.Tid)
			rceNew = true
		}

	// XXX serial -> revCacheEntry
	} else {
		// TODO
	}

	oce.Unlock()
	return rce, rceNew
}

// loadRCE performs data loading from database into rce.
//
// rce must be new just created by lookupRCE() with returned rceNew=true.
func (c *Cache) loadRCE(rce *revCacheEntry, xid zodb.Xid) {
	oce := rce.parent
	data, serial, err := c.loader.Load(xid)

	// normailize data/serial if it was error
	if err != nil {
		data = nil
		serial = 0
	}
	rce.serial = serial
	rce.data = data
	rce.err = err
	// verify db gives serial < before
	if rce.serial >= rce.before {
		// XXX loadSerial?
		rce.errDB(xid.Oid, "load(<%v) -> %v", rce.before, serial)
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
	if i + 1 < len(oce.rcev) {
		rceNext := oce.rcev[i+1]
		if rceNext.loaded() && tryMerge(rce, rceNext, rce, xid.Oid) {
			// not δsize -= len(rce.data)
			// tryMerge can change rce.data if consistency is broken
			δsize = 0
			rce = rceNext
		}
	}

	// if rcePrev & rce cover the same range -> drop rcePrev
	if i > 0 {
		rcePrev := oce.rcev[i-1]
		if rcePrev.loaded() && tryMerge(rcePrev, rce, rce, xid.Oid) {
			δsize -= len(rcePrev.data)
		}
	}

	oce.Unlock()

	// update lru & cache size
	c.gcMu.Lock()
	rce.inLRU.MoveBefore(&c.lru)
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
// ( ErrXidMissing containts xid for which it is missing. In cache we keep such
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
