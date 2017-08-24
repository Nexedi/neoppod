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
	revv []*revCacheEntry
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
	return &Cache{loader: loader}
}

// newReveEntry creates new revCacheEntry with .before and inserts it into .revv @i
// (if i == len(oce.revv) - entry is appended)
func (oce *oidCacheEntry) newRevEntry(i int, before zodb.Tid) *revCacheEntry {
	rce := &revCacheEntry{
		parent: oce,
		serial: 0,
		before: before,
		ready:  make(chan struct{}),
	}
	rce.inLRU.Init()

	oce.revv = append(oce.revv, nil)
	copy(oce.revv[i+1:], oce.revv[i:])
	oce.revv[i] = rce

	return rce
}

// find finds rce under oce and returns its index in oce.revv.
// not found -> -1.
func (oce *oidCacheEntry) find(rce *revCacheEntry) int {
	for i, r := range oce.revv {
		if r == rce {
			return i
		}
	}
	return -1
}

func (oce *oidCacheEntry) deli(i int) {
	n := len(oce.revv) - 1
	copy(oce.revv[i:], oce.revv[i+1:])
	// release ptr to revCacheEntry so it won't confusingly stay live when
	// its turn to be deleted come.
	oce.revv[n] = nil
	oce.revv = oce.revv[:n]
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
	var rce *revCacheEntry
	var rceNew bool		// whether we created rce anew

	if xid.TidBefore {
		l := len(oce.revv)
		i := sort.Search(l, func(i int) bool {
			before := oce.revv[i].before
			if before == zodb.TidMax {
				before = cacheBefore
			}
			return xid.Tid <= before
		})

		switch {
		// not found - tid > max(revv.before) - insert new max entry
		case i == l:
			rce = oce.newRevEntry(i, xid.Tid)
			if rce.before == cacheBefore {
				// FIXME better do this when the entry becomes loaded ?
				// XXX vs concurrent invalidations?
				rce.before = zodb.TidMax
			}
			rceNew = true

		// found:
		// tid <= revv[i].before
		// tid >  revv[i-1].before

		// exact match - we already have entry for this before
		case xid.Tid == oce.revv[i].before:
			rce = oce.revv[i]

		// non-exact match - same entry if inside (serial, before]
		// XXX do we need `oce.revv[i].serial != 0` check vvv ?
		case oce.revv[i].loaded() && oce.revv[i].serial != 0 && oce.revv[i].serial < xid.Tid:
			rce = oce.revv[i]

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

	// entry was already in cache - use it
	if !rceNew {
		<-rce.ready
		c.gcMu.Lock()
		rce.inLRU.MoveBefore(&c.lru)
		c.gcMu.Unlock()

		// XXX for ErrXidMissing xtid needs to be adjusted to what was queried by user

		return rce.data, rce.serial, rce.err
	}

	// entry was not in cache - this goroutine becomes responsible for loading it
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
		return rce.data, rce.serial, rce.err
	}

	// if rce & rceNext cover the same range -> drop rce
	if i + 1 < len(oce.revv) {
		rceNext := oce.revv[i+1]
		if rceNext.loaded() && tryMerge(rce, rceNext, rce, xid.Oid) {
			// not δsize -= len(rce.data)
			// tryMerge can change rce.data if consistency is broken
			δsize = 0
			rce = rceNext
		}
	}

	// if rcePrev & rce cover the same range -> drop rcePrev
	if i > 0 {
		rcePrev := oce.revv[i-1]
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

	return rce.data, rce.serial, rce.err
}

// tryMerge tries to merge rce prev into next
//
// both prev and next must be already loaded.
// prev and next must come adjacent to each other in parent.revv with
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
	//	Ne   Pe     (Pe="nodata") = (Ne="nodata")
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

	if prev.err != nil && isErrNoData(prev.err) == isErrNoData(next.err) {
		// drop prev
		prev.parent.del(prev)

		// not checking consistency - error is already there and
		// (Pe="nodata") = (Ne="nodata") already indicates prev & next are consistent.

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
