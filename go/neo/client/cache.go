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
	"sync"
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// Cache adds RAM caching layer over a storage
type Cache struct {
	mu sync.RWMutex

	// cache is fully synchronized with storage for transactions with tid < before.
	// XXX clarify ^^^ (it means if revCacheEntry.before=∞ it is Cache.before)
	before	zodb.Tid

	entryMap map[zodb.Oid]*oidCacheEntry	// oid -> cache entries for this oid

	// garbage collection:
	gcMu sync.Mutex
	lru  listHead	// revCacheEntries in LRU order
	size int64	// cached data size in bytes
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

// loaded reports whether rce was already loaded
func (rce *revCacheEntry) loaded() bool {
	select {
	case <-rce.ready:
		return true
	default:
		return false
	}
}

// XXX doc
func (oce *oidCacheEntry) newRevEntry(before zodb.Tid) *revCacheEntry {
	rce := &revCacheEntry{
		parent: oce,
		serial: 0,
		before: before,
		ready:  make(chan struct{})}
	}
	rce.inLRU.init()
	return rce
}

// find finds rce under oce and returns its index in oce.revv.
// not found -> -1.
func (oce *oidCacheEntry) find(rce *revCacheEntry) int {
	for i, r := oce.revv {
		if r == rce {
			return i
		}
	}
	return -1
}

func (oce *oidCacheEntry) deli(i int) {
	oce.revv = append(oce.revv[:i], oce.revv[i+1:])
}

// XXX doc; must be called with oce lock held
func (oce *oidCacheEntry) del(rce *revCacheEntry) {
	i := oce.find(rce)
	if i == -1 {
		panic("rce not found")
	}

	rce.revv = append(rce.revv[:i], rce.revv[i+1:])
}

// lock order: Cache > cacheEntry > (?) revCacheEntry

// XXX maintain nhit / nmiss?

func (c *cache) Load(xid zodb.Xid) (data []byte, tid Tid, err error) {
	// oid -> oce (oidCacheEntry)  ; creating new empty if not yet there
	// exit with oce locked and cache.before read consistently
	c.mu.RLock()

	oce := c.entryMap[xid.Oid]
	cacheBefore := c.before
	cacheMemUsed := c.memUsed

	if oce != nil {
		oce.Lock()
		c.mu.RUnlock()
	} else {
		c.mu.RUnlock()
		c.mu.Lock()
		oce = c.entryMap[xid.Oid]
		if oce == nil {
			oce = &cacheEntry{}
			c.entryMap[xid.Oid] = oce
		}
		cacheBefore = c.before // reload c.before becuase we relocked the cache
		oce.Lock()
		c.mu.Unlock()
	}

	// oce, before -> rce (revCacheEntry)
	var rce *revCacheEntry
	var rceNew bool		// whether rce created anew

	if xid.TidBefore {
		l := len(oce.revv)
		i := sort.Search(l, func(i int) bool {
			before := oce.revv[i].before
			if before == zodb.TidMax {
				before = cacheBefore
			}
			xid.Tid <= before
		})

		switch {
		// not found - tid > max(revv.before) - insert new max entry
		case i == l:
			rce = oce.newRevEntry(xid.Tid)
			if rce.before == cacheBefore {
				// FIXME better do this when the entry becomes loaded ?
				// XXX vs concurrent invalidations?
				rce.before = zodb.TidMax
			}
			rceNew = true
			oce.revv = append(oce.revv, rce)	// XXX -> newRevEntry ?

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
			rce = oce.newRevEntry(xid.Tid)
			rceNew = true
			oce.revv = append(oce.revv[:i], rce, oce.revv[i:]) // XXX -> newRevEntry ?
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
		return rce.data, rce.serial, rce.err
	}

	// entry was not in cache - this goroutine becomes responsible for loading it
	data, serial, err := c.stor.Load(xid)

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

	close(ready)
	δsize := len(rce.data)

	// merge rce with adjacent entries in parent
	// ( e.g. loadBefore(3) and loadBefore(4) results in the same data loaded if
	//   there are only revisions with serials 1 and 5 )
	oce.Lock()
	i := oce.find(rce)
	if i == -1 {
		// rce was already dropped / evicted
		oce.Unlock()
		return rce.data, rce.serial, rce.err
	}

	// if rce & rceNext cover the same range -> drop rce
	if i + 1 < len(oce.revv) {
		rceNext := oce.revv[i+1]
		if rceNext.loaded() && rceNext.serial < rce.before {	// XXX rceNext.serial=0 ?
			// drop rce
			oce.deli(i)
			δsize -= len(rce.data)

			// verify rce.serial == rceNext.serial
			if rce.serial != rceNext.serial {
				rce.errDB(xid.Oid, "load(<%v) -> %v; load(<%v) -> %v", rce.before, rce.serial, rceNext.before, rceNext.serial)
			}

			rce = rceNext
		}
	}

	// if rcePrev & rce cover the same range -> drop rcePrev
	if i > 0 {
		rcePrev = oce.revv[i-1]
		if rce.serial < rcePrev.before {
			// XXX drop rcePrev here?

			// verify rce.serial == rcePrev.serial (if that is ready)
			if rcePrev.loaded() && rcePrev.serial != rce.serial {	// XXX rcePrev.serial=0 ?
				rce.errDB(xid.Oid, "load(<%v) -> %v; load(<%v) -> %v", rcePrev.before, rcePrev.serial, rce.before, rce.serial)
			}

			// drop rcePrev
			oce.deli(i-1)
			δsize -= len(rcePrev.data)
		}
	}

	oce.Unlock()

	// update lru & cache size
	c.gcMu.Lock()
	rce.inLRU.MoveBefore(&c.lru)
	c.size += δsize
	if c.size > c.sizeTarget {
		-> run gc
	}
	c.gcMu.Unlock()

	return rce.data, rce.serial, rce.err
}

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



// revCacheEntry: .inLRU -> .
func (h *listHead) rceFromInLRU() (rce *revCacheEntry) {
	return (*revCacheEntry)(unsafe.Pointer(h) - unsafe.OffsetOf(rce.inLRU))
}

// errDB returns error about database being inconsistent
func errDB(oid zodb.Oid, format string, argv ...interface{}) error {
	// XXX -> separate type?
	return fmt.Errorf("cache: database inconsistency: oid: %v: " + format, oid, ...argv)
}

// errDB marks rce with database inconsistency error
func (rce *revCacheEntry) errDB(oid zodb.Oid, format string, argv ...interface{}) {
	rce.err = errDB(oid, format, argv...)
	rce.data = nil
	rce.serial = 0
}
