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

package zodb
// cache management

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xcontainer/list"
)

// XXX managing LRU under 1 big gcMu might be bad for scalability.
// TODO maintain nhit / nmiss + way to read cache stats
// TODO optimize cache more so that miss overhead becomes negligible

// Cache provides RAM caching layer that can be used over a storage.
type Cache struct {
	loader interface {
		Loader
		URL() string
	}

	mu sync.RWMutex

	// cache is fully synchronized with storage for transactions with tid <= head.
	// XXX clarify ^^^ (it means if revCacheEntry.head=∞ it is Cache.head)
	head Tid

	entryMap map[Oid]*oidCacheEntry // oid -> oid's cache entries

	gcMu    sync.Mutex
	lru     lruHead // revCacheEntries in LRU order
	size    int     // cached data size in bytes
	sizeMax int     // cache is allowed to occupy not more than this
}

// oidCacheEntry maintains cached revisions for 1 oid.
type oidCacheEntry struct {
	oid Oid

	sync.Mutex

	// cached revisions in ascending order
	// [i].serial <= [i].head < [i+1].serial <= [i+1].head
	//
	// NOTE ^^^ .serial = 0 while loading is in progress
	// NOTE ^^^ .serial = 0 if .err != nil
	rcev []*revCacheEntry
}

// revCacheEntry is information about 1 cached oid revision.
type revCacheEntry struct {
	parent *oidCacheEntry // oidCacheEntry holding us
	inLRU  lruHead        // in Cache.lru; protected by Cache.gcMu

	// we know that load(oid, .head) will give this .serial:oid.
	//
	// this is only what we currently know - not necessarily covering
	// whole correct range - e.g. if oid revisions in db are 1 and 5 if we
	// query db with load(@3) on return we'll get serial=1 and
	// remember .head as 3. But for load(@4) we have to redo
	// database query again.
	//
	// if .head=∞ here, that actually means head is cache.head
	// ( this way we do not need to bump .head to next tid in many
	//   unchanged cache entries when a transaction invalidation comes )
	//
	// .head can be > cache.head and still finite - that represents a
	// case when load with tid > cache.head was called.
	head Tid

	// loading result: object (buf, serial) or error
	buf    *mem.Buf
	serial Tid
	err    error

	// done when loading finished
	// (like closed-when-ready `chan struct{}` but does not allocate on
	//  make and is faster)
	ready sync.WaitGroup

	// protected by .parent's lock:

	accounted bool // whether rce size was accounted in cache size

	// how many waiters for buf is there while rce is being loaded.
	// after data for this RCE is loaded loadRCE will do .buf.XIncref() .waitBufRef times.
	// = -1 after loading is complete.
	waitBufRef int32
}

// lock order: Cache.mu   > oidCacheEntry
//             Cache.gcMu > oidCacheEntry


// NewCache creates new cache backed up by loader.
//
// The cache will use not more than ~ sizeMax bytes of RAM for cached data.
func NewCache(loader interface { Loader; URL() string }, sizeMax int) *Cache {
	c := &Cache{
		loader:   loader,
		entryMap: make(map[Oid]*oidCacheEntry),
		sizeMax:  sizeMax,
	}
	c.lru.Init()
	return c
}

// SetSizeMax adjusts how much RAM cache can use for cached data.
func (c *Cache) SetSizeMax(sizeMax int) {
	c.gcMu.Lock()
	c.sizeMax = sizeMax
	if c.size > c.sizeMax {
		c.gc()
	}
	c.gcMu.Unlock()
}

// Load loads data from database via cache.
//
// If data is already in cache - cached content is returned.
func (c *Cache) Load(ctx context.Context, xid Xid) (buf *mem.Buf, serial Tid, err error) {
	rce, rceNew := c.lookupRCE(xid, +1)

	// rce is already in cache - use it
	if !rceNew {
		rce.ready.Wait()
		c.gcMu.Lock()
		rce.inLRU.MoveBefore(&c.lru.Head)
		c.gcMu.Unlock()

	// rce is not in cache - this goroutine becomes responsible for loading it
	} else {
		c.loadRCE(ctx, rce)
	}

	if rce.err != nil {
		return nil, 0, &OpError{URL: c.loader.URL(), Op: "load", Args: xid, Err: rce.err}
	}

	return rce.buf, rce.serial, nil
}

// Prefetch arranges for data to be eventually present in cache.
//
// If data is not yet in cache loading for it is started in the background.
// Prefetch is not blocking operation and does not wait for loading, if any was
// started, to complete.
//
// Prefetch does not return any error.
func (c *Cache) Prefetch(ctx context.Context, xid Xid) {
	rce, rceNew := c.lookupRCE(xid, +0)

	// !rceNew -> no need to adjust LRU - it will be adjusted by further actual data Load.
	// More: we must not expose not-yet-loaded RCEs to Cache.lru because
	// their rce.waitBufRef was not yet synced to rce.buf.
	// See loadRCE for details.

	// spawn loading in the background if rce was not yet loaded
	if rceNew {
		go c.loadRCE(ctx, rce)
	}

}


// lookupRCE returns revCacheEntry corresponding to xid.
//
// rceNew indicates whether rce is new and so loading on it has not been
// initiated yet. If so the caller should proceed to loading rce via loadRCE.
//
// wantBufRef indicates how much caller wants returned rce.buf to be incref'ed.
//
// This increment will be done only after rce is loaded either by lookupRCE
// here - if it find the rce to be already loaded, or by future loadRCE - if
// rce is not loaded yet - to which the increment will be scheduled.
//
// In any way - either by lookupRCE or loadRCE - the increment will be done
// consistently while under rce.parent lock - this way making sure concurrent gc
// won't release rce.buf while it does not yet hold all its wanted references.
func (c *Cache) lookupRCE(xid Xid, wantBufRef int) (rce *revCacheEntry, rceNew bool) {
	// oid -> oce (oidCacheEntry)  ; create new empty oce if not yet there
	// exit with oce locked and cache.head read consistently
	c.mu.RLock()

	oce := c.entryMap[xid.Oid]
	cacheHead := c.head

	if oce != nil {
		oce.Lock()
		c.mu.RUnlock()
	} else {
		// relock cache in write mode to create oce
		c.mu.RUnlock()
		c.mu.Lock()
		oce = c.entryMap[xid.Oid]
		if oce == nil {
			oce = oceAlloc(xid.Oid)
			c.entryMap[xid.Oid] = oce
		}
		cacheHead = c.head // reload c.head because we relocked the cache
		oce.Lock()
		c.mu.Unlock()
	}

	// oce, at -> rce (revCacheEntry)
	l := len(oce.rcev)
	i := sort.Search(l, func(i int) bool {
		head_i := oce.rcev[i].head
		if head_i == TidMax {
			head_i = cacheHead
		}
		return xid.At <= head_i
	})

	switch {
	// not found - at > max(rcev.head) - insert new max entry
	case i == l:
		rce = oce.newRevEntry(i, xid.At)
		if rce.head == cacheHead {
			// FIXME better do this when the entry becomes loaded ?
			// XXX vs concurrent invalidations?
			rce.head = TidMax
		}
		rceNew = true

	// found:
	// at <= rcev[i].head
	// at >  rcev[i-1].head

	// exact match - we already have entry for this at
	case xid.At == oce.rcev[i].head:
		rce = oce.rcev[i]

	// non-exact match:
	// - same entry if q(at) ∈ [serial, head]
	// - we can also reuse this entry if q(at) <= head and err="nodata"
	case oce.rcev[i].loaded() && (
		(oce.rcev[i].err == nil && oce.rcev[i].serial <= xid.At) ||
		(isErrNoData(oce.rcev[i].err) && xid.At <= oce.rcev[i].head)):
		rce = oce.rcev[i]

	// otherwise - insert new entry
	default:
		rce = oce.newRevEntry(i, xid.At)
		rceNew = true
	}

	// wantBufRef -> either incref loaded buf, or schedule this incref to
	// loadRCE to be done after loading is complete.
	for ; wantBufRef > 0; wantBufRef-- {
		if rce.loaded() {
			rce.buf.XIncref()
		} else {
			rce.waitBufRef++
		}
	}

	oce.Unlock()
	return rce, rceNew
}

// loadRCE performs data loading from database into rce.
//
// rce must be new just created by lookupRCE() with returned rceNew=true.
// loading completion is signalled by marking rce.ready done.
func (c *Cache) loadRCE(ctx context.Context, rce *revCacheEntry) {
	oce := rce.parent
	buf, serial, err := c.loader.Load(ctx, Xid{At: rce.head, Oid: oce.oid})

	// normalize buf/serial if it was error
	if err != nil {
		e := err.(*OpError) // XXX better driver return *OpError explicitly

		// only remember problem cause - full OpError will be
		// reconstructed in Load with actual requested there xid.
		// XXX check .Op == "load" ?
		err = e.Err
		// TODO err == canceled? -> don't remember
		buf.XRelease()
		buf = nil
		serial = 0
	}
	rce.serial = serial
	rce.buf = buf
	rce.err = err
	// verify db gives serial <= head
	if rce.serial > rce.head {
		rce.markAsDBError("load(@%v) -> %v", rce.head, serial)
	}

	δsize := rce.buf.Len()

	oce.Lock()

	// sync .waitBufRef -> .buf
	//
	// this is needed so that we always put into Cache.lru an RCE with proper
	// refcount = 1 for cache + n·Load waiters. If we do not account for
	// n·Load waiters here - under .parent's lock, gc might run before Load
	// resumes, see .buf.refcnt = 1 and return .buf to freelist -> oops.
	for ; rce.waitBufRef > 0; rce.waitBufRef-- {
		rce.buf.XIncref()
	}
	rce.waitBufRef = -1 // mark as loaded

	i := oce.find(rce)
	if i == -1 {
		// rce was already dropped by merge / evicted
		// (XXX recheck about evicted)
		oce.Unlock()
		rce.ready.Done()
		return
	}

	// merge rce with adjacent entries in parent
	// ( e.g. load(@3) and load(@4) results in the same data loaded if
	//   there are only revisions with serials 1 and 5 )
	//
	// if rce & rceNext cover the same range -> drop rce
	//
	// if we drop rce - do not update c.lru as:
	// 1. new rce is not on lru list,
	// 2. rceNext (which becomes rce) might not be there on lru list.
	//
	// if rceNext is not yet there on lru list its loadRCE is in progress
	// and will update lru and cache size for it itself.
	rceOrig := rce
	rceDropped := false
	if i+1 < len(oce.rcev) {
		rceNext := oce.rcev[i+1]
		if rceNext.loaded() && tryMerge(rce, rceNext, rce) {
			// not δsize -= len(rce.buf.Data)
			// tryMerge can change rce.buf if consistency is broken
			δsize = 0
			rce.buf.XRelease()
			rce = rceNext
			rceDropped = true
		}
	}

	// if rcePrev & rce cover the same range -> drop rcePrev
	// (if we drop rcePrev we'll later remove it from c.lru when under c.gcMu)
	var rcePrevDropped *revCacheEntry
	if i > 0 {
		rcePrev := oce.rcev[i-1]
		if rcePrev.loaded() && tryMerge(rcePrev, rce, rce) {
			rcePrevDropped = rcePrev
			if rcePrev.accounted {
				δsize -= rcePrev.buf.Len()
			}
			rcePrev.buf.XRelease()
		}
	}

	if !rceDropped {
		rce.accounted = true
	}

	oce.Unlock()

	// now after .waitBufRef was synced to .buf notify to waiters that
	// original rce in question was loaded. Do so outside .parent lock.
	rceOrig.ready.Done()


	// update lru & cache size
	c.gcMu.Lock()

	if rcePrevDropped != nil {
		rcePrevDropped.inLRU.Delete()
	}
	if !rceDropped {
		rce.inLRU.MoveBefore(&c.lru.Head)
	}
	c.size += δsize
	if c.size > c.sizeMax {
		c.gc()
	}

	c.gcMu.Unlock()
}

// tryMerge tries to merge rce prev into next.
//
// both prev and next must be already loaded.
// prev and next must come adjacent to each other in parent.rcev with
// prev.head < next.head .
//
// cur must be one of either prev or next and indicates which rce is current
// and so may be adjusted with consistency check error.
//
// return: true if merging done and thus prev was dropped from parent
//
// must be called with .parent locked
func tryMerge(prev, next, cur *revCacheEntry) bool {

	//		  can merge if    consistent if
	//	                          (if merging)
	//
	//	Pok  Nok    Ns <= Ph        Ps  = Ns
	//	Pe   Nok    Ns <= Ph        Pe != "nodata"	(e.g. it was IO loading error for P)
	//	Pok  Ne       ---
	//	Ne   Pe     (Pe="nodata") && (Ne="nodata")	-> XXX vs deleteObject?
	//							-> let deleted object actually read
	//							-> as special non-error value
	//
	// h - head
	// s - serial
	// e - error

	if next.err == nil && next.serial <= prev.head {
		// drop prev
		prev.parent.del(prev)

		// check consistency
		switch {
		case prev.err == nil && prev.serial != next.serial:
			cur.markAsDBError("load(@%v) -> %v; load(@%v) -> %v",
				prev.head, prev.serial, next.head, next.serial)

		case prev.err != nil && !isErrNoData(prev.err):
			if cur.err == nil {
				cur.markAsDBError("load(@%v) -> %v; load(@%v) -> %v",
					prev.head, prev.err, next.head, next.serial)
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

// ---- garbage collection ----

// gc performs garbage-collection.
//
// must be called with .gcMu locked.
func (c *Cache) gc() {
	//fmt.Printf("\n> gc\n")
	//defer fmt.Printf("< gc\n")

	for {
		if c.size <= c.sizeMax {
			return
		}

		// kill 1 least-used rce
		h := c.lru.Next()
		if h == &c.lru {
			panic("cache: gc: empty .lru but .size > .sizeMax")
		}

		rce := h.rceFromInLRU()
		oce := rce.parent
		oceFree := false // whether to GC whole rce.parent OCE cache entry

		oce.Lock()
		i := oce.find(rce)
		if i != -1 {	// rce could be already deleted by e.g. merge
			oce.deli(i)
			if len(oce.rcev) == 0 {
				oceFree = true
			}

			c.size -= rce.buf.Len()
			//fmt.Printf("gc: free %d bytes\n", rce.buf.Len()))

			// not-yet-loaded rce must not be on Cache.lru
			if !rce.loaded() {
				panic("cache: gc: found !loaded rce on lru")
			}

			rce.buf.XRelease()
		}
		oce.Unlock()

		h.Delete()

		if oceFree {
			c.mu.Lock()
			oce.Lock()
			// recheck once again oce is still not used
			// (it could be looked up again in the meantime we were not holding its lock)
			if len(oce.rcev) == 0 {
				delete(c.entryMap, oce.oid)
			} else {
				oceFree = false
			}
			oce.Unlock()
			c.mu.Unlock()

			if oceFree {
				oce.release()
			}
		}
	}
}

// freelist(OCE)
var ocePool = sync.Pool{New: func() interface{} { return &oidCacheEntry{} }}

// oceAlloc allocates oidCacheEntry from freelist.
func oceAlloc(oid Oid) *oidCacheEntry {
	oce := ocePool.Get().(*oidCacheEntry)
	oce.oid = oid
	return oce
}

// release puts oce back into freelist.
//
// Oce must be empty and caller must not use oce after call to release.
func (oce *oidCacheEntry) release() {
	if len(oce.rcev) != 0 {
		panic("oce.release: .rcev != []")
	}

	oce.oid = 0 // just in case
	ocePool.Put(oce)
}

// ----------------------------------------

// isErrNoData returns whether an error is due to "there is no such data in
// database", not e.g. some IO loading error.
func isErrNoData(err error) bool {
	switch err.(type) {
	default:
		return false

	case *NoObjectError:
	case *NoDataError:
	}
	return true
}

// newRevEntry creates new revCacheEntry with .head and inserts it into .rcev @i.
// (if i == len(oce.rcev) - entry is appended)
//
// oce must be locked.
func (oce *oidCacheEntry) newRevEntry(i int, head Tid) *revCacheEntry {
	rce := &revCacheEntry{
		parent: oce,
		head:   head,
	}
	rce.ready.Add(1)
	rce.inLRU.Init() // initially not on Cache.lru list

	oce.rcev = append(oce.rcev, nil)
	copy(oce.rcev[i+1:], oce.rcev[i:])
	oce.rcev[i] = rce

	return rce
}

// find finds rce in .rcev and returns its index
// not found -> -1.
//
// oce must be locked.
func (oce *oidCacheEntry) find(rce *revCacheEntry) int {
	for i, r := range oce.rcev {
		if r == rce {
			return i
		}
	}
	return -1
}

// deli deletes .rcev[i]
//
// oce must be locked.
func (oce *oidCacheEntry) deli(i int) {
	n := len(oce.rcev) - 1
	copy(oce.rcev[i:], oce.rcev[i+1:])
	// release ptr to revCacheEntry so it won't confusingly stay live when
	// its turn to be deleted come.
	oce.rcev[n] = nil
	oce.rcev = oce.rcev[:n]
}

// del deletes rce from .rcev.
// it panics if rce is not there.
//
// oce must be locked.
func (oce *oidCacheEntry) del(rce *revCacheEntry) {
	i := oce.find(rce)
	if i == -1 {
		panic("rce not found")
	}

	oce.deli(i)
}


// loaded reports whether rce was already loaded.
//
// must be called with rce.parent locked.
func (rce *revCacheEntry) loaded() bool {
	return (rce.waitBufRef == -1)
}

// list head that knows it is in revCacheEntry.inLRU
type lruHead struct {
	list.Head
}

// XXX vvv strictly speaking -unsafe.Offsetof(h.Head)
func (h *lruHead) Next() *lruHead { return (*lruHead)(unsafe.Pointer(h.Head.Next())) }
func (h *lruHead) Prev() *lruHead { return (*lruHead)(unsafe.Pointer(h.Head.Prev())) }

// revCacheEntry: .inLRU -> .
func (h *lruHead) rceFromInLRU() (rce *revCacheEntry) {
	urce := unsafe.Pointer(uintptr(unsafe.Pointer(h)) - unsafe.Offsetof(rce.inLRU))
	return (*revCacheEntry)(urce)
}

// errDB returns error about database being inconsistent.
func errDB(oid Oid, format string, argv ...interface{}) error {
	// XXX -> separate type?
	return fmt.Errorf("cache: database inconsistency: oid: %v: "+format,
		append([]interface{}{oid}, argv...)...)
}

// markAsDBError marks rce with database inconsistency error.
//
// Caller must be the only one to access rce.
// In practice this means rce was just loaded but neither yet signalled to be
// ready to waiter, nor yet made visible to GC (via adding to Cache.lru list).
func (rce *revCacheEntry) markAsDBError(format string, argv ...interface{}) {
	rce.err = errDB(rce.parent.oid, format, argv...)
	rce.buf.XRelease()
	rce.buf = nil
	rce.serial = 0
}
