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

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// Cache adds RAM caching layer over a storage
// XXX -> zodb ?
// XXX -> RAMCache ?
type Cache struct {
	mu sync.RWMutex

	// cache is fully synchronized with storage for transactions with tid < before.
	before	zodb.Tid

	oidDir map[zodb.Oid]*oidEntry

	bytesUsed int64

	lru revListHead
}

// oidCacheEntry maintains cached revisions for 1 oid
type oidCacheEntry struct {
	sync.Mutex	// XXX -> rw ?

	// cached revisions in ascending order
	// .serial < .before <= next.serial < next.before
	//
	// XXX or?
	// cached revisions in descending order
	// .before > .serial >= next.before > next.serial ?
	revv []*revCacheEntry
}

// revCacheEntry is information about 1 cached oid revision
type revCacheEntry struct {
	inLRU  revListHead
	parent *oidCacheEntry

//	sync.Mutex	XXX not needed

	// oid revision
	// 0 if don't know yet - loadBefore(.before) is in progress and actual
	// serial not yet obtained from database
	serial    zodb.Tid

	// we know that loadBefore(oid, before) will give this serial:oid.
	//
	// this is only what we currently know - not neccessarily covering
	// whole correct range - e.g. if oid revisions in db are 1 and 5 if we
	// query db with loadBefore(3) on return we'll get serial=1 and
	// remember .before as 3. But for loadBefore(4) we have to redo
	// database query again.
	//
	// if an .before=∞ that actually mean before is cache.before
	// ( this way we do not need to bump before to next tid in many
	//   unchanged cache entries when a transaction invalidation comes )
	//
	// .before can be > cache.before and still finite - that represents a
	// case when loadBefore with tid > cache.before was called.
	before zodb.Tid

	// object data or loading error
	data []byte
	err  error

	ready chan struct{} // closed when loading finished
}

type revListHead struct {
	// XXX needs to be created with .next = .prev = self
	next, prev *revCacheEntry
}

// XXX -> to ctor?
func (h *revListHead) init() {
	h.next = h
	h.prev = h
}

// Delete deletes h from list
func (h *revListHead) Delete() {
	h.next.prev = h.prev
	h.prev.next = h.next
}

// MoveBefore moves a to be before b
// XXX ok to move if a was not previously on the list?
func (a *revListHead) MoveBefore(b *revListHead) {
	a.Delete()

	a.next = b
	b.prev = a
	a.prev = b.prev
	a.prev.next = a
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


// lock order: Cache > cacheEntry > (?) revCacheEntry

// XXX maintain nhit / lru

func (c *cache) Load(xid zodb.Xid) (data []byte, tid Tid, err error) {
	// oid -> cacheEntry  ; creating new empty if not yet there
	// exit with cacheEntry locked and cache.before read consistently
	c.mu.RLock()

	entry := c.entryMap[xid.Oid]
	cacheBefore := c.before
	cacheMemUsed := c.memUsed

	if entry != nil {
		entry.Lock()
		c.mu.RUnlock()
	} else {
		c.mu.RUnlock()
		c.mu.Lock()
		entry = c.entryMap[xid.Oid]
		if entry == nil {
			entry = &cacheEntry{}
			c.entryMap[xid.Oid] = entry
		}
		cacheBefore = c.before // reload c.before for correctness
		entry.Lock()
		c.mu.Unlock()
	}

	var rce *revCacheEntry
	var rceNew bool		// whether new revCacheEntry was created

	// before -> revCacheEntry
	if xid.TidBefore {
		l := len(entry.revv)
		i := sort.Search(l, func(i int) bool {
			before := entry.revv[i].before
			if before == zodb.TidMax {
				before = cacheBefore
			}
			xid.Tid <= before
		})

		switch {
		// not found - tid > max(revv.before) - create new max entry
		case i == l:
			rce = &revCacheEntry{serial: 0, before: before}
			if rce.before == cacheBefore {
				// XXX better do this when the entry becomes loaded ?
				rce.before = zodb.TidMax	// XXX vs concurrent invalidations?
			}
			rceNew = true
			entry.revv = append(entry.revv, rce)

		// found:
		// tid <= revv[i].before
		// tid >  revv[i-1].before

		// exact match - we already have it
		case xid.Tid == revv[i].before:
			rce = entry.revv[i]

		// if outside [serial, before) - insert new entry
		case !(revv[i].serial != 0 && revv[i].serial <= xid.Tid):
			rce = &revCacheEntry{serial: 0, before: xid.Tid}
			rceNew = true
			entry.revv = append(entry.revv[:i], rce, entry.revv[i:])
		}

	// XXX serial -> revCacheEntry
	} else {
		// TODO
	}

	if rceNew {
		rce.ready = make(chan struct{})
	}

	entry.Unlock()

	// entry was already in cache - use it
	if !rceNew {
		<-rce.ready
		return rce.data, rce.serial, rce.err
	}

	// entry was not in cache - this goroutine becomes responsible for loading it
	data, serial, err := c.stor.Load(xid)
	rce.serial = serial
	rce.data = data
	rce.err = err
	close(ready)

	entry.Lock()
	// XXX merge with adjacent entries in revv
	entry.Unlock()

	c.Lock()
	c.memUsed += len(data)
	// XXX GC if memUsed > threthold ?
	c.Unlock()

	return rce.data, rce.serial, rce.err
}
