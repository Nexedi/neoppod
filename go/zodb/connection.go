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
// application-level database connection.

import (
	"context"
	"fmt"
	"sync"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/weak"
)

// Connection represents application-level view of a ZODB database.
//
// The view is represented by IPersistent objects associated with the connection.
// Connection changes are private and are isolated from changes in other Connections.
// Connection's view corresponds to particular database state and is thus
// isolated from further database transactions.
//
// Connection is safe to access from multiple goroutines simultaneously.
//
// Connection and objects obtained from it must be used by application only
// inside transaction where Connection was opened.
//
// Use DB.Open to open a connection.
type Connection struct {
	db   *DB                     // Connection is part of this DB
	txn  transaction.Transaction // opened under this txn; nil after transaction ends.
	at   Tid                     // current view of database; stable inside a transaction.

	cache  LiveCache // cache of connection's in-RAM objects
	noPool bool      // connection is not returned to db.pool
}

// LiveCache keeps registry of live in-RAM objects for a Connection.
//
// It semantically consists of
//
//	{} oid -> obj
//
// but does not hold strong reference to cached objects.
//
// LiveCache is not safe to use from multiple goroutines simultaneously.
//
// Use .Lock() / .Unlock() to serialize access.
type LiveCache struct {
	sync.Mutex

	// pinned objects. may have referrers.
	pinned map[Oid]IPersistent

	// not pinned objects. may have referrers. cache keeps weak references to objects.
	//
	// rationale for using weakref:
	//
	// on invalidations: we need to go oid -> obj and invalidate it.
	// -> Connection need to keep {} oid -> obj.
	// -> we can use that {} when loading a persistent Ref twice to get to the same object.
	//
	// however: if Connection keeps strong link to obj, just
	// obj.PDeactivate will not fully release obj if there are no
	// references to it from other objects:
	//
	//	- deactivate will release obj state (ok)
	//	- but there will be still reference from connection `oid -> obj` map to this object,
	//	  which means the object won't be garbage-collected.
	//
	// -> we can solve it by using "weak" pointers in the map.
	//
	// NOTE we cannot use regular map and arbitrarily manually "gc" entries
	// there periodically: since for an obj we don't know whether other
	// objects are referencing it, we can't just remove obj's oid from
	// the map - if we do so and there are other live objects that
	// reference obj, user code can still reach obj via those
	// references. On the other hand, if another, not yet loaded, object
	// also references obj and gets loaded, traversing reference from
	// that loaded object will load second copy of obj, thus breaking 1
	// object in db <-> 1 live object invariant:
	//
	//	A  →  B  →  C
	//	↓           |
	//      D <--------- - - -> D2 (wrong)
	//
	// - A activate
	// - D activate
	// - B activate
	// - D gc, A still keeps link on D
	// - C activate -> it needs to get to D, but D was removed from objtab
	//   -> new D2 is wrongly created
	//
	// that's why we have to depend on Go's GC to know whether there are
	// still live references left or not. And that in turn means finalizers
	// and thus weak references.
	//
	// some link on the subject:
	// https://groups.google.com/forum/#!topic/golang-nuts/PYWxjT2v6ps
	//
	// NOTE2 finalizers don't run on when they are attached to an object in cycle.
	// Hopefully we don't have cycles with BTree/Bucket.
	objtab map[Oid]*weak.Ref // oid -> weak.Ref(IPersistent)

	// hooks for application to influence live caching decisions.
	control LiveCacheControl
}

// LiveCacheControl is the interface that allows applications to influence
// Connection's decisions with respect to Connection's LiveCache.
//
// See Connection.Cache and LiveCache.SetControl for how to install
// LiveCacheControl on a connection's live cache.
type LiveCacheControl interface {
	// PCacheClassify is called to classify an object and returns live
	// cache policy that should be used for this object.
	PCacheClassify(obj IPersistent) PCachePolicy
}

// PCachePolicy describes live caching policy for a persistent object.
//
// It is | combination of PCache* flags with 0 meaning "use default policy".
//
// See LiveCacheControl for how to apply a caching policy.
type PCachePolicy int

const (
	// keep object pinned into cache, even if in ghost state.
	//
	// This allows to rely on object being never evicted from live cache.
	//
	// Note: object's state can still be discarded and the object can go
	// into ghost state. Use PCacheKeepState to prevent such automatic
	// state eviction until state discard is semantically required.
	PCachePinObject PCachePolicy = 1 << iota

	// don't keep object in the cache.
	//
	// The object will be discarded from the cache completely as soon as it
	// is semantically valid to do so.
	PCacheDropObject

        // keep object state in the cache.
	//
	// This prevents object state to go away when !dirty object is no
	// longer used. However the object itself can go away unless it is
	// pinned into cache via PCachePinObject.
	//
	// Note: on invalidation, state of invalidated objects is discarded
	// unconditionally.
	PCacheKeepState

	// don't keep object state.
	//
	// Data access is likely non-temporal and object's state will be used
	// once and then won't be used for a long time. Don't pollute cache
	// with state of this object.
	PCacheDropState
)

// ----------------------------------------

// newConnection creates new Connection associated with db.
func newConnection(db *DB, at Tid) *Connection {
	return &Connection{
		db:    db,
		at:    at,
		cache: LiveCache{
			pinned: make(map[Oid]IPersistent),
			objtab: make(map[Oid]*weak.Ref),
		},
	}
}

// At returns database state corresponding to the connection.
func (conn *Connection) At() Tid {
	conn.checkLive("at")
	return conn.at
}

// Cache returns connection's cache of live objects.
func (conn *Connection) Cache() *LiveCache {
	return &conn.cache
}

// wrongClassError is the error cause returned when ZODB object's class is not what was expected.
type wrongClassError struct {
	want, have string
}

func (e *wrongClassError) Error() string {
	return fmt.Sprintf("wrong class: want %q; have %q", e.want, e.have)
}

// Get lookups object corresponding to oid in the cache.
//
// If object is found, it is guaranteed to stay in live cache while the caller keeps reference to it.
// LiveCacheControl can be used to extend that guarantee.
func (cache *LiveCache) Get(oid Oid) IPersistent {
	// 1. lookup in pinned objects (likely hottest ones)
	obj := cache.pinned[oid]
	if obj != nil {
		return obj
	}

	// 2. lookup in !pinned referenced object (they are likely to be loaded
	//    going from a referee)
	wobj := cache.objtab[oid]
	if wobj != nil {
		if xobj := wobj.Get(); xobj != nil {
			obj = xobj.(IPersistent)
		}
	}

	// 3. TODO lookup in non-referenced LRU cache

	return obj
}

// set sets objects corresponding to oid.
// XXX -> setNew?
func (cache *LiveCache) set(oid Oid, obj IPersistent) {
	var cp PCachePolicy
	if cc := cache.control; cc != nil {
		cp = cache.control.PCacheClassify(obj)
	}
	// XXX remember cp in obj .pcachePolicy?
	// XXX del .objtab[oid] ?
	// XXX del .pinned[oid] ?
	if cp & PCachePinObject != 0 {
		cache.pinned[oid] = obj
	} else {
		cache.objtab[oid] = weak.NewRef(obj)
	}
}

// forEach calls f for all objects in the cache.
func (cache *LiveCache) forEach(f func(IPersistent)) {
	for _, obj := range cache.pinned {
		f(obj)
	}
	for _, wobj := range cache.objtab {
		if xobj := wobj.Get(); xobj != nil {
			f(xobj.(IPersistent))
		}
	}
}

// SetControl installs c to handle cache decisions.
//
// Any previously installed cache control is uninstalled.
// Passing nil sets the cache to have no control installed at all.
//
// It is not safe to call SetControl simultaneously to other cache operations.
func (cache *LiveCache) SetControl(c LiveCacheControl) {
	cache.control = c

	// reclassify all objects
	c2 := *cache
	cache.objtab = make(map[Oid]*weak.Ref)
	cache.pinned = make(map[Oid]IPersistent)
	c2.forEach(func(obj IPersistent) {
		cache.set(obj.POid(), obj)
	})
}

// get is like Get, but used when we already know object class.
//
// Use-case: in ZODB references are (pyclass, oid), so new ghost is created
// without further loading anything.
func (conn *Connection) get(class string, oid Oid) (IPersistent, error) {
	checkClass := true
	conn.cache.Lock() // XXX -> rlock?
	obj := conn.cache.Get(oid)
	if obj == nil {
		obj = newGhost(class, oid, conn)
		conn.cache.set(oid, obj)
		checkClass = false
	}
	conn.cache.Unlock()

	if checkClass {
		if cls := ClassOf(obj); class != cls {
			var err error = &wrongClassError{class, cls}
			xerr.Contextf(&err, "get %s", Xid{conn.at, oid})
			return nil, err
		}
	}

	return obj, nil
}

// Get returns in-RAM object corresponding to specified ZODB object according to current database view.
//
// If there is already in-RAM object that corresponds to oid, that in-RAM object is returned.
// Otherwise new in-RAM object is created and filled with object's class loaded from the database.
//
// The scope of the object returned is the Connection.
//
// The object's data is not necessarily loaded after Get returns. Use
// PActivate to make sure the object is fully loaded.
func (conn *Connection) Get(ctx context.Context, oid Oid) (_ IPersistent, err error) {
	conn.checkTxnCtx(ctx, "Get")
	defer xerr.Contextf(&err, "Get %s", oid)

	conn.cache.Lock() // XXX -> rlock?
	obj := conn.cache.Get(oid)
	conn.cache.Unlock()

	// object was already there in cache.
	if obj != nil {
		return obj, nil
	}

	// object is not in cache - raw load it, get its class -> get(pyclass, oid)
	// XXX "py always" hardcoded
	class, pystate, serial, err := conn.loadpy(ctx, oid)
	if err != nil {
		xerr.Contextf(&err, "Get %s", Xid{conn.at, oid})
		return nil, err
	}

	obj, err = conn.get(class, oid)
	if err != nil {
		return nil, err
	}

	// XXX we are dropping just loaded pystate. Usually Get should be used
	// to only load root object, so maybe that is ok.
	//
	// TODO -> use (pystate, serial) to activate.
	_, _ = pystate, serial
	return obj, nil
}

// load loads object specified by oid.
func (conn *Connection) load(ctx context.Context, oid Oid) (_ *mem.Buf, serial Tid, _ error) {
	conn.checkTxnCtx(ctx, "load")
	return conn.db.stor.Load(ctx, Xid{Oid: oid, At: conn.at})
}

// ----------------------------------------

// checkTxnCtx asserts that current transaction is the same as conn.txn .
func (conn *Connection) checkTxnCtx(ctx context.Context, who string) {
	conn.checkTxn(transaction.Current(ctx), who)
}

// checkTxn asserts that specified "current" transaction is the same as conn.txn .
func (conn *Connection) checkTxn(txn transaction.Transaction, who string) {
	if txn != conn.txn {
		panic("connection: " + who + ": current transaction is different from connection transaction")
	}
}

// checkLive asserts that the connection is alive - the transaction under which
// it has been opened is not yet complete.
func (conn *Connection) checkLive(who string) {
	if conn.txn == nil {
		panic("connection: " + who + ": connection is not live")
	}
}
