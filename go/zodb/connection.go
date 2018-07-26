// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE

package zodb
// application-level database connection.

// Connection represents a view of ZODB database.
//
// The view is representing state of ZODB objects as of `at` transaction.
//
// Connection changes are private and are isolated from changes in other Connections.
//
// Connection is safe to access from multiple goroutines simultaneously.
//
// XXX ^^^ modifications?
type Connection struct {
	stor	IStorage	// underlying storage
	at	Tid		// current view of database

	// {} oid -> obj
	//
	// rationale:
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
	//	- but there will be still reference from connection `oid -> obj` map to this object.
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
	// Hopefully we don't have cycles with ZBTree/ZBucket	XXX verify this
	objmu  sync.Mutex
	objtab map[Oid]*weak.Ref // oid -> weak.Ref(IPersistent)

	// hooks for application to influence live caching decisions.
	cacheControl LiveCacheControl
}

// LiveCacheControl is the interface that allows applications to influence
// Connection's decisions with respect to Connection's live cache.
type LiveCacheControl interface {
	// WantEvict is called when object is going to be evicted from live
	// cache on deactivation and made ghost.
	//
	// If !ok the object will remain live.
	//
	// NOTE on invalidation invalidated objects are evicted from live cache
	// unconditionally.
	WantEvict(obj IPersistent) (ok bool)
}


