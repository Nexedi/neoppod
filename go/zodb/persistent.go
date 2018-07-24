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
// persistent objects.

import (
	"context"
	"sync"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/weak"
)

// IPersistent is the interface that every in-RAM object representing any database object implements.
//
// It is based on IPersistent from ZODB/py:
//
//	https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/persistent/interfaces.py#L22
//
// but is not exactly equal to it.
type IPersistent interface {
	PJar()    *Connection	// Connection this in-RAM object is part of.
	POid()    Oid		// object ID in the database.

	// object serial in the database as of particular Connection (PJar) view.
	// 0 (invalid tid) if not yet loaded (XXX ok?)
	PSerial() Tid


	// PActivate brings object to live state.
	//
	// It requests to persistency layer that in-RAM object data to be present.
	// If object state was not in RAM - it is loaded from the database.
	//
	// On successful return the object data is either the same as in the
	// database or, if this data was previously modified by user of
	// object's jar, that modified data.
	//
	// Object data must be accessed only after corresponding PActivate
	// call, which marks that object's data as being in use.
	PActivate(ctx context.Context) error

	// PDeactivate indicates that corresponding PActivate caller finished access to object's data.
	//
	// As PActivate makes sure object's data is present in-RAM, PDeactivate
	// tells persistency layer that this data is no longer used by
	// corresponding PActivate caller.
	//
	// Note that it is valid to have several concurrent uses of object
	// data, each protected with corresponding PActivate/PDeactivate pair:
	// as long as there is still any PActivate not yet compensated with
	// corresponding PDeactivate, object data will assuredly stay alive in RAM.
	//
	// Besides exotic cases, the caller thus must not use object's data
	// after PDeactivate call.
	PDeactivate()

	// PInvalidate requests in-RAM object data to be discarded.
	//
	// Irregardless of whether in-RAM object data is the same as in the
	// database, or it was modified, that in-RAM data must be forgotten.
	//
	// PInvalidate must not be called while there is any in-progress
	// object's data use (PActivate till PDeactivate).
	//
	// In practice this means that:
	//
	//	- application must make sure to finish all objects accesses
	//	  before transaction boundary: at transaction boundary - either
	//	  at abort or commit, the persistency layer will sync to
	//	  database and process invalidations.
	//
	//	- if PInvalidate is explicitly called by application, the
	//	  application must care to make sure it does not access the
	//	  object data simultaneously.
	PInvalidate()

	// PModify marks in-RAM object state as modified.
	//
	// It informs persistency layer that object's data was changed and so
	// its state needs to be either saved back into database on transaction
	// commit, or discarded on transaction abort.
	//
	// The object must be already activated.
	//PModify()	TODO

	// XXX probably don't need this.
	//PState()  ObjectState	// in-RAM object state.


	// Object must be stateful for persistency to work.
	// XXX try to move out of IPersistent?
	Stateful
}

// ObjectState describes state of in-RAM object.
type ObjectState int

const (
	GHOST ObjectState = -1
	UPTODATE          = 0
	CHANGED           = 1
	// no STICKY - we pin objects in RAM with PActivate
)

// Persistent is common base implementation for in-RAM representation of database objects.
type Persistent struct {
	jar	*Connection
	oid	Oid
	serial	Tid

	mu	 sync.Mutex
	state	 ObjectState
	refcnt	 int32
	instance IPersistent	// Persistent should be the base for the instance
	loading  *loadState
}

func (obj *Persistent) PJar() *Connection	{ return obj.jar	}
func (obj *Persistent) POid() Oid		{ return obj.oid	}
func (obj *Persistent) PSerial() Tid		{ return obj.serial	}

// loadState indicates object's load state/result.
//
// when !ready the loading is in progress.
// when ready the loading has been completed.
type loadState struct {
	ready chan struct{} // closed when loading finishes

	// error from the load.
	// if there was no error, loaded data goes to object state.
	err   error
}

// Stateful is the interface describing in-RAM object whose data state can be
// exchanged as raw bytes.
type Stateful interface {
	// DropState should discard in-RAM object state.
	// XXX move out of Stateful? -> Ghostable?
	DropState()

	// SetState should set state of the in-RAM object from raw data.
	//
	// state ownership is not passed to SetState, so if state needs to be
	// retained after SetState returns it needs to be incref'ed.
	SetState(state *mem.Buf) error

	// GetState should return state of the in-RAM object as raw data.
	//GetState() *mem.Buf	TODO
}


// Connection represents a view of ZODB database.
//
// The view is representing state of ZODB objects as of `at` transaction.
//
// Connection changes are private and are isolated from changes in other Connections.
//
// XXX Connection, and I{Py}Persistent methods that relate to it, are not safe for
// modifications from multiple goroutines simultaneously.
//
// XXX ^^^ better must be safe - use case: e.g. prefetch.
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


// ---- activate/deactivate/invalidate ----

// PActivate implements IPersistent.
func (obj *Persistent) PActivate(ctx context.Context) (err error) {
	obj.mu.Lock()
	obj.refcnt++
	doload := (obj.refcnt == 1 && obj.state == GHOST)
	defer func() {
		if err != nil {
			obj.PDeactivate()
		}
	}()
	if !doload {
		// someone else is already activated/activating the object.
		// wait for its loading to complete and we are done.
		loading := obj.loading
		obj.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()	// XXX err ctx
		case <-loading.ready:
			return loading.err	// XXX err ctx?
		}
	}

	// we become responsible for loading the object
	loading := &loadState{ready: make(chan struct{})}
	obj.loading = loading	// XXX assert before it was = nil ?
	obj.mu.Unlock()

	// do the loading outside of obj lock
	state, serial, err := obj.jar.load(ctx, obj.oid)

	// relock the object
	obj.mu.Lock()

	// XXX assert obj.loading == loading
	// XXX assert obj.state   == GHOST

	obj.serial  = serial

	// try to pass loaded state to object
	if err == nil {
		err = obj.instance.SetState(state)	// XXX err ctx
		state.Release()
		if err == nil {
			obj.state = UPTODATE
		}
	}

	loading.err = err

	obj.mu.Unlock()
	close(loading.ready)

	return err	// XXX err ctx
}

// PDeactivate implements IPersistent.
func (obj *Persistent) PDeactivate() {
	obj.mu.Lock()
	defer obj.mu.Unlock()

	obj.refcnt--
	if obj.refcnt < 0 {
		panic("deactivate: refcnt < 0")
	}
	if obj.refcnt > 0 {
		return // users still left
	}

	// no users left. Let's see whether we should transition this object to ghost.
	if obj.state >= CHANGED {
		return
	}

	if cc := obj.jar.cacheControl; cc != nil {
		if !cc.WantEvict(obj.instance) {
			return
		}
	}

	obj.serial = 0
	obj.instance.DropState()
	obj.state = GHOST
	obj.loading = nil
}

// PInvalidate() implements IPersistent.
func (obj *Persistent) PInvalidate() {
	obj.mu.Lock()
	defer obj.mu.Unlock()

	if obj.refcnt != 0 {
		// object is currently in use
		panic("invalidate: refcnt != 0")
	}

	obj.serial = 0
	obj.instance.DropState()
	obj.state = GHOST
	obj.loading = nil
}


// ----------------------------------------

// XXX Connection.{Get,get} without py dependency?
// but then how to create a ghost of correct class? -> reflect.Type?

// load loads object specified by oid.
//
// XXX must be called ... (XXX e.g. outside transaction boundary) so that there is no race on .at .
func (conn *Connection) load(ctx context.Context, oid Oid) (_ *mem.Buf, serial Tid, _ error) {
	return conn.stor.Load(ctx, Xid{Oid: oid, At: conn.at})
}