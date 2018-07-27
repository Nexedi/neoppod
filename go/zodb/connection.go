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

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/weak"
)

// Connection represents a view of ZODB database. XXX + live application objects.
//
// The view is representing state of ZODB objects as of `at` transaction.
//
// Connection changes are private and are isolated from changes in other Connections.
//
// Connection is safe to access from multiple goroutines simultaneously.
//
// XXX ^^^ modifications?
//
// Connection and objects obtained from it must be used by application only
// inside transaction where Connection was opened.
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


// ---- class <-> type; new ghost ----

// zclass describes one ZODB class in relation to Go type.
type zclass struct {
	class     string
	typ       reflect.Type // application go type corresponding to class
	stateType reflect.Type // *typ and *stateType are convertible; *stateType provides Statufl & co.
}

var classTab = make(map[string]*zclass)       // {} class -> zclass
var typeTab  = make(map[reflect.Type]*zclass) // {} type  -> zclass

// zclassOf returns ZODB class of a Go object.
//
// If ZODB class was not registered for obj's type, "" is returned.
func zclassOf(obj IPersistent) string {
	zc, ok := typeTab[reflect.TypeOf(obj)]
	if !ok {
		return ""
	}
	return zc.class
}

var rIPersistent = reflect.TypeOf((*IPersistent)(nil)).Elem() // typeof(IPersistent)
var rGhostable   = reflect.TypeOf((*Ghostable)(nil)).Elem()   // typeof(Ghostable)
var rStateful    = reflect.TypeOf((*Stateful)(nil)).Elem()    // typeof(Stateful)
var rPyStateful  = reflect.TypeOf((*PyStateful)(nil)).Elem()  // typeof(PyStateful)


// RegisterClass registers ZODB class to correspond to Go type.
//
// typ must embed IPersistent; *typ must implement IPersistent.
//
// typ must be convertible to stateType; stateType must implement Ghostable and
// either Stateful or PyStateful(*)
//
// RegisterClass must be called from global init().
//
// (*) the rationale for stateType coming separately is that this way for
// application types it is possible not to expose Ghostable and Stateful
// methods in their public API.
func RegisterClass(class string, typ, stateType reflect.Type) {
	badf := func(format string, argv ...interface{}) {
		msg := fmt.Sprintf(format, argv...)
		panic(fmt.Sprintf("zodb: register class (%q, %q): %s", class, typ, msg))
	}

	if class == "" {
		badf("class must be not empty")
	}
	if zc, already := classTab[class]; already {
		badf("class already registered for %q", zc.typ)
	}

	// typ must have IPersistent embedded
	// TODO later change to directly embedding non-pointer Persistent | PyPersistent
	// (optimize memory + less allocation)
	basef, ok := typ.FieldByName("IPersistent")
	if !(ok && basef.Anonymous && basef.Type == rIPersistent) {
		badf("type does not embed IPersistent")
	}

	switch {
	case !typ.Implements(rIPersistent):
		// typ must not override IPersistent methods with e.g. different signature
		badf("does not implement IPersistent")

	case !typ.Implements(rGhostable):
		badf("does not implement Ghostable")
	}

	stateful := typ.Implements(rStateful)
	pystateful := typ.Implements(rPyStateful)
	if !(stateful || pystateful) {
		badf("does not implement any of Stateful or PyStateful")
	}

	// find out if typ implements PyStateful and, if yes, use PyPersistent as base
	if pystateful {
		// XXX
	}


	// XXX check if class was already registered
	// XXX check class != ""

	zc := &zclass{class: class, typ: typ, stateType: stateType}
	classTab[class] = zc
	typeTab[typ] = zc
}


// newGhost creates new ghost object corresponding to class and oid.
func (conn *Connection) newGhost(class string, oid Oid) IPersistent {
	// switch on class and transform e.g. "zodb.BTree.Bucket" -> btree.Bucket
	var xpobj reflect.Value // *typ
	zc := classTab[class]
	if zc == nil {
		xpobj = reflect.ValueOf(&Broken{class: class})
	} else {
		xpobj = reflect.New(zc.typ)
	}

	base  := &Persistent{jar: conn, oid: oid, serial: 0, state: GHOST}
	xobj  := xpobj.Elem() // typ
	xobjBase := xobj.FieldByName("IPersistent")
	xobjBase.Set(reflect.ValueOf(base))

	obj := xpobj.Interface()
	base.instance = obj.(interface{IPersistent; Ghostable; Stateful})
	return base.instance
}

// Broken is used for classes that were not registered.
type Broken struct {
	*Persistent
	class string
	state *mem.Buf
}

type brokenState Broken

func (b *brokenState) DropState() {
	b.state.XRelease()
	b.state = nil
}

func (b *brokenState) SetState(state *mem.Buf) error	{
	b.state.XRelease()
	state.Incref()
	b.state = state
	return nil
}

// ----------------------------------------

// wrongClassError is the error cause returned when ZODB object's class is not what was expected.
type wrongClassError struct {
	want, have string
}

func (e *wrongClassError) Error() string {
	return fmt.Sprintf("wrong class: want %q; have %q", e.want, e.have)
}


// get returns in-RAM object corresponding to specified ZODB object according to current database view.
//
// If there is already in-RAM object that corresponds to oid, that in-RAM object is returned.
// Otherwise new in-RAM object is created according to specified class.
//
// The object's data is not necessarily loaded after get returns. Use
// PActivate to make sure the object is fully loaded.
//
// XXX object scope.
//
// Use-case: in ZODB references are (pyclass, oid), so new ghost is created
// without further loading anything.
func (conn *Connection) get(class string, oid Oid) (IPersistent, error) {
	conn.objmu.Lock()		// XXX -> rlock
	wobj := conn.objtab[oid]
	var obj IPersistent
	checkClass := false
	if wobj != nil {
		if xobj := wobj.Get(); xobj != nil {
			obj = xobj.(IPersistent)
		}
	}
	if obj == nil {
		obj = conn.newGhost(class, oid)
		conn.objtab[oid] = weak.NewRef(obj)
	} else {
		checkClass = true
	}
	conn.objmu.Unlock()

	if checkClass {
		if cls := zclassOf(obj); class != cls {
			return nil, &OpError{
				URL:  conn.stor.URL(),
				Op:   fmt.Sprintf("@%s: get", conn.at), // XXX abuse
				Args: oid,
				Err:  &wrongClassError{class, cls},
			}
		}
	}

	return obj, nil
}


// Get returns in-RAM object corresponding to specified ZODB object according to current database view.
//
// If there is already in-RAM object that corresponds to oid, that in-RAM object is returned.
// Otherwise new in-RAM object is created and filled with object's class loaded from the database.
//
// The scope of the object returned is the Connection.	XXX ok?
//
// The object's data is not necessarily loaded after Get returns. Use
// PActivate to make sure the object is fully loaded.
func (conn *Connection) Get(ctx context.Context, oid Oid) (IPersistent, error) {
	conn.objmu.Lock()		// XXX -> rlock
	wobj := conn.objtab[oid]
	var xobj interface{}
	if wobj != nil {
		xobj = wobj.Get()
	}
	conn.objmu.Unlock()

	// object was already there in objtab.
	if xobj != nil {
		return xobj.(IPersistent), nil
	}

	// object is not there in objtab - raw load it, get its class -> get(pyclass, oid)
	// XXX py hardcoded
	class, pystate, serial, err := conn.loadpy(ctx, oid)
	if err != nil {
		return nil, err		// XXX errctx
	}

	obj, err := conn.get(class, oid)
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





// XXX Connection.{Get,get} without py dependency?
// but then how to create a ghost of correct class? -> reflect.Type?

// load loads object specified by oid.
//
// XXX must be called ... (XXX e.g. outside transaction boundary) so that there is no race on .at .
func (conn *Connection) load(ctx context.Context, oid Oid) (_ *mem.Buf, serial Tid, _ error) {
	return conn.stor.Load(ctx, Xid{Oid: oid, At: conn.at})
}
