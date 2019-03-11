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
// persistent objects.

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"
)

// Persistent is common base IPersistent implementation for in-RAM
// representation of database objects.
//
// To use - a class needs to embed Persistent and register itself additionally
// providing Ghostable and (Py)Stateful methods. For example:
//
//	type MyObject struct {
//		Persistent
//		...
//	}
//
//	type myObjectState MyObject
//
//	func (o *myObjectState) DropState() { ... }
//	func (o *myObjectState) GetState() *mem.Buf { ... }
//	func (o *myObjectState) SetState(state *mem.Buf) error { ... }
//
//	func init() {
//		t := reflect.TypeOf
//		zodb.RegisterClass("mymodule.MyObject", t(MyObject{}), t(myObjectState))
//	}
//
// A new instance of the class that embeds Persistent must be created via
// NewPersistent, for example:
//
//	obj := zodb.NewPersistent(reflect.TypeOf(MyObject{}), jar).(*MyObject)
type Persistent struct {
	// ZODB class of this object.
	// XXX it could be deduced via typeTab[reflect.TypeOf(.instance)]
	// XXX or better drop .instance and deduce it via casting to .zclass.typ
	zclass *zclass

	jar    *Connection
	oid    Oid
	serial Tid // also protected by mu

	mu     sync.Mutex
	state  ObjectState
	refcnt int32

	// Persistent is the base for the instance.
	// instance, via its state type, is additionally Ghostable and (Stateful | PyStateful).
	instance IPersistent
	loading  *loadState
}

func (obj *Persistent) persistent() *Persistent { return obj }

func (obj *Persistent) PJar() *Connection { return obj.jar }
func (obj *Persistent) POid() Oid         { return obj.oid }

func (obj *Persistent) PSerial() Tid {
	obj.mu.Lock()
	defer obj.mu.Unlock()
	return obj.serial
}

// loadState indicates object's load state/result.
//
// when !ready the loading is in progress.
// when ready the loading has been completed.
type loadState struct {
	ready chan struct{} // closed when loading finishes

	// error from the load.
	// if there was no error, loaded data goes to object state.
	err error
}

// Ghostable is the interface describing in-RAM object who can release its in-RAM state.
type Ghostable interface {
	// DropState should discard in-RAM object state.
	//
	// It is called by persistency machinery only on non-ghost objects,
	// i.e. when the objects has its in-RAM state.
	DropState()
}

// Stateful is the interface describing in-RAM object whose data state can be
// exchanged as raw bytes.
type Stateful interface {
	// GetState should return state of the in-RAM object as raw data.
	//
	// It is called by persistency machinery only on non-ghost objects,
	// i.e. when the object has its in-RAM state.
	//
	// GetState should return a new buffer reference.
	GetState() *mem.Buf

	// SetState should set state of the in-RAM object from raw data.
	//
	// It is called by persistency machinery only on ghost objects, i.e.
	// when the objects does not yet have its in-RAM state.
	//
	// state ownership is not passed to SetState, so if state needs to be
	// retained after SetState returns it needs to be incref'ed.
	//
	// The error returned does not need to have object/setstate prefix -
	// persistent machinery is adding such prefix automatically.
	SetState(state *mem.Buf) error
}

// ---- RAM → DB: serialize ----

// pSerialize returns object in serialized form to be saved in the database.
//
// pSerialize is non-public method that is exposed and used only by ZODB internally.
// pSerialize is called only on non-ghost objects.
func (obj *Persistent) pSerialize() *mem.Buf {
	obj.mu.Lock()
	defer obj.mu.Unlock()
	if obj.state == GHOST {
		panic(obj.badf("serialize: ghost object"))
	}

	switch istate := obj.istate().(type) {
	case Stateful:
		return istate.GetState()

	case PyStateful:
		return pyGetState(istate, ClassOf(obj.instance))

	default:
		panic(obj.badf("serialize: !stateful instance"))
	}
}

// ---- RAM ← DB: activate/deactivate/invalidate ----

// PActivate implements IPersistent.
func (obj *Persistent) PActivate(ctx context.Context) (err error) {
	obj.mu.Lock()
	obj.refcnt++
	doload := (obj.refcnt == 1 && obj.state == GHOST)
	defer func() {
		if err != nil {
			obj.PDeactivate()
			xerr.Contextf(&err, "%s(%s): activate", obj.zclass.class, obj.oid)
		}
	}()
	//fmt.Printf("activate %p(%v)\t%T(%s): refcnt=%d state=%v\n",
	//	obj, obj.zclass, obj.instance, obj.oid, obj.refcnt, obj.state)
	if !doload {
		// someone else is already activated/activating the object.
		// wait for its loading to complete and we are done.
		loading := obj.loading
		obj.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-loading.ready:
			return loading.err
		}
	}

	if obj.loading != nil {
		obj.mu.Unlock()
		panic(obj.badf("activate: need to load, but .loading != nil"))
	}

	// we become responsible for loading the object
	loading := &loadState{ready: make(chan struct{})}
	obj.loading = loading
	obj.mu.Unlock()

	// do the loading outside of obj lock
	state, serial, err := obj.jar.load(ctx, obj.oid)

	// relock the object
	obj.mu.Lock()

	if l, s := obj.loading, obj.state; !(l == loading && s == GHOST) {
		obj.mu.Unlock()
		panic(obj.badf("activate: after load: object state unexpected: "+
			"%v (want %v); .loading = %p (want %p)", s, GHOST, l, loading))
	}

	obj.serial = serial

	// try to pass loaded state to object
	if err == nil {
		switch istate := obj.istate().(type) {
		case Stateful:
			err = istate.SetState(state)
			xerr.Context(&err, "setstate")

		case PyStateful:
			err = pySetState(istate, obj.zclass.class, state, obj.jar)
			xerr.Context(&err, "pysetstate")

		default:
			panic(obj.badf("activate: !stateful instance"))
		}

		state.Release()
		if err == nil {
			obj.state = UPTODATE
		}
	}

	// XXX set state to load error? (to avoid panic on second activate after load error)
	loading.err = err

	obj.mu.Unlock()
	close(loading.ready)

	return err
}

// PDeactivate implements IPersistent.
func (obj *Persistent) PDeactivate() {
	obj.mu.Lock()
	defer obj.mu.Unlock()

	obj.refcnt--
	if obj.refcnt < 0 {
		panic(obj.badf("deactivate: refcnt < 0  (= %d)", obj.refcnt))
	}
	if obj.refcnt > 0 {
		return // users still left
	}

	// no users left. Let's see whether we should transition this object to ghost.
	if obj.state >= CHANGED {
		return
	}

	// TODO try to keep some pool of object in live state so that there is
	// no constant load/unload on object access. XXX  -> MRU cache?
	// NOTE wcfs manages its objects explicitly and does not need this.

	var cp PCachePolicy
	if cc := obj.jar.cache.control; cc != nil {
		// XXX catch inconsistency in PCacheClassify result
		// XXX locking for .control ?
		cp = cc.PCacheClassify(obj.instance)
	}

	if cp & PCacheKeepState != 0 {
		return
	}

	// already ghost
	if obj.state == GHOST {
		return
	}

	// XXX cp & PCacheNonTemporal -> drop unconditionally; otherwise -> LRU

	obj.serial = InvalidTid
	obj.istate().DropState()
	obj.state = GHOST
	obj.loading = nil
}

// PInvalidate() implements IPersistent.
func (obj *Persistent) PInvalidate() {
	obj.mu.Lock()
	defer obj.mu.Unlock()

	if obj.refcnt != 0 {
		// object is currently in use
		panic(obj.badf("invalidate: refcnt != 0  (= %d)", obj.refcnt))
	}

	// already ghost
	if obj.state == GHOST {
		return
	}

	obj.serial = InvalidTid
	obj.istate().DropState()
	obj.state = GHOST
	obj.loading = nil
}

// istate returns .instance casted to corresponding stateType.
//
// returns: Ghostable + (Stateful | PyStateful).
func (obj *Persistent) istate() Ghostable {
	xstateful := reflect.ValueOf(obj.instance).Convert(reflect.PtrTo(obj.zclass.stateType))
	return xstateful.Interface().(Ghostable)
}

// badf returns formatted error prefixed with obj's class and oid.
func (obj *Persistent) badf(format string, argv ...interface{}) error {
	return fmt.Errorf("%s(%s): "+format,
		append([]interface{}{obj.zclass.class, obj.oid}, argv...)...)
}


// ---- class <-> type; new ghost ----

// zclass describes one ZODB class in relation to a Go type.
type zclass struct {
	class     string
	typ       reflect.Type // application go type corresponding to class
	stateType reflect.Type // *typ and *stateType are convertible; *stateType provides Stateful & co.
}

var classTab = make(map[string]*zclass)       // {} class -> zclass
var typeTab  = make(map[reflect.Type]*zclass) // {} type  -> zclass

// ClassOf returns ZODB class of a Go object.
//
// The following is returned:
//
//	- if obj's type was registered (RegisterClass) -- corresponding class.
//	- for Broken objects -- ZODB.Broken("<broken-class>").
//	- else -- ZODB.Go("<fully-qualified-type(obj)>")
//
// XXX -> IPersistent.ZClass() ?
func ClassOf(obj IPersistent) string {
	zb, broken := obj.(*Broken)
	if broken {
		return fmt.Sprintf("ZODB.Broken(%q)", zb.class)
	}

	typ := reflect.TypeOf(obj)
	typ = typ.Elem() // *MyPersistent -> MyPersistent
	zc, ok := typeTab[typ]
	if ok {
		return zc.class
	}

	// the type was not registered to ZODB
	fullType := typ.PkgPath()
	if typ.PkgPath() != "" {
		fullType += "."
	}
	fullType += typ.Name()
	if fullType == "" {
		// fallback, since it is possible if the type is anonymous
		// XXX not fully qualified
		fullType = fmt.Sprintf("*%T", typ)
	}
	return fmt.Sprintf("ZODB.Go(%q)", fullType)
}

var rIPersistent = reflect.TypeOf((*IPersistent)(nil)).Elem() // typeof(IPersistent)
var rPersistent  = reflect.TypeOf(Persistent{})               // typeof(Persistent)
var rGhostable   = reflect.TypeOf((*Ghostable)(nil)).Elem()   // typeof(Ghostable)
var rStateful    = reflect.TypeOf((*Stateful)(nil)).Elem()    // typeof(Stateful)
var rPyStateful  = reflect.TypeOf((*PyStateful)(nil)).Elem()  // typeof(PyStateful)

// RegisterClass registers ZODB class to correspond to Go type.
//
// Only registered classes can be saved to database, and are converted to
// corresponding application-level objects on load. When ZODB loads an object
// whose class is not known, it will represent it as Broken object.
//
// class is a full class path for registered class, e.g. "BTrees.LOBTree.LOBucket".
// typ is Go type corresponding to class.
//
// typ must embed Persistent; *typ must implement IPersistent.
//
// typ must be convertible to stateType; stateType must implement Ghostable and
// either Stateful or PyStateful(*).
//
// RegisterClass must be called from global init().
//
// (*) the rationale for stateType coming separately is that this way for
// application types it is possible not to expose Ghostable and Stateful
// methods in their public API.
func RegisterClass(class string, typ, stateType reflect.Type) {
	badf := func(format string, argv ...interface{}) {
		msg := fmt.Sprintf(format, argv...)
		panic(fmt.Sprintf("zodb: register class (%q, %q, %q): %s", class, typ, stateType, msg))
	}

	if class == "" {
		badf("class must be not empty")
	}
	if zc, already := classTab[class]; already {
		badf("class already registered for type %q", zc.typ)
	}
	if zc, already := typeTab[typ]; already {
		badf("type already registered for class %q", zc.class)
	}

	// typ must embed Persistent
	basef, ok := typ.FieldByName("Persistent")
	if !(ok && basef.Anonymous && basef.Type == rPersistent) {
		badf("%q does not embed Persistent", typ)
	}

	ptype := reflect.PtrTo(typ)
	ptstate := reflect.PtrTo(stateType)

	switch {
	case !ptype.Implements(rIPersistent):
		badf("%q does not implement IPersistent", ptype)

	case !ptstate.Implements(rGhostable):
		badf("%q does not implement Ghostable", ptstate)
	}

	stateful := ptstate.Implements(rStateful)
	pystateful := ptstate.Implements(rPyStateful)
	if !(stateful || pystateful) {
		badf("%q does not implement any of Stateful or PyStateful", ptstate)
	}

	zc := &zclass{class: class, typ: typ, stateType: stateType}
	classTab[class] = zc
	typeTab[typ] = zc
}

// RegisterClassAlias registers alias for a ZODB class.
//
// When ZODB loads an object whose class is alias, it will be handled like
// object with specified ZODB class.
//
// Class aliases are useful for backward compatibility - sometimes class name
// of an object changes, but to support loading previously-saved objects, the
// old class name has to be also supported.
func RegisterClassAlias(alias, class string) {
	badf := func(format string, argv ...interface{}) {
		msg := fmt.Sprintf(format, argv...)
		panic(fmt.Sprintf("zodb: register class alias (%q -> %q): %s", alias, class, msg))
	}

	if alias == "" {
		badf("alias must be not empty")
	}
	if class == "" {
		badf("class must be not empty")
	}
	if zc, already := classTab[alias]; already {
		badf("class %q already registered for type %q", alias, zc.typ)
	}
	if _, already := classTab[class]; !already {
		badf("class %q is not yet registered", class)
	}

	classTab[alias] = classTab[class]
	// don't touch typeTab - this way type -> zclass will always go to
	// original class, not alias.
}

// NewPersistent creates new instance of persistent type.
//
// typ must embed Persistent and must be registered with RegisterClass.
//
// Created instance will be associated with jar, but will have no oid assigned
// until transaction commit.
func NewPersistent(typ reflect.Type, jar *Connection) IPersistent {
	zc := typeTab[typ]
	if zc == nil {
		panic(fmt.Sprintf("new persistent: type %s not registered", typ))
	}

	xpobj := reflect.New(zc.typ)
	return persistentInit(xpobj, zc, jar, InvalidOid, InvalidTid, UPTODATE /*XXX ok?*/)
}

// newGhost creates new ghost object corresponding to class, oid and jar.
//
// Returned object's PJar is set to provided jar. However the object is not
// registered to jar in any way. The caller must complete created object
// registration to jar by himself.
//
// If class was not registered, a Broken object is returned.
func newGhost(class string, oid Oid, jar *Connection) IPersistent {
	// switch on class and transform e.g. "BTrees.Bucket" -> btree.Bucket
	var xpobj reflect.Value // *typ
	zc := classTab[class]
	if zc == nil {
		zc = brokenZClass
		xpobj = reflect.ValueOf(&Broken{class: class})
	} else {
		xpobj = reflect.New(zc.typ)
	}

	return persistentInit(xpobj, zc, jar, oid, InvalidTid, GHOST)
}

// persistentInit inits Persistent embedded into an object and returns .instance .
func persistentInit(xpobj reflect.Value, zc *zclass, jar *Connection, oid Oid, serial Tid, state ObjectState) IPersistent {
	xobj := xpobj.Elem() // typ
	pbase := xobj.FieldByName("Persistent").Addr().Interface().(*Persistent)
	pbase.zclass = zc
	pbase.jar = jar
	pbase.oid = oid
	pbase.serial = serial
	pbase.state = state

	if state > GHOST {
		// if state is not ghost, init loading state so that activate works.
		pbase.loading = &loadState{ready: make(chan struct{})}
		close(pbase.loading.ready)
	}

	obj := xpobj.Interface()
	pbase.instance = obj.(IPersistent)
	return pbase.instance
}

// Broken objects are used to represent loaded ZODB objects with classes that
// were not registered to zodb Go package.
//
// See RegisterClass for details.
type Broken struct {
	Persistent
	class string
	state *mem.Buf
}

// XXX register (Broken, brokenState) ?
var _ interface { Ghostable; Stateful} = (*brokenState)(nil)

type brokenState Broken // hide state methods from public API

func (b *brokenState) DropState() {
	b.state.XRelease()
	b.state = nil
}

func (b *brokenState) GetState() *mem.Buf {
	b.state.Incref()
	return b.state
}

func (b *brokenState) SetState(state *mem.Buf) error {
	b.state.XRelease()
	state.Incref()
	b.state = state
	return nil
}

// brokenZClass is used for Persistent.zclass for Broken objects.
var brokenZClass = &zclass{
	class:     "",
	typ:       reflect.TypeOf(Broken{}),
	stateType: reflect.TypeOf(brokenState{}),
}


// ---- basic persistent objects provided by zodb ----

// Map is equivalent of persistent.mapping.PersistentMapping in ZODB/py.
type Map struct {
	Persistent

	// XXX it is not possible to embed map. And even if we embed a map via
	// another type = map, then it is not possible to use indexing and
	// range over Map. -> just provide access to the map as .Data .
	Data map[interface{}]interface{}
}

type mapState Map // hide state methods from public API

func (m *mapState) DropState() {
	m.Data = nil
}

func (m *mapState) PyGetState() interface{} {
	return map[interface{}]interface{}{
		"data": m.Data,
	}
}

func (m *mapState) PySetState(pystate interface{}) error {
	// before 2009 PersistentMapping could keep data in ._container, not .data
	// https://github.com/zopefoundation/ZODB/commit/aa1f2622e1
	xdata, err := pystateDict1(pystate, "data", "_container")
	if err != nil {
		return err
	}

	data, ok := xdata.(map[interface{}]interface{})
	if !ok {
		return fmt.Errorf("state data must be dict, not %T", xdata)
	}

	m.Data = data
	return nil
}

// List is equivalent of persistent.list.PersistentList in ZODB/py.
type List struct {
	Persistent

	// XXX it is not possible to embed slice - see Map for similar issue and more details.
	Data []interface{}
}

type listState List // hide state methods from public API

func (l *listState) DropState() {
	l.Data = nil
}

func (l *listState) PyGetState() interface{} {
	return map[interface{}]interface{}{
		"data": l.Data,
	}
}

func (l *listState) PySetState(pystate interface{}) error {
	xdata, err := pystateDict1(pystate, "data")
	if err != nil {
		return err
	}

	data, ok := xdata.([]interface{})
	if !ok {
		return fmt.Errorf("state data must be list, not %T", xdata)
	}

	l.Data = data
	return nil
}

// pystateDict1 decodes pystate that is expected to be {} with single key and
// returns data for that key.
func pystateDict1(pystate interface{}, acceptKeys ...string) (data interface{}, _ error) {
	d, ok := pystate.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("state must be dict, not %T", pystate)
	}

	if l := len(d); l != 1 {
		return nil, fmt.Errorf("state dict has %d keys, must be only 1", l)
	}

	for _, key := range acceptKeys {
		data, ok := d[key]
		if ok {
			return data, nil
		}
	}

	return nil, fmt.Errorf("noone of %q is present in state dict", acceptKeys)
}

func init() {
	t := reflect.TypeOf
	RegisterClass("persistent.mapping.PersistentMapping", t(Map{}),  t(mapState{}))
	RegisterClass("persistent.list.PersistentList",       t(List{}), t(listState{}))

	// PersistentMapping was also available as PersistentDict for some time
	RegisterClassAlias("persistent.dict.PersistentDict", "persistent.mapping.PersistentMapping")
}
