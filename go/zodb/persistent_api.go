// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018-2019  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE

package zodb
// IPersistent.

import (
	"context"
)

// IPersistent is the interface that every in-RAM object representing any database object implements.
//
// It is based on IPersistent from ZODB/py:
//
//	https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/persistent/interfaces.py#L22
//
// but is not exactly equal to it.
//
// It is safe to access IPersistent from multiple goroutines simultaneously.
//
// Use Persistent as the base for application-level types that need to provide persistency.
type IPersistent interface {
	PJar() *Connection // Connection this in-RAM object is part of.
	POid() Oid         // object ID in the database.

	// object serial in the database as of particular Connection (PJar) view.
	// InvalidTid if not yet loaded.
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


	// IPersistent can be implemented only by objects that embed Persistent.
	persistent() *Persistent
}

// ObjectState describes state of in-RAM object.
type ObjectState int

const (
	GHOST    ObjectState = -1 // object data is not yet loaded from the database
	UPTODATE ObjectState = 0  // object is live and in-RAM data is the same as in database
	CHANGED  ObjectState = 1  // object is live and in-RAM data was changed
	// no STICKY - we pin objects in RAM with PActivate
)
