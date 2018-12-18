// Copyright (C) 2016-2018  Nexedi SA and Contributors.
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

// Package zodb provides API to work with ZODB databases.
//
// ZODB (http://zodb.org) was originally created in Python world by Jim Fulton et al.
// Data model and API this package provides are partly based on ZODB/py
// (https://github.com/zopefoundation/ZODB) to maintain compatibility in
// between Python and Go implementations.
//
// Data model
//
// A ZODB database is conceptually modeled as transactional log of changes to objects.
// Oid identifies an object and Tid - a transaction. A transaction can change
// several objects and also has metadata, like user and description, associated
// with it. If an object is changed by transaction, it is said that there is
// revision of the object with particular object state committed by that transaction.
// Object revision is the same as tid of the transaction that modified the object.
// The combination of object identifier and particular revision (serial)
// uniquely addresses corresponding data record.
//
// Tids of consecutive database transactions are monotonically increasing and
// are connected with time when transaction in question was committed.
// This way, besides identifying a transaction with changes, Tid can also be
// used to specify whole database state constructed by all cumulated
// transaction changes from database beginning up to, and including,
// transaction specified by it. Xid is "extended" oid that specifies particular
// object state: it is (oid, at) pair that is mapped to object's latest
// revision with serial ≤ at.
//
// Object state data is generally opaque, but is traditionally based on Python
// pickles in ZODB/py world.
//
// An object can reference other objects in the database by their oid.
//
//
// Storage layer
//
// The storage layer provides access to a ZODB database in terms of database
// records with raw bytes payload.
//
// At storage level a ZODB database can be opened with OpenStorage. Once opened
// IStorage interface is returned that represents access to the database.
// Please see IStorage, and interfaces it embeds, for details.
//
//
// Application layer
//
// The application layer provides access to a ZODB database in terms of in-RAM
// application-level objects whose in-RAM state is synchronized with data in the database. For
// the synchronization to work, objects must be explicitly activated before
// access (contrary to zodb/py where activation is implicit, hooked into
// __getattr__), for example:
//
//	var obj *MyObject // *MyObject must implement IPersistent (see below)
//	... // init obj pointer, usually by traversing from another persistent object.
//
//	// make sure object's in-RAM data is present.
//	//
//	// ZODB will load corresponding data and decode it into obj.
//	// On success, obj will be live and application can use its state.
//	err := obj.PActivate(ctx)
//	if err != nil {
//		return ... // handle error
//	}
//
//	obj.xxx // use object.
//	if ... {
//		obj.xxx++     // change object.
//		obj.PModify() // let persistency layer know we modified the object.
//	}
//
//	// tell persistency layer we no longer need obj's in-RAM data to be present.
//	// if obj was not modified, its in-RAM state might go away after.
//	obj.PDeactivate()
//
// IPersistent interface describes the details of the activation protocol.
//
// For MyObject to implement IPersistent it must embed Persistent type.
// MyObject also has to register itself to persistency machinery with RegisterClass.
//
// In-RAM application objects are handled in groups. During the scope of
// corresponding in-progress transaction(*), a group corresponds to particular
// view of the database (at) and has isolation guarantee from further database
// transactions, and from in-progress changes to in-RAM objects in other
// groups.
//
// If object₁ references object₂ in the database, the database reference will
// be represented with corresponding reference between in-RAM application
// objects. If there are multiple database references to one object, it will be
// represented by the same number of references to only one in-RAM application object.
// An in-RAM application object can have reference to another in-RAM
// application object only from the same group(+).
// Reference cycles are also allowed. In general objects graph in the database
// is isomorphly mapped to application objects graph in RAM.
//
// A particular view of the database together with corresponding group of
// application objects isolated for modifications is represented by Connection.
// Connection is also sometimes called a "jar" in ZODB terminology.
//
// DB represents a handle to database at application level and contains pool
// of connections. DB.Open opens database connection. The connection will be
// automatically put back into DB pool for future reuse after corresponding
// transaction is complete. DB thus provides service to maintain live objects
// cache and reuse live objects from transaction to transaction.
//
// Note that it is possible to have several DB handles to the same database.
// This might be useful if application accesses distinctly different sets of
// objects in different transactions and knows beforehand which set it will be
// next time. Then, to avoid huge live cache misses, it makes sense to keep DB
// handles opened for every possible case of application access.
//
//
// All DB, Connection and object activation protocol is safe to access from
// multiple goroutines simultaneously.
//
//
// --------
//
// (*) see package lab.nexedi.com/kirr/neo/go/transaction.
// (+) if both objects are from the same database.
//
// Python data
//
// To maintain database data compatibility with ZODB/py, ZODB/go provides
// first class support for Python data. At storage-level PyData provides way to
// treat raw data record content as serialized by ZODB/py, and at application
// level types that are registered with state type providing PyStateful (see
// RegisterClass) are automatically (de)serialized as Python pickles(*).
//
// An example of application-level type with ZODB/py compatibility can be seen in
// package lab.nexedi.com/kirr/neo/go/zodb/btree which provides BTree handling
// for ZODB/go.
//
// --------
//
// (*) for pickle support package github.com/kisielk/og-rek is used.
//
//
// Storage drivers
//
// To implement a ZODB storage one need to provide IStorageDriver interface and
// register it to ZODB with RegisterDriver. Package
// lab.nexedi.com/kirr/neo/go/zodb/wks links-in and registers support for
// well-known ZODB storages, such as FileStorage, ZEO and NEO.
//
//
// Miscellaneous
//
// See also package lab.nexedi.com/kirr/neo/go/zodb/zodbtools and associated
// zodb command that provide tools for managing ZODB databases.
package zodb

import (
	"context"
	"fmt"

	"lab.nexedi.com/kirr/go123/mem"
)

// ---- data model ----

// Tid is transaction identifier.
//
// In ZODB transaction identifiers are unique 64-bit integers corresponding to
// time when transaction in question was committed.
//
// This way tid can also be used to specify whole database state constructed
// by all cumulated transaction changes from database beginning up to, and
// including, transaction specified by tid.
//
// 0 is invalid Tid, but canonical invalid Tid value is InvalidTid.
type Tid uint64

// ZODB/py defines maxtid to be max signed int64 since Jun 7 2016:
// https://github.com/zopefoundation/ZODB/commit/baee84a6
// (same in neo/py with "SQLite does not accept numbers above 2^63-1" comment)

const (
	TidMax     Tid = 1<<63 - 1 // 0x7fffffffffffffff
	InvalidTid Tid = 1<<64 - 1 // 0xffffffffffffffff
)

// Oid is object identifier.
//
// In ZODB objects are uniquely identified by 64-bit integer.
// An object can have several revisions - each committed in different transaction.
// The combination of object identifier and particular transaction (serial)
// uniquely addresses corresponding data record.
//
// 0 is valid Oid and represents root database object.
// InvalidOid represents an invalid Oid.
//
// See also: Xid.
type Oid uint64

const InvalidOid Oid = 1<<64 - 1 // 0xffffffffffffffff

// Xid is "extended" oid - that fully specifies object and query for its revision.
//
// At specifies whole database state at which object identified with Oid should
// be looked up. The object revision is taken from latest transaction modifying
// the object with tid ≤ At.
//
// Note that Xids are not unique - the same object revision can be addressed
// with several xids.
//
// See also: Tid, Oid.
type Xid struct {
	At  Tid
	Oid Oid
}


// TxnInfo is metadata information about one transaction.
type TxnInfo struct {
	Tid         Tid
	Status      TxnStatus
	User        []byte
	Description []byte

	// additional information about transaction. ZODB/py usually puts py
	// dict here but it can be arbitrary raw bytes.
	Extension   []byte
}

// DataInfo is information about one object change.
type DataInfo struct {
	Oid	Oid
	Tid	Tid    // changed by this transaction
	Data	[]byte // new object data; nil if object becomes deleted

	// DataTidHint is optional hint from a storage that the same data was
	// already originally committed in earlier transaction, for example in
	// case of undo. It is 0 if there is no such hint.
	//
	// Storages are not obliged to provide this hint, and in particular it
	// is valid for a storage to always return this as zero.
	//
	// In ZODB/py world this originates from
	// https://github.com/zopefoundation/ZODB/commit/2b0c9aa4.
	DataTidHint Tid
}

// TxnStatus represents status of a transaction.
type TxnStatus byte

const (
	TxnComplete   TxnStatus = ' ' // completed transaction that hasn't been packed
	TxnPacked     TxnStatus = 'p' // completed transaction that has been packed
	TxnInprogress TxnStatus = 'c' // checkpoint -- a transaction in progress; it's been thru vote() but not finish()
)


// ---- storage interfaces ----

// NoObjectError is the error which tells that there is no such object in the database at all.
type NoObjectError struct {
	Oid Oid
}

func (e NoObjectError) Error() string {
	return fmt.Sprintf("%s: no such object", e.Oid)
}

// NoDataError is the error which tells that object exists in the database,
// but there is no its non-empty revision satisfying search criteria.
type NoDataError struct {
	Oid Oid

	// DeletedAt explains object state wrt used search criteria:
	// - 0:  object was not created at time of searched xid.At
	// - !0: object was deleted by transaction with tid=DeletedAt
	DeletedAt Tid
}

func (e *NoDataError) Error() string {
	if e.DeletedAt == 0 {
		return fmt.Sprintf("%s: object was not yet created", e.Oid)
	} else {
		return fmt.Sprintf("%s: object was deleted @%s", e.Oid, e.DeletedAt)
	}
}

// OpError is the error returned by IStorageDriver operations.	XXX -> by ZODB operations?
type OpError struct {
	URL  string	 // URL of the storage
	Op   string	 // operation that failed
	Args interface{} // operation arguments, if any
	Err  error	 // actual error that occurred during the operation
}

func (e *OpError) Error() string {
	s := e.URL + ": " + e.Op
	if e.Args != nil {
		s += fmt.Sprintf(" %s", e.Args)
	}
	// XXX if e.Err = OpError with the same URL - don't print URL twice.
	s += ": " + e.Err.Error()
	return s
}

func (e *OpError) Cause() error {
	return e.Err
}


// IStorage is the interface provided by opened ZODB storage.
type IStorage interface {
	//IStorageDriver

	// same as in IStorageDriver
	URL() string
	Close() error
	LastTid(context.Context) (Tid, error)
	Loader
	Iterator
	// no watcher

	// additional to IStorageDriver
	Prefetcher
	//XXXNotifier() -> Notifier // dedicated notifier for every open?
}

// Prefetcher provides functionality to prefetch objects.
type Prefetcher interface {
	// Prefetch prefetches object addressed by xid.
	//
	// If data is not yet in cache loading for it is started in the background.
	// Prefetch is not blocking operation and does not wait for loading, if any was
	// started, to complete.
	//
	// Prefetch does not return any error.
	// Prefetch is noop if storage was opened with NoCache option.
	Prefetch(ctx context.Context, xid Xid)
}

// IStorageDriver is the raw interface provided by ZODB storage drivers.
type IStorageDriver interface {
	// URL returns URL of how the storage was opened
	URL() string

	// Close closes storage
	Close() error

	// LastTid returns the id of the last committed transaction.
	//
	// If no transactions have been committed yet, LastTid returns 0.
	LastTid(ctx context.Context) (Tid, error)

	Loader
	Iterator

	// A storage driver also delivers database change events to watchq
	// channel, which is passed to it when the driver is created.

/* XXX kill
	Watcher

	// Notifier returns storage driver notifier.
	//
	// The notifier represents invalidation channel (notify about changes
	// made to DB).	XXX
	//
	// To simplify drivers, there must be only 1 logical user of
	// storage-driver level notifier interface. Contrary IStorage allows
	// for several users of notification channel.	XXX ok?
	//Notifier() Notifier

	// XXX Watch() -> Watcher
	// XXX SetWatcher(watchq)	SetWatchSink() ? XXX -> ctor ?
*/
}

// Loader provides functionality to load objects.
type Loader interface {
	// Load loads object data addressed by xid from database.
	//
	// Returned are:
	//
	//	- if there is data to load: buf is non-empty, serial indicates
	//	  transaction which matched xid criteria and err=nil.
	//
	//	  caller must not modify buf memory.
	//
	// otherwise buf=nil, serial=0 and err is *OpError with err.Err
	// describing the error cause:
	//
	//	- *NoObjectError if there is no such object in database at all,
	//	- *NoDataError   if object exists in database but there is no
	//	                 its data matching xid,
	//	- some other error indicating e.g. IO problem.
	//
	//
	// NOTE 1: ZODB/py provides 2 entrypoints in IStorage for loading:
	// loadSerial and loadBefore but in ZODB/go we have only Load which is
	// a bit different from both:
	//
	//	- Load loads object data for object at database state specified by xid.At
	//	- loadBefore loads object data for object at database state previous to xid.At
	//	  it is thus equivalent to Load(..., xid.At-1)
	//	- loadSerial loads object data from revision exactly modified
	//	  by transaction with tid = xid.At.
	//	  it is thus equivalent to Load(..., xid.At) with followup
	//	  check that returned serial is exactly xid.At(*)
	//
	// (*) loadSerial is used only in a few places in ZODB/py - mostly in
	//     conflict resolution code where plain Load semantic - without
	//     checking object was particularly modified at that revision - would
	//     suffice.
	//
	// NOTE 2: in ZODB/py loadBefore, in addition to serial, also returns
	// serial_next, which constraints storage implementations unnecessarily
	// and is used only in client cache.
	//
	// In ZODB/go Cache shows that it is possible to build efficient client
	// cache without serial_next returned from Load. For this reason in ZODB/go
	// Load specification comes without specifying serial_next return.
	Load(ctx context.Context, xid Xid) (buf *mem.Buf, serial Tid, err error)
}

// Committer provides functionality to commit transactions.
type Committer interface {
	// TODO: write mode

	// Store(ctx, oid Oid, serial Tid, data []byte, txn ITransaction) error
	// StoreKeepCurrent(ctx, oid Oid, serial Tid, txn ITransaction)

	// TpcBegin(txn)
	// TpcVote(txn)
	// TpcFinish(txn, callback)
	// TpcAbort(txn)
}


/*
// Notifier allows to be notified of changes made to database by other clients.
type Notifier interface {

	// Read returns next notification event.
	//
	// XXX ...
	// XXX overflow -> special error
	Read(ctx context.Context) (Tid, []Oid, error)
}
*/

// WatchEvent is one event describing observed database change.
type WatchEvent struct {
	Tid     Tid
	Changev []Oid	// XXX name
}

// Watcher allows to be notified of changes to database.
type Watcher interface {

	// Watch waits-for and returns next event corresponding to comitted transaction.
	//
	// XXX queue overflow -> special error?
	Watch(ctx context.Context) (Tid, []Oid, error)
}


	// TODO: History(ctx, oid, size=1)

// Iterator provides functionality to iterate through storage transactions sequentially.
type Iterator interface {
	// Iterate creates iterator to iterate storage in [tidMin, tidMax] range.
	//
	// Iterate does not return any error. If there was error when setting
	// iteration up - it will be returned on first NextTxn call.
	//
	// TODO allow iteration both ways (forward & backward)
	Iterate(ctx context.Context, tidMin, tidMax Tid) ITxnIterator
}

// ITxnIterator is the interface to iterate transactions.
type ITxnIterator interface {
	// NextTxn yields information about next database transaction:
	// 1. transaction metadata, and
	// 2. iterator over transaction's data records.
	// transaction metadata stays valid until next call to NextTxn().
	// end of iteration is indicated with io.EOF
	NextTxn(ctx context.Context) (*TxnInfo, IDataIterator, error)
}

// IDataIterator is the interface to iterate data records.
type IDataIterator interface {
	// NextData yields information about next storage data record.
	// returned data stays valid until next call to NextData().
	// end of iteration is indicated with io.EOF
	NextData(ctx context.Context) (*DataInfo, error)
}

// ---- misc ----

// Valid returns whether tid is in valid transaction identifiers range.
func (tid Tid) Valid() bool {
	// NOTE 0 is invalid tid
	if 0 < tid && tid <= TidMax {
		return true
	} else {
		return false
	}
}

// Valid returns true if transaction status value is well-known and valid.
func (ts TxnStatus) Valid() bool {
	switch ts {
	case TxnComplete, TxnPacked, TxnInprogress:
		return true

	default:
		return false
	}
}
