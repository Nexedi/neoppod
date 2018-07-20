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

// Package zodb defines types, interfaces and errors to work with ZODB databases.
//
// ZODB (http://zodb.org) was originally created in Python world by Jim Fulton et al.
// Data model this package provides is partly based on ZODB/py
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
// Object revision is the same as tid of transaction that modified the object.
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
//
// Operations
//
// A ZODB database can be opened with OpenStorage. Once opened IStorage
// interface is returned that represents access to the database. Please see
// documentation of IStorage, and other interfaces it embeds, for details.
//
//
// --------
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
// 0 is invalid Tid.
type Tid uint64

// ZODB/py defines maxtid to be max signed int64 since Jun 7 2016:
// https://github.com/zopefoundation/ZODB/commit/baee84a6
// (same in neo/py with "SQLite does not accept numbers above 2^63-1" comment)

const TidMax Tid = 1<<63 - 1 // 0x7fffffffffffffff

// Oid is object identifier.
//
// In ZODB objects are uniquely identified by 64-bit integer.
// An object can have several revisions - each committed in different transaction.
// The combination of object identifier and particular transaction (serial)
// uniquely addresses corresponding data record.
//
// 0 is valid Oid and represents root database object.
//
// See also: Xid.
type Oid uint64

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
	TxnComplete TxnStatus = ' ' // completed transaction that hasn't been packed
	TxnPacked             = 'p' // completed transaction that has been packed
	TxnInprogress         = 'c' // checkpoint -- a transaction in progress; it's been thru vote() but not finish()
)


// ---- interfaces ----

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

// OpError is the error returned by IStorageDriver operations.
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
	s += ": " + e.Err.Error()
	return s
}

func (e *OpError) Cause() error {
	return e.Err
}


// IStorage is the interface provided by opened ZODB storage.
type IStorage interface {
	IStorageDriver

	Prefetcher
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


// Notifier allows to be notified of database changes made by other clients.
type Notifier interface {
	// TODO: invalidation channel (notify about changes made to DB not by us from outside)
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
