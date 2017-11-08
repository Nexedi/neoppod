// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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
package zodb

import (
	"context"
	"fmt"
)

// ---- data model ----

// Tid is transaction identifier.
//
// In ZODB transaction identifiers are unique 64-bit integers connected to time
// when corresponding transaction was created.
//
// See also: XTid.
type Tid uint64

// ZODB/py defines maxtid to be max signed int64 since Jun 7 2016:
// https://github.com/zopefoundation/ZODB/commit/baee84a6
// (same in neo/py with "SQLite does not accept numbers above 2^63-1" comment)

const TidMax Tid = 1<<63 - 1 // 0x7fffffffffffffff

// Oid is object identifier.
//
// In ZODB objects are uniquely identified by 64-bit integer.
// Every object can have several revisions - each committed in different transaction.
// The combination of object identifier and particular transaction (serial)
// uniquely addresses corresponding data record.
//
// See also: Xid.
type Oid uint64

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


// DataInfo represents information about one data record.
type DataInfo struct {
	Oid	Oid
	Tid	Tid
	Data	[]byte	// nil means: deleted	XXX -> *Buf ?

	// original tid data was committed at (e.g. in case of undo)
	//
	// FIXME we don't really need this and this unnecessarily constraints interfaces.
	// originates from: https://github.com/zopefoundation/ZODB/commit/2b0c9aa4
	DataTid	Tid
}

// TxnStatus represents status of a transaction
type TxnStatus byte

const (
	TxnComplete TxnStatus = ' ' // completed transaction that hasn't been packed
	TxnPacked             = 'p' // completed transaction that has been packed
	TxnInprogress         = 'c' // checkpoint -- a transaction in progress; it's been thru vote() but not finish()
)


// XTid is "extended" transaction identifier.
//
// It defines a transaction for oid lookup - either exactly by serial, or by < beforeTid.
type XTid struct {
	Tid
	TidBefore bool	// XXX merge into Tid itself (high bit) ?
}

// Xid is "extended" oid = oid + serial/beforeTid, completely specifying object address query.
type Xid struct {
	XTid
	Oid
}

// XXX add XidBefore() and XidSerial() as syntax convenience?

// ---- interfaces ----

// ErrOidMissing is an error which tells that there is no such oid in the database at all
//
// XXX do we need distinction in between ErrOidMissing & ErrXidMissing ?
// (think how client should handle error from Load ?)
type ErrOidMissing struct {
	Oid	Oid
}

func (e ErrOidMissing) Error() string {
	return fmt.Sprintf("%v: no such oid", e.Oid)
}

// ErrXidMissing is an error which tells that oid exists in the database,
// but there is no its revision satisfying xid.XTid search criteria.
type ErrXidMissing struct {
	Xid	Xid
}

func (e *ErrXidMissing) Error() string {
	return fmt.Sprintf("%v: no matching data record found", e.Xid)
}

// IStorage is the interface provided by ZODB storages
type IStorage interface {
	// StorageName returns storage name
	StorageName() string

	// Close closes storage
	Close() error

	// LastTid returns the id of the last committed transaction.
	//
	// If no transactions have been committed yet, LastTid returns Tid zero value.
	LastTid(ctx context.Context) (Tid, error)

	// LastOid returns highest object id of objects committed to storage.
	//
	// if there is no data committed yet, LastOid returns Oid zero value	XXX -> better -1 ?
	// XXX ZODB/py does not define this in IStorage.
	LastOid(ctx context.Context) (Oid, error)

	// Load loads data from database.
	//
	// The object to load is addressed by xid.
	//
	// NOTE ZODB/py provides 2 entrypoints in IStorage: LoadSerial and
	// LoadBefore. Load generalizes them into one (see Xid for details).
	//
	// XXX zodb.loadBefore() returns (data, serial, serial_next) -> add serial_next?
	// XXX currently deleted data is returned as buf.Data=nil	-- is it ok?
	// TODO specify error when data not found -> ErrOidMissing | ErrXidMissing
	Load(ctx context.Context, xid Xid) (buf *Buf, serial Tid, err error)	// XXX -> DataInfo ?

	// Prefetch(ctx, xid Xid)	(no error)

	// TODO add invalidation channel (notify about changes made to DB not by us)

	// Store(oid Oid, serial Tid, data []byte, txn ITransaction) error
	// XXX Restore ?
	// CheckCurrentSerialInTransaction(oid Oid, serial Tid, txn ITransaction)   // XXX naming

	// TODO:
	// tpc_begin(txn)
	// tpc_vote(txn)
	// tpc_finish(txn, callback)    XXX clarify about callback
	// tpc_abort(txn)

	// TODO: History(ctx, oid, size=1)

	// Iterate creates iterator to iterate storage in [tidMin, tidMax] range.
	//
	// XXX allow iteration both ways (forward & backward)
	Iterate(tidMin, tidMax Tid) ITxnIterator	// XXX ctx , error ?
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

// Valid returns whether tid is in valid transaction identifiers range
func (tid Tid) Valid() bool {
	if 0 <= tid && tid <= TidMax {
		return true
	} else {
		return false
	}
}

// Valid returns true if transaction status value is well-known and valid
func (ts TxnStatus) Valid() bool {
	switch ts {
	case TxnComplete, TxnPacked, TxnInprogress:
		return true

	default:
		return false
	}
}
