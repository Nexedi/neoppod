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

// XXX partly based on ZODB/py

// Package zodb defines types, interfaces and errors used in ZODB databases
package zodb

import (
	"context"
	"fmt"
)

// ZODB types
type Tid uint64  // transaction identifier
type Oid uint64  // object identifier

// XTid is "extended" transaction identifier. It defines a transaction for
// oid lookup - either exactly by serial, or by < beforeTid.
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

const (
	//Tid0	Tid = 0			// XXX -> simply Tid(0) ?
	TidMax	Tid = 1<<63 - 1		// 0x7fffffffffffffff
					// ZODB defines maxtid to be max signed int64 since baee84a6 (Jun 7 2016)
					// (XXX in neo: SQLite does not accept numbers above 2^63-1)

	//Oid0	Oid = 0			// XXX -> simply Oid(0)
)

func (tid Tid) Valid() bool {
	// XXX if Tid becomes signed also check wrt 0
	if tid <= TidMax {
		return true
	} else {
		return false
	}
}

// ----------------------------------------

// ErrOidMissing is an error which tells that there is no such oid in the database at all
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

// ----------------------------------------

// TxnStatus represents status of a transaction
type TxnStatus byte

const (
	TxnComplete TxnStatus = ' ' // completed transaction that hasn't been packed
	TxnPacked             = 'p' // completed transaction that has been packed
	TxnInprogress         = 'c' // checkpoint -- a transaction in progress; it's been thru vote() but not finish()
)

// Valid returns true if transaction status value is well-known and valid
func (ts TxnStatus) Valid() bool {
	switch ts {
	case TxnComplete, TxnPacked, TxnInprogress:
		return true

	default:
		return false
	}
}

// Metadata information about single transaction
type TxnInfo struct {
	Tid         Tid
	Status      TxnStatus
	User        []byte
	Description []byte
	Extension   []byte
}

// Information about single storage record
// XXX naming
type StorageRecordInformation struct {
	Oid	Oid
	Tid	Tid
	Data	[]byte	// nil means: deleted

	// original tid data was committed (e.g. in case of undo)
	// XXX we don't really need this
	DataTid	Tid
}



type IStorage interface {
	// XXX add invalidation channel

	// StorageName returns storage name
	StorageName() string

	// Close closes storage
	Close() error

	// History(oid, size=1)

	// LastTid returns the id of the last committed transaction.
	// if no transactions have been committed yet, LastTid returns Tid zero value
	LastTid(ctx context.Context) (Tid, error)

	// LastOid returns highest object id of objects committed to storage.
	// if there is no data committed yet, LastOid returns Oid zero value
	// XXX ZODB/py does not define this in IStorage
	LastOid(ctx context.Context) (Oid, error)

	// LoadSerial and LoadBefore generalized into 1 Load  (see Xid for details)
	//
	// XXX zodb.loadBefore() returns (data, serial, serial_next) -> add serial_next?
	//
	// XXX currently deleted data is returned as data.Data=nil	-- is it ok?
	// TODO specify error when data not found -> ErrOidMissing | ErrXidMissing
	Load(ctx context.Context, xid Xid) (data *Buf, serial Tid, err error)	// XXX -> StorageRecordInformation ?

	// Prefetch(ctx, xid Xid)	(no error)

	// Store(oid Oid, serial Tid, data []byte, txn ITransaction) error
	// XXX Restore ?
	// CheckCurrentSerialInTransaction(oid Oid, serial Tid, txn ITransaction)   // XXX naming

	// tpc_begin(txn)
	// tpc_vote(txn)
	// tpc_finish(txn, callback)    XXX clarify about callback
	// tpc_abort(txn)

	// XXX allow iteration both ways (forward & backward)
	// XXX text
	Iterate(tidMin, tidMax Tid) IStorageIterator	// XXX ctx , error ?
}

type IStorageIterator interface {
	// NextTxn yields information about next database transaction:
	// 1. transaction metadata, and
	// 2. iterator over transaction data records. 
	// transaction metadata stays valid until next call to NextTxn().
	// end of iteration is indicated with io.EOF
	NextTxn() (*TxnInfo, IStorageRecordIterator, error)	// XXX ctx
}

type IStorageRecordIterator interface {         // XXX naming -> IRecordIterator
	// NextData yields information about next storage data record.
	// returned data stays valid until next call to NextData().
	// end of iteration is indicated with io.EOF
	NextData() (*StorageRecordInformation, error)	// XXX ctx
}
