// TODO copyright / license

// Package zodb defines types, interfaces and errors used in ZODB databases

// XXX partly based on ZODB/py

package zodb

import (
	"fmt"
)

// ZODB types
type Tid uint64  // transaction identifier
type Oid uint64  // object identifier

/*
// XXX "extended" oid - oid + serial, completely specifying object revision
type Xid struct {
	Tid
	Oid
}
*/

const (
	Tid0	Tid = 0			// XXX or simply Tid(0) ?
	TidMax	Tid = 1<<63 - 1		// 0x7fffffffffffffff
					// ZODB defines maxtid to be max signed int64 since baee84a6 (Jun 7 2016)
					// (XXX in neo: SQLite does not accept numbers above 2^63-1)

	Oid0	Oid = 0
)

func (tid Tid) String() string {
	// XXX also print "tid:" prefix ?
	return fmt.Sprintf("%016x", uint64(tid))
}

func (oid Oid) String() string {
	// XXX also print "oid:" prefix ?
	return fmt.Sprintf("%016x", uint64(oid))
}

// ----------------------------------------

type ErrOidMissing struct {
	Oid	Oid
}

func (e ErrOidMissing) Error() string {
	return "%v: no such oid"
}

// ----------------------------------------

// TxnStatus represents status of a transaction
type TxnStatus byte

const (
	TxnComplete TxnStatus = ' ' // completed transaction that hasn't been packed
	TxnPacked             = 'p' // completed transaction that has been packed
	TxnInprogress         = 'c' // checkpoint -- a transaction in progress; it's been thru vote() but not finish()
)

// TODO Tid.String(), Oid.String() +verbose, scanning (?)

// Information about single storage transaction
// XXX -> storage.ITransactionInformation
//type IStorageTransactionInformation interface {
type StorageTransactionInformation struct {
	Tid         Tid
	Status      TxnStatus
	User        []byte
	Description []byte
	Extension   []byte

	// TODO iter -> IStorageRecordInformation
	Iter	IStorageRecordIterator
}

// Information about single storage record
type StorageRecordInformation struct {
	Oid         Oid
	Tid         Tid
	Data        []byte
	// XXX .version ?
	// XXX .data_txn    (The previous transaction id)
}



type IStorage interface {
	Close() error

	// StorageName returns storage name
	StorageName() string

	// History(oid, size=1)

	// LastTid returns the id of the last committed transaction.
	// if not transactions have been committed yet, LastTid returns Tid zero value
	// XXX ^^^ ok ?
	LastTid() Tid	// XXX -> Tid, ok ?

	// TODO data []byte -> something allocated from slab ?
	LoadBefore(oid Oid, beforeTid Tid) (data []byte, tid Tid, err error)
	LoadSerial(oid Oid, serial Tid)    (data []byte, err error)
	// PrefetchBefore(oidv []Oid, beforeTid Tid) error (?)

	// Store(oid Oid, serial Tid, data []byte, txn ITransaction) error
	// XXX Restore ?
	// CheckCurrentSerialInTransaction(oid Oid, serial Tid, txn ITransaction)   // XXX naming

	// tpc_begin(txn)
	// tpc_vote(txn)
	// tpc_finish(txn, callback)    XXX clarify about callback
	// tpc_abort(txn)

	Iterate(start, stop Tid) IStorageIterator   // XXX -> Iter() ?
}

type IStorageIterator interface {
	Next() (*StorageTransactionInformation, error)  // XXX -> NextTxn() ?
}

type IStorageRecordIterator interface {         // XXX naming -> IRecordIterator
	Next() (*StorageRecordInformation, error)    // XXX vs ptr & nil ?
	                                            // XXX -> NextTxnObject() ?
}
