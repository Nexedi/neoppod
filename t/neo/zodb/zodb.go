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

// XTid is "extended" transaction identifier. It defines a transaction for
// oid lookup - either exactly by serial, or <beforeTid	XXX
type XTid struct {
	Tid
	TidBefore bool	// XXX merge into Tid itself (high bit) ?
}

// Xid is "extended" oid = oid + serial/beforeTid, completely specifying object revision	XXX text
type Xid struct {
	XTid
	Oid
}

const (
	//Tid0	Tid = 0			// XXX -> simply Tid(0) ?
	TidMax	Tid = 1<<63 - 1		// 0x7fffffffffffffff
					// ZODB defines maxtid to be max signed int64 since baee84a6 (Jun 7 2016)
					// (XXX in neo: SQLite does not accept numbers above 2^63-1)

	//Oid0	Oid = 0			// XXX -> simply Oid(0)
)

func (tid Tid) String() string {
	// XXX also print "tid:" prefix ?
	return fmt.Sprintf("%016x", uint64(tid))
}

func (oid Oid) String() string {
	// XXX also print "oid:" prefix ?
	return fmt.Sprintf("%016x", uint64(oid))
}

// XXX move me out of here
// bint converts bool to int with true => 1; false => 0
func bint(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func (xtid XTid) String() string {
	// XXX also print "tid:" prefix ?
	return fmt.Sprintf("%c%v", "=<"[bint(xtid.TidBefore)], xtid.Tid)
}

func (xid Xid) String() string {
	return xid.XTid.String() + ":" + xid.Oid.String()	// XXX use "·" instead of ":" ?
}

// ----------------------------------------

// ErrOidMissing is an error which tells that there is no such oid in the database at all
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

// Information about single transaction
// XXX -> storage.ITransactionInformation
//type IStorageTransactionInformation interface {
//type StorageTransactionInformation struct {
type TxnInfo struct {
	Tid         Tid
	Status      TxnStatus
	User        []byte
	Description []byte
	Extension   []byte

	// TODO iter -> IStorageRecordInformation
	Iter	IStorageRecordIterator
}

// Information about single storage record
// XXX naming
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

	// LoadSerial and LoadBefore generalized into 1 Load  (see Xid for details)
	// TODO data []byte -> something allocated from slab ?
	Load(xid Xid) (data []byte, tid Tid, err error)

	// -> Prefetch(xid Xid) ...
	// PrefetchBefore(oidv []Oid, beforeTid Tid) error (?)

	// Store(oid Oid, serial Tid, data []byte, txn ITransaction) error
	// XXX Restore ?
	// CheckCurrentSerialInTransaction(oid Oid, serial Tid, txn ITransaction)   // XXX naming

	// tpc_begin(txn)
	// tpc_vote(txn)
	// tpc_finish(txn, callback)    XXX clarify about callback
	// tpc_abort(txn)

	Iterate(tidMin, tidMax Tid) IStorageIterator   // XXX -> Iter() ?
}

type IStorageIterator interface {
	// NextTxn puts information about next storage transaction into *txnInfo.
	// data put into *txnInfo stays valid until next call to NextTxn().
	NextTxn(txnInfo *TxnInfo) (ok bool, err error)	// XXX ok -> stop ?
}

type IStorageRecordIterator interface {         // XXX naming -> IRecordIterator
	// NextData puts information about next storage data record into *dataInfo.
	// data put into *dataInfo stays vaild until next call to NextData().
	NextData(dataInfo *StorageRecordInformation) (ok bool, err error)
}
