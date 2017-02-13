// TODO copyright / license

// Package zodb defines types and interfaces used in ZODB databases

package zodb

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
	Tid0	Tid = 0
	TidMax	Tid = 1<<63 - 1		// 0x7fffffffffffffff
					// ZODB defines maxtid to be max signed int64 since baee84a6 (Jun 7 2016)
					// (XXX in neo: SQLite does not accept numbers above 2^63-1)
)

// ----------------------------------------

type TxnStatus byte

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

    // TODO:
    // Name()
    // History(oid, size=1)
    // LastTid()

    // LoadBefore(oid Oid, beforeTid Tid) (data []bytes, tid Tid, err error)
    // LoadSerial(oid Oid, serial Tid)    (data []bytes, err error)
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
