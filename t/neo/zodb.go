// XXX license

// ZODB types
package neo // XXX -> zodb ?

// XXX naming -> TID, OID ?
type Tid uint64  // XXX or [8]byte ?
type Oid uint64  // XXX or [8]byte ?

// XXX "extended" oid - oid + serial, completely specifying object revision
type Xid struct {
    Tid
    Oid
}

const (
	INVALID_UUID UUID = 0
	INVALID_TID  TID = 1<<64 - 1            // 0xffffffffffffffff   TODO recheck it is the same
	INVALID_OID  OID = 0xffffffffffffffff   // 1<<64 - 1
	ZERO_TID     TID = 0        // XXX or simply TID{} ?    // XXX -> TID0 ?
	ZERO_OID     OID = 0        // XXX or simply OID{} ?    // XXX -> OID0
	// OID_LEN = 8
	// TID_LEN = 8
	MAX_TID      TID = 0x7fffffffffffffff    // SQLite does not accept numbers above 2^63-1 // XXX -> TIDMAX ?
)


type TxnStatus byte

// TODO Tid.String(), Oid.String() +verbose, scanning (?)

// Information about single storage transaction
// XXX -> storage.ITransactionInformation
//type IStorageTransactionInformation interface {
type IStorageTransactionInformation struct {
    Tid         Tid
    Status      TxnStatus
    User        []byte
    Description []byte
    Extension   []byte

    // TODO iter -> IStorageRecordInformation
    Iter() IStorageRecordIterator
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
