package pkt

// TODO .TID -> .Tid etc ?

const (
	PROTOCOL_VERSION = 6

	MIN_PACKET_SIZE = 10    // XXX link this to len(pkthead) ?
	MAX_PACKET_SIZE = 0x4000000

	RESPONSE_MASK   = 0x800
)

type ErrorCode int
const (
	ACK ErrorCode = iota
	NOT_READY
	OID_NOT_FOUND
	TID_NOT_FOUND
	OID_DOES_NOT_EXIST
	PROTOCOL_ERROR
	BROKEN_NODE
	ALREADY_PENDING
	REPLICATION_ERROR
	CHECKING_ERROR
	BACKEND_NOT_IMPLEMENTED
)

type ClusterState int
const (
	// NOTE cluster states descriptions is in protocol.py
	RECOVERING ClusterState = iota
	VERIFYING
	RUNNING
	STOPPING
	STARTING_BACKUP
	BACKINGUP
	STOPPING_BACKUP
)

type NodeType int
const (
	MASTER NodeType = iota
	STORAGE
	CLIENT
	ADMIN
)

type NodeState int
const (
	RUNNING NodeState = iota //short: R     // XXX tag prefix name ?
	TEMPORARILY_DOWN         //short: T
	DOWN                     //short: D
	BROKEN                   //short: B
	HIDDEN                   //short: H
	PENDING                  //short: P
	UNKNOWN                  //short: U
)

type CellState int
const (
	// NOTE cell states description is in protocol.py
	UP_TO_DATE CellState = iota //short: U     // XXX tag prefix name ?
	OUT_OF_DATE                 //short: O
	FEEDING                     //short: F
	DISCARDED                   //short: D
	CORRUPTED                   //short: C
)

type LockState int
const (
	NOT_LOCKED LockState = iota
	GRANTED
	GRANTED_TO_OTHER
)

// XXX move out of here ?
type TID uint64  // XXX or [8]byte ?
type OID uint64  // XXX or [8]byte ?

const (
	INVALID_UUID UUID = 0
	INVALID_TID  TID = 0xffffffffffffffff    // 1<<64 - 1
	INVALID_OID  OID = 0xffffffffffffffff    // 1<<64 - 1
	ZERO_TID     TID = 0        // XXX or simply TID{} ?
	ZERO_OID     OID = 0        // XXX or simply OID{} ?
	// OID_LEN = 8
	// TID_LEN = 8
	MAX_TID      TID = 0x7fffffffffffffff    // SQLite does not accept numbers above 2^63-1
)

// An UUID (node identifier, 4-bytes signed integer)
type UUID int32

// TODO UUID_NAMESPACES

type CellList []struct {
	UUID      UUID          // XXX maybe simply 'UUID' ?
	CellState CellState     // ----///----
}

type RowList []struct {
	Offset   uint32  // PNumber
	CellList CellList
}


type OIDList []struct {
	OID     OID
}

// XXX link request <-> answer ?
// TODO ensure len(encoded packet header) == 10
type Packet struct {
	Id   uint32
	Code uint16 // XXX we don't need this as field - this is already encoded in type
	Len  uint32 // XXX we don't need this as field - only on the wire
}

// TODO generate .Encode() / .Decode()

// General purpose notification (remote logging)
type Notify struct {
	Packet
	Message string
}

// Error is a special type of message, because this can be sent against
// any other message, even if such a message does not expect a reply
// usually. Any -> Any.
type Error struct {
	Packet
	Code    uint32  // PNumber
	Message string
}

// Check if a peer is still alive. Any -> Any.
type Ping struct {
	Packet
	// TODO _answer = PFEmpty
}

// Tell peer it can close the connection if it has finished with us. Any -> Any
type CloseClient struct {
	Packet
}

// Request a node identification. This must be the first packet for any
// connection. Any -> Any.
type RequestIdentification struct {
	Packet
	ProtocolVersion PProtocol       // TODO
	NodeType        NodeType        // XXX name
	UUID            UUID
	Address         PAddress        // TODO
	Name            string
}

// XXX -> ReplyIdentification? RequestIdentification.Answer somehow ?
type AcceptIdentification struct {
	Packet
	NodeType        NodeType        // XXX name
	MyUUID          UUID
	NumPartitions   uint32          // PNumber
	NumReplicas     uint32          // PNumber
	YourUUID        UUID
	Primary         PAddress        // TODO
	KnownMasterList []struct {
		Address PAddress
		UUID    UUID
	}
}

// Ask current primary master's uuid. CTL -> A.
type PrimaryMaster struct {
	Packet
}

type AnswerPrimary struct {
	Packet
	PrimaryUUID UUID
}

// Announce a primary master node election. PM -> SM.
type AnnouncePrimary struct {
	Packet
}

// Force a re-election of a primary master node. M -> M.
type ReelectPrimary struct {
	Packet
}

// Ask all data needed by master to recover. PM -> S, S -> PM.
type Recovery struct {
	Packet
}

type AnswerRecovery struct {
	Packet
	PTID        PPTID   // TODO
	BackupTID   TID
	TruncateTID TID
}

// Ask the last OID/TID so that a master can initialize its TransactionManager.
// PM -> S, S -> PM.
type LastIDs struct {
	Packet
}

type AnswerLastIDs struct {
	Packet
	LastOID OID
	LastTID TID
}

// Ask the full partition table. PM -> S.
// Answer rows in a partition table. S -> PM.
type PartitionTable struct {
	Packet
}

type AnswerPartitionTable struct {
	Packet
        PTID    PPTID   // TODO
        RowList RowList
}


// Send rows in a partition table to update other nodes. PM -> S, C.
type NotifyPartitionTable struct {
	Packet
        PTID    PPTID   // TODO
        RowList RowList
}

// Notify a subset of a partition table. This is used to notify changes.
// PM -> S, C.
type PartitionChanges struct
	Packet

        PTID     PPTID   // TODO
        CellList []struct {
	        // XXX does below correlate with Cell inside top-level CellList ?
	        Offset    uint32  // PNumber
	        UUID      UUID
                CellState CellState
        }
}

// Tell a storage nodes to start an operation. Until a storage node receives
// this message, it must not serve client nodes. PM -> S.
type StartOperation struct {
	Packet
        // XXX: Is this boolean needed ? Maybe this
        //      can be deduced from cluster state.
        Backup bool
}

// Tell a storage node to stop an operation. Once a storage node receives
// this message, it must not serve client nodes. PM -> S.
type StopOperation struct {
	Packet
}

// Ask unfinished transactions  S -> PM.
// Answer unfinished transactions  PM -> S.
type UnfinishedTransactions struct {
	Packet
}

type AnswerUnfinishedTransactions struct {
	Packet
	MaxTID  TID
	TidList []struct{
		UnfinishedTID TID
	}
}

// Ask locked transactions  PM -> S.
// Answer locked transactions  S -> PM.
type LockedTransactions struct {
	Packet
}

type AnswerLockedTransactions struct {
	Packet
	TidDict map[TID]TID     // ttid -> tid
}

// Return final tid if ttid has been committed. * -> S. C -> PM.
type FinalTID struct {
	Packet
	TTID    TID
}

type AnswerFinalTID struct {
	Packet
        TID     TID
}

// Commit a transaction. PM -> S.
type ValidateTransaction struct {
	Packet
        TTID TID
        Tid  TID
}


// Ask to begin a new transaction. C -> PM.
// Answer when a transaction begin, give a TID if necessary. PM -> C.
type BeginTransaction struct {
	Packet
	TID     TID
}

type AnswerBeginTransaction struct {
	Packet
	TID     TID
}

// Finish a transaction. C -> PM.
// Answer when a transaction is finished. PM -> C.
type FinishTransaction struct {
	Packet
        TID         TID
        OIDList     OIDList
        CheckedList OIDList
}

type AnswerFinishTransaction struct {
	Packet
        TTID    TID
        TID     TID
}

// Notify that a transaction blocking a replication is now finished
// M -> S
type NotifyTransactionFinished struct {
	Packet
        TTID    TID
        MaxTID  TID
}

// Lock information on a transaction. PM -> S.
// Notify information on a transaction locked. S -> PM.
type LockInformation struct {
	Packet
        Ttid TID
        Tid  TID
}

// XXX AnswerInformationLocked ?
type AnswerLockInformation struct {
        Ttid TID
}

// Invalidate objects. PM -> C.
// XXX ask_finish_transaction ?
type InvalidateObjects struct {
	Packet
        TID     TID
        OidList OidList
}

// Unlock information on a transaction. PM -> S.
type UnlockInformation struct {
	Packet
        TTID    TID
}

// Ask new object IDs. C -> PM.
// Answer new object IDs. PM -> C.
type GenerateOIDs struct {
	Packet
	NumOIDs uint32  // PNumber
}

// XXX answer_new_oids ?
type AnswerGenerateOIDs struct {
	Packet
        OidList
}


// Ask to store an object. Send an OID, an original serial, a current
// transaction ID, and data. C -> S.
// Answer if an object has been stored. If an object is in conflict,
// a serial of the conflicting transaction is returned. In this case,
// if this serial is newer than the current transaction ID, a client
// node must not try to resolve the conflict. S -> C.
type StoreObject struct {
	Packet
        OID             OID
        Serial          TID
        Compression     bool
        Checksum        PChecksum       // TODO
        Data            []byte          // XXX or string ?
        DataSerial      TID
        TID             TID
        Unlock          bool
}

type AnswerStoreObject struct {
	Packet
	Conflicting     bool
	OID             OID
	Serial          TID
}

// Abort a transaction. C -> S, PM.
type AbortTransaction struct {
	Packet
	TID     TID
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type StoreTransaction struct {
	Packet
        TID             TID
        User            string
        Description     string
        Extension       string
        OidList
	// TODO _answer = PFEmpty
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type VoteTransaction struct {
	Packet
        TID     TID
	// TODO _answer = PFEmpty
}

// Ask a stored object by its OID and a serial or a TID if given. If a serial
// is specified, the specified revision of an object will be returned. If
// a TID is specified, an object right before the TID will be returned. C -> S.
// Answer the requested object. S -> C.
type GetObject struct {
	Packet
	OID     OID
	Serial  TID
	TID     TID
}

// XXX answer_object ?
type AnswerGetObject struct {
	Packet
        OID             OID
        SerialStart     TID
        SerialEnd       TID
        Compression     bool
        Checksum        PChecksum
        Data            []byte          // XXX or string ?
        DataSerial      TID
}

