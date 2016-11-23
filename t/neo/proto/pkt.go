package proto

import (
	. "../"
)

const (
	PROTOCOL_VERSION = 7

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
	READ_ONLY_ACCESS
)

type ClusterState int
const (
	// NOTE cluster states descriptions is in protocol.py
	RECOVERING ClusterState = iota
	VERIFYING
	CLUSTER_RUNNING			// XXX conflict with NodeState.RUNNING
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

// An UUID (node identifier, 4-bytes signed integer)
type UUID int32

// TODO UUID_NAMESPACES

type Address struct {
	Host string
	Port uint16	// TODO if Host == 0 -> Port not added to wire (see py.PAddress)
}

// A SHA1 hash
type Checksum [20]byte

// Partition Table identifier
// Zero value means "invalid id" (<-> None in py.PPTID)
type PTid uint64	// XXX move to common place ?

// NOTE original NodeList = []NodeInfo
type NodeInfo struct {
	NodeType
	Address
	UUID
	NodeState
}

// XXX -> CellInfo      (and use []CellInfo) ?
type CellList []struct {
	UUID      UUID          // XXX maybe simply 'UUID' ?
	CellState CellState     // ----///----
}

// XXX -> RowInfo       (and use []RowInfo) ?
type RowList []struct {
	Offset   uint32  // PNumber
	CellList CellList
}



// XXX link request <-> answer ?
// TODO ensure len(encoded packet header) == 10
// XXX -> PktHeader ?
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
	ProtocolVersion uint32		// TODO py.PProtocol upon decoding checks for != PROTOCOL_VERSION
	NodeType        NodeType        // XXX name
	UUID            UUID
	Address
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
	Primary         Address        // TODO
	KnownMasterList []struct {
		Address
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
	PTid
	BackupTID   Tid
	TruncateTID Tid
}

// Ask the last OID/TID so that a master can initialize its TransactionManager.
// PM -> S, S -> PM.
type LastIDs struct {
	Packet
}

type AnswerLastIDs struct {
	Packet
	LastOID Oid
	LastTID Tid
}

// Ask the full partition table. PM -> S.
// Answer rows in a partition table. S -> PM.
type PartitionTable struct {
	Packet
}

type AnswerPartitionTable struct {
	Packet
        PTid
        RowList RowList
}


// Send rows in a partition table to update other nodes. PM -> S, C.
type NotifyPartitionTable struct {
	Packet
        PTid
        RowList RowList
}

// Notify a subset of a partition table. This is used to notify changes.
// PM -> S, C.
type PartitionChanges struct {
	Packet

        PTid
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
	MaxTID  Tid
	TidList []struct{
		UnfinishedTID Tid
	}
}

// Ask locked transactions  PM -> S.
// Answer locked transactions  S -> PM.
type LockedTransactions struct {
	Packet
}

type AnswerLockedTransactions struct {
	Packet
	TidDict map[Tid]Tid     // ttid -> tid
}

// Return final tid if ttid has been committed. * -> S. C -> PM.
type FinalTID struct {
	Packet
	TTID    Tid
}

type AnswerFinalTID struct {
	Packet
        Tid     Tid
}

// Commit a transaction. PM -> S.
type ValidateTransaction struct {
	Packet
        TTID Tid
        Tid  Tid
}


// Ask to begin a new transaction. C -> PM.
// Answer when a transaction begin, give a TID if necessary. PM -> C.
type BeginTransaction struct {
	Packet
	Tid     Tid
}

type AnswerBeginTransaction struct {
	Packet
	Tid     Tid
}

// Finish a transaction. C -> PM.
// Answer when a transaction is finished. PM -> C.
type FinishTransaction struct {
	Packet
        Tid         Tid
        OIDList     []Oid
        CheckedList []Oid
}

type AnswerFinishTransaction struct {
	Packet
        TTID    Tid
        Tid     Tid
}

// Notify that a transaction blocking a replication is now finished
// M -> S
type NotifyTransactionFinished struct {
	Packet
        TTID    Tid
        MaxTID  Tid
}

// Lock information on a transaction. PM -> S.
// Notify information on a transaction locked. S -> PM.
type LockInformation struct {
	Packet
        Ttid Tid
        Tid  Tid
}

// XXX AnswerInformationLocked ?
type AnswerLockInformation struct {
        Ttid Tid
}

// Invalidate objects. PM -> C.
// XXX ask_finish_transaction ?
type InvalidateObjects struct {
	Packet
        Tid     Tid
        OidList []Oid
}

// Unlock information on a transaction. PM -> S.
type UnlockInformation struct {
	Packet
        TTID    Tid
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
        OidList []Oid
}


// Ask to store an object. Send an OID, an original serial, a current
// transaction ID, and data. C -> S.
// Answer if an object has been stored. If an object is in conflict,
// a serial of the conflicting transaction is returned. In this case,
// if this serial is newer than the current transaction ID, a client
// node must not try to resolve the conflict. S -> C.
type StoreObject struct {
	Packet
        Oid             Oid
        Serial          Tid
        Compression     bool
        Checksum        Checksum
        Data            []byte          // XXX or string ?
        DataSerial      Tid
        Tid             Tid
        Unlock          bool
}

type AnswerStoreObject struct {
	Packet
	Conflicting     bool
	Oid             Oid
	Serial          Tid
}

// Abort a transaction. C -> S, PM.
type AbortTransaction struct {
	Packet
	Tid     Tid
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type StoreTransaction struct {
	Packet
        Tid             Tid
        User            string
        Description     string
        Extension       string
        OidList         []Oid
	// TODO _answer = PFEmpty
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type VoteTransaction struct {
	Packet
        Tid     Tid
	// TODO _answer = PFEmpty
}

// Ask a stored object by its OID and a serial or a TID if given. If a serial
// is specified, the specified revision of an object will be returned. If
// a TID is specified, an object right before the TID will be returned. C -> S.
// Answer the requested object. S -> C.
type GetObject struct {
	Packet
	Oid     Oid
	Serial  Tid
	Tid     Tid
}

// XXX answer_object ?
type AnswerGetObject struct {
	Packet
        Oid             Oid
        SerialStart     Tid
        SerialEnd       Tid
        Compression     bool
        Checksum        Checksum
        Data            []byte          // XXX or string ?
        DataSerial      Tid
}

// Ask for TIDs between a range of offsets. The order of TIDs is descending,
// and the range is [first, last). C -> S.
// Answer the requested TIDs. S -> C.
type TIDList struct {
	Packet
	First           uint64  // PIndex       XXX this is TID actually ? -> no it is offset in list
	Last            uint64  // PIndex       ----//----
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDList struct {
	Packet
        TIDList []Tid
}

// Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
// C -> S.
// Answer the requested TIDs. S -> C
type TIDListFrom struct {
	Packet
	MinTID          Tid
	MaxTID          Tid
	Length          uint32  // PNumber
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDListFrom struct {
	Packet
	TidList []Tid
}

// Ask information about a transaction. Any -> S.
// Answer information (user, description) about a transaction. S -> Any.
type TransactionInformation struct {
	Packet
	Tid     Tid
}

type AnswerTransactionInformation struct {
	Packet
        Tid             Tid
        User            string
        Description     string
        Extension       string
        Packed          bool
        OidList         []Oid
}

// Ask history information for a given object. The order of serials is
// descending, and the range is [first, last]. C -> S.
// Answer history information (serial, size) for an object. S -> C.
type ObjectHistory struct {
	Packet
	Oid     Oid
	First   uint64  // PIndex       XXX this is actually TID
	Last    uint64  // PIndex       ----//----
}

type AnswerObjectHistory struct {
	Packet
	Oid         Oid
        HistoryList []struct {
	        Serial  Tid
	        Size    uint32  // PNumber
	}
}

// All the following messages are for neoctl to admin node
// Ask information about partition
// Answer information about partition
type PartitionList struct {
	Packet
	MinOffset   uint32      // PNumber
	MaxOffset   uint32      // PNumber
	UUID        UUID
}

type AnswerPartitionList struct {
	Packet
	PTid
        RowList RowList
}

// Ask information about nodes
// Answer information about nodes
type X_NodeList struct {
	Packet
        NodeType
}

type AnswerNodeList struct {
	Packet
        NodeList []NodeInfo
}

// Set the node state
type SetNodeState struct {
	Packet
	UUID
        NodeState

	// XXX _answer = Error ?
}

// Ask the primary to include some pending node in the partition table
type AddPendingNodes struct {
	Packet
        UUIDList []UUID

	// XXX _answer = Error
}

// Ask the primary to optimize the partition table. A -> PM.
type TweakPartitionTable struct {
	Packet
        UUIDList []UUID

	// XXX _answer = Error
}

// Notify information about one or more nodes. PM -> Any.
type NotifyNodeInformation struct {
	Packet
        NodeList []NodeInfo
}

// Ask node information
type NodeInformation struct {
	Packet
	// XXX _answer = PFEmpty
}

// Set the cluster state
type SetClusterState struct {
	Packet
	State   ClusterState

	// XXX _answer = Error
}

// Notify information about the cluster
type ClusterInformation struct {
	Packet
	State   ClusterState
}

// Ask state of the cluster
// Answer state of the cluster
type X_ClusterState struct {	// XXX conflicts with ClusterState enum
	Packet
        State   ClusterState
}


// Ask storage the serial where object data is when undoing given transaction,
// for a list of OIDs.
// C -> S
// Answer serials at which object data is when undoing a given transaction.
// object_tid_dict has the following format:
//     key: oid
//     value: 3-tuple
//         current_serial (TID)
//             The latest serial visible to the undoing transaction.
//         undo_serial (TID)
//             Where undone data is (tid at which data is before given undo).
//         is_current (bool)
//             If current_serial's data is current on storage.
// S -> C
type ObjectUndoSerial struct {
	Packet
        Tid             Tid
        LTID            Tid
        UndoneTID       Tid
        OidList         []Oid
}

// XXX answer_undo_transaction ?
type AnswerObjectUndoSerial struct {
	Packet
	ObjectTIDDict map[Oid]struct {
                CurrentSerial   Tid
                UndoSerial      Tid
                IsCurrent       bool
	}
}

// Ask a storage is oid is locked by another transaction.
// C -> S
// Answer whether a transaction holds the write lock for requested object.
type HasLock struct {
	Packet
        Tid     Tid
        Oid     Oid
}

type AnswerHasLock struct {
	Packet
        Oid       Oid
        LockState LockState
}


// Verifies if given serial is current for object oid in the database, and
// take a write lock on it (so that this state is not altered until
// transaction ends).
// Answer to AskCheckCurrentSerial.
// Same structure as AnswerStoreObject, to handle the same way, except there
// is nothing to invalidate in any client's cache.
type CheckCurrentSerial struct {
	Packet
	Tid     Tid
	Serial  Tid
	Oid     Oid
}

// XXX answer_store_object ?
type AnswerCheckCurrentSerial struct {
        Conflicting     bool
        Oid             Oid
        Serial          Tid
}

// Request a pack at given TID.
// C -> M
// M -> S
// Inform that packing it over.
// S -> M
// M -> C
type Pack struct {
	Packet
        Tid     Tid
}

type AnswerPack struct {
	Packet
	Status  bool
}


// ctl -> A
// A -> M
type CheckReplicas struct {
	Packet
	PartitionDict map[uint32/*PNumber*/]UUID        // partition -> source
	MinTID  Tid
	MaxTID  Tid

	// XXX _answer = Error
}

// M -> S
type CheckPartition struct {
	Packet
	Partition uint32  // PNumber
	Source    struct {
		UpstreamName string
		Address      Address
	}
	MinTID    Tid
	MaxTID    Tid
}


// Ask some stats about a range of transactions.
// Used to know if there are differences between a replicating node and
// reference node.
// S -> S
// Stats about a range of transactions.
// Used to know if there are differences between a replicating node and
// reference node.
// S -> S
type CheckTIDRange struct {
	Packet
        Partition uint32        // PNumber
        Length    uint32        // PNumber
        MinTID    Tid
        MaxTID    Tid
}

type AnswerCheckTIDRange struct {
	Packet
	Count    uint32         // PNumber
	Checksum Checksum
        MaxTID   Tid
}

// Ask some stats about a range of object history.
// Used to know if there are differences between a replicating node and
// reference node.
// S -> S
// Stats about a range of object history.
// Used to know if there are differences between a replicating node and
// reference node.
// S -> S
type CheckSerialRange struct {
	Packet
        Partition       uint32  // PNumber
        Length          uint32  // PNumber
        MinTID          Tid
        MaxTID          Tid
        MinOID          Oid
}

type AnswerCheckSerialRange struct {
        Count           uint32  // PNumber
        TidChecksum     Checksum
        MaxTID          Tid
        OidChecksum     Checksum
        MaxOID          Oid
}

// S -> M
type PartitionCorrupted struct {
	Packet
        Partition       uint32  // PNumber
        CellList        []UUID
}


// Ask last committed TID.
// C -> M
// Answer last committed TID.
// M -> C
type LastTransaction struct {
	Packet
}

type AnswerLastTransaction struct {
	Packet
	Tid     Tid
}


// Notify that node is ready to serve requests.
// S -> M
type NotifyReady struct {
	Packet
}

// replication

// TODO
