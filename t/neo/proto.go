// NEO. Protocol description

package neo
//package proto

/*
import (
	. "../"
)
*/

const (
	PROTOCOL_VERSION = 8

	MIN_PACKET_SIZE = 10    // XXX link this to len(pkthead) ?
	PktHeadLen	= MIN_PACKET_SIZE	// TODO link this to PktHead.Encode/Decode size
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

// TODO None encodes as '\xff' * 8	(XXX use nan for None ?)
type Float64 float64

// NOTE original NodeList = []NodeInfo
type NodeInfo struct {
	NodeType
	Address
	UUID
	NodeState
	IdTimestamp Float64
}

//type CellList []struct {
type CellInfo struct {
	UUID      UUID          // XXX maybe simply 'UUID' ?
	CellState CellState     // ----///----
}

//type RowList []struct {
type RowInfo struct {
	Offset   uint32  // PNumber
	CellList []CellInfo
}



// XXX link request <-> answer ?
// TODO ensure len(encoded packet header) == 10
type PktHead struct {
	ConnId  be32	// NOTE is .msgid in py
	MsgCode be16
	Len	be32	// whole packet length (including header)
}

// TODO generate .Encode() / .Decode()

// General purpose notification (remote logging)
type Notify struct {
	PktHead
	Message string
}

// Error is a special type of message, because this can be sent against
// any other message, even if such a message does not expect a reply
// usually. Any -> Any.
type Error struct {
	PktHead
	Code    uint32  // PNumber
	Message string
}

// Check if a peer is still alive. Any -> Any.
type Ping struct {
	PktHead
	// TODO _answer = PFEmpty
}

// Tell peer it can close the connection if it has finished with us. Any -> Any
type CloseClient struct {
	PktHead
}

// Request a node identification. This must be the first packet for any
// connection. Any -> Any.
type RequestIdentification struct {
	PktHead
	ProtocolVersion uint32		// TODO py.PProtocol upon decoding checks for != PROTOCOL_VERSION
	NodeType        NodeType        // XXX name
	UUID            UUID
	Address
	Name            string
	IdTimestamp	Float64
}

// XXX -> ReplyIdentification? RequestIdentification.Answer somehow ?
type AcceptIdentification struct {
	PktHead
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
	PktHead
}

type AnswerPrimary struct {
	PktHead
	PrimaryUUID UUID
}

// Announce a primary master node election. PM -> SM.
type AnnouncePrimary struct {
	PktHead
}

// Force a re-election of a primary master node. M -> M.
type ReelectPrimary struct {
	PktHead
}

// Ask all data needed by master to recover. PM -> S, S -> PM.
type Recovery struct {
	PktHead
}

type AnswerRecovery struct {
	PktHead
	PTid
	BackupTID   Tid
	TruncateTID Tid
}

// Ask the last OID/TID so that a master can initialize its TransactionManager.
// PM -> S, S -> PM.
type LastIDs struct {
	PktHead
}

type AnswerLastIDs struct {
	PktHead
	LastOID Oid
	LastTID Tid
}

// Ask the full partition table. PM -> S.
// Answer rows in a partition table. S -> PM.
type PartitionTable struct {
	PktHead
}

type AnswerPartitionTable struct {
	PktHead
        PTid
        RowList []RowInfo
}


// Send rows in a partition table to update other nodes. PM -> S, C.
type NotifyPartitionTable struct {
	PktHead
        PTid
        RowList []RowInfo
}

// Notify a subset of a partition table. This is used to notify changes.
// PM -> S, C.
type PartitionChanges struct {
	PktHead

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
	PktHead
        // XXX: Is this boolean needed ? Maybe this
        //      can be deduced from cluster state.
        Backup bool
}

// Tell a storage node to stop an operation. Once a storage node receives
// this message, it must not serve client nodes. PM -> S.
type StopOperation struct {
	PktHead
}

// Ask unfinished transactions  S -> PM.
// Answer unfinished transactions  PM -> S.
type UnfinishedTransactions struct {
	PktHead
}

type AnswerUnfinishedTransactions struct {
	PktHead
	MaxTID  Tid
	TidList []struct{
		UnfinishedTID Tid
	}
}

// Ask locked transactions  PM -> S.
// Answer locked transactions  S -> PM.
type LockedTransactions struct {
	PktHead
}

type AnswerLockedTransactions struct {
	PktHead
	TidDict map[Tid]Tid     // ttid -> tid
}

// Return final tid if ttid has been committed. * -> S. C -> PM.
type FinalTID struct {
	PktHead
	TTID    Tid
}

type AnswerFinalTID struct {
	PktHead
        Tid     Tid
}

// Commit a transaction. PM -> S.
type ValidateTransaction struct {
	PktHead
        TTID Tid
        Tid  Tid
}


// Ask to begin a new transaction. C -> PM.
// Answer when a transaction begin, give a TID if necessary. PM -> C.
type BeginTransaction struct {
	PktHead
	Tid     Tid
}

type AnswerBeginTransaction struct {
	PktHead
	Tid     Tid
}

// Finish a transaction. C -> PM.
// Answer when a transaction is finished. PM -> C.
type FinishTransaction struct {
	PktHead
        Tid         Tid
        OIDList     []Oid
        CheckedList []Oid
}

type AnswerFinishTransaction struct {
	PktHead
        TTID    Tid
        Tid     Tid
}

// Notify that a transaction blocking a replication is now finished
// M -> S
type NotifyTransactionFinished struct {
	PktHead
        TTID    Tid
        MaxTID  Tid
}

// Lock information on a transaction. PM -> S.
// Notify information on a transaction locked. S -> PM.
type LockInformation struct {
	PktHead
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
	PktHead
        Tid     Tid
        OidList []Oid
}

// Unlock information on a transaction. PM -> S.
type UnlockInformation struct {
	PktHead
        TTID    Tid
}

// Ask new object IDs. C -> PM.
// Answer new object IDs. PM -> C.
type GenerateOIDs struct {
	PktHead
	NumOIDs uint32  // PNumber
}

// XXX answer_new_oids ?
type AnswerGenerateOIDs struct {
	PktHead
        OidList []Oid
}


// Ask to store an object. Send an OID, an original serial, a current
// transaction ID, and data. C -> S.
// Answer if an object has been stored. If an object is in conflict,
// a serial of the conflicting transaction is returned. In this case,
// if this serial is newer than the current transaction ID, a client
// node must not try to resolve the conflict. S -> C.
type StoreObject struct {
	PktHead
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
	PktHead
	Conflicting     bool
	Oid             Oid
	Serial          Tid
}

// Abort a transaction. C -> S, PM.
type AbortTransaction struct {
	PktHead
	Tid     Tid
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type StoreTransaction struct {
	PktHead
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
	PktHead
        Tid     Tid
	// TODO _answer = PFEmpty
}

// Ask a stored object by its OID and a serial or a TID if given. If a serial
// is specified, the specified revision of an object will be returned. If
// a TID is specified, an object right before the TID will be returned. C -> S.
// Answer the requested object. S -> C.
type GetObject struct {
	PktHead
	Oid     Oid
	Serial  Tid
	Tid     Tid
}

// XXX answer_object ?
type AnswerGetObject struct {
	PktHead
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
	PktHead
	First           uint64  // PIndex       XXX this is TID actually ? -> no it is offset in list
	Last            uint64  // PIndex       ----//----
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDList struct {
	PktHead
        TIDList []Tid
}

// Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
// C -> S.
// Answer the requested TIDs. S -> C
type TIDListFrom struct {
	PktHead
	MinTID          Tid
	MaxTID          Tid
	Length          uint32  // PNumber
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDListFrom struct {
	PktHead
	TidList []Tid
}

// Ask information about a transaction. Any -> S.
// Answer information (user, description) about a transaction. S -> Any.
type TransactionInformation struct {
	PktHead
	Tid     Tid
}

type AnswerTransactionInformation struct {
	PktHead
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
	PktHead
	Oid     Oid
	First   uint64  // PIndex       XXX this is actually TID
	Last    uint64  // PIndex       ----//----
}

type AnswerObjectHistory struct {
	PktHead
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
	PktHead
	MinOffset   uint32      // PNumber
	MaxOffset   uint32      // PNumber
	UUID        UUID
}

type AnswerPartitionList struct {
	PktHead
	PTid
        RowList []RowInfo
}

// Ask information about nodes
// Answer information about nodes
type X_NodeList struct {
	PktHead
        NodeType
}

type AnswerNodeList struct {
	PktHead
        NodeList []NodeInfo
}

// Set the node state
type SetNodeState struct {
	PktHead
	UUID
        NodeState

	// XXX _answer = Error ?
}

// Ask the primary to include some pending node in the partition table
type AddPendingNodes struct {
	PktHead
        UUIDList []UUID

	// XXX _answer = Error
}

// Ask the primary to optimize the partition table. A -> PM.
type TweakPartitionTable struct {
	PktHead
        UUIDList []UUID

	// XXX _answer = Error
}

// Notify information about one or more nodes. PM -> Any.
type NotifyNodeInformation struct {
	PktHead
        NodeList []NodeInfo
}

// Ask node information
type NodeInformation struct {
	PktHead
	// XXX _answer = PFEmpty
}

// Set the cluster state
type SetClusterState struct {
	PktHead
	State   ClusterState

	// XXX _answer = Error
}

// Notify information about the cluster
type ClusterInformation struct {
	PktHead
	State   ClusterState
}

// Ask state of the cluster
// Answer state of the cluster
type X_ClusterState struct {	// XXX conflicts with ClusterState enum
	PktHead
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
	PktHead
        Tid             Tid
        LTID            Tid
        UndoneTID       Tid
        OidList         []Oid
}

// XXX answer_undo_transaction ?
type AnswerObjectUndoSerial struct {
	PktHead
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
	PktHead
        Tid     Tid
        Oid     Oid
}

type AnswerHasLock struct {
	PktHead
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
	PktHead
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
	PktHead
        Tid     Tid
}

type AnswerPack struct {
	PktHead
	Status  bool
}


// ctl -> A
// A -> M
type CheckReplicas struct {
	PktHead
	PartitionDict map[uint32/*PNumber*/]UUID        // partition -> source
	MinTID  Tid
	MaxTID  Tid

	// XXX _answer = Error
}

// M -> S
type CheckPartition struct {
	PktHead
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
	PktHead
        Partition uint32        // PNumber
        Length    uint32        // PNumber
        MinTID    Tid
        MaxTID    Tid
}

type AnswerCheckTIDRange struct {
	PktHead
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
	PktHead
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
	PktHead
        Partition       uint32  // PNumber
        CellList        []UUID
}


// Ask last committed TID.
// C -> M
// Answer last committed TID.
// M -> C
type LastTransaction struct {
	PktHead
}

type AnswerLastTransaction struct {
	PktHead
	Tid     Tid
}


// Notify that node is ready to serve requests.
// S -> M
type NotifyReady struct {
	PktHead
}

// replication

// TODO
