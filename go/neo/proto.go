//go:generate sh -c "go run protogen.go >proto-marshal.go"

package neo
// protocol definition


// NOTE for some packets it is possible to decode raw packet -> go version from
// PktBuf in place. E.g. for GetObject.
//
// TODO work this out

// XXX move imports out of here
import (
	"../zodb"

	"encoding/binary"
	"errors"
	"math"
)

const (
	// The protocol version must be increased whenever upgrading a node may require
	// to upgrade other nodes. It is encoded as a 4-bytes big-endian integer and
	// the high order byte 0 is different from TLS Handshake (0x16).
	PROTOCOL_VERSION = 1
	// XXX ENCODED_VERSION ?

	PktHeadLen = 10	// XXX unsafe.Sizeof(PktHead{}) give _typed_ constant (uintptr)
			// TODO link this to PktHead.Encode/Decode size ? XXX -> pkt.go ?

	MAX_PACKET_SIZE = 0x4000000

	RESPONSE_MASK   = 0x8000
)

type ErrorCode uint32
const (
	ACK ErrorCode = iota
	NOT_READY
	OID_NOT_FOUND
	TID_NOT_FOUND
	OID_DOES_NOT_EXIST
	PROTOCOL_ERROR
	REPLICATION_ERROR
	CHECKING_ERROR
	BACKEND_NOT_IMPLEMENTED
	NON_READABLE_CELL
	READ_ONLY_ACCESS
	INCOMPLETE_TRANSACTION
)

type ClusterState int32
const (
	// Once the primary master is elected, the cluster has a state, which is
	// initially ClusterRecovery, during which the master:
	// - first recovers its own data by reading it from storage nodes;
	// - waits for the partition table be operational;
	// - automatically switch to ClusterVerifying if the cluster can be safely started. XXX not automatic
	// Whenever the partition table becomes non-operational again, the cluster
	// goes back to this state.
	ClusterRecovering	ClusterState = iota
	// Transient state, used to:
	// - replay the transaction log, in case of unclean shutdown;
	// - and actually truncate the DB if the user asked to do so.
	// Then, the cluster either goes to ClusterRunning or STARTING_BACKUP state.
	ClusterVerifying	// XXX = ClusterStarting
	// Normal operation. The DB is read-writable by clients.
	ClusterRunning
	// Transient state to shutdown the whole cluster.
	ClusterStopping
	// Transient state, during which the master (re)connect to the upstream
	// master.
	STARTING_BACKUP
	// Backup operation. The master is notified of new transactions thanks to
	// invalidations and orders storage nodes to fetch them from upstream.
	// Because cells are synchronized independently, the DB is often
	// inconsistent.
	BACKINGUP
	// Transient state, when the user decides to go back to RUNNING state.
	// The master stays in this state until the DB is consistent again.
	// In case of failure, the cluster will go back to backup mode.
	STOPPING_BACKUP
)

type NodeType int32
const (
	MASTER NodeType = iota
	STORAGE
	CLIENT
	ADMIN
)

type NodeState int32
const (
	UNKNOWN NodeState = iota //short: U     // XXX tag prefix name ?
	DOWN                     //short: D
	RUNNING                  //short: R
	PENDING                  //short: P
)

type CellState int32
const (
	// Normal state: cell is writable/readable, and it isn't planned to drop it.
	UP_TO_DATE CellState = iota //short: U     // XXX tag prefix name ?
	// Write-only cell. Last transactions are missing because storage is/was down
	// for a while, or because it is new for the partition. It usually becomes
	// UP_TO_DATE when replication is done.
	OUT_OF_DATE                 //short: O
	// Same as UP_TO_DATE, except that it will be discarded as soon as another
	// node finishes to replicate it. It means a partition is moved from 1 node
	// to another.
	FEEDING                     //short: F
	// Not really a state: only used in network packets to tell storages to drop
	// partitions.
	DISCARDED                   //short: D
	// A check revealed that data differs from other replicas. Cell is neither
	// readable nor writable.
	CORRUPTED                   //short: C
)

// NodeUUID is a node identifier, 4-bytes signed integer
//
// High-order byte:
// 7 6 5 4 3 2 1 0
// | | | | +-+-+-+-- reserved (0)
// | +-+-+---------- node type
// +---------------- temporary if negative
// UUID namespaces are required to prevent conflicts when the master generate
// new uuid before it knows uuid of existing storage nodes. So only the high
// order bit is really important and the 31 other bits could be random.
// Extra namespace information and non-randomness of 3 LOB help to read logs.
//
// TODO -> back to 16-bytes randomly generated UUID
type NodeUUID int32

// TODO NodeType -> base NodeUUID


// ErrDecodeOverflow is the error returned by NEOPktDecode when decoding hit buffer overflow
var ErrDecodeOverflow = errors.New("decode: bufer overflow")

// Pkt is the interface implemented by NEO packets to marshal/unmarshal them into/from wire format
type Pkt interface {
	// NEOPktMsgCode returns message code needed to be used for particular packet type
	// on the wire
	NEOPktMsgCode() uint16

	// NEOPktEncodedLen returns how much space is needed to encode current state
	NEOPktEncodedLen() int

	// NEOPktEncode encodes current state into buf.
	// len(buf) must be >= NEOPktEncodedLen()
	NEOPktEncode(buf []byte)

	// NEOPktDecode decodes data into current packet state.
	NEOPktDecode(data []byte) (nread int, err error)
}


type Address struct {
	Host string
	Port uint16
}

// NOTE if Host == "" -> Port not added to wire (see py.PAddress):
// func (a *Address) NEOPktEncode(b []byte) int {
// 	n := string_NEOEncode(a.Host, b[0:])
// 	if a.Host != "" {
// 		BigEndian.PutUint16(b[n:], a.Port)
// 		n += 2
// 	}
// 	return n
// }
// 
// func (a *Address) NEODecode(b []byte) int {
// 	n := string_NEODecode(&a.Host, b)
// 	if a.Host != "" {
// 		a.Port = BigEndian.Uint16(b[n:])
// 		n += 2
// 	} else {
// 		a.Port = 0
// 	}
// 	return n
// }

// A SHA1 hash
type Checksum [20]byte

// Partition Table identifier
// Zero value means "invalid id" (<-> None in py.PPTID)
type PTid uint64


// XXX move -> marshalutil.go ?
func byte2bool(b byte) bool {
	return b != 0
}

func bool2byte(b bool) byte {
	if b {
		return 1
	} else {
		return 0
	}
}

// NOTE py.None encodes as '\xff' * 8	(-> we use NaN for None)
// NOTE '\xff' * 8 represents FP NaN but many other NaN bits representations exist
func float64_NEOEncode(b []byte, f float64) {
	var fu uint64
	if !math.IsNaN(f) {
		fu = math.Float64bits(f)
	} else {
		// convert all NaNs to canonical \xff * 8
		fu = 1<<64 - 1
	}

	binary.BigEndian.PutUint64(b, fu)
}

func float64_NEODecode(b []byte) float64 {
	fu := binary.BigEndian.Uint64(b)
	return math.Float64frombits(fu)
}

// NodeInfo is information about a node
// XXX -> nodetab.go ?
type NodeInfo struct {
	NodeType
	Address		// serving address
	NodeUUID
	NodeState
	IdTimestamp float64	// FIXME clarify semantic where it is used
}

// XXX -> parttab.go ?
type CellInfo struct {
	NodeUUID
	CellState
}

type RowInfo struct {
	Offset   uint32  // PNumber
	CellList []CellInfo
}



// Error is a special type of message, because this can be sent against
// any other message, even if such a message does not expect a reply
// usually. Any -> Any.
type Error struct {
	Code    ErrorCode  // PNumber
	Message string
}

// Check if a peer is still alive. Any -> Any.
type Ping struct {
	// TODO _answer = PFEmpty
}

// Tell peer it can close the connection if it has finished with us. Any -> Any
type CloseClient struct {
}

// Request a node identification. This must be the first packet for any
// connection. Any -> Any.
type RequestIdentification struct {
	NodeType        NodeType        // XXX name
	NodeUUID	NodeUUID
	Address		Address		// where requesting node is also accepting connections
	ClusterName	string
	IdTimestamp	float64
}

// XXX -> ReplyIdentification? RequestIdentification.Answer somehow ?
type AcceptIdentification struct {
	NodeType        NodeType        // XXX name
	MyNodeUUID      NodeUUID
	NumPartitions   uint32          // PNumber
	NumReplicas     uint32          // PNumber
	YourNodeUUID    NodeUUID
}

// Ask current primary master's uuid. CTL -> A.
type PrimaryMaster struct {
}

type AnswerPrimary struct {
	PrimaryNodeUUID NodeUUID
}

// Send list of known master nodes. SM -> Any.
type NotPrimaryMaster struct {
	Primary         NodeUUID	// XXX PSignedNull in py
	KnownMasterList []struct {
		Address
	}
}

// Ask all data needed by master to recover. PM -> S, S -> PM.
type Recovery struct {
}

type AnswerRecovery struct {
	PTid
	BackupTid   zodb.Tid
	TruncateTid zodb.Tid
}

// Ask the last OID/TID so that a master can initialize its TransactionManager.
// PM -> S, S -> PM.
type LastIDs struct {
}

type AnswerLastIDs struct {
	LastOid zodb.Oid
	LastTid zodb.Tid
}

// Ask the full partition table. PM -> S.
// Answer rows in a partition table. S -> PM.
// XXX overlap with PartitionTable struct
type X_PartitionTable struct {
}

type AnswerPartitionTable struct {
	PTid
	RowList []RowInfo
}


// Send rows in a partition table to update other nodes. PM -> S, C.
type NotifyPartitionTable struct {
	PTid
	RowList []RowInfo
}

// Notify a subset of a partition table. This is used to notify changes.
// PM -> S, C.
type PartitionChanges struct {
	PTid
	CellList []struct {
		// XXX does below correlate with Cell inside top-level CellList ?
		Offset    uint32  // PNumber
		NodeUUID  NodeUUID
		CellState CellState
	}
}

// Tell a storage nodes to start an operation. Until a storage node receives
// this message, it must not serve client nodes. PM -> S.
type StartOperation struct {
	// XXX: Is this boolean needed ? Maybe this
	//      can be deduced from cluster state.
	Backup bool
}

// Tell a storage node to stop an operation. Once a storage node receives
// this message, it must not serve client nodes. PM -> S.
type StopOperation struct {
}

// Ask unfinished transactions  S -> PM.
// Answer unfinished transactions  PM -> S.
type UnfinishedTransactions struct {
	RowList []struct{
		Offset uint32	// PNumber
	}
}

type AnswerUnfinishedTransactions struct {
	MaxTID  zodb.Tid
	TidList []struct{
		UnfinishedTID zodb.Tid
	}
}

// Ask locked transactions  PM -> S.
// Answer locked transactions  S -> PM.
type LockedTransactions struct {
}

type AnswerLockedTransactions struct {
	TidDict map[zodb.Tid]zodb.Tid     // ttid -> tid
}

// Return final tid if ttid has been committed. * -> S. C -> PM.
type FinalTID struct {
	TTID    zodb.Tid
}

type AnswerFinalTID struct {
	Tid     zodb.Tid
}

// Commit a transaction. PM -> S.
type ValidateTransaction struct {
	TTID zodb.Tid
	Tid  zodb.Tid
}


// Ask to begin a new transaction. C -> PM.
// Answer when a transaction begin, give a TID if necessary. PM -> C.
type BeginTransaction struct {
	Tid     zodb.Tid
}

type AnswerBeginTransaction struct {
	Tid     zodb.Tid
}


// Report storage nodes for which vote failed. C -> M
// True is returned if it's still possible to finish the transaction.
type FailedVote struct {
	Tid	 zodb.Tid
	NodeList []NodeUUID

	// XXX _answer = Error
}

// Finish a transaction. C -> PM.
// Answer when a transaction is finished. PM -> C.
type FinishTransaction struct {
	Tid	 zodb.Tid
	OIDList     []zodb.Oid
	CheckedList []zodb.Oid
}

type AnswerFinishTransaction struct {
	TTID    zodb.Tid
	Tid     zodb.Tid
}

// Notify that a transaction blocking a replication is now finished
// M -> S
type NotifyTransactionFinished struct {
	TTID    zodb.Tid
	MaxTID  zodb.Tid
}

// Lock information on a transaction. PM -> S.
// Notify information on a transaction locked. S -> PM.
type LockInformation struct {
	Ttid zodb.Tid
	Tid  zodb.Tid
}

// XXX AnswerInformationLocked ?
type AnswerLockInformation struct {
	Ttid zodb.Tid
}

// Invalidate objects. PM -> C.
// XXX ask_finish_transaction ?
type InvalidateObjects struct {
	Tid     zodb.Tid
	OidList []zodb.Oid
}

// Unlock information on a transaction. PM -> S.
type UnlockInformation struct {
	TTID    zodb.Tid
}

// Ask new object IDs. C -> PM.
// Answer new object IDs. PM -> C.
type GenerateOIDs struct {
	NumOIDs uint32  // PNumber
}

// XXX answer_new_oids ?
type AnswerGenerateOIDs struct {
	OidList []zodb.Oid
}


// Ask master to generate a new TTID that will be used by the client
// to rebase a transaction. S -> PM -> C
type Deadlock struct {
	TTid		zodb.Tid
	LockingTid	zodb.Tid
}

// Rebase transaction. C -> S.
type RebaseTransaction struct {
	TTid		zodb.Tid
	LockingTid	zodb.Tid
}

type AnswerRebaseTransaction struct {
	OidList	[]zodb.Oid
}

// Rebase object. C -> S.
type RebaseObject struct {
	TTid	zodb.Tid
	Oid	zodb.Oid
}

type AnswerRebaseObject struct {
	// FIXME POption('conflict')
        Serial		zodb.Tid
        ConflictSerial	zodb.Tid
	// FIXME POption('data')
		Compression	bool
		Checksum	Checksum
		Data		[]byte	// XXX was string
}


// Ask to store an object. Send an OID, an original serial, a current
// transaction ID, and data. C -> S.
// As for IStorage, 'serial' is ZERO_TID for new objects.
type StoreObject struct {
	Oid             zodb.Oid
	Serial          zodb.Tid
	Compression     bool
	Checksum        Checksum
	Data            []byte          // TODO separately (for writev)
	DataSerial      zodb.Tid
	Tid             zodb.Tid
}

type AnswerStoreObject struct {
	Conflict	zodb.Tid
}

// Abort a transaction. C -> S and C -> PM -> S.
type AbortTransaction struct {
	Tid		zodb.Tid
	NodeList	[]NodeUUID		// unused for * -> S
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type StoreTransaction struct {
	Tid	     zodb.Tid
	User            string
	Description     string
	Extension       string
	OidList         []zodb.Oid
	// TODO _answer = PFEmpty
}

// Ask to store a transaction. C -> S.
// Answer if transaction has been stored. S -> C.
type VoteTransaction struct {
	Tid     zodb.Tid
	// TODO _answer = PFEmpty
}

// Ask a stored object by its OID and a serial or a TID if given. If a serial
// is specified, the specified revision of an object will be returned. If
// a TID is specified, an object right before the TID will be returned. C -> S.
// Answer the requested object. S -> C.
type GetObject struct {
	Oid     zodb.Oid
	Serial  zodb.Tid
	Tid     zodb.Tid
}

// XXX answer_object ?
type AnswerGetObject struct {
	Oid             zodb.Oid
	Serial		zodb.Tid	// XXX strictly is SerialStart/SerialEnd in proto.py
	NextSerial      zodb.Tid	// XXX but there it is out of sync
	Compression     bool
	Checksum        Checksum
	Data            []byte          // TODO separately (for writev)
	DataSerial      zodb.Tid
}

// Ask for TIDs between a range of offsets. The order of TIDs is descending,
// and the range is [first, last). C -> S.
// Answer the requested TIDs. S -> C.
type TIDList struct {
	First           uint64  // PIndex       XXX this is TID actually ? -> no it is offset in list
	Last            uint64  // PIndex       ----//----
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDList struct {
	TIDList []zodb.Tid
}

// Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
// C -> S.
// Answer the requested TIDs. S -> C
type TIDListFrom struct {
	MinTID          zodb.Tid
	MaxTID          zodb.Tid
	Length          uint32  // PNumber
	Partition       uint32  // PNumber
}

// XXX answer_tids ?
type AnswerTIDListFrom struct {
	TidList []zodb.Tid
}

// Ask information about a transaction. Any -> S.
// Answer information (user, description) about a transaction. S -> Any.
type TransactionInformation struct {
	Tid     zodb.Tid
}

type AnswerTransactionInformation struct {
	Tid             zodb.Tid
	User            string
	Description     string
	Extension       string
	Packed          bool
	OidList         []zodb.Oid
}

// Ask history information for a given object. The order of serials is
// descending, and the range is [first, last]. C -> S.
// Answer history information (serial, size) for an object. S -> C.
type ObjectHistory struct {
	Oid     zodb.Oid
	First   uint64  // PIndex       XXX this is actually TID
	Last    uint64  // PIndex       ----//----
}

type AnswerObjectHistory struct {
	Oid         zodb.Oid
	HistoryList []struct {
	        Serial  zodb.Tid
	        Size    uint32  // PNumber
	}
}

// All the following messages are for neoctl to admin node
// Ask information about partition
// Answer information about partition
type PartitionList struct {
	MinOffset   uint32      // PNumber
	MaxOffset   uint32      // PNumber
	NodeUUID    NodeUUID
}

type AnswerPartitionList struct {
	PTid
	RowList []RowInfo
}

// Ask information about nodes
// Answer information about nodes
//
// XXX neoctl -> A	(A just extracts data from its nodetab)
type NodeList struct {
	NodeType
}

type AnswerNodeList struct {
	NodeList []NodeInfo
}

// Set the node state
type SetNodeState struct {
	NodeUUID
	NodeState

	// XXX _answer = Error ?
}

// Ask the primary to include some pending node in the partition table
type AddPendingNodes struct {
	NodeList []NodeUUID

	// XXX _answer = Error
}

// Ask the primary to optimize the partition table. A -> PM.
type TweakPartitionTable struct {
	NodeList []NodeUUID

	// XXX _answer = Error
}

// Notify information about one or more nodes. PM -> Any.
type NotifyNodeInformation struct {
	IdTimestamp	float64
	NodeList	[]NodeInfo
}

// Ask node information
type NodeInformation struct {
	// XXX _answer = PFEmpty
}

// Set the cluster state
type SetClusterState struct {
	State   ClusterState

	// XXX _answer = Error
}

// XXX only helper: should not be presented as packet
type repairFlags struct {
	DryRun   bool
	// pruneOrphan bool

	// XXX _answer = Error
}

// Ask storage nodes to repair their databases. ctl -> A -> M
type Repair struct {
	NodeList []NodeUUID
	repairFlags
}

// See Repair. M -> S
type RepairOne struct {
	repairFlags
}

// Notify information about the cluster
type ClusterInformation struct {
	State   ClusterState
}

// Ask state of the cluster
// Answer state of the cluster
type X_ClusterState struct {	// XXX conflicts with ClusterState enum
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
	Tid             zodb.Tid
	LTID            zodb.Tid
	UndoneTID       zodb.Tid
	OidList         []zodb.Oid
}

// XXX answer_undo_transaction ?
type AnswerObjectUndoSerial struct {
	ObjectTIDDict map[zodb.Oid]struct {
		CurrentSerial   zodb.Tid
		UndoSerial      zodb.Tid
		IsCurrent       bool
	}
}


// Verifies if given serial is current for object oid in the database, and
// take a write lock on it (so that this state is not altered until
// transaction ends).
// Answer to AskCheckCurrentSerial.
// Same structure as AnswerStoreObject, to handle the same way, except there
// is nothing to invalidate in any client's cache.
type CheckCurrentSerial struct {
	Tid     zodb.Tid
	Oid     zodb.Oid
	Serial  zodb.Tid
}

// XXX answer_store_object ?	(was _answer = StoreObject._answer in py)
type AnswerCheckCurrentSerial AnswerStoreObject
//type AnswerCheckCurrentSerial struct {
//	Conflict	bool
//}

// Request a pack at given TID.
// C -> M
// M -> S
// Inform that packing it over.
// S -> M
// M -> C
type Pack struct {
	Tid     zodb.Tid
}

type AnswerPack struct {
	Status  bool
}


// ctl -> A
// A -> M
type CheckReplicas struct {
	PartitionDict map[uint32]NodeUUID        // partition -> source	(PNumber)
	MinTID  zodb.Tid
	MaxTID  zodb.Tid

	// XXX _answer = Error
}

// M -> S
type CheckPartition struct {
	Partition uint32  // PNumber
	Source    struct {
		UpstreamName string
		Address      Address
	}
	MinTID    zodb.Tid
	MaxTID    zodb.Tid
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
	Partition uint32        // PNumber
	Length    uint32        // PNumber
	MinTID    zodb.Tid
	MaxTID    zodb.Tid
}

type AnswerCheckTIDRange struct {
	Count    uint32         // PNumber
	Checksum Checksum
	MaxTID   zodb.Tid
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
	Partition       uint32  // PNumber
	Length          uint32  // PNumber
	MinTID          zodb.Tid
	MaxTID          zodb.Tid
	MinOID          zodb.Oid
}

type AnswerCheckSerialRange struct {
	Count           uint32  // PNumber
	TidChecksum     Checksum
	MaxTID          zodb.Tid
	OidChecksum     Checksum
	MaxOID          zodb.Oid
}

// S -> M
type PartitionCorrupted struct {
	Partition       uint32  // PNumber
	CellList        []NodeUUID
}


// Ask last committed TID.
// C -> M
// Answer last committed TID.
// M -> C
type LastTransaction struct {
}

type AnswerLastTransaction struct {
	Tid     zodb.Tid
}


// Notify that node is ready to serve requests.
// S -> M
type NotifyReady struct {
}

// replication

// TODO
