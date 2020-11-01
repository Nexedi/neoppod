// Copyright (C) 2006-2020  Nexedi SA and Contributors.
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

// Package proto provides definition of NEO messages and their marshalling
// to/from wire format.
//
// Two NEO nodes can exchange messages over underlying network link after
// performing NEO-specific handshake. A message is sent as a packet specifying
// ID of subconnection multiplexed on top of the underlying link, carried
// message code and message data.
//
// PktHeader describes packet header structure.
//
// Messages are represented by corresponding types that all implement Msg interface.
//
// A message type can be looked up by message code with MsgType.
//
// The proto packages provides only message definitions and low-level
// primitives for their marshalling. Package lab.nexedi.com/kirr/neo/go/neo/neonet
// provides actual service for message exchange over network.
package proto

// This file defines everything that relates to messages on the wire.
// In particular every type that is included in a message is defined here as well.
//
// By default a structure defined in this file becomes a separate network message.
// If a structure is defined only to represent basic type that is e.g. included in
// several messages and does not itself denote a separate message, its
// definition is prefixed with `//neo:proto typeonly` comment.
//
// The order of message definitions is significant - messages are assigned
// message codes in the same order they are defined.
//
// For compatibility with neo/py a message has its code assigned with "answer"
// bit set if either message name starts with "Answer" or message definition is
// prefixed with `//neo:proto answer` comment.
//
// Packet structure and messages are bit-to-bit compatible with neo/py (see protocol.py).
//
// The code to marshal/unmarshal messages is generated by protogen.go .

//go:generate sh -c "go run protogen.go >zproto-marshal.go"


// TODO regroup messages definitions to stay more close to 1 communication topic
// TODO document protocol itself better (who sends who what with which semantic)

// NOTE for some packets it is possible to decode raw packet -> go version from
// PktBuf in place. E.g. for GetObject.
//
// TODO work this out

import (
	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/neo/go/internal/packed"

	"encoding/binary"
	"errors"
	"math"
)

const (
	// The protocol version must be increased whenever upgrading a node may require
	// to upgrade other nodes. It is encoded as a 4-bytes big-endian integer and
	// the high order byte 0 is different from TLS Handshake (0x16).
	Version = 1

	// length of packet header
	PktHeaderLen = 10 // = unsafe.Sizeof(PktHeader{}), but latter gives typed constant (uintptr)

	// packets larger than PktMaxSize are not allowed.
	// this helps to avoid out-of-memory error on packets with corrupt message len.
	PktMaxSize = 0x4000000

	// answerBit is set in message code in answer messages for compatibility with neo/py
	answerBit = 0x8000

	//INVALID_UUID UUID = 0

	INVALID_TID zodb.Tid = 1<<64 - 1 // 0xffffffffffffffff
	INVALID_OID zodb.Oid = 1<<64 - 1
)

// PktHeader represents header of a raw packet.
//
// A packet contains connection ID and message.
//
//neo:proto typeonly
type PktHeader struct {
	ConnId  packed.BE32 // NOTE is .msgid in py
	MsgCode packed.BE16 // payload message code
	MsgLen  packed.BE32 // payload message length (excluding packet header)
}

// Msg is the interface implemented by all NEO messages.
type Msg interface {
	// marshal/unmarshal into/from wire format:

	// NEOMsgCode returns message code needed to be used for particular message type
	// on the wire.
	NEOMsgCode() uint16

	// NEOMsgEncodedLen returns how much space is needed to encode current message payload.
	NEOMsgEncodedLen() int

	// NEOMsgEncode encodes current message state into buf.
	//
	// len(buf) must be >= neoMsgEncodedLen().
	NEOMsgEncode(buf []byte)

	// NEOMsgDecode decodes data into message in-place.
	NEOMsgDecode(data []byte) (nread int, err error)
}

// ErrDecodeOverflow is the error returned by neoMsgDecode when decoding hits buffer overflow
var ErrDecodeOverflow = errors.New("decode: buffer overflow")

// ---- messages ----

type ErrorCode int8
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

type ClusterState int8
const (
	// The cluster is initially in the RECOVERING state, and it goes back to
	// this state whenever the partition table becomes non-operational again.
	// An election of the primary master always happens, in case of a network
	// cut between a primary master and all other nodes. The primary master:
	// - first recovers its own data by reading it from storage nodes;
	// - waits for the partition table be operational;
	// - automatically switch to ClusterVerifying if the cluster can be safely started. XXX not automatic
	ClusterRecovering ClusterState = iota
	// Transient state, used to:
	// - replay the transaction log, in case of unclean shutdown;
	// - and actually truncate the DB if the user asked to do so.
	// Then, the cluster either goes to ClusterRunning or STARTING_BACKUP state.
	ClusterVerifying
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

type NodeType int8
const (
	MASTER NodeType = iota
	STORAGE
	CLIENT
	ADMIN
)

type NodeState int8
const (
	UNKNOWN NodeState = iota //short: U     // XXX tag prefix name ?
	DOWN                     //short: D
	RUNNING                  //short: R
	PENDING                  //short: P
)

type CellState int8
const (
	// Write-only cell. Last transactions are missing because storage is/was down
	// for a while, or because it is new for the partition. It usually becomes
	// UP_TO_DATE when replication is done.
	OUT_OF_DATE CellState = iota //short: O    // XXX tag prefix name ?
	// Normal state: cell is writable/readable, and it isn't planned to drop it.
	UP_TO_DATE                   //short: U
	// Same as UP_TO_DATE, except that it will be discarded as soon as another
	// node finishes to replicate it. It means a partition is moved from 1 node
	// to another. It is also discarded immediately if out-of-date.
	FEEDING                      //short: F
	// A check revealed that data differs from other replicas. Cell is neither
	// readable nor writable.
	CORRUPTED                    //short: C
	// Not really a state: only used in network messages to tell storages to drop
	// partitions.
	DISCARDED                    //short: D
)

// NodeUUID is a node identifier, 4-bytes signed integer
//
// High-order byte:
//
//	7 6 5 4 3 2 1 0
// 	| | | | +-+-+-+-- reserved (0)
// 	| +-+-+---------- node type
// 	+---------------- temporary if negative
//
// UUID namespaces are required to prevent conflicts when the master generate
// new uuid before it knows uuid of existing storage nodes. So only the high
// order bit is really important and the 31 other bits could be random.
// Extra namespace information and non-randomness of 3 LOB help to read logs.
//
// 0 is invalid NodeUUID	XXX correct?
//
// TODO -> back to 16-bytes randomly generated UUID
type NodeUUID int32

// TODO NodeType -> base NodeUUID


// Address represents host:port network endpoint.
//
//neo:proto typeonly
type Address struct {
	Host string
	Port uint16
}

// NOTE if Host == "" -> Port not added to wire (see py.PAddress):
func (a *Address) neoEncodedLen() int {
	l := string_neoEncodedLen(a.Host)
	if a.Host != "" {
		l += 2
	}
	return l
}

func (a *Address) neoEncode(b []byte) int {
	n := string_neoEncode(a.Host, b[0:])
	if a.Host != "" {
		binary.BigEndian.PutUint16(b[n:], a.Port)
		n += 2
	}
	return n
}

func (a *Address) neoDecode(b []byte) (uint64, bool) {
	n, ok := string_neoDecode(&a.Host, b)
	if !ok {
		return 0, false
	}
	if a.Host != "" {
		b = b[n:]
		if len(b) < 2 {
			return 0, false
		}
		a.Port = binary.BigEndian.Uint16(b)
		n += 2
	} else {
		a.Port = 0
	}
	return n, true
}

// Checksum is a SHA1 hash.
type Checksum [20]byte

// PTid is Partition Table identifier.
//
// Zero value means "invalid id" (<-> None in py.PPTID)
type PTid uint64

// IdTime represents time of identification.
type IdTime float64

func (t IdTime) neoEncodedLen() int {
	return 8
}

func (t IdTime) neoEncode(b []byte) int {
	// use -inf as value for no data (NaN != NaN -> hard to use NaN in tests)
	// NOTE neo/py uses None for "no data"; we use 0 for "no data" to avoid pointer
	tt := float64(t)
	if tt == math.Inf(-1) {
		tt = math.NaN()
	}
	float64_neoEncode(b, tt)
	return 8
}

func (t *IdTime) neoDecode(data []byte) (uint64, bool) {
	if len(data) < 8 {
		return 0, false
	}
	tt := float64_neoDecode(data)
	if math.IsNaN(tt) {
		tt = math.Inf(-1)
	}
	*t = IdTime(tt)
	return 8, true
}

// NodeInfo is information about a node.
//
//neo:proto typeonly
type NodeInfo struct {
	Type   NodeType
	Addr   Address // serving address
	UUID   NodeUUID
	State  NodeState
	IdTime IdTime // XXX clarify semantic where it is used
}

//neo:proto typeonly
type CellInfo struct {
	UUID  NodeUUID
	State CellState
}

//neo:proto typeonly
type RowInfo struct {
	Offset   uint32 // PNumber	XXX -> Pid
	CellList []CellInfo
}



// Error is a special type of message, because this can be sent against
// any other message, even if such a message does not expect a reply
// usually.
//
//neo:nodes * -> *
//neo:proto answer
type Error struct {
	Code    ErrorCode // PNumber
	Message string
}

// Request a node identification. This must be the first message for any
// connection.
//
//neo:nodes * -> *
type RequestIdentification struct {
	NodeType    NodeType // XXX name
	UUID        NodeUUID
	Address     Address // where requesting node is also accepting connections
	ClusterName string
	DevPath     []string // [] of devid
	IdTime      IdTime
}

//neo:proto answer
type AcceptIdentification struct {
	NodeType      NodeType // XXX name
	MyUUID        NodeUUID
	NumPartitions uint32 // PNumber
	NumReplicas   uint32 // PNumber
	YourUUID      NodeUUID
}

// Empty request used as network barrier.
//
//neo:nodes * -> *
type Ping struct{}

//neo:proto answer
type Pong struct{}

// Tell peer that it can close the connection if it has finished with us.
//
//neo:nodes * -> *
type CloseClient struct {
}

// Ask node identier of the current primary master.
//
//neo:nodes ctl -> A
type PrimaryMaster struct {
}

type AnswerPrimary struct {
	PrimaryNodeUUID NodeUUID
}

// Notify peer that I'm not the primary master. Attach any extra information
// to help the peer joining the cluster.
//
//neo:nodes SM -> *
type NotPrimaryMaster struct {
	Primary         NodeUUID // XXX PSignedNull in py
	KnownMasterList []struct {
		Address
	}
}

// Notify information about one or more nodes.
//
//neo:nodes M -> *
type NotifyNodeInformation struct {
	// NOTE in py this is monotonic_time() of call to broadcastNodesInformation() & friends
	IdTime   IdTime
	NodeList []NodeInfo
}

// Ask storage nodes data needed by master to recover.
// Reused by `neoctl print ids`.
//
//neo:nodes M -> S; ctl -> A -> M
type Recovery struct {
}

type AnswerRecovery struct {
	PTid
	BackupTid   zodb.Tid
	TruncateTid zodb.Tid
}

// Ask the last OID/TID so that a master can initialize its TransactionManager.
// Reused by `neoctl print ids`.
//
//neo:nodes M -> S; ctl -> A -> M
type LastIDs struct {
}

type AnswerLastIDs struct {
	LastOid zodb.Oid
	LastTid zodb.Tid
}

// Ask storage node the remaining data needed by master to recover.
// This is also how the clients get the full partition table on connection.
//
//neo:nodes M -> S; C -> M
type AskPartitionTable struct {
}

type AnswerPartitionTable struct {
	PTid
	RowList []RowInfo
}

// Send the full partition table to admin/storage nodes on connection.
//
//neo:nodes M -> A, S
type SendPartitionTable struct {
	PTid
	RowList []RowInfo
}

// Notify about changes in the partition table.
//
//neo:nodes M -> *
type NotifyPartitionChanges struct {
	PTid
	CellList []struct {
		Offset   uint32 // PNumber	XXX -> Pid
		CellInfo CellInfo
	}
}

// Tell a storage node to start operation. Before this message, it must only
// communicate with the primary master.
//
//neo:nodes M -> S
type StartOperation struct {
	// XXX: Is this boolean needed ? Maybe this
	//      can be deduced from cluster state.
	Backup bool
}

// Notify that the cluster is not operational anymore. Any operation between
// nodes must be aborted.
//
//neo:nodes M -> S, C
type StopOperation struct {
}

// Ask unfinished transactions, which will be replicated when they're finished.
//
//neo:nodes S -> M
type UnfinishedTransactions struct {
	RowList []struct {
		Offset uint32 // PNumber	XXX -> Pid
	}
}

type AnswerUnfinishedTransactions struct {
	MaxTID  zodb.Tid
	TidList []struct {
		UnfinishedTID zodb.Tid
	}
}

// Ask locked transactions to replay committed transactions that haven't been
// unlocked.
//
//neo:nodes M -> S
type LockedTransactions struct {
}

type AnswerLockedTransactions struct {
	TidDict map[zodb.Tid]zodb.Tid // ttid -> tid
}

// Return final tid if ttid has been committed, to recover from certain
// failures during tpc_finish.
//
//neo:nodes M -> S; C -> M, S
type FinalTID struct {
	TTID zodb.Tid
}

type AnswerFinalTID struct {
	Tid zodb.Tid
}

// Do replay a committed transaction that was not unlocked.
//
//neo:nodes M -> S
type ValidateTransaction struct {
	TTID zodb.Tid
	Tid  zodb.Tid
}


// Ask to begin a new transaction. This maps to `tpc_begin`.
//
//neo:nodes C -> M
type BeginTransaction struct {
	Tid zodb.Tid
}

type AnswerBeginTransaction struct {
	Tid zodb.Tid
}


// Report storage nodes for which vote failed.
// True is returned if it's still possible to finish the transaction.
//
//neo:nodes C -> M
type FailedVote struct {
	Tid      zodb.Tid
	NodeList []NodeUUID

	// answer = Error
}

// Finish a transaction. Return the TID of the committed transaction.
// This maps to `tpc_finish`.
//
//neo:nodes C -> M
type FinishTransaction struct {
	Tid         zodb.Tid // XXX this is ttid
	OIDList     []zodb.Oid
	CheckedList []zodb.Oid
}

type AnswerTransactionFinished struct {
	TTid zodb.Tid
	Tid  zodb.Tid
}

// Commit a transaction. The new data is read-locked.
//
//neo:nodes M -> S
type LockInformation struct {
	Ttid zodb.Tid
	Tid  zodb.Tid
}

type AnswerInformationLocked struct {
	Ttid zodb.Tid
}

// Notify about a new transaction modifying objects,
// invalidating client caches.
//
//neo:nodes M -> C
type InvalidateObjects struct {
	Tid     zodb.Tid
	OidList []zodb.Oid
}

// Notify about a successfully committed transaction. The new data can be
// unlocked.
//
//neo:nodes M -> S
// XXX -> InformationUnlocked?
type NotifyUnlockInformation struct {
	TTID zodb.Tid
}

// Ask new OIDs to create objects.
//
//neo:nodes C -> M
type AskNewOIDs struct {
	NumOIDs uint32 // PNumber
}

type AnswerNewOIDs struct {
	OidList []zodb.Oid
}


// Ask master to generate a new TTID that will be used by the client to solve
// a deadlock by rebasing the transaction on top of concurrent changes.
//
//neo:nodes S -> M -> C
// XXX -> Deadlocked?
type NotifyDeadlock struct {
	TTid       zodb.Tid
	LockingTid zodb.Tid
}

// Rebase a transaction to solve a deadlock.
//
//neo:nodes C -> S
type RebaseTransaction struct {
	TTid       zodb.Tid
	LockingTid zodb.Tid
}

type AnswerRebaseTransaction struct {
	OidList []zodb.Oid
}

// Rebase an object change to solve a deadlock.
//
//neo:nodes C -> S
//
// XXX: It is a request packet to simplify the implementation. For more
//      efficiency, this should be turned into a notification, and the
//      RebaseTransaction should answered once all objects are rebased
//      (so that the client can still wait on something).
type RebaseObject struct {
	TTid zodb.Tid
	Oid  zodb.Oid
}

type AnswerRebaseObject struct {
	// FIXME POption('conflict')
	Serial         zodb.Tid
	ConflictSerial zodb.Tid
	// FIXME POption('data')
		Compression bool
		Checksum    Checksum
		Data        *mem.Buf
}


// Ask to create/modify an object. This maps to `store`.
//
// As for IStorage, 'serial' is ZERO_TID for new objects.
//
//neo:nodes C -> S
type StoreObject struct {
	Oid         zodb.Oid
	Serial      zodb.Tid
	Compression bool
	Checksum    Checksum
	Data        []byte // TODO -> msg.Buf, separately (for writev)
	DataSerial  zodb.Tid
	Tid         zodb.Tid
}

type AnswerStoreObject struct {
	Conflict zodb.Tid
}

// Abort a transaction. This maps to `tpc_abort`.
//
//neo:nodes C -> S; C -> M -> S
type AbortTransaction struct {
	Tid      zodb.Tid
	NodeList []NodeUUID // unused for * -> S
}

// Ask to store a transaction. Implies vote.
//
//neo:nodes C -> S
type StoreTransaction struct {
	Tid         zodb.Tid
	User        string
	Description string
	Extension   string
	OidList     []zodb.Oid
}

type AnswerStoreTransaction struct{}

// Ask to vote a transaction.
//
//neo:nodes C -> S
type VoteTransaction struct {
	Tid zodb.Tid
}

type AnswerVoteTransaction struct{}

// Ask a stored object by its OID, optionally at/before a specific tid.
// This maps to `load/loadBefore/loadSerial`.
//
//neo:nodes C -> S
type GetObject struct {
	Oid    zodb.Oid
	At     zodb.Tid
	Before zodb.Tid
}

type AnswerObject struct {
	Oid         zodb.Oid
	Serial      zodb.Tid
	NextSerial  zodb.Tid
	Compression bool
	Checksum    Checksum
	Data        *mem.Buf // TODO encode -> separately (for writev)
	DataSerial  zodb.Tid
}

// Ask for TIDs between a range of offsets. The order of TIDs is descending,
// and the range is [first, last). This maps to `undoLog`.
//
//neo:nodes C -> S
type AskTIDs struct {
	First     uint64 // PIndex       [first, last) are offsets that define
	Last      uint64 // PIndex       range in tid list on remote.
	Partition uint32 // PNumber
}

type AnswerTIDs struct {
	TIDList []zodb.Tid
}

// Ask for transaction metadata.
//
//neo:nodes C -> S
type TransactionInformation struct {
	Tid zodb.Tid
}

type AnswerTransactionInformation struct {
	Tid         zodb.Tid
	User        string
	Description string
	Extension   string
	Packed      bool
	OidList     []zodb.Oid
}

// Ask history information for a given object. The order of serials is
// descending, and the range is [first, last]. This maps to `history`.
//
//neo:nodes C -> S
type ObjectHistory struct {
	Oid   zodb.Oid
	First uint64 // PIndex
	Last  uint64 // PIndex
}

type AnswerObjectHistory struct {
	Oid         zodb.Oid
	HistoryList []struct {
		Serial zodb.Tid
		Size   uint32 // PNumber
	}
}

// Ask information about partitions.
//
//neo:nodes ctl -> A
type PartitionList struct {
	MinOffset uint32 // PNumber
	MaxOffset uint32 // PNumber
	NodeUUID  NodeUUID
}

type AnswerPartitionList struct {
	PTid
	RowList []RowInfo
}

// Ask information about nodes.
//
//neo:nodes ctl -> A
type NodeList struct {
	NodeType
}

type AnswerNodeList struct {
	NodeList []NodeInfo
}

// Change the state of a node.
//
//neo:nodes ctl -> A -> M
type SetNodeState struct {
	NodeUUID
	NodeState

	// answer = Error
}

// Mark given pending nodes as running, for future inclusion when tweaking
// the partition table.
//
//neo:nodes ctl -> A -> M
type AddPendingNodes struct {
	NodeList []NodeUUID

	// answer = Error
}

// Ask the master to balance the partition table, optionally excluding
// specific nodes in anticipation of removing them.
//
//neo:nodes ctl -> A -> M
type TweakPartitionTable struct {
	NodeList []NodeUUID

	// answer = Error
}

// Set the cluster state.
//
//neo:nodes ctl -> A -> M
type SetClusterState struct {
	State ClusterState

	// answer = Error
}

//neo:proto typeonly
type repairFlags struct {
	DryRun bool
	// pruneOrphan bool

	// answer = Error
}

// Ask storage nodes to repair their databases.
//
//neo:nodes ctl -> A -> M
type Repair struct {
	NodeList []NodeUUID
	repairFlags
}

// Repair is translated to this message, asking a specific storage node to
// repair its database.
//
//neo:nodes M -> S
type RepairOne struct {
	repairFlags
}

// Notify about a cluster state change.
//
//neo:nodes M -> *
type NotifyClusterState struct {
	State ClusterState
}

// Ask the state of the cluster
//
//neo:nodes ctl -> A; A -> M
type AskClusterState struct {
}

type AnswerClusterState struct {
	State ClusterState
}


// Ask storage the serial where object data is when undoing given transaction,
// for a list of OIDs.
//
// object_tid_dict has the following format:
//     key: oid
//     value: 3-tuple
//         current_serial (TID)
//             The latest serial visible to the undoing transaction.
//         undo_serial (TID)
//             Where undone data is (tid at which data is before given undo).
//         is_current (bool)
//             If current_serial's data is current on storage.
//
//neo:nodes C -> S
type ObjectUndoSerial struct {
	Tid       zodb.Tid
	LTID      zodb.Tid
	UndoneTID zodb.Tid
	OidList   []zodb.Oid
}

type AnswerObjectUndoSerial struct {
	ObjectTIDDict map[zodb.Oid]struct {
		CurrentSerial zodb.Tid
		UndoSerial    zodb.Tid
		IsCurrent     bool
	}
}

// Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
// Used by `iterator`.
//
//neo:nodes C -> S
type AskTIDsFrom struct {
	MinTID    zodb.Tid
	MaxTID    zodb.Tid
	Length    uint32 // PNumber
	Partition uint32 // PNumber
}

type AnswerTIDsFrom struct {
	TidList []zodb.Tid
}


// Request a pack at given TID.
//
//neo:nodes C -> M -> S
type Pack struct {
	Tid zodb.Tid
}

type AnswerPack struct {
	Status bool
}


// Ask the cluster to search for mismatches between replicas, metadata only,
// and optionally within a specific range. Reference nodes can be specified.
//
//neo:nodes ctl -> A -> M
type CheckReplicas struct {
	PartitionDict map[uint32]NodeUUID // partition -> source	(PNumber)
	MinTID        zodb.Tid
	MaxTID        zodb.Tid

	// answer = Error
}

// Ask a storage node to compare a partition with all other nodes.
// Like for CheckReplicas, only metadata are checked, optionally within a
// specific range. A reference node can be specified.
//
//neo:nodes M -> S
type CheckPartition struct {
	Partition uint32 // PNumber
	Source    struct {
		UpstreamName string
		Address      Address
	}
	MinTID zodb.Tid
	MaxTID zodb.Tid
}


// Ask some stats about a range of transactions.
// Used to know if there are differences between a replicating node and
// reference node.
//
//neo:nodes S -> S
type CheckTIDRange struct {
	Partition uint32 // PNumber
	Length    uint32 // PNumber
	MinTID    zodb.Tid
	MaxTID    zodb.Tid
}

type AnswerCheckTIDRange struct {
	Count    uint32 // PNumber
	Checksum Checksum
	MaxTID   zodb.Tid
}

// Ask some stats about a range of object history.
// Used to know if there are differences between a replicating node and
// reference node.
//
//neo:nodes S -> S
type CheckSerialRange struct {
	Partition uint32 // PNumber
	Length    uint32 // PNumber
	MinTID    zodb.Tid
	MaxTID    zodb.Tid
	MinOID    zodb.Oid
}

type AnswerCheckSerialRange struct {
	Count       uint32 // PNumber
	TidChecksum Checksum
	MaxTID      zodb.Tid
	OidChecksum Checksum
	MaxOID      zodb.Oid
}

// Notify that mismatches were found while check replicas for a partition.
//
//neo:nodes S -> M
type PartitionCorrupted struct {
	Partition uint32 // PNumber
	CellList  []NodeUUID
}

// Notify that node is ready to serve requests.
//
//neo:nodes S -> M
type NotifyReady struct {
}


// Ask last committed TID.
//
//neo:nodes C -> M; ctl -> A -> M
type LastTransaction struct {
}

type AnswerLastTransaction struct {
	Tid zodb.Tid
}

// Check if given serial is current for the given oid, and lock it so that
// this state is not altered until transaction ends.
// This maps to `checkCurrentSerialInTransaction`.
//
//neo:nodes C -> S
type CheckCurrentSerial struct {
	Tid    zodb.Tid
	Oid    zodb.Oid
	Serial zodb.Tid
}

type AnswerCheckCurrentSerial struct {
	// was _answer = StoreObject._answer in py
	// XXX can we do without embedding e.g. `type AnswerCheckCurrentSerial AnswerStoreObject` ?
	AnswerStoreObject
}

// Notify that a transaction blocking a replication is now finished.
//
//neo:nodes M -> S
type NotifyTransactionFinished struct {
	TTID   zodb.Tid
	MaxTID zodb.Tid
}


// replication

// Notify a storage node to replicate partitions up to given 'tid'
// and from given sources.
//
// - upstream_name: replicate from an upstream cluster
// - address: address of the source storage node, or None if there's no new
//            data up to 'tid' for the given partition
//
//neo:nodes M -> S
type Replicate struct {
	Tid          zodb.Tid
	UpstreamName string
	SourceDict   map[uint32/*PNumber*/]string // partition -> address	FIXME string -> Address
}

// Notify the master node that a partition has been successfully replicated
// from a storage to another.
//
//neo:nodes S -> M
type ReplicationDone struct {
	Offset uint32 // PNumber
	Tid    zodb.Tid
}

// Ask a storage node to send all transaction data we don't have,
// and reply with the list of transactions we should not have.
//
//neo:nodes S -> S
type FetchTransactions struct {
	Partition    uint32 // PNumber
	Length       uint32 // PNumber
	MinTid       zodb.Tid
	MaxTid       zodb.Tid
	TxnKnownList []zodb.Tid // already known transactions
}

type AnswerFetchTransactions struct {
	PackTid       zodb.Tid
	NextTid       zodb.Tid
	TxnDeleteList []zodb.Tid // transactions to delete
}

// Ask a storage node to send object records we don't have,
// and reply with the list of records we should not have.
//
//neo:nodes S -> S
type FetchObjects struct {
	Partition uint32 // PNumber
	Length    uint32 // PNumber
	MinTid    zodb.Tid
	MaxTid    zodb.Tid
	MinOid    zodb.Oid

	// already known objects
	ObjKnownDict map[zodb.Tid][]zodb.Oid // serial -> []oid
}

type AnswerFetchObjects struct {
	PackTid zodb.Tid
	NextTid zodb.Tid
	NextOid zodb.Oid

	// objects to delete
	ObjDeleteDict map[zodb.Tid][]zodb.Oid // serial -> []oid
}

// Send metadata of a transaction to a node that do not have them.
//
//neo:nodes S -> S
type AddTransaction struct {
	Tid         zodb.Tid
	User        string
	Description string
	Extension   string
	Packed      bool
	TTid        zodb.Tid
	OidList     []zodb.Oid
}

// Send an object record to a node that do not have it.
//
//neo:nodes S -> S
type AddObject struct {
	Oid         zodb.Oid
	Serial      zodb.Tid
	Compression bool
	Checksum    Checksum
	Data        *mem.Buf
	DataSerial  zodb.Tid
}

// Request DB to be truncated. Also used to leave backup mode.
//
//neo:nodes ctl -> A -> M; M -> S
type Truncate struct {
	Tid zodb.Tid

	// answer = Error
}

// ---- runtime support for protogen and custom codecs ----

// customCodec is the interface that is implemented by types with custom encodings.
//
// its semantic is very similar to Msg.
type customCodec interface {
	neoEncodedLen() int
	neoEncode(buf []byte) (nwrote int)
	neoDecode(data []byte) (nread uint64, ok bool) // XXX uint64 or int here?
}

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
func float64_neoEncode(b []byte, f float64) {
	var fu uint64
	if !math.IsNaN(f) {
		fu = math.Float64bits(f)
	} else {
		// convert all NaNs to canonical \xff * 8
		fu = 1<<64 - 1
	}

	binary.BigEndian.PutUint64(b, fu)
}

func float64_neoDecode(b []byte) float64 {
	fu := binary.BigEndian.Uint64(b)
	return math.Float64frombits(fu)
}


// XXX we need string_neo* only for Address
// XXX dup of genSlice1 in protogen.go

func string_neoEncodedLen(s string) int {
	return 4 + len(s)
}

func string_neoEncode(s string, data []byte) int {
	l := len(s)
	binary.BigEndian.PutUint32(data, uint32(l))
	copy(data[4:4+l], s) // NOTE [:l] to catch data overflow as copy copies minimal len
	return 4 + l
}

func string_neoDecode(sp *string, data []byte) (nread uint64, ok bool) {
	if len(data) < 4 {
		return 0, false
	}
	l := binary.BigEndian.Uint32(data)
	data = data[4:]
	if uint64(len(data)) < uint64(l) {
		return 0, false
	}

	*sp = string(data[:l])
	return 4 + uint64(l), true
}
