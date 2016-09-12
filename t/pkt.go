package pkt

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
type TID struct uint64  // XXX or [8]byte ?
type OID struct uint64  // XXX or [8]byte ?

const (
	INVALID_TID TID = 0xffffffffffffffff    // 1<<64 - 1
	INVALID_OID OID = 0xffffffffffffffff    // 1<<64 - 1
	ZERO_TID    TID = 0        // XXX or simply TID{} ?
	ZERO_OID    OID = 0        // XXX or simply OID{} ?
	// OID_LEN = 8
	// TID_LEN = 8
	MAX_TID     TID = 0x7fffffffffffffff    // SQLite does not accept numbers above 2^63-1
)

// TODO UUID_NAMESPACES

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
	UUID            PUUID           // TODO
	Address         PAddress        // TODO
	Name            string
}

// XXX -> ReplyIdentification? RequestIdentification.Answer somehow ?
type AcceptIdentification struct {
	Packet
	NodeType        NodeType        // XXX name
	MyUUID          PUUID           // TODO
	NumPartitions   uint32          // PNumber
	NumReplicas     uint32          // PNumber
	YourUUID        PUUID           // TODO
	Primary         PAddress        // TODO
	KnownMasterList []struct {
		Address PAddress
		UUID    PUUID
	}
}

// Ask current primary master's uuid. CTL -> A.
type PrimaryMaster struct {
	Packet
}

type AnswerPrimary struct {
	Packet
	PrimaryUUID PUUID       // TODO
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
        PFRowList,
    )

