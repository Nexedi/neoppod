
# Copyright (C) 2006-2019  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import traceback
from cStringIO import StringIO
from struct import Struct

# The protocol version must be increased whenever upgrading a node may require
# to upgrade other nodes. It is encoded as a 4-bytes big-endian integer and
# the high order byte 0 is different from TLS Handshake (0x16).
PROTOCOL_VERSION = 5
ENCODED_VERSION = Struct('!L').pack(PROTOCOL_VERSION)

# Avoid memory errors on corrupted data.
MAX_PACKET_SIZE = 0x4000000

PACKET_HEADER_FORMAT = Struct('!LHL')
RESPONSE_MASK = 0x8000

class Enum(tuple):

    class Item(int):
        __slots__ = '_name', '_enum'
        def __str__(self):
            return self._name
        def __repr__(self):
            return "<EnumItem %s (%d)>" % (self._name, self)
        def __eq__(self, other):
            if type(other) is self.__class__:
                assert other._enum is self._enum
                return other is self
            return other == int(self)

    def __new__(cls, func):
        names = func.func_code.co_names
        self = tuple.__new__(cls, map(cls.Item, xrange(len(names))))
        self._name = func.__name__
        for item, name in zip(self, names):
            setattr(self, name, item)
            item._name = name
            item._enum = self
        return self

    def __repr__(self):
        return "<Enum %s>" % self._name


@Enum
def ErrorCodes():
    ACK
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

@Enum
def ClusterStates():
    # The cluster is initially in the RECOVERING state, and it goes back to
    # this state whenever the partition table becomes non-operational again.
    # An election of the primary master always happens, in case of a network
    # cut between a primary master and all other nodes. The primary master:
    # - first recovers its own data by reading it from storage nodes;
    # - waits for the partition table be operational;
    # - automatically switch to VERIFYING if the cluster can be safely started.
    RECOVERING
    # Transient state, used to:
    # - replay the transaction log, in case of unclean shutdown;
    # - and actually truncate the DB if the user asked to do so.
    # Then, the cluster either goes to RUNNING or STARTING_BACKUP state.
    VERIFYING
    # Normal operation. The DB is read-writable by clients.
    RUNNING
    # Transient state to shutdown the whole cluster.
    STOPPING
    # Transient state, during which the master (re)connect to the upstream
    # master.
    STARTING_BACKUP
    # Backup operation. The master is notified of new transactions thanks to
    # invalidations and orders storage nodes to fetch them from upstream.
    # Because cells are synchronized independently, the DB is often
    # inconsistent.
    BACKINGUP
    # Transient state, when the user decides to go back to RUNNING state.
    # The master stays in this state until the DB is consistent again.
    # In case of failure, the cluster will go back to backup mode.
    STOPPING_BACKUP

@Enum
def NodeTypes():
    MASTER
    STORAGE
    CLIENT
    ADMIN

@Enum
def NodeStates():
    UNKNOWN
    DOWN
    RUNNING
    PENDING

@Enum
def CellStates():
    # Write-only cell. Last transactions are missing because storage is/was down
    # for a while, or because it is new for the partition. It usually becomes
    # UP_TO_DATE when replication is done.
    OUT_OF_DATE
    # Normal state: cell is writable/readable, and it isn't planned to drop it.
    UP_TO_DATE
    # Same as UP_TO_DATE, except that it will be discarded as soon as another
    # node finishes to replicate it. It means a partition is moved from 1 node
    # to another. It is also discarded immediately if out-of-date.
    FEEDING
    # A check revealed that data differs from other replicas. Cell is neither
    # readable nor writable.
    CORRUPTED
    # Not really a state: only used in network packets to tell storages to drop
    # partitions.
    DISCARDED

# used for logging
node_state_prefix_dict = {
    NodeStates.RUNNING: 'R',
    NodeStates.DOWN: 'D',
    NodeStates.UNKNOWN: 'U',
    NodeStates.PENDING: 'P',
}

# used for logging
cell_state_prefix_dict = {
    CellStates.UP_TO_DATE: 'U',
    CellStates.OUT_OF_DATE: 'O',
    CellStates.FEEDING: 'F',
    CellStates.DISCARDED: 'D',
    CellStates.CORRUPTED: 'C',
}

# Other constants.
INVALID_TID = \
INVALID_OID = '\xff' * 8
INVALID_PARTITION = 0xffffffff
ZERO_HASH = '\0' * 20
ZERO_TID = \
ZERO_OID = '\0' * 8
MAX_TID = '\x7f' + '\xff' * 7 # SQLite does not accept numbers above 2^63-1

# High-order byte:
# 7 6 5 4 3 2 1 0
# | | | | +-+-+-+-- reserved (0)
# | +-+-+---------- node type
# +---------------- temporary if negative
# UUID namespaces are required to prevent conflicts when the master generate
# new uuid before it knows uuid of existing storage nodes. So only the high
# order bit is really important and the 31 other bits could be random.
# Extra namespace information and non-randomness of 3 LOB help to read logs.
UUID_NAMESPACES = {
    NodeTypes.STORAGE: 0x00,
    NodeTypes.MASTER: -0x10,
    NodeTypes.CLIENT: -0x20,
    NodeTypes.ADMIN: -0x30,
}
uuid_str = (lambda ns: lambda uuid:
    ns[uuid >> 24] + str(uuid & 0xffffff) if uuid else str(uuid)
    )({v: str(k)[0] for k, v in UUID_NAMESPACES.iteritems()})

class ProtocolError(Exception):
    """ Base class for protocol errors, close the connection """

class PacketMalformedError(ProtocolError):
    """Close the connection"""

class UnexpectedPacketError(ProtocolError):
    """Close the connection"""

class NotReadyError(ProtocolError):
    """ Just close the connection """

class BackendNotImplemented(Exception):
    """ Method not implemented by backend storage """

class NonReadableCell(Exception):
    """Read-access to a cell that is actually non-readable

    This happens in case of race condition at processing partition table
    updates: client's PT is older or newer than storage's. The latter case is
    possible because the master must validate any end of replication, which
    means that the storage node can't anticipate the PT update (concurrently,
    there may be a first tweaks that moves the replicated cell to another node,
    and a second one that moves it back).

    On such event, the client must retry, preferably another cell.
    """

class Packet(object):
    """
        Base class for any packet definition. The _fmt class attribute must be
        defined for any non-empty packet.
    """
    _ignore_when_closed = False
    _request = None
    _answer = None
    _body = None
    _code = None
    _fmt = None
    _id = None
    nodelay = True
    poll_thread = False

    def __init__(self, *args):
        assert self._code is not None, "Packet class not registered"
        if args:
            buf = StringIO()
            self._fmt.encode(buf.write, args)
            self._body = buf.getvalue()
        else:
            self._body = ''

    def decode(self):
        assert self._body is not None
        if self._fmt is None:
            return ()
        buf = StringIO(self._body)
        try:
            return self._fmt.decode(buf.read)
        except ParseError, msg:
            name = self.__class__.__name__
            raise PacketMalformedError("%s fail (%s)" % (name, msg))

    def setContent(self, msg_id, body):
        """ Register the packet content for future decoding """
        self._id = msg_id
        self._body = body

    def setId(self, value):
        self._id = value

    def getId(self):
        assert self._id is not None, "No identifier applied on the packet"
        return self._id

    def encode(self):
        """ Encode a packet as a string to send it over the network """
        content = self._body
        return (PACKET_HEADER_FORMAT.pack(self._id, self._code, len(content)),
                content)

    def __len__(self):
        return PACKET_HEADER_FORMAT.size + len(self._body)

    def __repr__(self):
        return '%s[%r]' % (self.__class__.__name__, self._id)

    def __eq__(self, other):
        """ Compare packets with their code instead of content """
        if other is None:
            return False
        assert isinstance(other, Packet)
        return self._code == other._code

    def isError(self):
        return isinstance(self, Error)

    def isResponse(self):
        return self._code & RESPONSE_MASK == RESPONSE_MASK

    def getAnswerClass(self):
        return self._answer

    def ignoreOnClosedConnection(self):
        """
        Tells if this packet must be ignored when its connection is closed
        when it is handled.
        """
        return self._ignore_when_closed

class ParseError(Exception):
    """
        An exception that encapsulate another and build the 'path' of the
        packet item that generate the error.
    """
    def __init__(self, item, trace):
        Exception.__init__(self)
        self._trace = trace
        self._items = [item]

    def append(self, item):
        self._items.append(item)

    def __repr__(self):
        chain = '/'.join([item.getName() for item in reversed(self._items)])
        return 'at %s:\n%s' % (chain, self._trace)

    __str__ = __repr__

# packet parsers

class PItem(object):
    """
        Base class for any packet item, _encode and _decode must be overridden
        by subclasses.
    """
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return self.__class__.__name__

    def getName(self):
        return self._name

    def _trace(self, method, *args):
        try:
            return method(*args)
        except ParseError, e:
            # trace and forward exception
            e.append(self)
            raise
        except Exception:
            # original exception, encapsulate it
            trace = ''.join(traceback.format_exception(*sys.exc_info())[2:])
            raise ParseError(self, trace)

    def encode(self, writer, items):
        return self._trace(self._encode, writer, items)

    def decode(self, reader):
        return self._trace(self._decode, reader)

    def _encode(self, writer, items):
        raise NotImplementedError, self.__class__.__name__

    def _decode(self, reader):
        raise NotImplementedError, self.__class__.__name__

class PStruct(PItem):
    """
        Aggregate other items
    """
    def __init__(self, name, *items):
        PItem.__init__(self, name)
        self._items = items

    def _encode(self, writer, items):
        assert len(self._items) == len(items), (items, self._items)
        for item, value in zip(self._items, items):
            item.encode(writer, value)

    def _decode(self, reader):
        return tuple([item.decode(reader) for item in self._items])

class PStructItem(PItem):
    """
        A single value encoded with struct
    """
    def __init__(self, name):
        PItem.__init__(self, name)
        struct = Struct(self._fmt)
        self.pack = struct.pack
        self.unpack = struct.unpack
        self.size = struct.size

    def _encode(self, writer, value):
        writer(self.pack(value))

    def _decode(self, reader):
        return self.unpack(reader(self.size))[0]

class PStructItemOrNone(PStructItem):

    def _encode(self, writer, value):
        return writer(self._None if value is None else self.pack(value))

    def _decode(self, reader):
        value = reader(self.size)
        return None if value == self._None else self.unpack(value)[0]

class POption(PStruct):

    def _encode(self, writer, value):
        if value is None:
            writer('\0')
        else:
            writer('\1')
            PStruct._encode(self, writer, value)

    def _decode(self, reader):
        if '\0\1'.index(reader(1)):
            return PStruct._decode(self, reader)

class PList(PStructItem):
    """
        A list of homogeneous items
    """
    _fmt = '!L'

    def __init__(self, name, item):
        PStructItem.__init__(self, name)
        self._item = item

    def _encode(self, writer, items):
        writer(self.pack(len(items)))
        item = self._item
        for value in items:
            item.encode(writer, value)

    def _decode(self, reader):
        length = self.unpack(reader(self.size))[0]
        item = self._item
        return [item.decode(reader) for _ in xrange(length)]

class PDict(PStructItem):
    """
        A dictionary with custom key and value formats
    """
    _fmt = '!L'

    def __init__(self, name, key, value):
        PStructItem.__init__(self, name)
        self._key = key
        self._value = value

    def _encode(self, writer, item):
        assert isinstance(item , dict), (type(item), item)
        writer(self.pack(len(item)))
        key, value = self._key, self._value
        for k, v in item.iteritems():
            key.encode(writer, k)
            value.encode(writer, v)

    def _decode(self, reader):
        length = self.unpack(reader(self.size))[0]
        key, value = self._key, self._value
        new_dict = {}
        for _ in xrange(length):
            k = key.decode(reader)
            v = value.decode(reader)
            new_dict[k] = v
        return new_dict

class PEnum(PStructItem):
    """
        Encapsulate an enumeration value
    """
    _fmt = 'b'

    def __init__(self, name, enum):
        PStructItem.__init__(self, name)
        self._enum = enum

    def _encode(self, writer, item):
        if item is None:
            item = -1
        writer(self.pack(item))

    def _decode(self, reader):
        code = self.unpack(reader(self.size))[0]
        if code == -1:
            return None
        try:
            return self._enum[code]
        except KeyError:
            enum = self._enum.__class__.__name__
            raise ValueError, 'Invalid code for %s enum: %r' % (enum, code)

class PString(PStructItem):
    """
        A variable-length string
    """
    _fmt = '!L'

    def _encode(self, writer, value):
        writer(self.pack(len(value)))
        writer(value)

    def _decode(self, reader):
        length = self.unpack(reader(self.size))[0]
        return reader(length)

class PAddress(PString):
    """
        An host address (IPv4/IPv6)
    """

    def __init__(self, name):
        PString.__init__(self, name)
        self._port = Struct('!H')

    def _encode(self, writer, address):
        if address:
            host, port = address
            PString._encode(self, writer, host)
            writer(self._port.pack(port))
        else:
            PString._encode(self, writer, '')

    def _decode(self, reader):
        host = PString._decode(self, reader)
        if host:
            p = self._port
            return host, p.unpack(reader(p.size))[0]

class PBoolean(PStructItem):
    """
        A boolean value, encoded as a single byte
    """
    _fmt = '!?'

class PNumber(PStructItem):
    """
        A integer number (4-bytes length)
    """
    _fmt = '!L'

class PIndex(PStructItem):
    """
        A big integer to defined indexes in a huge list.
    """
    _fmt = '!Q'

class PPTID(PStructItemOrNone):
    """
        A None value means an invalid PTID
    """
    _fmt = '!Q'
    _None = Struct(_fmt).pack(0)

class PChecksum(PItem):
    """
        A hash (SHA1)
    """
    def _encode(self, writer, checksum):
        assert len(checksum) == 20, (len(checksum), checksum)
        writer(checksum)

    def _decode(self, reader):
        return reader(20)

class PSignedNull(PStructItemOrNone):
    _fmt = '!l'
    _None = Struct(_fmt).pack(0)

class PUUID(PSignedNull):
    """
        An UUID (node identifier, 4-bytes signed integer)
    """

class PTID(PItem):
    """
        A transaction identifier
    """
    def _encode(self, writer, tid):
        if tid is None:
            tid = INVALID_TID
        assert len(tid) == 8, (len(tid), tid)
        writer(tid)

    def _decode(self, reader):
        tid = reader(8)
        if tid == INVALID_TID:
            tid = None
        return tid

# same definition, for now
POID = PTID

class PFloat(PStructItemOrNone):
    """
        A float number (8-bytes length)
    """
    _fmt = '!d'
    _None = '\xff' * 8

# common definitions

PFEmpty = PStruct('no_content')
PFNodeType = PEnum('type', NodeTypes)
PFNodeState = PEnum('state', NodeStates)
PFCellState = PEnum('state', CellStates)

PFNodeList = PList('node_list',
    PStruct('node',
        PFNodeType,
        PAddress('address'),
        PUUID('uuid'),
        PFNodeState,
        PFloat('id_timestamp'),
    ),
)

PFCellList = PList('cell_list',
    PStruct('cell',
        PUUID('uuid'),
        PFCellState,
    ),
)

PFRowList = PList('row_list',
    PStruct('row',
        PNumber('offset'),
        PFCellList,
    ),
)

PFHistoryList = PList('history_list',
    PStruct('history_entry',
        PTID('serial'),
        PNumber('size'),
    ),
)

PFUUIDList = PList('uuid_list',
    PUUID('uuid'),
)

PFTidList = PList('tid_list',
    PTID('tid'),
)

PFOidList = PList('oid_list',
    POID('oid'),
)

# packets definition

class Error(Packet):
    """
    Error is a special type of message, because this can be sent against
    any other message, even if such a message does not expect a reply
    usually.

    :nodes: * -> *
    """
    _fmt = PStruct('error',
        PNumber('code'),
        PString('message'),
    )

class Ping(Packet):
    """
    Empty request used as network barrier.

    :nodes: * -> *
    """
    _answer = PFEmpty

class CloseClient(Packet):
    """
    Tell peer that it can close the connection if it has finished with us.

    :nodes: * -> *
    """

class RequestIdentification(Packet):
    """
    Request a node identification. This must be the first packet for any
    connection.

    :nodes: * -> *
    """
    poll_thread = True

    _fmt = PStruct('request_identification',
        PFNodeType,
        PUUID('uuid'),
        PAddress('address'),
        PString('name'),
        PFloat('id_timestamp'),
        # storage:
            PList('devpath', PString('devid')),
            PList('new_nid', PNumber('offset')),
    )

    _answer = PStruct('accept_identification',
        PFNodeType,
        PUUID('my_uuid'),
        PNumber('num_partitions'),
        PNumber('num_replicas'),
        PUUID('your_uuid'),
    )

class PrimaryMaster(Packet):
    """
    Ask node identier of the current primary master.

    :nodes: ctl -> A
    """
    _answer = PStruct('answer_primary',
        PUUID('primary_uuid'),
    )

class NotPrimaryMaster(Packet):
    """
    Notify peer that I'm not the primary master. Attach any extra information
    to help the peer joining the cluster.

    :nodes: SM -> *
    """
    _fmt = PStruct('not_primary_master',
        PSignedNull('primary'),
        PList('known_master_list',
            PAddress('address'),
        ),
    )

class Recovery(Packet):
    """
    Ask storage nodes data needed by master to recover.
    Reused by `neoctl print ids`.

    :nodes: M -> S; ctl -> A -> M
    """
    _answer = PStruct('answer_recovery',
        PPTID('ptid'),
        PTID('backup_tid'),
        PTID('truncate_tid'),
    )

class LastIDs(Packet):
    """
    Ask the last OID/TID so that a master can initialize its TransactionManager.
    Reused by `neoctl print ids`.

    :nodes: M -> S; ctl -> A -> M
    """
    _answer = PStruct('answer_last_ids',
        POID('last_oid'),
        PTID('last_tid'),
    )

class PartitionTable(Packet):
    """
    Ask storage node the remaining data needed by master to recover.
    This is also how the clients get the full partition table on connection.

    :nodes: M -> S; C -> M
    """
    _answer = PStruct('answer_partition_table',
        PPTID('ptid'),
        PFRowList,
    )

class NotifyPartitionTable(Packet):
    """
    Send the full partition table to admin/storage nodes on connection.

    :nodes: M -> A, S
    """
    _fmt = PStruct('send_partition_table',
        PPTID('ptid'),
        PFRowList,
    )

class PartitionChanges(Packet):
    """
    Notify about changes in the partition table.

    :nodes: M -> *
    """
    _fmt = PStruct('notify_partition_changes',
        PPTID('ptid'),
        PList('cell_list',
            PStruct('cell',
                PNumber('offset'),
                PUUID('uuid'),
                PFCellState,
            ),
        ),
    )

class StartOperation(Packet):
    """
    Tell a storage node to start operation. Before this message, it must only
    communicate with the primary master.

    :nodes: M -> S
    """
    _fmt = PStruct('start_operation',
        # XXX: Is this boolean needed ? Maybe this
        #      can be deduced from cluster state.
        PBoolean('backup'),
    )

class StopOperation(Packet):
    """
    Notify that the cluster is not operational anymore. Any operation between
    nodes must be aborted.

    :nodes: M -> S, C
    """

class UnfinishedTransactions(Packet):
    """
    Ask unfinished transactions, which will be replicated when they're finished.

    :nodes: S -> M
    """
    _fmt = PStruct('ask_unfinished_transactions',
        PList('row_list',
            PNumber('offset'),
        ),
    )

    _answer = PStruct('answer_unfinished_transactions',
        PTID('max_tid'),
        PList('tid_list',
            PTID('unfinished_tid'),
        ),
    )

class LockedTransactions(Packet):
    """
    Ask locked transactions to replay committed transactions that haven't been
    unlocked.

    :nodes: M -> S
    """
    _answer = PStruct('answer_locked_transactions',
        PDict('tid_dict',
            PTID('ttid'),
            PTID('tid'),
        ),
    )

class FinalTID(Packet):
    """
    Return final tid if ttid has been committed, to recover from certain
    failures during tpc_finish.

    :nodes: M -> S; C -> M, S
    """
    _fmt = PStruct('final_tid',
        PTID('ttid'),
    )

    _answer = PStruct('final_tid',
        PTID('tid'),
    )

class ValidateTransaction(Packet):
    """
    Do replay a committed transaction that was not unlocked.

    :nodes: M -> S
    """
    _fmt = PStruct('validate_transaction',
        PTID('ttid'),
        PTID('tid'),
    )

class BeginTransaction(Packet):
    """
    Ask to begin a new transaction. This maps to `tpc_begin`.

    :nodes: C -> M
    """
    _fmt = PStruct('ask_begin_transaction',
        PTID('tid'),
    )

    _answer = PStruct('answer_begin_transaction',
        PTID('tid'),
    )

class FailedVote(Packet):
    """
    Report storage nodes for which vote failed.
    True is returned if it's still possible to finish the transaction.

    :nodes: C -> M
    """
    _fmt = PStruct('failed_vote',
        PTID('tid'),
        PFUUIDList,
    )

    _answer = Error

class FinishTransaction(Packet):
    """
    Finish a transaction. Return the TID of the committed transaction.
    This maps to `tpc_finish`.

    :nodes: C -> M
    """
    poll_thread = True

    _fmt = PStruct('ask_finish_transaction',
        PTID('tid'),
        PFOidList,
        PList('checked_list',
            POID('oid'),
        ),
    )

    _answer = PStruct('answer_information_locked',
        PTID('ttid'),
        PTID('tid'),
    )

class NotifyTransactionFinished(Packet):
    """
    Notify that a transaction blocking a replication is now finished.

    :nodes: M -> S
    """
    _fmt = PStruct('notify_transaction_finished',
        PTID('ttid'),
        PTID('max_tid'),
    )

class LockInformation(Packet):
    """
    Commit a transaction. The new data is read-locked.

    :nodes: M -> S
    """
    _fmt = PStruct('ask_lock_informations',
        PTID('ttid'),
        PTID('tid'),
    )

    _answer = PStruct('answer_information_locked',
        PTID('ttid'),
    )

class InvalidateObjects(Packet):
    """
    Notify about a new transaction modifying objects,
    invalidating client caches.

    :nodes: M -> C
    """
    _fmt = PStruct('ask_finish_transaction',
        PTID('tid'),
        PFOidList,
    )

class UnlockInformation(Packet):
    """
    Notify about a successfully committed transaction. The new data can be
    unlocked.

    :nodes: M -> S
    """
    _fmt = PStruct('notify_unlock_information',
        PTID('ttid'),
    )

class GenerateOIDs(Packet):
    """
    Ask new OIDs to create objects.

    :nodes: C -> M
    """
    _fmt = PStruct('ask_new_oids',
        PNumber('num_oids'),
    )

    _answer = PStruct('answer_new_oids',
        PFOidList,
    )

class Deadlock(Packet):
    """
    Ask master to generate a new TTID that will be used by the client to solve
    a deadlock by rebasing the transaction on top of concurrent changes.

    :nodes: S -> M -> C
    """
    _fmt = PStruct('notify_deadlock',
        PTID('ttid'),
        PTID('locking_tid'),
    )

class RebaseTransaction(Packet):
    """
    Rebase a transaction to solve a deadlock.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_rebase_transaction',
        PTID('ttid'),
        PTID('locking_tid'),
    )

    _answer = PStruct('answer_rebase_transaction',
        PFOidList,
    )

class RebaseObject(Packet):
    """
    Rebase an object change to solve a deadlock.

    :nodes: C -> S

    XXX: It is a request packet to simplify the implementation. For more
         efficiency, this should be turned into a notification, and the
         RebaseTransaction should answered once all objects are rebased
         (so that the client can still wait on something).
    """
    _fmt = PStruct('ask_rebase_object',
        PTID('ttid'),
        PTID('oid'),
    )

    _answer = PStruct('answer_rebase_object',
        POption('conflict',
            PTID('serial'),
            PTID('conflict_serial'),
            POption('data',
                PBoolean('compression'),
                PChecksum('checksum'),
                PString('data'),
            ),
        )
    )

class StoreObject(Packet):
    """
    Ask to create/modify an object. This maps to `store`.

    As for IStorage, 'serial' is ZERO_TID for new objects.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_store_object',
        POID('oid'),
        PTID('serial'),
        PBoolean('compression'),
        PChecksum('checksum'),
        PString('data'),
        PTID('data_serial'),
        PTID('tid'),
    )

    _answer = PStruct('answer_store_object',
        PTID('conflict'),
    )

class AbortTransaction(Packet):
    """
    Abort a transaction. This maps to `tpc_abort`.

    :nodes: C -> S; C -> M -> S
    """
    _fmt = PStruct('abort_transaction',
        PTID('tid'),
        PFUUIDList, # unused for * -> S
    )

class StoreTransaction(Packet):
    """
    Ask to store a transaction. Implies vote.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_store_transaction',
        PTID('tid'),
        PString('user'),
        PString('description'),
        PString('extension'),
        PFOidList,
    )
    _answer = PFEmpty

class VoteTransaction(Packet):
    """
    Ask to vote a transaction.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_vote_transaction',
        PTID('tid'),
    )
    _answer = PFEmpty

class GetObject(Packet):
    """
    Ask a stored object by its OID, optionally at/before a specific tid.
    This maps to `load/loadBefore/loadSerial`.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_object',
        POID('oid'),
        PTID('at'),
        PTID('before'),
    )

    _answer = PStruct('answer_object',
        POID('oid'),
        PTID('serial_start'),
        PTID('serial_end'),
        PBoolean('compression'),
        PChecksum('checksum'),
        PString('data'),
        PTID('data_serial'),
    )

class TIDList(Packet):
    """
    Ask for TIDs between a range of offsets. The order of TIDs is descending,
    and the range is [first, last). This maps to `undoLog`.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_tids',
        PIndex('first'),
        PIndex('last'),
        PNumber('partition'),
    )

    _answer = PStruct('answer_tids',
        PFTidList,
    )

class TIDListFrom(Packet):
    """
    Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
    Used by `iterator`.

    :nodes: C -> S
    """
    _fmt = PStruct('tid_list_from',
        PTID('min_tid'),
        PTID('max_tid'),
        PNumber('length'),
        PNumber('partition'),
    )

    _answer = PStruct('answer_tids',
        PFTidList,
    )

class TransactionInformation(Packet):
    """
    Ask for transaction metadata.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_transaction_information',
        PTID('tid'),
    )

    _answer = PStruct('answer_transaction_information',
        PTID('tid'),
        PString('user'),
        PString('description'),
        PString('extension'),
        PBoolean('packed'),
        PFOidList,
    )

class ObjectHistory(Packet):
    """
    Ask history information for a given object. The order of serials is
    descending, and the range is [first, last]. This maps to `history`.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_object_history',
        POID('oid'),
        PIndex('first'),
        PIndex('last'),
    )

    _answer = PStruct('answer_object_history',
        POID('oid'),
        PFHistoryList,
    )

class PartitionList(Packet):
    """
    Ask information about partitions.

    :nodes: ctl -> A
    """
    _fmt = PStruct('ask_partition_list',
        PNumber('min_offset'),
        PNumber('max_offset'),
        PUUID('uuid'),
    )

    _answer = PStruct('answer_partition_list',
        PPTID('ptid'),
        PFRowList,
    )

class NodeList(Packet):
    """
    Ask information about nodes.

    :nodes: ctl -> A
    """
    _fmt = PStruct('ask_node_list',
        PFNodeType,
    )

    _answer = PStruct('answer_node_list',
        PFNodeList,
    )

class SetNodeState(Packet):
    """
    Change the state of a node.

    :nodes: ctl -> A -> M
    """
    _fmt = PStruct('set_node_state',
        PUUID('uuid'),
        PFNodeState,
    )

    _answer = Error

class AddPendingNodes(Packet):
    """
    Mark given pending nodes as running, for future inclusion when tweaking
    the partition table.

    :nodes: ctl -> A -> M
    """
    _fmt = PStruct('add_pending_nodes',
        PFUUIDList,
    )

    _answer = Error

class TweakPartitionTable(Packet):
    """
    Ask the master to balance the partition table, optionally excluding
    specific nodes in anticipation of removing them.

    :nodes: ctl -> A -> M
    """
    _fmt = PStruct('tweak_partition_table',
        PFUUIDList,
    )

    _answer = Error

class NotifyNodeInformation(Packet):
    """
    Notify information about one or more nodes.

    :nodes: M -> *
    """
    _fmt = PStruct('notify_node_informations',
        PFloat('id_timestamp'),
        PFNodeList,
    )

class SetClusterState(Packet):
    """
    Set the cluster state.

    :nodes: ctl -> A -> M
    """
    _fmt = PStruct('set_cluster_state',
        PEnum('state', ClusterStates),
    )

    _answer = Error

class Repair(Packet):
    """
    Ask storage nodes to repair their databases.

    :nodes: ctl -> A -> M
    """
    _flags = map(PBoolean, ('dry_run',
        # 'prune_orphan' (commented because it's the only option for the moment)
        ))
    _fmt = PStruct('repair',
        PFUUIDList,
        *_flags)

    _answer = Error

class RepairOne(Packet):
    """
    Repair is translated to this message, asking a specific storage node to
    repair its database.

    :nodes: M -> S
    """
    _fmt = PStruct('repair', *Repair._flags)

class ClusterInformation(Packet):
    """
    Notify about a cluster state change.

    :nodes: M -> *
    """
    _fmt = PStruct('notify_cluster_information',
        PEnum('state', ClusterStates),
    )

class ClusterState(Packet):
    """
    Ask the state of the cluster

    :nodes: ctl -> A; A -> M
    """

    _answer = PStruct('answer_cluster_state',
        PEnum('state', ClusterStates),
    )

class ObjectUndoSerial(Packet):
    """
    Ask storage the serial where object data is when undoing given transaction,
    for a list of OIDs.

    object_tid_dict has the following format:
        key: oid
        value: 3-tuple
            current_serial (TID)
                The latest serial visible to the undoing transaction.
            undo_serial (TID)
                Where undone data is (tid at which data is before given undo).
            is_current (bool)
                If current_serial's data is current on storage.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_undo_transaction',
        PTID('tid'),
        PTID('ltid'),
        PTID('undone_tid'),
        PFOidList,
    )

    _answer = PStruct('answer_undo_transaction',
        PDict('object_tid_dict',
            POID('oid'),
            PStruct('object_tid_value',
                PTID('current_serial'),
                PTID('undo_serial'),
                PBoolean('is_current'),
            ),
        ),
    )

class CheckCurrentSerial(Packet):
    """
    Check if given serial is current for the given oid, and lock it so that
    this state is not altered until transaction ends.
    This maps to `checkCurrentSerialInTransaction`.

    :nodes: C -> S
    """
    _fmt = PStruct('ask_check_current_serial',
        PTID('tid'),
        POID('oid'),
        PTID('serial'),
    )

    _answer = StoreObject._answer

class Pack(Packet):
    """
    Request a pack at given TID.

    :nodes: C -> M -> S
    """
    _fmt = PStruct('ask_pack',
        PTID('tid'),
    )

    _answer = PStruct('answer_pack',
        PBoolean('status'),
    )

class CheckReplicas(Packet):
    """
    Ask the cluster to search for mismatches between replicas, metadata only,
    and optionally within a specific range. Reference nodes can be specified.

    :nodes: ctl -> A -> M
    """
    _fmt = PStruct('check_replicas',
        PDict('partition_dict',
            PNumber('partition'),
            PUUID('source'),
        ),
        PTID('min_tid'),
        PTID('max_tid'),
    )
    _answer = Error

class CheckPartition(Packet):
    """
    Ask a storage node to compare a partition with all other nodes.
    Like for CheckReplicas, only metadata are checked, optionally within a
    specific range. A reference node can be specified.

    :nodes: M -> S
    """
    _fmt = PStruct('check_partition',
        PNumber('partition'),
        PStruct('source',
            PString('upstream_name'),
            PAddress('address'),
        ),
        PTID('min_tid'),
        PTID('max_tid'),
    )

class CheckTIDRange(Packet):
    """
    Ask some stats about a range of transactions.
    Used to know if there are differences between a replicating node and
    reference node.

    :nodes: S -> S
    """
    _fmt = PStruct('ask_check_tid_range',
        PNumber('partition'),
        PNumber('length'),
        PTID('min_tid'),
        PTID('max_tid'),
    )

    _answer = PStruct('answer_check_tid_range',
        PNumber('count'),
        PChecksum('checksum'),
        PTID('max_tid'),
    )

class CheckSerialRange(Packet):
    """
    Ask some stats about a range of object history.
    Used to know if there are differences between a replicating node and
    reference node.

    :nodes: S -> S
    """
    _fmt = PStruct('ask_check_serial_range',
        PNumber('partition'),
        PNumber('length'),
        PTID('min_tid'),
        PTID('max_tid'),
        POID('min_oid'),
    )

    _answer = PStruct('answer_check_serial_range',
        PNumber('count'),
        PChecksum('tid_checksum'),
        PTID('max_tid'),
        PChecksum('oid_checksum'),
        POID('max_oid'),
    )

class PartitionCorrupted(Packet):
    """
    Notify that mismatches were found while check replicas for a partition.

    :nodes: S -> M
    """
    _fmt = PStruct('partition_corrupted',
        PNumber('partition'),
        PList('cell_list',
            PUUID('uuid'),
        ),
    )

class LastTransaction(Packet):
    """
    Ask last committed TID.

    :nodes: C -> M; ctl -> A -> M
    """
    poll_thread = True

    _answer = PStruct('answer_last_transaction',
        PTID('tid'),
    )

class NotifyReady(Packet):
    """
    Notify that we're ready to serve requests.

    :nodes: S -> M
    """

class FetchTransactions(Packet):
    """
    Ask a storage node to send all transaction data we don't have,
    and reply with the list of transactions we should not have.

    :nodes: S -> S
    """
    _fmt = PStruct('ask_transaction_list',
        PNumber('partition'),
        PNumber('length'),
        PTID('min_tid'),
        PTID('max_tid'),
        PFTidList,           # already known transactions
    )
    _answer = PStruct('answer_transaction_list',
        PTID('pack_tid'),
        PTID('next_tid'),
        PFTidList,           # transactions to delete
    )

class AddTransaction(Packet):
    """
    Send metadata of a transaction to a node that do not have them.

    :nodes: S -> S
    """
    nodelay = False

    _fmt = PStruct('add_transaction',
        PTID('tid'),
        PString('user'),
        PString('description'),
        PString('extension'),
        PBoolean('packed'),
        PTID('ttid'),
        PFOidList,
    )

class FetchObjects(Packet):
    """
    Ask a storage node to send object records we don't have,
    and reply with the list of records we should not have.

    :nodes: S -> S
    """
    _fmt = PStruct('ask_object_list',
        PNumber('partition'),
        PNumber('length'),
        PTID('min_tid'),
        PTID('max_tid'),
        POID('min_oid'),
        PDict('object_dict', # already known objects
            PTID('serial'),
            PFOidList,
        ),
    )
    _answer = PStruct('answer_object_list',
        PTID('pack_tid'),
        PTID('next_tid'),
        POID('next_oid'),
        PDict('object_dict', # objects to delete
            PTID('serial'),
            PFOidList,
        ),
    )

class AddObject(Packet):
    """
    Send an object record to a node that do not have it.

    :nodes: S -> S
    """
    nodelay = False

    _fmt = PStruct('add_object',
        POID('oid'),
        PTID('serial'),
        PBoolean('compression'),
        PChecksum('checksum'),
        PString('data'),
        PTID('data_serial'),
    )

class Replicate(Packet):
    """
    Notify a storage node to replicate partitions up to given 'tid'
    and from given sources.

    - upstream_name: replicate from an upstream cluster
    - address: address of the source storage node, or None if there's no new
               data up to 'tid' for the given partition

    :nodes: M -> S
    """
    _fmt = PStruct('replicate',
        PTID('tid'),
        PString('upstream_name'),
        PDict('source_dict',
            PNumber('partition'),
            PAddress('address'),
        )
    )

class ReplicationDone(Packet):
    """
    Notify the master node that a partition has been successfully replicated
    from a storage to another.

    :nodes: S -> M
    """
    _fmt = PStruct('notify_replication_done',
        PNumber('offset'),
        PTID('tid'),
    )

class Truncate(Packet):
    """
    Request DB to be truncated. Also used to leave backup mode.

    :nodes: ctl -> A -> M; M -> S
    """
    _fmt = PStruct('truncate',
        PTID('tid'),
    )

    _answer = Error

class FlushLog(Packet):
    """
    Request all nodes to flush their logs.

    :nodes: ctl -> A -> M -> *
    """


_next_code = 0
def register(request, ignore_when_closed=None):
    """ Register a packet in the packet registry """
    global _next_code
    code = _next_code
    assert code < RESPONSE_MASK
    _next_code = code + 1
    if request is Error:
        code |= RESPONSE_MASK
    # register the request
    request._code = code
    answer = request._answer
    if ignore_when_closed is None:
        # By default, on a closed connection:
        # - request: ignore
        # - answer: keep
        # - notification: keep
        ignore_when_closed = answer is not None
    request._ignore_when_closed = ignore_when_closed
    if answer in (Error, None):
        return request
    # build a class for the answer
    answer = type('Answer' + request.__name__, (Packet, ), {})
    answer._fmt = request._answer
    answer.poll_thread = request.poll_thread
    answer._request = request
    assert answer._code is None, "Answer of %s is already used" % (request, )
    answer._code = code | RESPONSE_MASK
    request._answer = answer
    return request, answer

class Packets(dict):
    """
    Packet registry that checks packet code uniqueness and provides an index
    """
    def __metaclass__(name, base, d):
        # this builds a "singleton"
        cls = type('PacketRegistry', base, d)()
        for k, v in d.iteritems():
            if isinstance(v, type) and issubclass(v, Packet):
                v.handler_method_name = k[0].lower() + k[1:]
                cls[v._code] = v
        return cls

    Error = register(
                    Error)
    RequestIdentification, AcceptIdentification = register(
                    RequestIdentification, ignore_when_closed=True)
    Ping, Pong = register(
                    Ping)
    CloseClient  = register(
                    CloseClient)
    AskPrimary, AnswerPrimary = register(
                    PrimaryMaster)
    NotPrimaryMaster = register(
                    NotPrimaryMaster)
    NotifyNodeInformation = register(
                    NotifyNodeInformation)
    AskRecovery, AnswerRecovery = register(
                    Recovery)
    AskLastIDs, AnswerLastIDs = register(
                    LastIDs)
    AskPartitionTable, AnswerPartitionTable = register(
                    PartitionTable)
    SendPartitionTable = register(
                    NotifyPartitionTable)
    NotifyPartitionChanges = register(
                    PartitionChanges)
    StartOperation = register(
                    StartOperation)
    StopOperation = register(
                    StopOperation)
    AskUnfinishedTransactions, AnswerUnfinishedTransactions = register(
                    UnfinishedTransactions)
    AskLockedTransactions, AnswerLockedTransactions = register(
                    LockedTransactions)
    AskFinalTID, AnswerFinalTID = register(
                    FinalTID)
    ValidateTransaction = register(
                    ValidateTransaction)
    AskBeginTransaction, AnswerBeginTransaction = register(
                    BeginTransaction)
    FailedVote = register(
                    FailedVote)
    AskFinishTransaction, AnswerTransactionFinished = register(
                    FinishTransaction, ignore_when_closed=False)
    AskLockInformation, AnswerInformationLocked = register(
                    LockInformation, ignore_when_closed=False)
    InvalidateObjects = register(
                    InvalidateObjects)
    NotifyUnlockInformation = register(
                    UnlockInformation)
    AskNewOIDs, AnswerNewOIDs = register(
                    GenerateOIDs)
    NotifyDeadlock = register(
                    Deadlock)
    AskRebaseTransaction, AnswerRebaseTransaction = register(
                    RebaseTransaction)
    AskRebaseObject, AnswerRebaseObject = register(
                    RebaseObject)
    AskStoreObject, AnswerStoreObject = register(
                    StoreObject)
    AbortTransaction = register(
                    AbortTransaction)
    AskStoreTransaction, AnswerStoreTransaction = register(
                    StoreTransaction)
    AskVoteTransaction, AnswerVoteTransaction = register(
                    VoteTransaction)
    AskObject, AnswerObject = register(
                    GetObject)
    AskTIDs, AnswerTIDs = register(
                    TIDList)
    AskTransactionInformation, AnswerTransactionInformation = register(
                    TransactionInformation)
    AskObjectHistory, AnswerObjectHistory = register(
                    ObjectHistory)
    AskPartitionList, AnswerPartitionList = register(
                    PartitionList)
    AskNodeList, AnswerNodeList = register(
                    NodeList)
    SetNodeState = register(
                    SetNodeState, ignore_when_closed=False)
    AddPendingNodes = register(
                    AddPendingNodes, ignore_when_closed=False)
    TweakPartitionTable = register(
                    TweakPartitionTable, ignore_when_closed=False)
    SetClusterState = register(
                    SetClusterState, ignore_when_closed=False)
    Repair = register(
                    Repair)
    NotifyRepair = register(
                    RepairOne)
    NotifyClusterInformation = register(
                    ClusterInformation)
    AskClusterState, AnswerClusterState = register(
                    ClusterState)
    AskObjectUndoSerial, AnswerObjectUndoSerial = register(
                    ObjectUndoSerial)
    AskTIDsFrom, AnswerTIDsFrom = register(
                    TIDListFrom)
    AskPack, AnswerPack = register(
                    Pack, ignore_when_closed=False)
    CheckReplicas = register(
                    CheckReplicas)
    CheckPartition = register(
                    CheckPartition)
    AskCheckTIDRange, AnswerCheckTIDRange = register(
                    CheckTIDRange)
    AskCheckSerialRange, AnswerCheckSerialRange = register(
                    CheckSerialRange)
    NotifyPartitionCorrupted = register(
                    PartitionCorrupted)
    NotifyReady = register(
                    NotifyReady)
    AskLastTransaction, AnswerLastTransaction = register(
                    LastTransaction)
    AskCheckCurrentSerial, AnswerCheckCurrentSerial = register(
                    CheckCurrentSerial)
    NotifyTransactionFinished = register(
                    NotifyTransactionFinished)
    Replicate = register(
                    Replicate)
    NotifyReplicationDone = register(
                    ReplicationDone)
    AskFetchTransactions, AnswerFetchTransactions = register(
                    FetchTransactions)
    AskFetchObjects, AnswerFetchObjects = register(
                    FetchObjects)
    AddTransaction = register(
                    AddTransaction)
    AddObject = register(
                    AddObject)
    Truncate = register(
                    Truncate)
    FlushLog = register(
                    FlushLog)

def Errors():
    registry_dict = {}
    handler_method_name_dict = {}
    def register_error(code):
        return lambda self, message='': Error(code, message)
    for error in ErrorCodes:
        name = ''.join(part.capitalize() for part in str(error).split('_'))
        registry_dict[name] = register_error(int(error))
        handler_method_name_dict[int(error)] = name[0].lower() + name[1:]
    return type('ErrorRegistry', (dict,),
                registry_dict)(handler_method_name_dict)

Errors = Errors()

# Common helpers between the 'neo' module and 'neolog'.

from datetime import datetime
from operator import itemgetter

def formatNodeList(node_list, prefix='', _sort_key=itemgetter(2)):
    if node_list:
        node_list.sort(key=_sort_key)
        node_list = [(
                uuid_str(uuid), str(node_type),
                ('[%s]:%s' if ':' in addr[0] else '%s:%s')
                % addr if addr else '', str(state),
                str(id_timestamp and datetime.utcfromtimestamp(id_timestamp)))
            for node_type, addr, uuid, state, id_timestamp in node_list]
        t = ''.join('%%-%us | ' % max(len(x[i]) for x in node_list)
                    for i in xrange(len(node_list[0]) - 1))
        return map((prefix + t + '%s').__mod__, node_list)
    return ()

NotifyNodeInformation._neolog = staticmethod(lambda timestamp, node_list:
    ((timestamp,), formatNodeList(node_list, ' ! ')))

Error._neolog = staticmethod(lambda *args: ((), ("%s (%s)" % args,)))
