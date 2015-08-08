
# Copyright (C) 2006-2015  Nexedi SA
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

import socket
import sys
import traceback
from cStringIO import StringIO
from struct import Struct

PROTOCOL_VERSION = 3

# Size restrictions.
MIN_PACKET_SIZE = 10
MAX_PACKET_SIZE = 0x4000000
PACKET_HEADER_FORMAT = Struct('!LHL')
# Check that header size is the expected value.
# If it is not, it means that struct module result is incompatible with
# "reference" platform (python 2.4 on x86-64).
assert PACKET_HEADER_FORMAT.size == 10, \
    'Unsupported platform, packet header length = %i' % \
    (PACKET_HEADER_FORMAT.size, )
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
    BROKEN_NODE
    ALREADY_PENDING
    REPLICATION_ERROR
    CHECKING_ERROR
    BACKEND_NOT_IMPLEMENTED

@Enum
def ClusterStates():
    RECOVERING
    VERIFYING
    RUNNING
    STOPPING
    STARTING_BACKUP
    BACKINGUP
    STOPPING_BACKUP

@Enum
def NodeTypes():
    MASTER
    STORAGE
    CLIENT
    ADMIN

@Enum
def NodeStates():
    RUNNING
    TEMPORARILY_DOWN
    DOWN
    BROKEN
    HIDDEN
    PENDING
    UNKNOWN

@Enum
def CellStates():
    # Normal state: cell is writable/readable, and it isn't planned to drop it.
    UP_TO_DATE
    # Write-only cell. Last transactions are missing because storage is/was down
    # for a while, or because it is new for the partition. It usually becomes
    # UP_TO_DATE when replication is done.
    OUT_OF_DATE
    # Same as UP_TO_DATE, except that it will be discarded as soon as another
    # node finishes to replicate it. It means a partition is moved from 1 node
    # to another.
    FEEDING
    # Not really a state: only used in network packets to tell storages to drop
    # partitions.
    DISCARDED
    # A check revealed that data differs from other replicas. Cell is neither
    # readable nor writable.
    CORRUPTED

@Enum
def LockState():
    NOT_LOCKED
    GRANTED
    GRANTED_TO_OTHER

# used for logging
node_state_prefix_dict = {
    NodeStates.RUNNING: 'R',
    NodeStates.TEMPORARILY_DOWN: 'T',
    NodeStates.DOWN: 'D',
    NodeStates.BROKEN: 'B',
    NodeStates.HIDDEN: 'H',
    NodeStates.PENDING: 'P',
    NodeStates.UNKNOWN: 'U',
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
INVALID_UUID = 0
INVALID_TID = '\xff' * 8
INVALID_OID = '\xff' * 8
INVALID_PARTITION = 0xffffffff
INVALID_ADDRESS_TYPE = socket.AF_UNSPEC
ZERO_HASH = '\0' * 20
ZERO_TID = '\0' * 8
ZERO_OID = '\0' * 8
OID_LEN = len(INVALID_OID)
TID_LEN = len(INVALID_TID)
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
    """ Close the connection and set the node as broken"""

class UnexpectedPacketError(ProtocolError):
    """ Close the connection and set the node as broken"""

class NotReadyError(ProtocolError):
    """ Just close the connection """

class BrokenNodeDisallowedError(ProtocolError):
    """ Just close the connection """

class BackendNotImplemented(Exception):
    """ Method not implemented by backend storage """

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

    def __init__(self, *args, **kw):
        assert self._code is not None, "Packet class not registered"
        if args or kw:
            args = list(args)
            buf = StringIO()
            # load named arguments
            for item in self._fmt._items[len(args):]:
                args.append(kw.get(item._name))
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
        length = PACKET_HEADER_FORMAT.size + len(content)
        return (PACKET_HEADER_FORMAT.pack(self._id, self._code, length), content)

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
        Base class for any packet item, _encode and _decode must be overriden
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
    def __init__(self, name, fmt):
        PItem.__init__(self, name)
        struct = Struct(fmt)
        self.pack = struct.pack
        self.unpack = struct.unpack
        self.size = struct.size

    def _encode(self, writer, value):
        writer(self.pack(value))

    def _decode(self, reader):
        return self.unpack(reader(self.size))[0]

class PList(PStructItem):
    """
        A list of homogeneous items
    """
    def __init__(self, name, item):
        PStructItem.__init__(self, name, '!L')
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
    def __init__(self, name, key, value):
        PStructItem.__init__(self, name, '!L')
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
    def __init__(self, name, enum):
        PStructItem.__init__(self, name, '!l')
        self._enum = enum

    def _encode(self, writer, item):
        if item is None:
            item = -1
        else:
            assert isinstance(item, int), item
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
    def __init__(self, name):
        PStructItem.__init__(self, name, '!L')

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
    def __init__(self, name):
        PStructItem.__init__(self, name, '!B')

    def _encode(self, writer, value):
        writer(self.pack(bool(value)))

    def _decode(self, reader):
        return bool(self.unpack(reader(self.size))[0])

class PNumber(PStructItem):
    """
        A integer number (4-bytes length)
    """
    def __init__(self, name):
        PStructItem.__init__(self, name, '!L')

class PIndex(PStructItem):
    """
        A big integer to defined indexes in a huge list.
    """
    def __init__(self, name):
        PStructItem.__init__(self, name, '!Q')

class PPTID(PStructItem):
    """
        A None value means an invalid PTID
    """
    def __init__(self, name):
        PStructItem.__init__(self, name, '!Q')

    def _encode(self, writer, value):
        if value is None:
            value = 0
        PStructItem._encode(self, writer, value)

    def _decode(self, reader):
        value = PStructItem._decode(self, reader)
        if value == 0:
            value = None
        return value

class PProtocol(PNumber):
    """
        The protocol version definition
    """
    def _encode(self, writer, version):
        writer(self.pack(version))

    def _decode(self, reader):
        version = self.unpack(reader(self.size))
        if version != (PROTOCOL_VERSION,):
            raise ProtocolError('protocol version mismatch')
        return version

class PChecksum(PItem):
    """
        A hash (SHA1)
    """
    def _encode(self, writer, checksum):
        assert len(checksum) == 20, (len(checksum), checksum)
        writer(checksum)

    def _decode(self, reader):
        return reader(20)

class PUUID(PStructItem):
    """
        An UUID (node identifier, 4-bytes signed integer)
    """
    def __init__(self, name):
        PStructItem.__init__(self, name, '!l')

    def _encode(self, writer, uuid):
        writer(self.pack(uuid or 0))

    def _decode(self, reader):
        return self.unpack(reader(self.size))[0] or None

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

class Notify(Packet):
    """
        General purpose notification (remote logging)
    """
    _fmt = PStruct('notify',
        PString('message'),
    )

class Error(Packet):
    """
    Error is a special type of message, because this can be sent against
    any other message, even if such a message does not expect a reply
    usually. Any -> Any.
    """
    _fmt = PStruct('error',
        PNumber('code'),
        PString('message'),
    )

class Ping(Packet):
    """
    Check if a peer is still alive. Any -> Any.
    """
    _answer = PFEmpty

class CloseClient(Packet):
    """
    Tell peer it can close the connection if it has finished with us. Any -> Any
    """

class RequestIdentification(Packet):
    """
    Request a node identification. This must be the first packet for any
    connection. Any -> Any.
    """

    _fmt = PStruct('request_identification',
        PProtocol('protocol_version'),
        PFNodeType,
        PUUID('uuid'),
        PAddress('address'),
        PString('name'),
    )

    _answer = PStruct('accept_identification',
        PFNodeType,
        PUUID('my_uuid'),
        PNumber('num_partitions'),
        PNumber('num_replicas'),
        PUUID('your_uuid'),
        PAddress('primary'),
        PList('known_master_list',
            PStruct('master',
                PAddress('address'),
                PUUID('uuid'),
            ),
        ),
    )

    def __init__(self, *args, **kw):
        if args or kw:
            # always announce current protocol version
            args = list(args)
            args.insert(0, PROTOCOL_VERSION)
        super(RequestIdentification, self).__init__(*args, **kw)

    def decode(self):
        return super(RequestIdentification, self).decode()[1:]

class PrimaryMaster(Packet):
    """
    Ask current primary master's uuid. CTL -> A.
    """
    _answer = PStruct('answer_primary',
        PUUID('primary_uuid'),
    )

class AnnouncePrimary(Packet):
    """
    Announce a primary master node election. PM -> SM.
    """

class ReelectPrimary(Packet):
    """
    Force a re-election of a primary master node. M -> M.
    """

class LastIDs(Packet):
    """
    Ask the last OID, the last TID and the last Partition Table ID so that
    a master recover. PM -> S, S -> PM.
    """
    _answer = PStruct('answer_last_ids',
        POID('last_oid'),
        PTID('last_tid'),
        PPTID('last_ptid'),
        PTID('backup_tid'),
    )

class PartitionTable(Packet):
    """
    Ask the full partition table. PM -> S.
    Answer rows in a partition table. S -> PM.
    """
    _answer = PStruct('answer_partition_table',
        PPTID('ptid'),
        PFRowList,
    )

class NotifyPartitionTable(Packet):
    """
    Send rows in a partition table to update other nodes. PM -> S, C.
    """
    _fmt = PStruct('send_partition_table',
        PPTID('ptid'),
        PFRowList,
    )

class PartitionChanges(Packet):
    """
    Notify a subset of a partition table. This is used to notify changes.
    PM -> S, C.
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
    Tell a storage nodes to start an operation. Until a storage node receives
    this message, it must not serve client nodes. PM -> S.
    """
    _fmt = PStruct('start_operation',
        PBoolean('backup'),
    )

class StopOperation(Packet):
    """
    Tell a storage node to stop an operation. Once a storage node receives
    this message, it must not serve client nodes. PM -> S.
    """

class UnfinishedTransactions(Packet):
    """
    Ask unfinished transactions  PM -> S.
    Answer unfinished transactions  S -> PM.
    """
    _answer = PStruct('answer_unfinished_transactions',
        PTID('max_tid'),
        PList('tid_list',
            PTID('unfinished_tid'),
        ),
    )

class ObjectPresent(Packet):
    """
    Ask if an object is present. If not present, OID_NOT_FOUND should be
    returned. PM -> S.
    Answer that an object is present. PM -> S.
    """
    _fmt = PStruct('object_present',
        POID('oid'),
        PTID('tid'),
    )

    _answer = PStruct('object_present',
        POID('oid'),
        PTID('tid'),
    )

class DeleteTransaction(Packet):
    """
    Delete a transaction. PM -> S.
    """
    _fmt = PStruct('delete_transaction',
        PTID('tid'),
        PFOidList,
    )

class CommitTransaction(Packet):
    """
    Commit a transaction. PM -> S.
    """
    _fmt = PStruct('commit_transaction',
        PTID('tid'),
    )

class BeginTransaction(Packet):
    """
    Ask to begin a new transaction. C -> PM.
    Answer when a transaction begin, give a TID if necessary. PM -> C.
    """
    _fmt = PStruct('ask_begin_transaction',
        PTID('tid'),
    )

    _answer = PStruct('answer_begin_transaction',
        PTID('tid'),
    )

class FinishTransaction(Packet):
    """
    Finish a transaction. C -> PM.
    Answer when a transaction is finished. PM -> C.
    """
    _fmt = PStruct('ask_finish_transaction',
        PTID('tid'),
        PFOidList,
    )

    _answer = PStruct('answer_information_locked',
        PTID('ttid'),
        PTID('tid'),
    )

class NotifyTransactionFinished(Packet):
    """
    Notify that a transaction blocking a replication is now finished
    M -> S
    """
    _fmt = PStruct('notify_transaction_finished',
        PTID('ttid'),
        PTID('max_tid'),
    )

class LockInformation(Packet):
    """
    Lock information on a transaction. PM -> S.
    Notify information on a transaction locked. S -> PM.
    """
    _fmt = PStruct('ask_lock_informations',
        PTID('ttid'),
        PTID('tid'),
        PFOidList,
    )

    _answer = PStruct('answer_information_locked',
        PTID('tid'),
    )

class InvalidateObjects(Packet):
    """
    Invalidate objects. PM -> C.
    """
    _fmt = PStruct('ask_finish_transaction',
        PTID('tid'),
        PFOidList,
    )

class UnlockInformation(Packet):
    """
    Unlock information on a transaction. PM -> S.
    """
    _fmt = PStruct('notify_unlock_information',
        PTID('tid'),
    )

class GenerateOIDs(Packet):
    """
    Ask new object IDs. C -> PM.
    Answer new object IDs. PM -> C.
    """
    _fmt = PStruct('ask_new_oids',
        PNumber('num_oids'),
    )

    _answer = PStruct('answer_new_oids',
        PFOidList,
    )

class StoreObject(Packet):
    """
    Ask to store an object. Send an OID, an original serial, a current
    transaction ID, and data. C -> S.
    Answer if an object has been stored. If an object is in conflict,
    a serial of the conflicting transaction is returned. In this case,
    if this serial is newer than the current transaction ID, a client
    node must not try to resolve the conflict. S -> C.
    """
    _fmt = PStruct('ask_store_object',
        POID('oid'),
        PTID('serial'),
        PBoolean('compression'),
        PChecksum('checksum'),
        PString('data'),
        PTID('data_serial'),
        PTID('tid'),
        PBoolean('unlock'),
    )

    _answer = PStruct('answer_store_object',
        PBoolean('conflicting'),
        POID('oid'),
        PTID('serial'),
    )

class AbortTransaction(Packet):
    """
    Abort a transaction. C -> S, PM.
    """
    _fmt = PStruct('abort_transaction',
        PTID('tid'),
    )

class StoreTransaction(Packet):
    """
    Ask to store a transaction. C -> S.
    Answer if transaction has been stored. S -> C.
    """
    _fmt = PStruct('ask_store_transaction',
        PTID('tid'),
        PString('user'),
        PString('description'),
        PString('extension'),
        PFOidList,
    )

    _answer = PStruct('answer_store_transaction',
        PTID('tid'),
    )

class GetObject(Packet):
    """
    Ask a stored object by its OID and a serial or a TID if given. If a serial
    is specified, the specified revision of an object will be returned. If
    a TID is specified, an object right before the TID will be returned. C -> S.
    Answer the requested object. S -> C.
    """
    _fmt = PStruct('ask_object',
        POID('oid'),
        PTID('serial'),
        PTID('tid'),
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
    and the range is [first, last). C -> S.
    Answer the requested TIDs. S -> C.
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
    C -> S.
    Answer the requested TIDs. S -> C
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
    Ask information about a transaction. Any -> S.
    Answer information (user, description) about a transaction. S -> Any.
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
    descending, and the range is [first, last]. C -> S.
    Answer history information (serial, size) for an object. S -> C.
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
    All the following messages are for neoctl to admin node
    Ask information about partition
    Answer information about partition
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
    Ask information about nodes
    Answer information about nodes
    """
    _fmt = PStruct('ask_node_list',
        PFNodeType,
    )

    _answer = PStruct('answer_node_list',
        PFNodeList,
    )

class SetNodeState(Packet):
    """
    Set the node state
    """
    _fmt = PStruct('set_node_state',
        PUUID('uuid'),
        PFNodeState,
    )

    _answer = Error

class AddPendingNodes(Packet):
    """
    Ask the primary to include some pending node in the partition table
    """
    _fmt = PStruct('add_pending_nodes',
        PFUUIDList,
    )

    _answer = Error

class TweakPartitionTable(Packet):
    """
    Ask the primary to optimize the partition table. A -> PM.
    """
    _fmt = PStruct('tweak_partition_table',
        PFUUIDList,
    )

    _answer = Error

class NotifyNodeInformation(Packet):
    """
    Notify information about one or more nodes. PM -> Any.
    """
    _fmt = PStruct('notify_node_informations',
        PFNodeList,
    )

class NodeInformation(Packet):
    """
    Ask node information
    """
    _answer = PFEmpty

class SetClusterState(Packet):
    """
    Set the cluster state
    """
    _fmt = PStruct('set_cluster_state',
        PEnum('state', ClusterStates),
    )

    _answer = Error

class ClusterInformation(Packet):
    """
    Notify information about the cluster
    """
    _fmt = PStruct('notify_cluster_information',
        PEnum('state', ClusterStates),
    )

class ClusterState(Packet):
    """
    Ask state of the cluster
    Answer state of the cluster
    """

    _answer = PStruct('answer_cluster_state',
        PEnum('state', ClusterStates),
    )

class ObjectUndoSerial(Packet):
    """
    Ask storage the serial where object data is when undoing given transaction,
    for a list of OIDs.
    C -> S
    Answer serials at which object data is when undoing a given transaction.
    object_tid_dict has the following format:
        key: oid
        value: 3-tuple
            current_serial (TID)
                The latest serial visible to the undoing transaction.
            undo_serial (TID)
                Where undone data is (tid at which data is before given undo).
            is_current (bool)
                If current_serial's data is current on storage.
    S -> C
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

class HasLock(Packet):
    """
    Ask a storage is oid is locked by another transaction.
    C -> S
    Answer whether a transaction holds the write lock for requested object.
    """
    _fmt = PStruct('has_load_lock',
        PTID('tid'),
        POID('oid'),
    )

    _answer = PStruct('answer_has_lock',
        POID('oid'),
        PEnum('lock_state', LockState),
    )

class CheckCurrentSerial(Packet):
    """
    Verifies if given serial is current for object oid in the database, and
    take a write lock on it (so that this state is not altered until
    transaction ends).
    Answer to AskCheckCurrentSerial.
    Same structure as AnswerStoreObject, to handle the same way, except there
    is nothing to invalidate in any client's cache.
    """
    _fmt = PStruct('ask_check_current_serial',
        PTID('tid'),
        PTID('serial'),
        POID('oid'),
    )

    _answer = PStruct('answer_store_object',
        PBoolean('conflicting'),
        POID('oid'),
        PTID('serial'),
    )

class Pack(Packet):
    """
    Request a pack at given TID.
    C -> M
    M -> S
    Inform that packing it over.
    S -> M
    M -> C
    """
    _fmt = PStruct('ask_pack',
        PTID('tid'),
    )

    _answer = PStruct('answer_pack',
        PBoolean('status'),
    )

class CheckReplicas(Packet):
    """
    ctl -> A
    A -> M
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
    M -> S
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
    S -> S
    Stats about a range of transactions.
    Used to know if there are differences between a replicating node and
    reference node.
    S -> S
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
    S -> S
    Stats about a range of object history.
    Used to know if there are differences between a replicating node and
    reference node.
    S -> S
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
    S -> M
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
    C -> M
    Answer last committed TID.
    M -> C
    """

    _answer = PStruct('answer_last_transaction',
        PTID('tid'),
    )

class NotifyReady(Packet):
    """
    Notify that node is ready to serve requests.
    S -> M
    """
    pass

# replication

class FetchTransactions(Packet):
    """
    S -> S
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
    S -> S
    """
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
    S -> S
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
    S -> S
    """
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
    M -> S

    - upstream_name: replicate from an upstream cluster
    - address: address of the source storage node, or None if there's no new
               data up to 'tid' for the given partition
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
    Notify the master node that a partition has been successully replicated from
    a storage to another.
    S -> M
    """
    _fmt = PStruct('notify_replication_done',
        PNumber('offset'),
        PTID('tid'),
    )

class Truncate(Packet):
    """
    XXX: Used for both make storage consistent and leave backup mode
    M -> S
    """
    _fmt = PStruct('truncate',
        PTID('tid'),
    )


StaticRegistry = {}
def register(request, ignore_when_closed=None):
    """ Register a packet in the packet registry """
    code = len(StaticRegistry)
    if request is Error:
        code |= RESPONSE_MASK
    # register the request
    StaticRegistry[code] = request
    if request is None:
        return # None registered only to skip a code number (for compatibility)
    request._code = code
    answer = request._answer
    if ignore_when_closed is None:
        # By default, on a closed connection:
        # - request: ignore
        # - answer: keep
        # - nofitication: keep
        ignore_when_closed = answer is not None
    request._ignore_when_closed = ignore_when_closed
    if answer in (Error, None):
        return request
    # build a class for the answer
    answer = type('Answer%s' % (request.__name__, ), (Packet, ), {})
    answer._fmt = request._answer
    # compute the answer code
    code = code | RESPONSE_MASK
    answer._request = request
    assert answer._code is None, "Answer of %s is already used" % (request, )
    answer._code = code
    request._answer = answer
    # and register the answer packet
    assert code not in StaticRegistry, "Duplicate response packet code"
    StaticRegistry[code] = answer
    return (request, answer)

class ParserState(object):
    """
    Parser internal state.
    To be considered opaque datatype outside of PacketRegistry.parse .
    """
    payload = None

    def set(self, payload):
        self.payload = payload

    def get(self):
        return self.payload

    def clear(self):
        self.payload = None

class Packets(dict):
    """
    Packet registry that check packet code unicity and provide an index
    """
    def __metaclass__(name, base, d):
        for k, v in d.iteritems():
            if isinstance(v, type) and issubclass(v, Packet):
                v.handler_method_name = k[0].lower() + k[1:]
        # this builds a "singleton"
        return type('PacketRegistry', base, d)(StaticRegistry)

    def parse(self, buf, state_container):
        state = state_container.get()
        if state is None:
            header = buf.read(PACKET_HEADER_FORMAT.size)
            if header is None:
                return None
            msg_id, msg_type, msg_len = PACKET_HEADER_FORMAT.unpack(header)
            try:
                packet_klass = self[msg_type]
            except KeyError:
                raise PacketMalformedError('Unknown packet type')
            if msg_len > MAX_PACKET_SIZE:
                raise PacketMalformedError('message too big (%d)' % msg_len)
            if msg_len < MIN_PACKET_SIZE:
                raise PacketMalformedError('message too small (%d)' % msg_len)
            msg_len -= PACKET_HEADER_FORMAT.size
        else:
            msg_id, packet_klass, msg_len = state
        data = buf.read(msg_len)
        if data is None:
            # Not enough.
            if state is None:
                state_container.set((msg_id, packet_klass, msg_len))
            return None
        if state:
            state_container.clear()
        packet = packet_klass()
        packet.setContent(msg_id, data)
        return packet

    # notifications
    Error = register(
                    Error)
    RequestIdentification, AcceptIdentification = register(
                    RequestIdentification)
    # Code of RequestIdentification packet must never change so that 2
    # incompatible nodes can reject themselves gracefully (i.e. comparing
    # protocol versions) instead of raising PacketMalformedError.
    assert RequestIdentification._code == 1

    Ping, Pong = register(
                    Ping)
    CloseClient  = register(
                    CloseClient)
    Notify = register(
                    Notify)
    AskPrimary, AnswerPrimary = register(
                    PrimaryMaster)
    AnnouncePrimary = register(
                    AnnouncePrimary)
    ReelectPrimary = register(
                    ReelectPrimary)
    NotifyNodeInformation = register(
                    NotifyNodeInformation)
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
    AskObjectPresent, AnswerObjectPresent = register(
                    ObjectPresent)
    DeleteTransaction = register(
                    DeleteTransaction)
    CommitTransaction = register(
                    CommitTransaction)
    AskBeginTransaction, AnswerBeginTransaction = register(
                    BeginTransaction)
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
    AskStoreObject, AnswerStoreObject = register(
                    StoreObject)
    AbortTransaction = register(
                    AbortTransaction)
    AskStoreTransaction, AnswerStoreTransaction = register(
                    StoreTransaction)
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
    AskNodeInformation, AnswerNodeInformation = register(
                    NodeInformation)
    SetClusterState = register(
                    SetClusterState, ignore_when_closed=False)
    NotifyClusterInformation = register(
                    ClusterInformation)
    AskClusterState, AnswerClusterState = register(
                    ClusterState)
    AskObjectUndoSerial, AnswerObjectUndoSerial = register(
                    ObjectUndoSerial)
    AskHasLock, AnswerHasLock = register(
                    HasLock)
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
