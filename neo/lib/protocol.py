
# Copyright (C) 2006-2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import socket
import sys
import traceback
from socket import inet_ntoa, inet_aton
from cStringIO import StringIO
from struct import Struct

from neo.lib.util import Enum, getAddressType

# The protocol version (major, minor).
PROTOCOL_VERSION = (4, 1)

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

class ErrorCodes(Enum):
    ACK = Enum.Item(0)
    NOT_READY = Enum.Item(1)
    OID_NOT_FOUND = Enum.Item(2)
    OID_DOES_NOT_EXIST = Enum.Item(6)
    TID_NOT_FOUND = Enum.Item(3)
    PROTOCOL_ERROR = Enum.Item(4)
    BROKEN_NODE = Enum.Item(5)
    ALREADY_PENDING = Enum.Item(7)
ErrorCodes = ErrorCodes()

class ClusterStates(Enum):
    RECOVERING = Enum.Item(1)
    VERIFYING = Enum.Item(2)
    RUNNING = Enum.Item(3)
    STOPPING = Enum.Item(4)
ClusterStates = ClusterStates()

class NodeTypes(Enum):
    MASTER = Enum.Item(1)
    STORAGE = Enum.Item(2)
    CLIENT = Enum.Item(3)
    ADMIN = Enum.Item(4)
NodeTypes = NodeTypes()

class NodeStates(Enum):
    RUNNING = Enum.Item(1)
    TEMPORARILY_DOWN = Enum.Item(2)
    DOWN = Enum.Item(3)
    BROKEN = Enum.Item(4)
    HIDDEN = Enum.Item(5)
    PENDING = Enum.Item(6)
    UNKNOWN = Enum.Item(7)
NodeStates = NodeStates()

class CellStates(Enum):
    UP_TO_DATE = Enum.Item(1)
    OUT_OF_DATE = Enum.Item(2)
    FEEDING = Enum.Item(3)
    DISCARDED = Enum.Item(4)
CellStates = CellStates()

class LockState(Enum):
    NOT_LOCKED = Enum.Item(1)
    GRANTED = Enum.Item(2)
    GRANTED_TO_OTHER = Enum.Item(3)
LockState = LockState()

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
}

# Other constants.
INVALID_UUID = '\0' * 16
INVALID_TID = '\xff' * 8
INVALID_OID = '\xff' * 8
INVALID_PARTITION = 0xffffffff
INVALID_ADDRESS_TYPE = socket.AF_UNSPEC
ZERO_HASH = '\0' * 20
ZERO_TID = '\0' * 8
ZERO_OID = '\0' * 8
OID_LEN = len(INVALID_OID)
TID_LEN = len(INVALID_TID)

UUID_NAMESPACES = {
    NodeTypes.STORAGE: 'S',
    NodeTypes.MASTER: 'M',
    NodeTypes.CLIENT: 'C',
    NodeTypes.ADMIN: 'A',
}

class ProtocolError(Exception):
    """ Base class for protocol errors, close the connection """
    pass

class PacketMalformedError(ProtocolError):
    """ Close the connection and set the node as broken"""
    pass

class UnexpectedPacketError(ProtocolError):
    """ Close the connection and set the node as broken"""
    pass

class NotReadyError(ProtocolError):
    """ Just close the connection """
    pass

class BrokenNodeDisallowedError(ProtocolError):
    """ Just close the connection """
    pass

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

class PAddressIPGeneric(PStructItem):

    def __init__(self, name, format):
        PStructItem.__init__(self, name, format)

    def encode(self, writer, address):
        host, port = address
        host = socket.inet_pton(self.af_type, host)
        writer(self.pack(host, port))

    def decode(self, reader):
        data = reader(self.size)
        address = self.unpack(data)
        host, port = address
        host =  socket.inet_ntop(self.af_type, host)
        return (host, port)

class PAddressIPv4(PAddressIPGeneric):
    af_type = socket.AF_INET

    def __init__(self, name):
        PAddressIPGeneric.__init__(self, name, '!4sH')

class PAddressIPv6(PAddressIPGeneric):
    af_type = socket.AF_INET6

    def __init__(self, name):
        PAddressIPGeneric.__init__(self, name, '!16sH')

class PAddress(PStructItem):
    """
        An host address (IPv4/IPv6)
    """

    address_format_dict = {
        socket.AF_INET: PAddressIPv4('ipv4'),
        socket.AF_INET6: PAddressIPv6('ipv6'),
    }

    def __init__(self, name):
        PStructItem.__init__(self, name, '!L')

    def _encode(self, writer, address):
        if address is None:
            writer(self.pack(INVALID_ADDRESS_TYPE))
            return
        af_type = getAddressType(address)
        writer(self.pack(af_type))
        encoder = self.address_format_dict[af_type]
        encoder.encode(writer, address)

    def _decode(self, reader):
        af_type  = self.unpack(reader(self.size))[0]
        if af_type == INVALID_ADDRESS_TYPE:
            return None
        decoder = self.address_format_dict[af_type]
        host, port = decoder.decode(reader)
        return (host, port)

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

class PProtocol(PStructItem):
    """
        The protocol version definition
    """
    def __init__(self, name):
        PStructItem.__init__(self, name, '!LL')

    def _encode(self, writer, version):
        writer(self.pack(*version))

    def _decode(self, reader):
        major, minor = self.unpack(reader(self.size))
        if (major, minor) != PROTOCOL_VERSION:
            raise ProtocolError('protocol version mismatch')
        return (major, minor)

class PChecksum(PItem):
    """
        A hash (SHA1)
    """
    def _encode(self, writer, checksum):
        assert len(checksum) == 20, (len(checksum), checksum)
        writer(checksum)

    def _decode(self, reader):
        return reader(20)

class PUUID(PItem):
    """
        An UUID (node identifier)
    """
    def _encode(self, writer, uuid):
        if uuid is None:
            uuid = INVALID_UUID
        assert len(uuid) == 16, (len(uuid), uuid)
        writer(uuid)

    def _decode(self, reader):
        uuid = reader(16)
        if uuid == INVALID_UUID:
            uuid = None
        return uuid

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
    Ask a current primary master node. This must be the second message when
    connecting to a master node. Any -> M.
    Reply to Ask Primary Master. This message includes a list of known master
    nodes to make sure that a peer has the same information. M -> Any.
    """
    _answer = PStruct('answer_primary',
        PUUID('primary_uuid'),
        PList('known_master_list',
            PStruct('master',
                PAddress('address'),
                PUUID('uuid'),
            ),
        ),
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
    Ask the last OID, the last TID and the last Partition Table ID that
    a storage node stores. Used to recover information. PM -> S, S -> PM.
    Reply to Ask Last IDs. S -> PM, PM -> S.
    """
    _answer = PStruct('answer_last_ids',
        POID('last_oid'),
        PTID('last_tid'),
        PPTID('last_ptid'),
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
                PFNodeState,
            ),
        ),
    )

class ReplicationDone(Packet):
    """
    Notify the master node that a partition has been successully replicated from
    a storage to another.
    S -> M
    """
    _fmt = PStruct('notify_replication_done',
        PNumber('offset'),
    )

class StartOperation(Packet):
    """
    Tell a storage nodes to start an operation. Until a storage node receives
    this message, it must not serve client nodes. PM -> S.
    """

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
    a TID is specified, an object right before the TID will be returned. S,C -> S.
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
    S -> S.
    Answer the requested TIDs. S -> S
    """
    _fmt = PStruct('tid_list_from',
        PTID('min_tid'),
        PTID('max_tid'),
        PNumber('length'),
        PList('partition_list',
            PNumber('partition'),
        ),
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

class ObjectHistoryFrom(Packet):
    """
    Ask history information for a given object. The order of serials is
    ascending, and starts at (or above) min_serial for min_oid. S -> S.
    Answer the requested serials. S -> S.
    """
    _fmt = PStruct('ask_object_history',
        POID('min_oid'),
        PTID('min_serial'),
        PTID('max_serial'),
        PNumber('length'),
        PNumber('partition'),
    )

    _answer = PStruct('ask_finish_transaction',
        PDict('object_dict',
            POID('oid'),
            PFTidList,
        ),
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
        PBoolean('modify_partition_table'),
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

class NotifyLastOID(Packet):
    """
    Notify last OID generated
    """
    _fmt = PStruct('notify_last_oid',
        POID('last_oid'),
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
        PTID('min_tid'),
        PTID('max_tid'),
        PNumber('length'),
        PNumber('partition'),
    )

    _answer = PStruct('answer_check_tid_range',
        PTID('min_tid'),
        PNumber('length'),
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
        POID('min_oid'),
        PTID('min_serial'),
        PTID('max_tid'),
        PNumber('length'),
        PNumber('partition'),
    )

    _answer = PStruct('answer_check_serial_range',
        POID('min_oid'),
        PTID('min_serial'),
        PNumber('length'),
        PNumber('count'),
        PChecksum('oid_checksum'),
        POID('max_oid'),
        PChecksum('serial_checksum'),
        PTID('max_serial'),
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

StaticRegistry = {}
def register(code, request, ignore_when_closed=None):
    """ Register a packet in the packet registry """
    # register the request
    assert code not in StaticRegistry, "Duplicate request packet code"
    request._code = code
    StaticRegistry[code] = request
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
            0x8000, Error)
    Ping, Pong = register(
            0x0001, Ping)
    Notify = register(
            0x0002, Notify)
    RequestIdentification, AcceptIdentification = register(
            0x0003, RequestIdentification)
    AskPrimary, AnswerPrimary = register(
            0x0004, PrimaryMaster)
    AnnouncePrimary = register(
            0x0005, AnnouncePrimary)
    ReelectPrimary = register(
            0x0006, ReelectPrimary)
    NotifyNodeInformation = register(
            0x0007, NotifyNodeInformation)
    AskLastIDs, AnswerLastIDs = register(
            0x0008, LastIDs)
    AskPartitionTable, AnswerPartitionTable = register(
            0x0009, PartitionTable)
    SendPartitionTable = register(
            0x000A, NotifyPartitionTable)
    NotifyPartitionChanges = register(
            0x000B, PartitionChanges)
    StartOperation = register(
            0x000C, StartOperation)
    StopOperation = register(
            0x000D, StopOperation)
    AskUnfinishedTransactions, AnswerUnfinishedTransactions = register(
            0x000E, UnfinishedTransactions)
    AskObjectPresent, AnswerObjectPresent = register(
            0x000F, ObjectPresent)
    DeleteTransaction = register(
            0x0010, DeleteTransaction)
    CommitTransaction = register(
            0x0011, CommitTransaction)
    AskBeginTransaction, AnswerBeginTransaction = register(
            0x0012, BeginTransaction)
    AskFinishTransaction, AnswerTransactionFinished = register(
            0x0013, FinishTransaction, ignore_when_closed=False)
    AskLockInformation, AnswerInformationLocked = register(
            0x0014, LockInformation, ignore_when_closed=False)
    InvalidateObjects = register(
            0x0015, InvalidateObjects)
    NotifyUnlockInformation = register(
            0x0016, UnlockInformation)
    AskNewOIDs, AnswerNewOIDs = register(
            0x0017, GenerateOIDs)
    AskStoreObject, AnswerStoreObject = register(
            0x0018, StoreObject)
    AbortTransaction = register(
            0x0019, AbortTransaction)
    AskStoreTransaction, AnswerStoreTransaction = register(
            0x001A, StoreTransaction)
    AskObject, AnswerObject = register(
            0x001B, GetObject)
    AskTIDs, AnswerTIDs = register(
            0x001C, TIDList)
    AskTransactionInformation, AnswerTransactionInformation = register(
            0x001D, TransactionInformation)
    AskObjectHistory, AnswerObjectHistory = register(
            0x001E, ObjectHistory)
    AskPartitionList, AnswerPartitionList = register(
            0x001F, PartitionList)
    AskNodeList, AnswerNodeList = register(
            0x0020, NodeList)
    SetNodeState = register(
            0x0021, SetNodeState, ignore_when_closed=False)
    AddPendingNodes = register(
            0x0022, AddPendingNodes, ignore_when_closed=False)
    AskNodeInformation, AnswerNodeInformation = register(
            0x0023, NodeInformation)
    SetClusterState = register(
            0x0024, SetClusterState, ignore_when_closed=False)
    NotifyClusterInformation = register(
            0x0025, ClusterInformation)
    AskClusterState, AnswerClusterState = register(
            0x0026, ClusterState)
    NotifyLastOID = register(
            0x0027, NotifyLastOID)
    NotifyReplicationDone = register(
            0x0028, ReplicationDone)
    AskObjectUndoSerial, AnswerObjectUndoSerial = register(
            0x0029, ObjectUndoSerial)
    AskHasLock, AnswerHasLock = register(
            0x002A, HasLock)
    AskTIDsFrom, AnswerTIDsFrom = register(
            0x002B, TIDListFrom)
    AskObjectHistoryFrom, AnswerObjectHistoryFrom = register(
            0x002C, ObjectHistoryFrom)
    # 2D
    AskPack, AnswerPack = register(
            0x002E, Pack, ignore_when_closed=False)
    AskCheckTIDRange, AnswerCheckTIDRange = register(
            0x002F, CheckTIDRange)
    AskCheckSerialRange, AnswerCheckSerialRange = register(
            0x0030, CheckSerialRange)
    NotifyReady = register(
            0x0031, NotifyReady)
    AskLastTransaction, AnswerLastTransaction = register(
            0x0032, LastTransaction)
    AskCheckCurrentSerial, AnswerCheckCurrentSerial = register(
            0x0033, CheckCurrentSerial)
    NotifyTransactionFinished = register(
            0x003E, NotifyTransactionFinished)

def Errors():
    registry_dict = {}
    handler_method_name_dict = {}
    def register_error(code):
        return lambda self, message='': Error(code, message)
    for code, error in ErrorCodes.iteritems():
        name = ''.join(part.capitalize() for part in str(error).split('_'))
        registry_dict[name] = register_error(error)
        handler_method_name_dict[code] = name[0].lower() + name[1:]
    return type('ErrorRegistry', (dict,),
                registry_dict)(handler_method_name_dict)

Errors = Errors()
