
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

from struct import pack, unpack, error, calcsize
from socket import inet_ntoa, inet_aton
from neo.profiling import profiler_decorator
from cStringIO import StringIO

from neo.util import Enum

# The protocol version (major, minor).
PROTOCOL_VERSION = (4, 1)

# Size restrictions.
MIN_PACKET_SIZE = 10
MAX_PACKET_SIZE = 0x4000000
PACKET_HEADER_FORMAT = '!LHL'
PACKET_HEADER_SIZE = calcsize(PACKET_HEADER_FORMAT)
# Check that header size is the expected value.
# If it is not, it means that struct module result is incompatible with
# "reference" platform (python 2.4 on x86-64).
assert PACKET_HEADER_SIZE == 10, \
    'Unsupported platform, packet header length = %i' % (PACKET_HEADER_SIZE, )
RESPONSE_MASK = 0x8000

class ErrorCodes(Enum):
    ACK = Enum.Item(0)
    NOT_READY = Enum.Item(1)
    OID_NOT_FOUND = Enum.Item(2)
    TID_NOT_FOUND = Enum.Item(3)
    PROTOCOL_ERROR = Enum.Item(4)
    BROKEN_NODE = Enum.Item(5)
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
INVALID_PTID = '\0' * 8
INVALID_SERIAL = INVALID_TID
INVALID_PARTITION = 0xffffffff
OID_LEN = len(INVALID_OID)

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


# packet parser
def _decodeClusterState(state):
    cluster_state = ClusterStates.get(state)
    if cluster_state is None:
        raise PacketMalformedError('invalid cluster state %d' % state)
    return cluster_state

def _decodeNodeState(state):
    node_state = NodeStates.get(state)
    if node_state is None:
        raise PacketMalformedError('invalid node state %d' % state)
    return node_state

def _decodeNodeType(original_node_type):
    node_type = NodeTypes.get(original_node_type)
    if node_type is None:
        raise PacketMalformedError('invalid node type %d' % original_node_type)
    return node_type

def _decodeErrorCode(original_error_code):
    error_code = ErrorCodes.get(original_error_code)
    if error_code is None:
        raise PacketMalformedError('invalid error code %d' %
                original_error_code)
    return error_code

def _decodeLockState(original_lock_state):
    lock_state = LockState.get(original_lock_state)
    if lock_state is None:
        raise PacketMalformedError('invalid lock state %d' % (
            original_lock_state, ))
    return lock_state

def _decodeAddress(address):
    if address == '\0' * 6:
        return None
    (ip, port) = unpack('!4sH', address)
    return (inet_ntoa(ip), port)

def _encodeAddress(address):
    if address is None:
        return '\0' * 6
    # address is a tuple (ip, port)
    return pack('!4sH', inet_aton(address[0]), address[1])

def _decodeUUID(uuid):
    if uuid == INVALID_UUID:
        return None
    return uuid

def _encodeUUID(uuid):
    if uuid is None:
        return INVALID_UUID
    return uuid

def _decodePTID(ptid):
    if ptid == INVALID_PTID:
        return None
    return ptid

def _encodePTID(ptid):
    if ptid is None:
        return INVALID_PTID
    return ptid

def _decodeTID(tid):
    if tid == INVALID_TID:
        return None
    return tid

def _encodeTID(tid):
    if tid is None:
        return INVALID_TID
    return tid

def _decodeString(buf, name, offset=0):
    buf = buf[offset:]
    (size, ) = unpack('!L', buf[:4])
    string = buf[4:4+size]
    if len(string) != size:
        raise PacketMalformedError("can't read string <%s>" % name)
    return (string, buf[offset+4+size:])

@profiler_decorator
def _encodeString(buf):
    return pack('!L', len(buf)) + buf

class Packet(object):
    """
    Base class for any packet definition.
    Each subclass should override _encode() and _decode() and return a string or
    a tuple respectively.
    """

    _header_format = None
    _header_len = None
    _request = None
    _answer = None
    _body = None
    _code = None
    _id = None

    def __init__(self, *args, **kw):
        assert self._code is not None, "Packet class not registered"
        if args != () or kw != {}:
            body = self._encode(*args, **kw)
        else:
            body = ''
        self._body = body

    def decode(self):
        assert self._body is not None
        try:
            return self._decode(self._body)
        except error, msg: # struct.error
            name = self.__class__.__name__
            raise PacketMalformedError("%s fail (%s)" % (name, msg))
        except PacketMalformedError, msg:
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

    def getCode(self):
        return self._code

    def getType(self):
        return self.__class__

    @profiler_decorator
    def encode(self):
        """ Encode a packet as a string to send it over the network """
        content = self._body
        length = PACKET_HEADER_SIZE + len(content)
        return (pack(PACKET_HEADER_FORMAT, self._id, self._code, length),
            content)

    @profiler_decorator
    def __len__(self):
        return PACKET_HEADER_SIZE + len(self._body)

    def __repr__(self):
        return '%s[%r]' % (self.__class__.__name__, self._id)

    def __eq__(self, other):
        """ Compare packets with their code instead of content """
        if other is None:
            return False
        assert isinstance(other, Packet)
        return self._code == other._code

    def _encode(self, *args, **kw):
        """ Default encoder, join all arguments """
        args = list(args)
        args.extend(kw.values())
        return ''.join([str(i) for i in args] or '')

    def _decode(self, body):
        """ Default decoder, message must be empty """
        assert body == '', "Non-empty packet decoding not implemented """
        return ()

    def isError(self):
        return isinstance(self, Error)

    def isResponse(self):
        return self._code & RESPONSE_MASK == RESPONSE_MASK

    def getAnswerClass(self):
        return self._answer

class Notify(Packet):
    """
        General purpose notification (remote logging)
    """
    def _encode(self, message):
        return message

    def _decode(self, body):
        return (body, )

class Ping(Packet):
    """
    Check if a peer is still alive. Any -> Any.
    """
    pass

class Pong(Packet):
    """
    Notify being alive. Any -> Any.
    """
    pass

class RequestIdentification(Packet):
    """
    Request a node identification. This must be the first packet for any
    connection. Any -> Any.
    """
    _header_format = '!LLH16s6s'

    def _encode(self, node_type, uuid, address, name):
        uuid = _encodeUUID(uuid)
        address = _encodeAddress(address)
        return pack(self._header_format, PROTOCOL_VERSION[0],
                          PROTOCOL_VERSION[1], node_type, uuid, address) + \
                          _encodeString(name)

    def _decode(self, body):
        r = unpack(self._header_format, body[:self._header_len])
        major, minor, node_type, uuid, address = r
        address = _decodeAddress(address)
        (name, _) = _decodeString(body, 'name', offset=self._header_len)
        node_type = _decodeNodeType(node_type)
        uuid = _decodeUUID(uuid)
        if (major, minor) != PROTOCOL_VERSION:
            raise PacketMalformedError('protocol version mismatch')
        return (node_type, uuid, address, name)

class AcceptIdentification(Packet):
    """
    Accept a node identification. This should be a reply to Request Node
    Identification. Any -> Any.
    """
    _header_format = '!H16sLL16s'

    def _encode(self, node_type, uuid,
             num_partitions, num_replicas, your_uuid):
        uuid = _encodeUUID(uuid)
        your_uuid = _encodeUUID(your_uuid)
        return pack(self._header_format, node_type, uuid,
                          num_partitions, num_replicas, your_uuid)

    def _decode(self, body):
        r = unpack(self._header_format, body)
        node_type, uuid, num_partitions, num_replicas, your_uuid = r
        node_type = _decodeNodeType(node_type)
        uuid = _decodeUUID(uuid)
        your_uuid = _decodeUUID(your_uuid)
        return (node_type, uuid, num_partitions, num_replicas, your_uuid)

class AskPrimary(Packet):
    """
    Ask a current primary master node. This must be the second message when
    connecting to a master node. Any -> M.
    """
    pass

class AnswerPrimary(Packet):
    """
    Reply to Ask Primary Master. This message includes a list of known master
    nodes to make sure that a peer has the same information. M -> Any.
    """
    _header_format = '!16sL'
    _list_entry_format = '!6s16s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, primary_uuid, known_master_list):
        primary_uuid = _encodeUUID(primary_uuid)
        body = [pack(self._header_format, primary_uuid,
            len(known_master_list))]
        for address, uuid in known_master_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack(self._list_entry_format, address, uuid))
        return ''.join(body)

    def _decode(self, body):
        packet_offset = self._header_len
        (primary_uuid, n) = unpack(self._header_format,
            body[:packet_offset])
        known_master_list = []
        list_entry_len = self._list_entry_len
        list_entry_format = self._list_entry_format
        for _ in xrange(n):
            next_packet_offset = packet_offset + list_entry_len
            address, uuid = unpack(list_entry_format,
                body[packet_offset:next_packet_offset])
            packet_offset = next_packet_offset
            address = _decodeAddress(address)
            uuid = _decodeUUID(uuid)
            known_master_list.append((address, uuid))
        primary_uuid = _decodeUUID(primary_uuid)
        return (primary_uuid, known_master_list)

class AnnouncePrimary(Packet):
    """
    Announce a primary master node election. PM -> SM.
    """
    pass

class ReelectPrimary(Packet):
    """
    Force a re-election of a primary master node. M -> M.
    """
    pass

class AskLastIDs(Packet):
    """
    Ask the last OID, the last TID and the last Partition Table ID that
    a storage node stores. Used to recover information. PM -> S, S -> PM.
    """
    pass

class AnswerLastIDs(Packet):
    """
    Reply to Ask Last IDs. S -> PM, PM -> S.
    """
    def _encode(self, loid, ltid, lptid):
        # in this case, loid is a valid OID but considered as invalid. This is
        # not an issue because the OID 0 is hard coded and will never be
        # generated
        if loid is None:
            loid = INVALID_OID
        ltid = _encodeTID(ltid)
        lptid = _encodePTID(lptid)
        return loid + ltid + lptid

    def _decode(self, body):
        (loid, ltid, lptid) = unpack('!8s8s8s', body)
        if loid == INVALID_OID:
            loid = None
        ltid = _decodeTID(ltid)
        lptid = _decodePTID(lptid)
        return (loid, ltid, lptid)

class AskPartitionTable(Packet):
    """
    Ask rows in a partition table that a storage node stores. Used to recover
    information. PM -> S.
    """
    _header_format = '!L'
    _list_entry_format = '!L'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, offset_list):
        body = [pack(self._header_format, len(offset_list))]
        list_entry_format = self._list_entry_format
        for offset in offset_list:
            body.append(pack(list_entry_format, offset))
        return ''.join(body)

    def _decode(self, body):
        packet_offset = self._header_len
        (n,) = unpack(self._header_format, body[:packet_offset])
        offset_list = []
        list_entry_len = self._list_entry_len
        list_entry_format = self._list_entry_format
        for _ in xrange(n):
            next_packet_offset = packet_offset + list_entry_len
            offset = unpack(list_entry_format,
                body[packet_offset:next_packet_offset])[0]
            packet_offset = next_packet_offset
            offset_list.append(offset)
        return (offset_list,)

class AnswerPartitionTable(Packet):
    """
    Answer rows in a partition table. S -> PM.
    """
    _header_format = '!8sL'
    _row_entry_format = '!LL'
    _row_entry_len = calcsize(_row_entry_format)
    _cell_entry_format = '!16sH'
    _cell_entry_len = calcsize(_cell_entry_format)

    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack(self._header_format, ptid, len(row_list))]
        row_entry_format = self._row_entry_format
        cell_entry_format = self._cell_entry_format
        for offset, cell_list in row_list:
            body.append(pack(row_entry_format, offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack(cell_entry_format, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = self._header_len
        (ptid, n) = unpack(self._header_format, body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        row_entry_format = self._row_entry_format
        row_entry_len = self._row_entry_len
        cell_entry_format = self._cell_entry_format
        cell_entry_len = self._cell_entry_len
        for _ in xrange(n):
            next_index = index + row_entry_len
            offset, m = unpack(row_entry_format, body[index:next_index])
            index = next_index
            for _ in xrange(m):
                next_index = index + cell_entry_len
                uuid, state = unpack(cell_entry_format, body[index:next_index])
                index = next_index
                state = CellStates.get(state)
                uuid = _decodeUUID(uuid)
                cell_list.append((uuid, state))
            row_list.append((offset, tuple(cell_list)))
            del cell_list[:]
        return (ptid, row_list)

class SendPartitionTable(Packet):
    """
    Send rows in a partition table to update other nodes. PM -> S, C.
    """
    _header_format = '!8sL'
    _row_entry_format = '!LL'
    _row_entry_len = calcsize(_row_entry_format)
    _cell_entry_format = '!16sH'
    _cell_entry_len = calcsize(_cell_entry_format)

    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack(self._header_format, ptid, len(row_list))]
        row_entry_format = self._row_entry_format
        cell_entry_format = self._cell_entry_format
        for offset, cell_list in row_list:
            body.append(pack(row_entry_format, offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack(cell_entry_format, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = self._header_len
        (ptid, n,) = unpack(self._header_format, body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        row_entry_format = self._row_entry_format
        row_entry_len = self._row_entry_len
        cell_entry_format = self._cell_entry_format
        cell_entry_len = self._cell_entry_len
        for _ in xrange(n):
            next_index = index + row_entry_len
            offset, m = unpack(row_entry_format, body[index:next_index])
            index = next_index
            for _ in xrange(m):
                next_index = index + cell_entry_len
                uuid, state = unpack(cell_entry_format, body[index:next_index])
                index = next_index
                state = CellStates.get(state)
                uuid = _decodeUUID(uuid)
                cell_list.append((uuid, state))
            row_list.append((offset, tuple(cell_list)))
            del cell_list[:]
        return (ptid, row_list)

class NotifyPartitionChanges(Packet):
    """
    Notify a subset of a partition table. This is used to notify changes.
    PM -> S, C.
    """
    _header_format = '!8sL'
    _list_entry_format = '!L16sH'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, ptid, cell_list):
        ptid = _encodePTID(ptid)
        body = [pack(self._header_format, ptid, len(cell_list))]
        list_entry_format = self._list_entry_format
        for offset, uuid, state in cell_list:
            uuid = _encodeUUID(uuid)
            body.append(pack(list_entry_format, offset, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        packet_offset = self._header_len
        (ptid, n) = unpack(self._header_format, body[:packet_offset])
        ptid = _decodePTID(ptid)
        cell_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_packet_offset = packet_offset + list_entry_len
            (offset, uuid, state) = unpack(list_entry_format,
                body[packet_offset:next_packet_offset])
            packet_offset = next_packet_offset
            state = CellStates.get(state)
            uuid = _decodeUUID(uuid)
            cell_list.append((offset, uuid, state))
        return (ptid, cell_list)

class NotifyReplicationDone(Packet):
    """
    Notify the master node that a partition has been successully replicated from
    a storage to another.
    S -> M
    """
    _header_format = '!L'

    def _encode(self, offset):
        return pack(self._header_format, offset)

    def _decode(self, body):
        (offset, ) = unpack(self._header_format, body)
        return (offset, )

class StartOperation(Packet):
    """
    Tell a storage nodes to start an operation. Until a storage node receives
    this message, it must not serve client nodes. PM -> S.
    """
    pass

class StopOperation(Packet):
    """
    Tell a storage node to stop an operation. Once a storage node receives
    this message, it must not serve client nodes. PM -> S.
    """
    pass

class AskUnfinishedTransactions(Packet):
    """
    Ask unfinished transactions  PM -> S.
    """
    pass

class AnswerUnfinishedTransactions(Packet):
    """
    Answer unfinished transactions  S -> PM.
    """
    _header_format = '!L'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, tid_list):
        body = [pack(self._header_format, len(tid_list))]
        body.extend(tid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n,) = unpack(self._header_format, body[:offset])
        tid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            tid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            tid_list.append(tid)
        return (tid_list,)

class AskObjectPresent(Packet):
    """
    Ask if an object is present. If not present, OID_NOT_FOUND should be
    returned. PM -> S.
    """
    def _decode(self, body):
        (oid, tid) = unpack('8s8s', body)
        return (oid, _decodeTID(tid))

class AnswerObjectPresent(Packet):
    """
    Answer that an object is present. PM -> S.
    """
    def _decode(self, body):
        (oid, tid) = unpack('8s8s', body)
        return (oid, _decodeTID(tid))

class DeleteTransaction(Packet):
    """
    Delete a transaction. PM -> S.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class CommitTransaction(Packet):
    """
    Commit a transaction. PM -> S.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class AskBeginTransaction(Packet):
    """
    Ask to begin a new transaction. C -> PM.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class AnswerBeginTransaction(Packet):
    """
    Answer when a transaction begin, give a TID if necessary. PM -> C.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (tid, )

class AskFinishTransaction(Packet):
    """
    Finish a transaction. C -> PM.
    """
    _header_format = '!8sL'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, tid, oid_list):
        body = [pack(self._header_format, tid, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (tid, n) = unpack(self._header_format, body[:offset])
        oid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            oid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            oid_list.append(oid)
        return (tid, oid_list)

class AnswerTransactionFinished(Packet):
    """
    Answer when a transaction is finished. PM -> C.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class AskLockInformation(Packet):
    """
    Lock information on a transaction. PM -> S.
    """
    # XXX: Identical to InvalidateObjects and AskFinishTransaction
    _header_format = '!8sL'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, tid, oid_list):
        body = [pack(self._header_format, tid, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (tid, n) = unpack(self._header_format, body[:offset])
        oid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            oid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            oid_list.append(oid)
        return (tid, oid_list)

class AnswerInformationLocked(Packet):
    """
    Notify information on a transaction locked. S -> PM.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class InvalidateObjects(Packet):
    """
    Invalidate objects. PM -> C.
    """
    _header_format = '!8sL'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, tid, oid_list):
        body = [pack(self._header_format, tid, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (tid, n) = unpack(self._header_format, body[:offset])
        oid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            oid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            oid_list.append(oid)
        return (tid, oid_list)

class NotifyUnlockInformation(Packet):
    """
    Unlock information on a transaction. PM -> S.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class AskNewOIDs(Packet):
    """
    Ask new object IDs. C -> PM.
    """
    _header_format = '!H'

    def _encode(self, num_oids):
        return pack(self._header_format, num_oids)

    def _decode(self, body):
        return unpack(self._header_format, body) # num oids

class AnswerNewOIDs(Packet):
    """
    Answer new object IDs. PM -> C.
    """
    _header_format = '!H'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, oid_list):
        body = [pack(self._header_format, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n,) = unpack(self._header_format, body[:offset])
        oid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            oid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            oid_list.append(oid)
        return (oid_list,)

class AskStoreObject(Packet):
    """
    Ask to store an object. Send an OID, an original serial, a current
    transaction ID, and data. C -> S.
    """
    _header_format = '!8s8s8sBL'

    @profiler_decorator
    def _encode(self, oid, serial, compression, checksum, data, tid):
        if serial is None:
            serial = INVALID_TID
        return pack(self._header_format, oid, serial, tid, compression,
                          checksum) + _encodeString(data)

    def _decode(self, body):
        header_len = self._header_len
        r = unpack(self._header_format, body[:header_len])
        oid, serial, tid, compression, checksum = r
        serial = _decodeTID(serial)
        (data, _) = _decodeString(body, 'data', offset=header_len)
        return (oid, serial, compression, checksum, data, tid)

class AnswerStoreObject(Packet):
    """
    Answer if an object has been stored. If an object is in conflict,
    a serial of the conflicting transaction is returned. In this case,
    if this serial is newer than the current transaction ID, a client
    node must not try to resolve the conflict. S -> C.
    """
    _header_format = '!B8s8s'

    def _encode(self, conflicting, oid, serial):
        if serial is None:
            serial = INVALID_TID
        return pack(self._header_format, conflicting, oid, serial)

    def _decode(self, body):
        (conflicting, oid, serial) = unpack(self._header_format, body)
        return (conflicting, oid, serial)

class AbortTransaction(Packet):
    """
    Abort a transaction. C -> S, PM.
    """
    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (tid, )

class AskStoreTransaction(Packet):
    """
    Ask to store a transaction. C -> S.
    """
    _header_format = '!8sLHHH'

    def _encode(self, tid, user, desc, ext, oid_list):
        lengths = (len(oid_list), len(user), len(desc), len(ext))
        body = [pack(self._header_format, tid, *lengths)]
        body.append(user)
        body.append(desc)
        body.append(ext)
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        r = unpack(self._header_format, body[:self._header_len])
        tid, oid_len, user_len, desc_len, ext_len = r
        body = body[self._header_len:]
        user = body[:user_len]
        body = body[user_len:]
        desc = body[:desc_len]
        body = body[desc_len:]
        ext = body[:ext_len]
        body = body[ext_len:]
        oid_list = []
        for _ in xrange(oid_len):
            (oid, ) = unpack('8s', body[:8])
            body = body[8:]
            oid_list.append(oid)
        return (tid, user, desc, ext, oid_list)

class AnswerStoreTransaction(Packet):
    """
    Answer if transaction has been stored. S -> C.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (tid, )

class AskObject(Packet):
    """
    Ask a stored object by its OID and a serial or a TID if given. If a serial
    is specified, the specified revision of an object will be returned. If
    a TID is specified, an object right before the TID will be returned. S,C -> S.
    """
    _header_format = '!8s8s8s'

    def _encode(self, oid, serial, tid):
        tid = _encodeTID(tid)
        serial = _encodeTID(serial) # serial is the previous TID
        return pack(self._header_format, oid, serial, tid)

    def _decode(self, body):
        (oid, serial, tid) = unpack(self._header_format, body)
        if serial == INVALID_TID:
            serial = None
        tid = _decodeTID(tid)
        return (oid, serial, tid)

class AnswerObject(Packet):
    """
    Answer the requested object. S -> C.
    """
    _header_format = '!8s8s8s8sBL'

    def _encode(self, oid, serial_start, serial_end, compression,
            checksum, data, data_serial):
        if serial_start is None:
            serial_start = INVALID_TID
        if serial_end is None:
            serial_end = INVALID_TID
        if data_serial is None:
            data_serial = INVALID_TID
        return pack(self._header_format, oid, serial_start, serial_end,
            data_serial, compression, checksum) + _encodeString(data)

    def _decode(self, body):
        header_len = self._header_len
        r = unpack(self._header_format, body[:header_len])
        oid, serial_start, serial_end, data_serial, compression, checksum = r
        if serial_end == INVALID_TID:
            serial_end = None
        if data_serial == INVALID_TID:
            data_serial = None
        (data, _) = _decodeString(body, 'data', offset=header_len)
        return (oid, serial_start, serial_end, compression, checksum, data,
            data_serial)

class AskTIDs(Packet):
    """
    Ask for TIDs between a range of offsets. The order of TIDs is descending,
    and the range is [first, last). C, S -> S.
    """
    _header_format = '!QQL'

    def _encode(self, first, last, partition):
        return pack(self._header_format, first, last, partition)

    def _decode(self, body):
        return unpack(self._header_format, body) # first, last, partition

class AnswerTIDs(Packet):
    """
    Answer the requested TIDs. S -> C, S.
    """
    _header_format = '!L'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, tid_list):
        body = [pack(self._header_format, len(tid_list))]
        body.extend(tid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n, ) = unpack(self._header_format, body[:offset])
        tid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            tid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            tid_list.append(tid)
        return (tid_list,)

class AskTransactionInformation(Packet):
    """
    Ask information about a transaction. Any -> S.
    """
    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (tid, )

class AnswerTransactionInformation(Packet):
    """
    Answer information (user, description) about a transaction. S -> Any.
    """
    _header_format = '!8sHHHBL'

    def _encode(self, tid, user, desc, ext, packed, oid_list):
        packed = packed and 1 or 0
        body = [pack(self._header_format, tid, len(user), len(desc), len(ext),
            packed, len(oid_list))]
        body.append(user)
        body.append(desc)
        body.append(ext)
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        r = unpack(self._header_format, body[:self._header_len])
        tid, user_len, desc_len, ext_len, packed, oid_len = r
        packed = bool(packed)
        body = body[self._header_len:]
        user = body[:user_len]
        body = body[user_len:]
        desc = body[:desc_len]
        body = body[desc_len:]
        ext = body[:ext_len]
        body = body[ext_len:]
        oid_list = []
        for _ in xrange(oid_len):
            (oid, ) = unpack('8s', body[:8])
            body = body[8:]
            oid_list.append(oid)
        return (tid, user, desc, ext, packed, oid_list)

class AskObjectHistory(Packet):
    """
    Ask history information for a given object. The order of serials is
    descending, and the range is [first, last]. C, S -> S.
    """
    _header_format = '!8sQQ'

    def _encode(self, oid, first, last):
        return pack(self._header_format, oid, first, last)

    def _decode(self, body):
        (oid, first, last) = unpack(self._header_format, body)
        return (oid, first, last)

class AnswerObjectHistory(Packet):
    """
    Answer history information (serial, size) for an object. S -> C, S.
    """
    _header_format = '!8sL'
    _list_entry_format = '!8sL'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, oid, history_list):
        body = [pack(self._header_format, oid, len(history_list))]
        list_entry_format = self._list_entry_format
        for serial, size in history_list:
            body.append(pack(list_entry_format, serial, size))
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (oid, length) = unpack(self._header_format, body[:offset])
        history_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(length):
            next_offset = offset + list_entry_len
            serial, size = unpack(list_entry_format, body[offset:next_offset])
            offset = next_offset
            history_list.append((serial, size))
        return (oid, history_list)

class AskOIDs(Packet):
    """
    Ask for OIDs between a range of offsets. The order of OIDs is descending,
    and the range is [first, last). S -> S.
    """
    _header_format = '!QQL'

    def _encode(self, first, last, partition):
        return pack(self._header_format, first, last, partition)

    def _decode(self, body):
        return unpack(self._header_format, body) # first, last, partition

class AnswerOIDs(Packet):
    """
    Answer the requested OIDs. S -> S.
    """
    _header_format = '!L'
    _list_entry_format = '8s'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, oid_list):
        body = [pack(self._header_format, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n,) = unpack(self._header_format, body[:offset])
        oid_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            oid = unpack(list_entry_format, body[offset:next_offset])[0]
            offset = next_offset
            oid_list.append(oid)
        return (oid_list,)

class AskPartitionList(Packet):
    """
    All the following messages are for neoctl to admin node
    Ask information about partition
    """
    _header_format = '!LL16s'

    def _encode(self, min_offset, max_offset, uuid):
        uuid = _encodeUUID(uuid)
        body = [pack(self._header_format, min_offset, max_offset, uuid)]
        return ''.join(body)

    def _decode(self, body):
        (min_offset, max_offset, uuid) =  unpack(self._header_format, body)
        uuid = _decodeUUID(uuid)
        return (min_offset, max_offset, uuid)

class AnswerPartitionList(Packet):
    """
    Answer information about partition
    """
    _header_format = '!8sL'
    _row_entry_format = '!LL'
    _row_entry_len = calcsize(_row_entry_format)
    _cell_entry_format = '!16sH'
    _cell_entry_len = calcsize(_cell_entry_format)

    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack(self._header_format, ptid, len(row_list))]
        row_entry_format = self._row_entry_format
        cell_entry_format = self._cell_entry_format
        for offset, cell_list in row_list:
            body.append(pack(row_entry_format, offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack(cell_entry_format, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = self._header_len
        (ptid, n) = unpack(self._header_format, body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        row_entry_format = self._row_entry_format
        row_entry_len = self._row_entry_len
        cell_entry_format = self._cell_entry_format
        cell_entry_len = self._cell_entry_len
        for _ in xrange(n):
            next_index = index + row_entry_len
            offset, m = unpack(row_entry_format, body[index:next_index])
            index = next_index
            for _ in xrange(m):
                next_index = index + cell_entry_len
                uuid, state = unpack(cell_entry_format, body[index:next_index])
                index = next_index
                state = CellStates.get(state)
                uuid = _decodeUUID(uuid)
                cell_list.append((uuid, state))
            row_list.append((offset, tuple(cell_list)))
            del cell_list[:]
        return (ptid, row_list)

class AskNodeList(Packet):
    """
    Ask information about nodes
    """
    _header_format = '!H'

    def _encode(self, node_type):
        return ''.join([pack(self._header_format, node_type)])

    def _decode(self, body):
        (node_type, ) = unpack(self._header_format, body)
        node_type = _decodeNodeType(node_type)
        return (node_type,)

class AnswerNodeList(Packet):
    """
    Answer information about nodes
    """
    _header_format = '!L'
    _list_entry_format = '!H6s16sH'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, node_list):
        body = [pack(self._header_format, len(node_list))]
        list_entry_format = self._list_entry_format
        for node_type, address, uuid, state in node_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack(list_entry_format, node_type, address, uuid,
                state))
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n,) = unpack(self._header_format, body[:offset])
        node_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            r = unpack(list_entry_format, body[offset:next_offset])
            offset = next_offset
            node_type, address, uuid, state = r
            address = _decodeAddress(address)
            node_type = _decodeNodeType(node_type)
            state = _decodeNodeState(state)
            uuid = _decodeUUID(uuid)
            node_list.append((node_type, address, uuid, state))
        return (node_list,)

class SetNodeState(Packet):
    """
    Set the node state
    """
    _header_format = '!16sHB'

    def _encode(self, uuid, state, modify_partition_table):
        uuid = _encodeUUID(uuid)
        return ''.join([pack(self._header_format, uuid, state,
            modify_partition_table)])

    def _decode(self, body):
        (uuid, state, modify) = unpack(self._header_format, body)
        state = _decodeNodeState(state)
        uuid = _decodeUUID(uuid)
        return (uuid, state, modify)

class AnswerNodeState(Packet):
    """
    Answer state of the node
    """
    _header_format = '!16sH'

    def _encode(self, uuid, state):
        uuid = _encodeUUID(uuid)
        return ''.join([pack(self._header_format, uuid, state)])

    def _decode(self, body):
        (uuid, state) = unpack(self._header_format, body)
        state = _decodeNodeState(state)
        uuid = _decodeUUID(uuid)
        return (uuid, state)

class AddPendingNodes(Packet):
    """
    Ask the primary to include some pending node in the partition table
    """
    _header_format = '!H'
    _list_header_format = '!16s'
    _list_header_len = calcsize(_list_header_format)

    def _encode(self, uuid_list=()):
        list_header_format = self._list_header_format
        # an empty list means all current pending nodes
        uuid_list = [pack(list_header_format, _encodeUUID(uuid)) \
            for uuid in uuid_list]
        return pack(self._header_format, len(uuid_list)) + ''.join(uuid_list)

    def _decode(self, body):
        header_len = self._header_len
        (n, ) = unpack(self._header_format, body[:header_len])
        list_header_format = self._list_header_format
        list_header_len = self._list_header_len
        uuid_list = [unpack(list_header_format,
            body[header_len+i*list_header_len:\
                 header_len+(i+1)*list_header_len])[0] for i in xrange(n)]
        uuid_list = [_decodeUUID(x) for x in uuid_list]
        return (uuid_list, )

class AnswerNewNodes(Packet):
    """
    Answer what are the nodes added in the partition table
    """
    _header_format = '!H'
    _list_header_format = '!16s'
    _list_header_len = calcsize(_list_header_format)

    def _encode(self, uuid_list):
        list_header_format = self._list_header_format
        # an empty list means no new nodes
        uuid_list = [pack(list_header_format, _encodeUUID(uuid)) for \
            uuid in uuid_list]
        return pack(self._header_format, len(uuid_list)) + ''.join(uuid_list)

    def _decode(self, body):
        header_len = self._header_len
        (n, ) = unpack(self._header_format, body[:header_len])
        list_header_format = self._list_header_format
        list_header_len = self._list_header_len
        uuid_list = [unpack(list_header_format,
            body[header_len+i*list_header_len:\
                 header_len+(i+1)*list_header_len])[0] for i in xrange(n)]
        uuid_list = [_decodeUUID(x) for x in uuid_list]
        return (uuid_list, )

class NotifyNodeInformation(Packet):
    """
    Notify information about one or more nodes. PM -> Any.
    """
    _header_format = '!L'
    _list_entry_format = '!H6s16sH'
    _list_entry_len = calcsize(_list_entry_format)

    def _encode(self, node_list):
        body = [pack(self._header_format, len(node_list))]
        list_entry_format = self._list_entry_format
        for node_type, address, uuid, state in node_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack(list_entry_format, node_type, address, uuid,
                state))
        return ''.join(body)

    def _decode(self, body):
        offset = self._header_len
        (n,) = unpack(self._header_format, body[:offset])
        node_list = []
        list_entry_format = self._list_entry_format
        list_entry_len = self._list_entry_len
        for _ in xrange(n):
            next_offset = offset + list_entry_len
            r = unpack(list_entry_format, body[offset:next_offset])
            offset = next_offset
            node_type, address, uuid, state = r
            address = _decodeAddress(address)
            node_type = _decodeNodeType(node_type)
            state = _decodeNodeState(state)
            uuid = _decodeUUID(uuid)
            node_list.append((node_type, address, uuid, state))
        return (node_list,)

class AskNodeInformation(Packet):
    """
    Ask node information
    """
    pass

class AnswerNodeInformation(Packet):
    """
    Answer node information
    """
    pass

class SetClusterState(Packet):
    """
    Set the cluster state
    """
    _header_format = '!H'

    def _encode(self, state):
        return pack(self._header_format, state)

    def _decode(self, body):
        (state, ) = unpack(self._header_format, body[:self._header_len])
        state = _decodeClusterState(state)
        return (state, )

class NotifyClusterInformation(Packet):
    """
    Notify information about the cluster
    """
    _header_format = '!H'

    def _encode(self, state):
        return pack(self._header_format, state)

    def _decode(self, body):
        (state, ) = unpack(self._header_format, body)
        state = _decodeClusterState(state)
        return (state, )

class AskClusterState(Packet):
    """
    Ask state of the cluster
    """
    pass

class AnswerClusterState(Packet):
    """
    Answer state of the cluster
    """
    _header_format = '!H'

    def _encode(self, state):
        return pack(self._header_format, state)

    def _decode(self, body):
        (state, ) = unpack(self._header_format, body)
        state = _decodeClusterState(state)
        return (state, )

class NotifyLastOID(Packet):
    """
    Notify last OID generated
    """
    def _decode(self, body):
        (loid, ) = unpack('8s', body)
        return (loid, )

class AskUndoTransaction(Packet):
    """
    Ask storage to undo given transaction
    C -> S
    """
    def _encode(self, tid, undone_tid):
        return _encodeTID(tid) + _encodeTID(undone_tid)

    def _decode(self, body):
        tid = _decodeTID(body[:8])
        undone_tid = _decodeTID(body[8:])
        return (tid, undone_tid)

class AnswerUndoTransaction(Packet):
    """
    Answer an undo request, telling if undo could be done, with an oid list.
    If undo failed, the list contains oid(s) causing problems.
    If undo succeeded; the list contains all undone oids for given storage.
    S -> C
    """
    _header_format = '!LLL'

    def _encode(self, oid_list, error_oid_list, conflict_oid_list):
        body = StringIO()
        write = body.write
        oid_list_list = [oid_list, error_oid_list, conflict_oid_list]
        write(pack(self._header_format, *[len(x) for x in oid_list_list]))
        for oid_list in oid_list_list:
            for oid in oid_list:
                write(oid)
        return body.getvalue()

    def _decode(self, body):
        body = StringIO(body)
        read = body.read
        oid_list_len, error_oid_list_len, conflict_oid_list_len = unpack(
            self._header_format, read(self._header_len))
        oid_list = []
        error_oid_list = []
        conflict_oid_list = []
        for some_list, some_list_len in (
                    (oid_list, oid_list_len),
                    (error_oid_list, error_oid_list_len),
                    (conflict_oid_list, conflict_oid_list_len),
                ):
            append = some_list.append
            for _ in xrange(some_list_len):
                append(read(OID_LEN))
        return (oid_list, error_oid_list, conflict_oid_list)

class AskHasLock(Packet):
    """
    Ask a storage is oid is locked by another transaction.
    C -> S
    """
    def _encode(self, tid, oid):
        return _encodeTID(tid) + _encodeTID(oid)

    def _decode(self, body):
        return (_decodeTID(body[:8]), _decodeTID(body[8:]))

class AnswerHasLock(Packet):
    """
    Answer whether a transaction holds the write lock for requested object.
    """
    _header_format = '!8sH'

    def _encode(self, oid, state):
        return pack(self._header_format, oid, state)

    def _decode(self, body):
        oid, state = unpack(self._header_format, body)
        return (oid, _decodeLockState(state))

class Error(Packet):
    """
    Error is a special type of message, because this can be sent against
    any other message, even if such a message does not expect a reply
    usually. Any -> Any.
    """
    _header_format = '!H'

    def _encode(self, code, message):
        return pack(self._header_format, code) + _encodeString(message)

    def _decode(self, body):
        offset = self._header_len
        (code, ) = unpack(self._header_format, body[:offset])
        code = _decodeErrorCode(code)
        (message, _) = _decodeString(body, 'message', offset=offset)
        return (code, message)


def initMessage(klass):
    if klass._header_format is not None:
        klass._header_len = calcsize(klass._header_format)

StaticRegistry = {}
def register(code, request, answer=None):
    """ Register a packet in the packet registry """
    # register the request
    # assert code & RESPONSE_MASK == 0
    assert code not in StaticRegistry, "Duplicate request packet code"
    initMessage(request)
    request._code = code
    request._answer = answer
    StaticRegistry[code] = request
    if answer not in (None, Error):
        initMessage(answer)
        # compute the answer code
        code = code | RESPONSE_MASK
        answer._request = request
        answer._code = code
        # and register the answer packet
        assert code not in StaticRegistry, "Duplicate response packet code"
        StaticRegistry[code] = answer
        return (request, answer)
    return request

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

class PacketRegistry(dict):
    """
    Packet registry that check packet code unicity and provide an index
    """

    def __init__(self):
        dict.__init__(self)
        # load packet classes
        self.update(StaticRegistry)

    def parse(self, buf, state_container):
        state = state_container.get()
        if state is None:
            header = buf.read(PACKET_HEADER_SIZE)
            if header is None:
                return None
            msg_id, msg_type, msg_len = unpack(PACKET_HEADER_FORMAT, header)
            try:
                packet_klass = self[msg_type]
            except KeyError:
                raise PacketMalformedError('Unknown packet type')
            if msg_len > MAX_PACKET_SIZE:
                raise PacketMalformedError('message too big (%d)' % msg_len)
            if msg_len < MIN_PACKET_SIZE:
                raise PacketMalformedError('message too small (%d)' % msg_len)
            msg_len -= PACKET_HEADER_SIZE
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

    # packets registration
    Error = register(0x8000, Error)
    Notify = register(0x0032, Notify)
    Ping, Pong = register(
            0x0001,
            Ping,
            Pong)
    RequestIdentification, AcceptIdentification = register(
            0x0002,
            RequestIdentification,
            AcceptIdentification)
    AskPrimary, AnswerPrimary = register(
            0x0003,
            AskPrimary,
            AnswerPrimary)
    AnnouncePrimary = register(0x0004, AnnouncePrimary)
    ReelectPrimary = register(0x0005, ReelectPrimary)
    NotifyNodeInformation = register(0x0006, NotifyNodeInformation)
    AskLastIDs, AnswerLastIDs = register(
            0x0007,
            AskLastIDs,
            AnswerLastIDs)
    AskPartitionTable, AnswerPartitionTable = register(
            0x0008,
            AskPartitionTable,
            AnswerPartitionTable)
    SendPartitionTable = register(0x0009, SendPartitionTable)
    NotifyPartitionChanges = register(0x000A, NotifyPartitionChanges)
    StartOperation = register(0x000B, StartOperation)
    StopOperation = register(0x000C, StopOperation)
    AskUnfinishedTransactions, AnswerUnfinishedTransactions = register(
            0x000D,
            AskUnfinishedTransactions,
            AnswerUnfinishedTransactions)
    AskObjectPresent, AnswerObjectPresent = register(
            0x000f,
            AskObjectPresent,
            AnswerObjectPresent)
    DeleteTransaction = register(0x0010, DeleteTransaction)
    CommitTransaction = register(0x0011, CommitTransaction)
    AskBeginTransaction, AnswerBeginTransaction = register(
            0x0012,
            AskBeginTransaction,
            AnswerBeginTransaction)
    AskFinishTransaction, AnswerTransactionFinished = register(
            0x0013,
            AskFinishTransaction,
            AnswerTransactionFinished)
    AskLockInformation, AnswerInformationLocked = register(
            0x0014,
            AskLockInformation,
            AnswerInformationLocked)
    InvalidateObjects = register(0x0015, InvalidateObjects)
    NotifyUnlockInformation = register(0x0016, NotifyUnlockInformation)
    AskNewOIDs, AnswerNewOIDs = register(
            0x0017,
            AskNewOIDs,
            AnswerNewOIDs)
    AskStoreObject, AnswerStoreObject = register(
            0x0018,
            AskStoreObject,
            AnswerStoreObject)
    AbortTransaction = register(0x0019, AbortTransaction)
    AskStoreTransaction, AnswerStoreTransaction = register(
            0x001A,
            AskStoreTransaction,
            AnswerStoreTransaction)
    AskObject, AnswerObject = register(
            0x001B,
            AskObject,
            AnswerObject)
    AskTIDs, AnswerTIDs = register(
            0x001C,
            AskTIDs,
            AnswerTIDs)
    AskTransactionInformation, AnswerTransactionInformation = register(
            0x001E,
            AskTransactionInformation,
            AnswerTransactionInformation)
    AskObjectHistory, AnswerObjectHistory = register(
            0x001F,
            AskObjectHistory,
            AnswerObjectHistory)
    AskOIDs, AnswerOIDs = register(
            0x0020,
            AskOIDs,
            AnswerOIDs)
    AskPartitionList, AnswerPartitionList = register(
            0x0021,
            AskPartitionList,
            AnswerPartitionList)
    AskNodeList, AnswerNodeList = register(
            0x0022,
            AskNodeList,
            AnswerNodeList)
    SetNodeState, AnswerNodeState = register(
            0x0023,
            SetNodeState,
            AnswerNodeState)
    AddPendingNodes, AnswerNewNodes = register(
            0x0024,
            AddPendingNodes,
            AnswerNewNodes)
    AskNodeInformation, AnswerNodeInformation = register(
            0x0025,
            AskNodeInformation,
            AnswerNodeInformation)
    SetClusterState = register(
            0x0026,
            SetClusterState,
            Error)
    NotifyClusterInformation = register(0x0027, NotifyClusterInformation)
    AskClusterState, AnswerClusterState = register(
            0x0028,
            AskClusterState,
            AnswerClusterState)
    NotifyLastOID = register(0x0030, NotifyLastOID)
    NotifyReplicationDone = register(0x0031, NotifyReplicationDone)
    AskUndoTransaction, AnswerUndoTransaction = register(
            0x0033,
            AskUndoTransaction,
            AnswerUndoTransaction)
    AskHasLock, AnswerHasLock = register(
            0x0034,
            AskHasLock,
            AnswerHasLock)

# build a "singleton"
Packets = PacketRegistry()

def register_error(code):
    def wrapper(registry, message=''):
        return Error(code, message)
    return wrapper

class ErrorRegistry(dict):
    """
        Error packet packet registry
    """

    def __init__(self):
        dict.__init__(self)

    Ack = register_error(ErrorCodes.ACK)
    ProtocolError = register_error(ErrorCodes.PROTOCOL_ERROR)
    TidNotFound = register_error(ErrorCodes.TID_NOT_FOUND)
    OidNotFound = register_error(ErrorCodes.OID_NOT_FOUND)
    NotReady = register_error(ErrorCodes.NOT_READY)
    Broken = register_error(ErrorCodes.BROKEN_NODE)

Errors = ErrorRegistry()

