#
# Copyright (C) 2006-2009  Nexedi SA
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

from struct import pack, unpack, error
from socket import inet_ntoa, inet_aton

from neo.util import Enum

# The protocol version (major, minor).
PROTOCOL_VERSION = (4, 0)

# Size restrictions.
MIN_PACKET_SIZE = 10
MAX_PACKET_SIZE = 0x4000000
PACKET_HEADER_SIZE = 10

class ErrorCodes(Enum):
    ACK = Enum.Item(0)
    NOT_READY = Enum.Item(1)
    OID_NOT_FOUND = Enum.Item(2)
    TID_NOT_FOUND = Enum.Item(3)
    PROTOCOL_ERROR = Enum.Item(4)
    BROKEN_NODE = Enum.Item(5)
ErrorCodes = ErrorCodes()

class ClusterStates(Enum):
    RECOVERING = Enum.Item(2)
    VERIFYING = Enum.Item(3)
    RUNNING = Enum.Item(4)
    STOPPING = Enum.Item(5)
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
INVALID_TID = '\0' * 8
INVALID_OID = '\0' * 8
INVALID_PTID = '\0' * 8
INVALID_SERIAL = INVALID_TID
INVALID_PARTITION = 0xffffffff

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

def _readString(buf, name, offset=0):
    buf = buf[offset:]
    (size, ) = unpack('!L', buf[:4])
    string = buf[4:4+size]
    if len(string) != size:
        raise PacketMalformedError("can't read string <%s>" % name)
    return (string, buf[offset+size:])


class Packet(object):
    """
    Base class for any packet definition.
    Each subclass should override _encode() and _decode() and return a string or
    a tuple respectively.
    """

    _body = None
    _code = None
    _args = None
    _id = None

    def __init__(self, *args, **kw):
        assert self._code is not None, "Packet class not registered"
        if args != () or kw != {}:
            body = self._encode(*args, **kw)
        else:
            body = ''
        self._body = body
        self._args = args

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

    def __str__(self):
        content = self._body
        length = PACKET_HEADER_SIZE + len(content)
        return pack('!LHL', self.getId(), self._code, length) + content

    def __len__(self):
        return PACKET_HEADER_SIZE + len(self._body)

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

    def isResponse(self):
        return self._code & 0x8000 == 0x8000


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
    def _encode(self, node_type, uuid, address, name):
        uuid = _encodeUUID(uuid)
        address = _encodeAddress(address)
        return pack('!LLH16s6sL', PROTOCOL_VERSION[0], PROTOCOL_VERSION[1],
                          node_type, uuid, address, len(name)) + name

    def _decode(self, body):
        r = unpack('!LLH16s6s', body[:32])
        major, minor, node_type, uuid, address = r
        address = _decodeAddress(address)
        (name, _) = _readString(body, 'name', offset=32)
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
    def _encode(self, node_type, uuid,
             num_partitions, num_replicas, your_uuid):
        uuid = _encodeUUID(uuid)
        your_uuid = _encodeUUID(your_uuid)
        return pack('!H16sLL16s', node_type, uuid,
                          num_partitions, num_replicas, your_uuid)

    def _decode(self, body):
        r = unpack('!H16sLL16s', body)
        node_type, uuid, num_partitions, num_replicas, your_uuid = r
        node_type = _decodeNodeType(node_type)
        uuid = _decodeUUID(uuid)
        your_uuid == _decodeUUID(uuid)
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
    def _encode(self, primary_uuid, known_master_list):
        primary_uuid = _encodeUUID(primary_uuid)
        body = [primary_uuid, pack('!L', len(known_master_list))]
        for address, uuid in known_master_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack('!6s16s', address, uuid))
        return ''.join(body)

    def _decode(self, body):
        (primary_uuid, n) = unpack('!16sL', body[:20])
        known_master_list = []
        for i in xrange(n):
            address, uuid = unpack('!6s16s', body[20+i*22:42+i*22])
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
        lptid = _decodePTID(lptid)
        return (loid, ltid, lptid)

class AskPartitionTable(Packet):
    """
    Ask rows in a partition table that a storage node stores. Used to recover
    information. PM -> S.
    """
    def _encode(self, offset_list):
        body = [pack('!L', len(offset_list))]
        for offset in offset_list:
            body.append(pack('!L', offset))
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!L', body[:4])
        offset_list = []
        for i in xrange(n):
            offset = unpack('!L', body[4+i*4:8+i*4])[0]
            offset_list.append(offset)
        return (offset_list,)

class AnswerPartitionTable(Packet):
    """
    Answer rows in a partition table. S -> PM.
    """
    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack('!8sL', ptid, len(row_list))]
        for offset, cell_list in row_list:
            body.append(pack('!LL', offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack('!16sH', uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = 12
        (ptid, n) = unpack('!8sL', body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        for i in xrange(n):
            offset, m = unpack('!LL', body[index:index+8])
            index += 8
            for j in xrange(m):
                uuid, state = unpack('!16sH', body[index:index+18])
                index += 18
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
    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack('!8sL', ptid, len(row_list))]
        for offset, cell_list in row_list:
            body.append(pack('!LL', offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack('!16sH', uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = 12
        (ptid, n,) = unpack('!8sL', body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        for i in xrange(n):
            offset, m = unpack('!LL', body[index:index+8])
            index += 8
            for j in xrange(m):
                uuid, state = unpack('!16sH', body[index:index+18])
                index += 18
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
    def _encode(self, ptid, cell_list):
        ptid = _encodePTID(ptid)
        body = [pack('!8sL', ptid, len(cell_list))]
        for offset, uuid, state in cell_list:
            uuid = _encodeUUID(uuid)
            body.append(pack('!L16sH', offset, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        (ptid, n) = unpack('!8sL', body[:12])
        ptid = _decodePTID(ptid)
        cell_list = []
        for i in xrange(n):
            (offset, uuid, state) = unpack('!L16sH', body[12+i*22:34+i*22])
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

    def _encode(self, offset):
        return pack('!L', offset)

    def _decode(self, body):
        (offset, ) = unpack('!L', body)
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
    def _encode(self, tid_list):
        body = [pack('!L', len(tid_list))]
        body.extend(tid_list)
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!L', body[:4])
        tid_list = []
        for i in xrange(n):
            tid = unpack('8s', body[4+i*8:12+i*8])[0]
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

class FinishTransaction(Packet):
    """
    Finish a transaction. C -> PM.
    """
    def _encode(self, oid_list, tid):
        body = [pack('!8sL', tid, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        (tid, n) = unpack('!8sL', body[:12])
        oid_list = []
        for i in xrange(n):
            oid = unpack('8s', body[12+i*8:20+i*8])[0]
            oid_list.append(oid)
        return (oid_list, tid)

class AnswerTransactionFinished(Packet):
    """
    Answer when a transaction is finished. PM -> C.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class LockInformation(Packet):
    """
    Lock information on a transaction. PM -> S.
    """
    def _encode(self, tid):
        return _encodeTID(tid)

    def _decode(self, body):
        (tid, ) = unpack('8s', body)
        return (_decodeTID(tid), )

class NotifyInformationLocked(Packet):
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
    def _encode(self, oid_list, tid):
        body = [pack('!8sL', tid, len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        (tid, n) = unpack('!8sL', body[:12])
        oid_list = []
        for i in xrange(12, 12 + n * 8, 8):
            oid = unpack('8s', body[i:i+8])[0]
            oid_list.append(oid)
        return (oid_list, tid)

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
    def _encode(self, num_oids):
        return pack('!H', num_oids)

    def _decode(self, body):
        return unpack('!H', body) # num oids

class AnswerNewOIDs(Packet):
    """
    Answer new object IDs. PM -> C.
    """
    def _encode(self, oid_list):
        body = [pack('!H', len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!H', body[:2])
        oid_list = []
        for i in xrange(n):
            oid = unpack('8s', body[2+i*8:10+i*8])[0]
            oid_list.append(oid)
        return (oid_list,)

class AskStoreObject(Packet):
    """
    Ask to store an object. Send an OID, an original serial, a current
    transaction ID, and data. C -> S.
    """
    def _encode(self, oid, serial, compression, checksum, data, tid):
        if serial is None:
            serial = INVALID_TID
        return pack('!8s8s8sBLL', oid, serial, tid, compression,
                          checksum, len(data)) + data

    def _decode(self, body):
        r = unpack('!8s8s8sBL', body[:29])
        oid, serial, tid, compression, checksum = r
        (data, _) = _readString(body, 'data', offset=29)
        return (oid, serial, compression, checksum, data, tid)

class AnswerStoreObject(Packet):
    """
    Answer if an object has been stored. If an object is in conflict,
    a serial of the conflicting transaction is returned. In this case,
    if this serial is newer than the current transaction ID, a client
    node must not try to resolve the conflict. S -> C.
    """
    def _encode(self, conflicting, oid, serial):
        if serial is None:
            serial = INVALID_TID
        return pack('!B8s8s', conflicting, oid, serial)

    def _decode(self, body):
        (conflicting, oid, serial) = unpack('!B8s8s', body)
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
    def _encode(self, tid, user, desc, ext, oid_list):
        lengths = (len(oid_list), len(user), len(desc), len(ext))
        body = [pack('!8sLHHH', tid, *lengths)]
        body.append(user)
        body.append(desc)
        body.append(ext)
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        r = unpack('!8sLHHH', body[:18])
        tid, oid_len, user_len, desc_len, ext_len = r
        body = body[18:]
        user = body[:user_len]
        body = body[user_len:]
        desc = body[:desc_len]
        body = body[desc_len:]
        ext = body[:ext_len]
        body = body[ext_len:]
        oid_list = []
        for i in xrange(oid_len):
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
    def _encode(self, oid, serial, tid):
        tid = _encodeTID(tid)
        serial = _encodeTID(serial) # serial is the previous TID
        return pack('!8s8s8s', oid, serial, tid)

    def _decode(self, body):
        (oid, serial, tid) = unpack('8s8s8s', body)
        if serial == INVALID_TID:
            serial = None
        tid = _decodeTID(tid)
        return (oid, serial, tid)

class AnswerObject(Packet):
    """
    Answer the requested object. S -> C.
    """
    def _encode(self, oid, serial_start, serial_end, compression,
            checksum, data):
        if serial_start is None:
            serial_start = INVALID_TID
        if serial_end is None:
            serial_end = INVALID_TID
        return pack('!8s8s8sBLL', oid, serial_start, serial_end,
                          compression, checksum, len(data)) + data

    def _decode(self, body):
        r = unpack('!8s8s8sBL', body[:29])
        oid, serial_start, serial_end, compression, checksum = r
        if serial_end == INVALID_TID:
            serial_end = None
        (data, _) = _readString(body, 'data', offset=29)
        return (oid, serial_start, serial_end, compression, checksum, data)

class AskTIDs(Packet):
    """
    Ask for TIDs between a range of offsets. The order of TIDs is descending,
    and the range is [first, last). C, S -> S.
    """
    def _encode(self, first, last, partition):
        return pack('!QQL', first, last, partition)

    def _decode(self, body):
        return unpack('!QQL', body) # first, last, partition

class AnswerTIDs(Packet):
    """
    Answer the requested TIDs. S -> C, S.
    """
    def _encode(self, tid_list):
        body = [pack('!L', len(tid_list))]
        body.extend(tid_list)
        return ''.join(body)

    def _decode(self, body):
        (n, ) = unpack('!L', body[:4])
        tid_list = []
        for i in xrange(n):
            tid = unpack('8s', body[4+i*8:12+i*8])[0]
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
    def _encode(self, tid, user, desc, ext, oid_list):
        body = [pack('!8sHHHL', tid, len(user), len(desc), len(ext),
            len(oid_list))]
        body.append(user)
        body.append(desc)
        body.append(ext)
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        r = unpack('!8sHHHL', body[:18])
        tid, user_len, desc_len, ext_len, oid_len = r
        body = body[18:]
        user = body[:user_len]
        body = body[user_len:]
        desc = body[:desc_len]
        body = body[desc_len:]
        ext = body[:ext_len]
        body = body[ext_len:]
        oid_list = []
        for i in xrange(oid_len):
            (oid, ) = unpack('8s', body[:8])
            body = body[8:]
            oid_list.append(oid)
        return (tid, user, desc, ext, oid_list)

class AskObjectHistory(Packet):
    """
    Ask history information for a given object. The order of serials is
    descending, and the range is [first, last]. C, S -> S.
    """
    def _encode(self, oid, first, last):
        return pack('!8sQQ', oid, first, last)

    def _decode(self, body):
        (oid, first, last) = unpack('!8sQQ', body)
        return (oid, first, last)

class AnswerObjectHistory(Packet):
    """
    Answer history information (serial, size) for an object. S -> C, S.
    """
    def _encode(self, oid, history_list):
        body = [pack('!8sL', oid, len(history_list))]
        for serial, size in history_list:
            body.append(pack('!8sL', serial, size))
        return ''.join(body)

    def _decode(self, body):
        (oid, length) = unpack('!8sL', body[:12])
        history_list = []
        for i in xrange(12, 12 + length * 12, 12):
            serial, size = unpack('!8sL', body[i:i+12])
            history_list.append((serial, size))
        return (oid, history_list)

class AskOIDs(Packet):
    """
    Ask for OIDs between a range of offsets. The order of OIDs is descending,
    and the range is [first, last). S -> S.
    """
    def _encode(self, first, last, partition):
        return pack('!QQL', first, last, partition)

    def _decode(self, body):
        return unpack('!QQL', body) # first, last, partition

class AnswerOIDs(Packet):
    """
    Answer the requested OIDs. S -> S.
    """
    def _encode(self, oid_list):
        body = [pack('!L', len(oid_list))]
        body.extend(oid_list)
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!L', body[:4])
        oid_list = []
        for i in xrange(n):
            oid = unpack('8s', body[4+i*8:12+i*8])[0]
            oid_list.append(oid)
        return (oid_list,)

class AskPartitionList(Packet):
    """
    All the following messages are for neoctl to admin node
    Ask information about partition
    """
    def _encode(self, min_offset, max_offset, uuid):
        uuid = _encodeUUID(uuid)
        body = [pack('!LL16s', min_offset, max_offset, uuid)]
        return ''.join(body)

    def _decode(self, body):
        (min_offset, max_offset, uuid) =  unpack('!LL16s', body)
        uuid = _decodeUUID(uuid)
        return (min_offset, max_offset, uuid)

class AnswerPartitionList(Packet):
    """
    Answer information about partition
    """
    def _encode(self, ptid, row_list):
        ptid = _encodePTID(ptid)
        body = [pack('!8sL', ptid, len(row_list))]
        for offset, cell_list in row_list:
            body.append(pack('!LL', offset, len(cell_list)))
            for uuid, state in cell_list:
                uuid = _encodeUUID(uuid)
                body.append(pack('!16sH', uuid, state))
        return ''.join(body)

    def _decode(self, body):
        index = 12
        (ptid, n) = unpack('!8sL', body[:index])
        ptid = _decodePTID(ptid)
        row_list = []
        cell_list = []
        for i in xrange(n):
            offset, m = unpack('!LL', body[index:index+8])
            index += 8
            for j in xrange(m):
                uuid, state = unpack('!16sH', body[index:index+18])
                index += 18
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
    def _encode(self, node_type):
        return ''.join([pack('!H', node_type)])

    def _decode(self, body):
        (node_type, ) = unpack('!H', body)
        node_type = _decodeNodeType(node_type)
        return (node_type,)

class AnswerNodeList(Packet):
    """
    Answer information about nodes
    """
    def _encode(self, node_list):
        body = [pack('!L', len(node_list))]
        for node_type, address, uuid, state in node_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack('!H6s16sH', node_type, address, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!L', body[:4])
        node_list = []
        for i in xrange(n):
            r = unpack('!H6s16sH', body[4+i*26:30+i*26])
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
    def _encode(self, uuid, state, modify_partition_table):
        uuid = _encodeUUID(uuid)
        return ''.join([pack('!16sHB', uuid, state, modify_partition_table)])

    def _decode(self, body):
        (uuid, state, modify) = unpack('!16sHB', body)
        state = _decodeNodeState(state)
        uuid = _decodeUUID(uuid)
        return (uuid, state, modify)

class AnswerNodeState(Packet):
    """
    Answer state of the node
    """
    def _encode(self, uuid, state):
        uuid = _encodeUUID(uuid)
        return ''.join([pack('!16sH', uuid, state)])

    def _decode(self, body):
        (uuid, state) = unpack('!16sH', body)
        state = _decodeNodeState(state)
        uuid = _decodeUUID(uuid)
        return (uuid, state)

class AddPendingNodes(Packet):
    """
    Ask the primary to include some pending node in the partition table
    """
    def _encode(self, uuid_list=()):
        # an empty list means all current pending nodes
        uuid_list = [pack('!16s', _encodeUUID(uuid)) for uuid in uuid_list]
        return pack('!H', len(uuid_list)) + ''.join(uuid_list)

    def _decode(self, body):
        (n, ) = unpack('!H', body[:2])
        uuid_list = [unpack('!16s', body[2+i*16:18+i*16])[0] for i in xrange(n)]
        uuid_list = map(_decodeUUID, uuid_list)
        return (uuid_list, )

class AnswerNewNodes(Packet):
    """
    Anwer what are the nodes added in the partition table
    """
    def _encode(self, uuid_list):
        # an empty list means no new nodes
        uuid_list = [pack('!16s', _encodeUUID(uuid)) for uuid in uuid_list]
        return pack('!H', len(uuid_list)) + ''.join(uuid_list)

    def _decode(self, body):
        (n, ) = unpack('!H', body[:2])
        uuid_list = [unpack('!16s', body[2+i*16:18+i*16])[0] for i in xrange(n)]
        uuid_list = map(_decodeUUID, uuid_list)
        return (uuid_list, )

class NotifyNodeInformation(Packet):
    """
    Notify information about one or more nodes. PM -> Any.
    """
    def _encode(self, node_list):
        body = [pack('!L', len(node_list))]
        for node_type, address, uuid, state in node_list:
            uuid = _encodeUUID(uuid)
            address = _encodeAddress(address)
            body.append(pack('!H6s16sH', node_type, address, uuid, state))
        return ''.join(body)

    def _decode(self, body):
        (n,) = unpack('!L', body[:4])
        node_list = []
        for i in xrange(n):
            r = unpack('!H6s16sH', body[4+i*26:30+i*26])
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
    def _encode(self, state):
        return pack('!H', state)

    def _decode(self, body):
        (state, ) = unpack('!H', body[:2])
        state = _decodeClusterState(state)
        return (state, )

class NotifyClusterInformation(Packet):
    """
    Notify information about the cluster
    """
    def _encode(self, state):
        return pack('!H', state)

    def _decode(self, body):
        (state, ) = unpack('!H', body)
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
    def _encode(self, state):
        return pack('!H', state)

    def _decode(self, body):
        (state, ) = unpack('!H', body)
        state = _decodeClusterState(state)
        return (state, )

class NotifyLastOID(Packet):
    """
    Notify last OID generated
    """
    def _decode(self, body):
        (loid, ) = unpack('8s', body)
        return (loid, )

class Error(Packet):
    """
    Error is a special type of message, because this can be sent against
    any other message, even if such a message does not expect a reply
    usually. Any -> Any.
    """
    def _encode(self, code, message):
        return pack('!HL', code, len(message)) + message

    def _decode(self, body):
        (code, ) = unpack('!H', body[:2])
        code = _decodeErrorCode(code)
        (message, _) = _readString(body, 'message', offset=2)
        return (code, message)


StaticRegistry = {}
def register(code, cls):
    """ Register a packet in the packet registry """
    assert code not in StaticRegistry, "Duplicate packet code"
    cls._code = code
    StaticRegistry[code] = cls
    return cls


class PacketRegistry(dict):
    """
    Packet registry that check packet code unicity and provide an index
    """

    def __init__(self):
        dict.__init__(self)
        # load packet classes
        self.update(StaticRegistry)

    def parse(self, msg):
        if len(msg) < MIN_PACKET_SIZE:
            return None
        msg_id, msg_type, msg_len = unpack('!LHL', msg[:PACKET_HEADER_SIZE])
        try:
            packet_klass = self[msg_type]
        except KeyError:
            raise PacketMalformedError('Unknown packet type')
        if msg_len > MAX_PACKET_SIZE:
            raise PacketMalformedError('message too big (%d)' % msg_len)
        if msg_len < MIN_PACKET_SIZE:
            raise PacketMalformedError('message too small (%d)' % msg_len)
        if len(msg) < msg_len:
            # Not enough.
            return None
        packet = packet_klass()
        packet.setContent(msg_id, msg[PACKET_HEADER_SIZE:msg_len])
        return packet

    # packets registration
    Error = register(0x8000, Error)
    Ping = register(0x0001, Ping)
    Pong = register(0x8001, Pong)
    RequestIdentification = register(0x0002, RequestIdentification)
    AcceptIdentification = register(0x8002, AcceptIdentification)
    AskPrimary = register(0x0003, AskPrimary)
    AnswerPrimary = register(0x8003, AnswerPrimary)
    AnnouncePrimary = register(0x0004, AnnouncePrimary)
    ReelectPrimary = register(0x0005, ReelectPrimary)
    NotifyNodeInformation = register(0x0006, NotifyNodeInformation)
    AskLastIDs = register(0x0007, AskLastIDs)
    AnswerLastIDs = register(0x8007, AnswerLastIDs)
    AskPartitionTable = register(0x0008, AskPartitionTable)
    AnswerPartitionTable = register(0x8008, AnswerPartitionTable)
    SendPartitionTable = register(0x0009, SendPartitionTable)
    NotifyPartitionChanges = register(0x000A, NotifyPartitionChanges)
    StartOperation = register(0x000B, StartOperation)
    StopOperation = register(0x000C, StopOperation)
    AskUnfinishedTransactions = register(0x000D, AskUnfinishedTransactions)
    AnswerUnfinishedTransactions = register(0x800d,
            AnswerUnfinishedTransactions)
    AskObjectPresent = register(0x000f, AskObjectPresent)
    AnswerObjectPresent = register(0x800f, AnswerObjectPresent)
    DeleteTransaction = register(0x0010, DeleteTransaction)
    CommitTransaction = register(0x0011, CommitTransaction)
    AskBeginTransaction = register(0x0012, AskBeginTransaction)
    AnswerBeginTransaction = register(0x8012, AnswerBeginTransaction)
    FinishTransaction = register(0x0013, FinishTransaction)
    AnswerTransactionFinished = register(0x8013, AnswerTransactionFinished)
    LockInformation = register(0x0014, LockInformation)
    NotifyInformationLocked = register(0x8014, NotifyInformationLocked)
    InvalidateObjects = register(0x0015, InvalidateObjects)
    NotifyUnlockInformation = register(0x0016, NotifyUnlockInformation)
    AskNewOIDs = register(0x0017, AskNewOIDs)
    AnswerNewOIDs = register(0x8017, AnswerNewOIDs)
    AskStoreObject = register(0x0018, AskStoreObject)
    AnswerStoreObject = register(0x8018, AnswerStoreObject)
    AbortTransaction = register(0x0019, AbortTransaction)
    AskStoreTransaction = register(0x001A, AskStoreTransaction)
    AnswerStoreTransaction = register(0x801A, AnswerStoreTransaction)
    AskObject = register(0x001B, AskObject)
    AnswerObject = register(0x801B, AnswerObject)
    AskTIDs = register(0x001C, AskTIDs)
    AnswerTIDs = register(0x801D, AnswerTIDs)
    AskTransactionInformation = register(0x001E, AskTransactionInformation)
    AnswerTransactionInformation = register(0x801E,
            AnswerTransactionInformation)
    AskObjectHistory = register(0x001F, AskObjectHistory)
    AnswerObjectHistory = register(0x801F, AnswerObjectHistory)
    AskOIDs = register(0x0020, AskOIDs)
    AnswerOIDs = register(0x8020, AnswerOIDs)
    AskPartitionList = register(0x0021, AskPartitionList)
    AnswerPartitionList = register(0x8021, AnswerPartitionList)
    AskNodeList = register(0x0022, AskNodeList)
    AnswerNodeList = register(0x8022, AnswerNodeList)
    SetNodeState = register(0x0023, SetNodeState)
    AnswerNodeState = register(0x8023, AnswerNodeState)
    AddPendingNodes = register(0x0024, AddPendingNodes)
    AnswerNewNodes = register(0x8024, AnswerNewNodes)
    AskNodeInformation = register(0x0025, AskNodeInformation)
    AnswerNodeInformation = register(0x8025, AnswerNodeInformation)
    SetClusterState = register(0x0026, SetClusterState)
    NotifyClusterInformation = register(0x0027, NotifyClusterInformation)
    AskClusterState = register(0x0028, AskClusterState)
    AnswerClusterState = register(0x8028, AnswerClusterState)
    NotifyLastOID = register(0x0030, NotifyLastOID)
    NotifyReplicationDone = register(0x0031, NotifyReplicationDone)

# build a "singleton"
Packets = PacketRegistry()


def _error(error_code, error_message):
    return Error(error_code, error_message)

def ack(message):
    return _error(ErrorCodes.ACK, message)

def protocolError(error_message):
    return _error(ErrorCodes.PROTOCOL_ERROR, 'protocol error: ' + error_message)

def notReady(error_message):
    return _error(ErrorCodes.NOT_READY, 'not ready: ' + error_message)

def brokenNodeDisallowedError(error_message):
    return _error(ErrorCodes.BROKEN_NODE,
                      'broken node disallowed error: ' + error_message)

def oidNotFound(error_message):
    return _error(ErrorCodes.OID_NOT_FOUND, 'oid not found: ' + error_message)

def tidNotFound(error_message):
    return _error(ErrorCodes.TID_NOT_FOUND, 'tid not found: ' + error_message)

