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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from struct import pack, unpack
from socket import inet_ntoa, inet_aton
import logging

from neo.util import dump

class EnumItem(int):
    """
      Enumerated value type.
      Not to be used outside of Enum class.
    """
    def __new__(cls, enum, name, value):
        instance = super(EnumItem, cls).__new__(cls, value)
        instance.enum = enum
        instance.name = name
        return instance

    def __eq__(self, other):
        """
          Raise if compared type doesn't match.
        """
        if not isinstance(other, EnumItem):
            raise TypeError, 'Comparing an enum with an int.'
        if other.enum is not self.enum:
            raise TypeError, 'Comparing enums of incompatible types: %s ' \
                             'and %s' % (self, other)
        return int(other) == int(self)

    def __ne__(self, other):
        return not(self == other)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<EnumItem %r (%r) of %r>' % (self.name, int(self), self.enum)

class Enum(object):
    """
      C-style enumerated type support with extended typechecking.
      Instantiate with a dict whose keys are variable names and values are
      the value of that variable.
      Variables are added to module's globals and can be used directly.

      The purpose of this class is purely to prevent developper from
      mistakenly comparing an enumerated value with a value from another enum,
      or even not from any enum at all.
    """
    def __init__(self, value_dict):
        global_dict = globals()
        self.enum_dict = enum_dict = {}
        for key, value in value_dict.iteritems():
            # Only integer types are supported. This should be enough, and
            # extending support to other types would only make moving to other
            # languages harder.
            if not isinstance(value, int):
                raise TypeError, 'Enum class only support integer values.'
            item = EnumItem(self, key, value)
            global_dict[key] = enum_dict[value] = item

    def get(self, value, default=None):
        return self.enum_dict.get(value, default)

    def __getitem__(self, value):
        return self.enum_dict[value]

# The protocol version (major, minor).
PROTOCOL_VERSION = (4, 0)

# Size restrictions.
MIN_PACKET_SIZE = 10
MAX_PACKET_SIZE = 0x100000
PACKET_HEADER_SIZE = 10
# Message types.

packet_types = Enum({

    # Error is a special type of message, because this can be sent against any other message,
    # even if such a message does not expect a reply usually. Any -> Any.
    'ERROR': 0x8000,

    # Check if a peer is still alive. Any -> Any.
    'PING': 0x0001,

    # Notify being alive. Any -> Any.
    'PONG': 0x8001,

    # Request a node identification. This must be the first packet for any connection.
    # Any -> Any.
    'REQUEST_NODE_IDENTIFICATION': 0x0002,

    # Accept a node identification. This should be a reply to Request Node Identification.
    # Any -> Any.
    'ACCEPT_NODE_IDENTIFICATION': 0x8002,

    # Ask a current primary master node. This must be the second message when connecting
    # to a master node. Any -> M.
    'ASK_PRIMARY_MASTER': 0x0003,

    # Reply to Ask Primary Master. This message includes a list of known master nodes,
    # to make sure that a peer has the same information. M -> Any.
    'ANSWER_PRIMARY_MASTER': 0x8003,

    # Announce a primary master node election. PM -> SM.
    'ANNOUNCE_PRIMARY_MASTER': 0x0004,

    # Force a re-election of a primary master node. M -> M.
    'REELECT_PRIMARY_MASTER': 0x0005,

    # Notify information about one or more nodes. Any -> PM, PM -> Any.
    'NOTIFY_NODE_INFORMATION': 0x0006,

    # Ask the last OID, the last TID and the last Partition Table ID that
    # a storage node stores. Used to recover information. PM -> S, S -> PM.
    'ASK_LAST_IDS': 0x0007,

    # Reply to Ask Last IDs. S -> PM, PM -> S.
    'ANSWER_LAST_IDS': 0x8007,

    # Ask rows in a partition table that a storage node stores. Used to recover
    # information. PM -> S.
    'ASK_PARTITION_TABLE': 0x0008,

    # Answer rows in a partition table. S -> PM.
    'ANSWER_PARTITION_TABLE': 0x8008,

    # Send rows in a partition table to update other nodes. PM -> S, C.
    'SEND_PARTITION_TABLE': 0x0009,

    # Notify a subset of a partition table. This is used to notify changes. PM -> S, C.
    'NOTIFY_PARTITION_CHANGES': 0x000a,

    # Tell a storage nodes to start an operation. Until a storage node receives this
    # message, it must not serve client nodes. PM -> S.
    'START_OPERATION': 0x000b,

    # Tell a storage node to stop an operation. Once a storage node receives this message,
    # it must not serve client nodes. PM -> S.
    'STOP_OPERATION': 0x000c,

    # Ask unfinished transactions' IDs. PM -> S.
    'ASK_UNFINISHED_TRANSACTIONS': 0x000d,

    # Answer unfinished transactions' IDs. S -> PM.
    'ANSWER_UNFINISHED_TRANSACTIONS': 0x800d,

    # Ask if an object is present. If not present, OID_NOT_FOUND should be returned. PM -> S.
    'ASK_OBJECT_PRESENT': 0x000f,

    # Answer that an object is present. PM -> S.
    'ANSWER_OBJECT_PRESENT': 0x800f,

    # Delete a transaction. PM -> S.
    'DELETE_TRANSACTION': 0x0010,

    # Commit a transaction. PM -> S.
    'COMMIT_TRANSACTION': 0x0011,

    # Ask a new transaction ID. C -> PM.
    'ASK_NEW_TID': 0x0012,

    # Answer a new transaction ID. PM -> C.
    'ANSWER_NEW_TID': 0x8012,

    # Finish a transaction. C -> PM.
    'FINISH_TRANSACTION': 0x0013,

    # Notify a transaction finished. PM -> C.
    'NOTIFY_TRANSACTION_FINISHED': 0x8013,

    # Lock information on a transaction. PM -> S.
    'LOCK_INFORMATION': 0x0014,

    # Notify information on a transaction locked. S -> PM.
    'NOTIFY_INFORMATION_LOCKED': 0x8014,

    # Invalidate objects. PM -> C.
    'INVALIDATE_OBJECTS': 0x0015,

    # Unlock information on a transaction. PM -> S.
    'UNLOCK_INFORMATION': 0x0016,

    # Ask new object IDs. C -> PM.
    'ASK_NEW_OIDS': 0x0017,

    # Answer new object IDs. PM -> C.
    'ANSWER_NEW_OIDS': 0x8017,

    # Ask to store an object. Send an OID, an original serial, a current
    # transaction ID, and data. C -> S.
    'ASK_STORE_OBJECT': 0x0018,

    # Answer if an object has been stored. If an object is in conflict,
    # a serial of the conflicting transaction is returned. In this case,
    # if this serial is newer than the current transaction ID, a client
    # node must not try to resolve the conflict. S -> C.
    'ANSWER_STORE_OBJECT': 0x8018,

    # Abort a transaction. C -> S, PM.
    'ABORT_TRANSACTION': 0x0019,

    # Ask to store a transaction. C -> S.
    'ASK_STORE_TRANSACTION': 0x001a,

    # Answer if transaction has been stored. S -> C.
    'ANSWER_STORE_TRANSACTION': 0x801a,

    # Ask a stored object by its OID and a serial or a TID if given. If a serial
    # is specified, the specified revision of an object will be returned. If
    # a TID is specified, an object right before the TID will be returned. C -> S.
    'ASK_OBJECT': 0x001b,

    # Answer the requested object. S -> C.
    'ANSWER_OBJECT': 0x801b,

    # Ask for TIDs between a range of offsets. The order of TIDs is descending,
    # and the range is [first, last). C, S -> S.
    'ASK_TIDS': 0x001d,

    # Answer the requested TIDs. S -> C, S.
    'ANSWER_TIDS': 0x801d,

    # Ask information about a transaction. Any -> S.
    'ASK_TRANSACTION_INFORMATION': 0x001e,

    # Answer information (user, description) about a transaction. S -> Any.
    'ANSWER_TRANSACTION_INFORMATION': 0x801e,

    # Ask history information for a given object. The order of serials is
    # descending, and the range is [first, last]. C, S -> S.
    'ASK_OBJECT_HISTORY': 0x001f,

    # Answer history information (serial, size) for an object. S -> C, S.
    'ANSWER_OBJECT_HISTORY': 0x801f,

    # Ask for OIDs between a range of offsets. The order of OIDs is descending,
    # and the range is [first, last). S -> S.
    'ASK_OIDS': 0x0020,

    # Answer the requested OIDs. S -> S.
    'ANSWER_OIDS': 0x8020,
})

# Error codes.
NOT_READY_CODE = 1
OID_NOT_FOUND_CODE = 2
SERIAL_NOT_FOUND_CODE = 3
TID_NOT_FOUND_CODE = 4
PROTOCOL_ERROR_CODE = 5
TIMEOUT_ERROR_CODE = 6
BROKEN_NODE_DISALLOWED_CODE = 7
INTERNAL_ERROR_CODE = 8

# Node types.
MASTER_NODE_TYPE = 1
STORAGE_NODE_TYPE = 2
CLIENT_NODE_TYPE = 3
ADMIN_NODE_TYPE = 4

VALID_NODE_TYPE_LIST = (MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, ADMIN_NODE_TYPE)

# Node states.
node_states = Enum({
'RUNNING_STATE': 0,
'TEMPORARILY_DOWN_STATE': 1,
'DOWN_STATE': 2,
'BROKEN_STATE': 3,
})

VALID_NODE_STATE_LIST = (RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE)

# Partition cell states.
partition_cell_states = Enum({
'UP_TO_DATE_STATE': 0,
'OUT_OF_DATE_STATE': 1,
'FEEDING_STATE': 2,
'DISCARDED_STATE': 3,
})

VALID_CELL_STATE_LIST = (UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE,
                         DISCARDED_STATE)

# Other constants.
INVALID_UUID = '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
INVALID_TID = '\0\0\0\0\0\0\0\0'
INVALID_SERIAL = '\0\0\0\0\0\0\0\0'
INVALID_OID = '\0\0\0\0\0\0\0\0'
INVALID_PTID = '\0\0\0\0\0\0\0\0'
INVALID_PARTITION = 0xffffffff

STORAGE_NS = 'S'
MASTER_NS = 'M'
CLIENT_NS = 'C'
ADMIN_NS = 'A'

UUID_NAMESPACES = { 
    STORAGE_NODE_TYPE: STORAGE_NS,
    MASTER_NODE_TYPE: MASTER_NS,
    CLIENT_NODE_TYPE: CLIENT_NS,
    ADMIN_NODE_TYPE: ADMIN_NS,
}

class ProtocolError(Exception): pass

class Packet(object):
    """A packet."""

    _id = None
    _type = None
    _len = None

    @classmethod
    def parse(cls, msg):
        # logging.debug('parsing %s', dump(msg))
        if len(msg) < MIN_PACKET_SIZE:
            return None
        msg_id, msg_type, msg_len = unpack('!LHL', msg[:PACKET_HEADER_SIZE])
        try:
            msg_type = packet_types[msg_type]
        except KeyError:
            raise ProtocolError(cls(msg_id, msg_type), 
                                'Unknown packet type')
        if msg_len > MAX_PACKET_SIZE:
            raise ProtocolError(cls(msg_id, msg_type),
                                'message too big (%d)' % msg_len)
        if msg_len < MIN_PACKET_SIZE:
            raise ProtocolError(cls(msg_id, msg_type),
                                'message too small (%d)' % msg_len)
        if len(msg) < msg_len:
            # Not enough.
            return None
        packet = cls(msg_type, msg[PACKET_HEADER_SIZE:msg_len])
        packet.setId(msg_id)
        return packet

    def __init__(self, msg_type, body=''):
        self._id = None
        self._type = msg_type
        self._body = body

    def getId(self):
        return self._id

    def setId(self, id):
        self._id = id

    def getType(self):
        return self._type

    def __len__(self):
        try:
            return PACKET_HEADER_SIZE + len(self._body)
        except TypeError:
            return PACKET_HEADER_SIZE

    # Encoders.
    def encode(self):
        msg = pack('!LHL', self._id, self._type, PACKET_HEADER_SIZE + len(self._body)) + self._body
        if len(msg) > MAX_PACKET_SIZE:
            raise ProtocolError('message too big (%d)' % len(msg))
        return msg

    __str__ = encode


    # Decoders.
    def decode(self):
        try:
            method = self.decode_table[self._type]
        except KeyError:
            raise ProtocolError(self, 'unknown message type 0x%x' % self._type)
        return method(self)

    decode_table = {}

    def _decodeError(self):
        try:
            body = self._body
            code, size = unpack('!HL', body[:6])
            message = body[6:]
        except:
            raise ProtocolError(self, 'invalid error message')
        if len(message) != size:
            raise ProtocolError(self, 'invalid error message size')
        return code, message
    decode_table[ERROR] = _decodeError

    def _decodePing(self):
        pass
    decode_table[PING] = _decodePing

    def _decodePong(self):
        pass
    decode_table[PONG] = _decodePong

    def _decodeRequestNodeIdentification(self):
        try:
            body = self._body
            major, minor, node_type, uuid, ip_address, port, size \
                    = unpack('!LLH16s4sHL', body[:36])
            ip_address = inet_ntoa(ip_address)
            name = body[36:]
        except:
            raise ProtocolError(self, 'invalid request node identification')
        if size != len(name):
            raise ProtocolError(self, 'invalid name size')
        if node_type not in VALID_NODE_TYPE_LIST:
            raise ProtocolError(self, 'invalid node type %d' % node_type)
        if (major, minor) != PROTOCOL_VERSION:
            raise ProtocolError(self, 'protocol version mismatch')
        return node_type, uuid, ip_address, port, name
    decode_table[REQUEST_NODE_IDENTIFICATION] = _decodeRequestNodeIdentification

    def _decodeAcceptNodeIdentification(self):
        try:
            node_type, uuid, ip_address, port, num_partitions, num_replicas, your_uuid \
                    = unpack('!H16s4sHLL16s', self._body)
            ip_address = inet_ntoa(ip_address)
        except:
            raise ProtocolError(self, 'invalid accept node identification')
        if node_type not in VALID_NODE_TYPE_LIST:
            raise ProtocolError(self, 'invalid node type %d' % node_type)
        return node_type, uuid, ip_address, port, num_partitions, num_replicas, your_uuid
    decode_table[ACCEPT_NODE_IDENTIFICATION] = _decodeAcceptNodeIdentification

    def _decodeAskPrimaryMaster(self):
        pass
    decode_table[ASK_PRIMARY_MASTER] = _decodeAskPrimaryMaster

    def _decodeAnswerPrimaryMaster(self):
        try:
            primary_uuid, n = unpack('!16sL', self._body[:20])
            known_master_list = []
            for i in xrange(n):
                ip_address, port, uuid = unpack('!4sH16s', self._body[20+i*22:42+i*22])
                ip_address = inet_ntoa(ip_address)
                known_master_list.append((ip_address, port, uuid))
        except:
            raise ProtocolError(self, 'invalid answer primary master')
        return primary_uuid, known_master_list
    decode_table[ANSWER_PRIMARY_MASTER] = _decodeAnswerPrimaryMaster

    def _decodeAnnouncePrimaryMaster(self):
        pass
    decode_table[ANNOUNCE_PRIMARY_MASTER] = _decodeAnnouncePrimaryMaster

    def _decodeReelectPrimaryMaster(self):
        pass
    decode_table[REELECT_PRIMARY_MASTER] = _decodeReelectPrimaryMaster

    def _decodeNotifyNodeInformation(self):
        try:
            n = unpack('!L', self._body[:4])[0]
            node_list = []
            for i in xrange(n):
                r = unpack('!H4sH16sH', self._body[4+i*26:30+i*26])
                node_type, ip_address, port, uuid, state = r
                ip_address = inet_ntoa(ip_address)
                if node_type not in VALID_NODE_TYPE_LIST:
                    raise ProtocolError(self, 'invalid node type %d' % node_type)
                state = node_states.get(state)
                if state not in VALID_NODE_STATE_LIST:
                    raise ProtocolError(self, 'invalid node state %d' % state)
                node_list.append((node_type, ip_address, port, uuid, state))
        except ProtocolError:
            raise
        except:
            raise ProtocolError(self, 'invalid answer node information')
        return (node_list,)
    decode_table[NOTIFY_NODE_INFORMATION] = _decodeNotifyNodeInformation

    def _decodeAskLastIDs(self):
        pass
    decode_table[ASK_LAST_IDS] = _decodeAskLastIDs

    def _decodeAnswerLastIDs(self):
        try:
            loid, ltid, lptid = unpack('!8s8s8s', self._body)
        except:
            raise ProtocolError(self, 'invalid answer last ids')
        return loid, ltid, lptid
    decode_table[ANSWER_LAST_IDS] = _decodeAnswerLastIDs

    def _decodeAskPartitionTable(self):
        try:
            n = unpack('!L', self._body[:4])[0]
            offset_list = []
            for i in xrange(n):
                offset = unpack('!L', self._body[4+i*4:8+i*4])[0]
                offset_list.append(offset)
        except:
            raise ProtocolError(self, 'invalid ask partition table')
        return (offset_list,)
    decode_table[ASK_PARTITION_TABLE] = _decodeAskPartitionTable

    def _decodeAnswerPartitionTable(self):
        try:
            ptid, n = unpack('!8sL', self._body[:12])
            index = 12
            row_list = []
            cell_list = []
            for i in xrange(n):
                offset, m = unpack('!LL', self._body[index:index+8])
                index += 8
                for j in xrange(m):
                    uuid, state = unpack('!16sH', self._body[index:index+18])
                    index += 18
                    state = partition_cell_states.get(state)
                    cell_list.append((uuid, state))
                row_list.append((offset, tuple(cell_list)))
                del cell_list[:]
        except:
            raise ProtocolError(self, 'invalid answer partition table')
        return ptid, row_list
    decode_table[ANSWER_PARTITION_TABLE] = _decodeAnswerPartitionTable

    def _decodeSendPartitionTable(self):
        try:
            ptid, n = unpack('!8sL', self._body[:12])
            index = 12
            row_list = []
            cell_list = []
            for i in xrange(n):
                offset, m = unpack('!LL', self._body[index:index+8])
                index += 8
                for j in xrange(m):
                    uuid, state = unpack('!16sH', self._body[index:index+18])
                    index += 18
                    state = partition_cell_states.get(state)
                    cell_list.append((uuid, state))
                row_list.append((offset, tuple(cell_list)))
                del cell_list[:]
        except:
            raise ProtocolError(self, 'invalid send partition table')
        return ptid, row_list
    decode_table[SEND_PARTITION_TABLE] = _decodeSendPartitionTable

    def _decodeNotifyPartitionChanges(self):
        try:
            ptid, n = unpack('!8sL', self._body[:12])
            cell_list = []
            for i in xrange(n):
                offset, uuid, state = unpack('!L16sH', self._body[12+i*22:34+i*22])
                state = partition_cell_states.get(state)
                cell_list.append((offset, uuid, state))
        except:
            raise ProtocolError(self, 'invalid notify partition changes')
        return ptid, cell_list
    decode_table[NOTIFY_PARTITION_CHANGES] = _decodeNotifyPartitionChanges

    def _decodeStartOperation(self):
        pass
    decode_table[START_OPERATION] = _decodeStartOperation

    def _decodeStopOperation(self):
        pass
    decode_table[STOP_OPERATION] = _decodeStopOperation

    def _decodeAskUnfinishedTransactions(self):
        pass
    decode_table[ASK_UNFINISHED_TRANSACTIONS] = _decodeAskUnfinishedTransactions

    def _decodeAnswerUnfinishedTransactions(self):
        try:
            n = unpack('!L', self._body[:4])[0]
            tid_list = []
            for i in xrange(n):
                tid = unpack('8s', self._body[4+i*8:12+i*8])[0]
                tid_list.append(tid)
        except:
            raise ProtocolError(self, 'invalid answer unfinished transactions')
        return (tid_list,)
    decode_table[ANSWER_UNFINISHED_TRANSACTIONS] = _decodeAnswerUnfinishedTransactions

    def _decodeAskObjectPresent(self):
        try:
            oid, tid = unpack('8s8s', self._body)
        except:
            raise ProtocolError(self, 'invalid ask object present')
        return oid, tid
    decode_table[ASK_OBJECT_PRESENT] = _decodeAskObjectPresent

    def _decodeAnswerObjectPresent(self):
        try:
            oid, tid = unpack('8s8s', self._body)
        except:
            raise ProtocolError(self, 'invalid answer object present')
        return oid, tid
    decode_table[ANSWER_OBJECT_PRESENT] = _decodeAnswerObjectPresent

    def _decodeDeleteTransaction(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid delete transaction')
        return (tid,)
    decode_table[DELETE_TRANSACTION] = _decodeDeleteTransaction

    def _decodeCommitTransaction(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid commit transaction')
        return (tid,)
    decode_table[COMMIT_TRANSACTION] = _decodeCommitTransaction

    def _decodeAskNewTID(self):
        pass
    decode_table[ASK_NEW_TID] = _decodeAskNewTID

    def _decodeAnswerNewTID(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid answer new tid')
        return (tid,)
    decode_table[ANSWER_NEW_TID] = _decodeAnswerNewTID

    def _decodeAskNewOIDs(self):
        try:
            num_oids = unpack('!H', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid ask new oids')
        return (num_oids,)
    decode_table[ASK_NEW_OIDS] = _decodeAskNewOIDs

    def _decodeAnswerNewOIDs(self):
        try:
            n = unpack('!H', self._body[:2])[0]
            oid_list = []
            for i in xrange(n):
                oid = unpack('8s', self._body[2+i*8:10+i*8])[0]
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid answer new oids')
        return (oid_list,)
    decode_table[ANSWER_NEW_OIDS] = _decodeAnswerNewOIDs

    def _decodeFinishTransaction(self):
        try:
            tid, n = unpack('!8sL', self._body[:12])
            oid_list = []
            for i in xrange(n):
                oid = unpack('8s', self._body[12+i*8:20+i*8])[0]
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid finish transaction')
        return oid_list, tid
    decode_table[FINISH_TRANSACTION] = _decodeFinishTransaction

    def _decodeNotifyTransactionFinished(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid notify transactin finished')
        return (tid,)
    decode_table[NOTIFY_TRANSACTION_FINISHED] = _decodeNotifyTransactionFinished

    def _decodeLockInformation(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid lock information')
        return (tid,)
    decode_table[LOCK_INFORMATION] = _decodeLockInformation

    def _decodeNotifyInformationLocked(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid notify information locked')
        return (tid,)
    decode_table[NOTIFY_INFORMATION_LOCKED] = _decodeNotifyInformationLocked

    def _decodeInvalidateObjects(self):
        try:
            tid, n = unpack('!8sL', self._body[:12])
            oid_list = []
            for i in xrange(12, 12 + n * 8, 8):
                oid = unpack('8s', self._body[i:i+8])[0]
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid finish transaction')
        return oid_list, tid
    decode_table[INVALIDATE_OBJECTS] = _decodeInvalidateObjects

    def _decodeUnlockInformation(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid unlock information')
        return (tid,)
    decode_table[UNLOCK_INFORMATION] = _decodeUnlockInformation

    def _decodeAbortTransaction(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid abort transaction')
        return (tid,)
    decode_table[ABORT_TRANSACTION] = _decodeAbortTransaction

    def _decodeAskStoreObject(self):
        try:
            oid, serial, tid, compression, checksum, data_len \
                 = unpack('!8s8s8sBLL', self._body[:33])
            data = self._body[33:]
        except:
            raise ProtocolError(self, 'invalid ask store object')
        if data_len != len(data):
            raise ProtocolError(self, 'invalid data size')
        return oid, serial, compression, checksum, data, tid
    decode_table[ASK_STORE_OBJECT] = _decodeAskStoreObject

    def _decodeAnswerStoreObject(self):
        try:
            conflicting, oid, serial = unpack('!B8s8s', self._body)
        except:
            raise ProtocolError(self, 'invalid answer store object')
        return conflicting, oid, serial
    decode_table[ANSWER_STORE_OBJECT] = _decodeAnswerStoreObject

    def _decodeAskStoreTransaction(self):
        try:
            tid, oid_len, user_len, desc_len, ext_len \
                    = unpack('!8sLHHH', self._body[:18])
            offset = 18
            user = self._body[offset:offset+user_len]
            offset += user_len
            desc = self._body[offset:offset+desc_len]
            offset += desc_len
            ext = self._body[offset:offset+ext_len]
            offset += ext_len
            oid_list = []
            for i in xrange(oid_len):
                oid = unpack('8s', self._body[offset:offset+8])[0]
                offset += 8
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid ask store transaction')
        return tid, user, desc, ext, oid_list
    decode_table[ASK_STORE_TRANSACTION] = _decodeAskStoreTransaction

    def _decodeAnswerStoreTransaction(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid answer store transaction')
        return (tid,)
    decode_table[ANSWER_STORE_TRANSACTION] = _decodeAnswerStoreTransaction

    def _decodeAskObject(self):
        try:
            oid, serial, tid = unpack('8s8s8s', self._body)
        except:
            raise ProtocolError(self, 'invalid ask object')
        return oid, serial, tid
    decode_table[ASK_OBJECT] = _decodeAskObject

    def _decodeAnswerObject(self):
        try:
            oid, serial_start, serial_end, compression, checksum, data_len \
                 = unpack('!8s8s8sBLL', self._body[:33])
            data = self._body[33:]
        except:
            raise ProtocolError(self, 'invalid answer object')
        if len(data) != data_len:
            raise ProtocolError(self, 'invalid data size')
        return oid, serial_start, serial_end, compression, checksum, data
    decode_table[ANSWER_OBJECT] = _decodeAnswerObject

    def _decodeAskTIDs(self):
        try:
            first, last, partition = unpack('!QQL', self._body)
        except:
            raise ProtocolError(self, 'invalid ask tids')
        return first, last, partition
    decode_table[ASK_TIDS] = _decodeAskTIDs

    def _decodeAnswerTIDs(self):
        try:
            n = unpack('!L', self._body[:4])[0]
            tid_list = []
            for i in xrange(n):
                tid = unpack('8s', self._body[4+i*8:12+i*8])[0]
                tid_list.append(tid)
        except:
            raise ProtocolError(self, 'invalid answer tids')
        return (tid_list,)
    decode_table[ANSWER_TIDS] = _decodeAnswerTIDs

    def _decodeAskTransactionInformation(self):
        try:
            tid = unpack('8s', self._body)[0]
        except:
            raise ProtocolError(self, 'invalid ask transaction information')
        return (tid,)
    decode_table[ASK_TRANSACTION_INFORMATION] = _decodeAskTransactionInformation

    def _decodeAnswerTransactionInformation(self):
        try:
            tid, user_len, desc_len, ext_len, oid_len \
                    = unpack('!8sHHHL', self._body[:18])
            offset = 18
            user = self._body[offset:offset+user_len]
            offset += user_len
            desc = self._body[offset:offset+desc_len]
            offset += desc_len
            ext = self._body[offset:offset+ext_len]
            offset += ext_len
            oid_list = []
            for i in xrange(oid_len):
                oid = unpack('8s', self._body[offset+i*8:offset+8+i*8])[0]
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid answer transaction information')
        return tid, user, desc, ext, oid_list
    decode_table[ANSWER_TRANSACTION_INFORMATION] = _decodeAnswerTransactionInformation

    def _decodeAskObjectHistory(self):
        try:
            oid, first, last = unpack('!8sQQ', self._body)
        except:
            raise ProtocolError(self, 'invalid ask object history')
        return oid, first, last
    decode_table[ASK_OBJECT_HISTORY] = _decodeAskObjectHistory

    def _decodeAnswerObjectHistory(self):
        try:
            oid, length = unpack('!8sL', self._body[:12])
            history_list = []
            for i in xrange(12, 12 + length * 12, 12):
                serial, size = unpack('!8sL', self._body[i:i+12])
                history_list.append((serial, size))
        except:
            raise ProtocolError(self, 'invalid answer object history')
        return oid, history_list
    decode_table[ANSWER_OBJECT_HISTORY] = _decodeAnswerObjectHistory

    def _decodeAskOIDs(self):
        try:
            first, last, partition = unpack('!QQL', self._body)
        except:
            raise ProtocolError(self, 'invalid ask oids')
        return first, last, partition
    decode_table[ASK_OIDS] = _decodeAskOIDs

    def _decodeAnswerOIDs(self):
        try:
            n = unpack('!L', self._body[:4])[0]
            oid_list = []
            for i in xrange(n):
                oid = unpack('8s', self._body[4+i*8:12+i*8])[0]
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid answer oids')
        return (oid_list,)
    decode_table[ANSWER_OIDS] = _decodeAnswerOIDs

# Packet constructors

def _error(error_code, error_message):
    body = pack('!HL', error_code, len(error_message)) + error_message
    return Packet(ERROR, body)

def protocolError(error_message):
    return _error(PROTOCOL_ERROR_CODE, 'protocol error: ' + error_message)

def internalError(error_message):
    return _error(INTERNAL_ERROR_CODE, 'internal error: ' + error_message)

def notReady(error_message):
    return _error(NOT_READY_CODE, 'not ready: ' + error_message)

def brokenNodeDisallowedError(error_message):
    return _error(BROKEN_NODE_DISALLOWED_CODE,
                      'broken node disallowed error: ' + error_message)

def oidNotFound(error_message):
    return _error(OID_NOT_FOUND_CODE, 'oid not found: ' + error_message)

def tidNotFound(error_message):
    return _error(TID_NOT_FOUND_CODE, 'tid not found: ' + error_message)

def ping():
    return Packet(PING)

def pong():
    return Packet(PONG)

def requestNodeIdentification(node_type, uuid, ip_address, port, name):
    body = pack('!LLH16s4sHL', PROTOCOL_VERSION[0], PROTOCOL_VERSION[1],
                      node_type, uuid, inet_aton(ip_address), port, len(name)) + name
    return Packet(REQUEST_NODE_IDENTIFICATION, body)

def acceptNodeIdentification(node_type, uuid, ip_address,
         port, num_partitions, num_replicas, your_uuid):
    body = pack('!H16s4sHLL16s', node_type, uuid, 
                      inet_aton(ip_address), port,
                      num_partitions, num_replicas, your_uuid)
    return Packet(ACCEPT_NODE_IDENTIFICATION, body)

def askPrimaryMaster():
    return Packet(ASK_PRIMARY_MASTER)

def answerPrimaryMaster(primary_uuid, known_master_list):
    body = [primary_uuid, pack('!L', len(known_master_list))]
    for master in known_master_list:
        body.append(pack('!4sH16s', inet_aton(master[0]), master[1], master[2]))
    body = ''.join(body)
    return Packet(ANSWER_PRIMARY_MASTER, body)

def announcePrimaryMaster():
    return Packet(ANNOUNCE_PRIMARY_MASTER)

def reelectPrimaryMaster():
    return Packet(REELECT_PRIMARY_MASTER)

def notifyNodeInformation(node_list):
    body = [pack('!L', len(node_list))]
    for node_type, ip_address, port, uuid, state in node_list:
        body.append(pack('!H4sH16sH', node_type, inet_aton(ip_address), port,
                         uuid, state))
    body = ''.join(body)
    return Packet(NOTIFY_NODE_INFORMATION, body)

def askLastIDs():
    return Packet(ASK_LAST_IDS)

def answerLastIDs(loid, ltid, lptid):
    return Packet(ANSWER_LAST_IDS, loid + ltid + lptid)

def askPartitionTable(offset_list):
    body = [pack('!L', len(offset_list))]
    for offset in offset_list:
        body.append(pack('!L', offset))
    body = ''.join(body)
    return Packet(ASK_PARTITION_TABLE, body)

def answerPartitionTable(ptid, row_list):
    body = [pack('!8sL', ptid, len(row_list))]
    for offset, cell_list in row_list:
        body.append(pack('!LL', offset, len(cell_list)))
        for uuid, state in cell_list:
            body.append(pack('!16sH', uuid, state))
    body = ''.join(body)
    return Packet(ANSWER_PARTITION_TABLE, body)

def sendPartitionTable(ptid, row_list):
    body = [pack('!8sL', ptid, len(row_list))]
    for offset, cell_list in row_list:
        body.append(pack('!LL', offset, len(cell_list)))
        for uuid, state in cell_list:
            body.append(pack('!16sH', uuid, state))
    body = ''.join(body)
    return Packet(SEND_PARTITION_TABLE, body)

def notifyPartitionChanges(ptid, cell_list):
    body = [pack('!8sL', ptid, len(cell_list))]
    for offset, uuid, state in cell_list:
        body.append(pack('!L16sH', offset, uuid, state))
    body = ''.join(body)
    return Packet(NOTIFY_PARTITION_CHANGES, body)

def startOperation():
    return Packet(START_OPERATION)

def stopOperation():
    return Packet(STOP_OPERATION)

def askUnfinishedTransactions():
    return Packet(ASK_UNFINISHED_TRANSACTIONS)

def answerUnfinishedTransactions(tid_list):
    body = [pack('!L', len(tid_list))]
    body.extend(tid_list)
    body = ''.join(body)
    return Packet(ANSWER_UNFINISHED_TRANSACTIONS, body)

def askObjectPresent(oid, tid):
    return Packet(ASK_OBJECT_PRESENT, oid + tid)

def answerObjectPresent(oid, tid):
    return Packet(ANSWER_OBJECT_PRESENT, oid + tid)

def deleteTransaction(tid):
    return Packet(DELETE_TRANSACTION, tid)

def commitTransaction(tid):
    return Packet(COMMIT_TRANSACTION, tid)

def askNewTID():
    return Packet(ASK_NEW_TID)

def answerNewTID(tid):
    return Packet(ANSWER_NEW_TID, tid)

def askNewOIDs(num_oids):
    return Packet(ASK_NEW_OIDS, pack('!H', num_oids))

def answerNewOIDs(oid_list):
    body = [pack('!H', len(oid_list))]
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(ANSWER_NEW_OIDS, body)

def finishTransaction(oid_list, tid):
    body = [pack('!8sL', tid, len(oid_list))]
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(FINISH_TRANSACTION, body)

def notifyTransactionFinished(tid):
    return Packet(NOTIFY_TRANSACTION_FINISHED, tid)

def lockInformation(tid):
    return Packet(LOCK_INFORMATION, tid)

def notifyInformationLocked(tid):
    return Packet(NOTIFY_INFORMATION_LOCKED, tid)

def invalidateObjects(oid_list, tid):
    body = [pack('!8sL', tid, len(oid_list))]
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(INVALIDATE_OBJECTS, body)

def unlockInformation(tid):
    return Packet(UNLOCK_INFORMATION, tid)

def abortTransaction(tid):
    return Packet(ABORT_TRANSACTION, tid)

def askStoreTransaction(tid, user, desc, ext, oid_list):
    user_len = len(user)
    desc_len = len(desc)
    ext_len = len(ext)
    body = [pack('!8sLHHH', tid, len(oid_list), len(user), len(desc), len(ext))]
    body.append(user)
    body.append(desc)
    body.append(ext)
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(ASK_STORE_TRANSACTION, body)

def answerStoreTransaction(tid):
    return Packet(ANSWER_STORE_TRANSACTION, tid)

def askStoreObject(oid, serial, compression, checksum, data, tid):
    body = pack('!8s8s8sBLL', oid, serial, tid, compression,
                      checksum, len(data)) + data
    return Packet(ASK_STORE_OBJECT, body)

def answerStoreObject(conflicting, oid, serial):
    body = pack('!B8s8s', conflicting, oid, serial)
    return Packet(ANSWER_STORE_OBJECT, body)

def askObject(oid, serial, tid):
    return Packet(ASK_OBJECT, pack('!8s8s8s', oid, serial, tid))

def answerObject(oid, serial_start, serial_end, compression,
                 checksum, data):
    body = pack('!8s8s8sBLL', oid, serial_start, serial_end,
                      compression, checksum, len(data)) + data
    return Packet(ANSWER_OBJECT, body)

def askTIDs(first, last, partition):
    return Packet(ASK_TIDS, pack('!QQL', first, last, partition))

def answerTIDs(tid_list):
    body = [pack('!L', len(tid_list))]
    body.extend(tid_list)
    body = ''.join(body)
    return Packet(ANSWER_TIDS, body)

def askTransactionInformation(tid):
    return Packet(ASK_TRANSACTION_INFORMATION, pack('!8s', tid))

def answerTransactionInformation(tid, user, desc, ext, oid_list):
    body = [pack('!8sHHHL', tid, len(user), len(desc), len(ext), len(oid_list))]
    body.append(user)
    body.append(desc)
    body.append(ext)
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(ANSWER_TRANSACTION_INFORMATION, body)

def askObjectHistory(oid, first, last):
    return Packet(ASK_OBJECT_HISTORY, pack('!8sQQ', oid, first, last))

def answerObjectHistory(oid, history_list):
    body = [pack('!8sL', oid, len(history_list))]
    # history_list is a list of tuple (serial, size)
    for history_tuple in history_list:
        body.append(pack('!8sL', history_tuple[0], history_tuple[1]))
    body = ''.join(body)
    return Packet(ANSWER_OBJECT_HISTORY, body)

def askOIDs(first, last, partition):
    return Packet(ASK_OIDS, pack('!QQL', first, last, partition))

def answerOIDs(oid_list):
    body = [pack('!L', len(oid_list))]
    body.extend(oid_list)
    body = ''.join(body)
    return Packet(ANSWER_OIDS, body)
