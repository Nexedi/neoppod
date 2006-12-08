from struct import pack, unpack
from socket import inet_ntoa, inet_aton
import logging

from neo.util import dump

# The protocol version (major, minor).
PROTOCOL_VERSION = (4, 0)

# Size restrictions.
MIN_PACKET_SIZE = 10
MAX_PACKET_SIZE = 0x100000

# Message types.

# Error is a special type of message, because this can be sent against any other message,
# even if such a message does not expect a reply usually. Any -> Any.
ERROR = 0x8000

# Check if a peer is still alive. Any -> Any.
PING = 0x0001

# Notify being alive. Any -> Any.
PONG = 0x8001

# Request a node identification. This must be the first packet for any connection.
# Any -> Any.
REQUEST_NODE_IDENTIFICATION = 0x0002

# Accept a node identification. This should be a reply to Request Node Identification.
# Any -> Any.
ACCEPT_NODE_IDENTIFICATION = 0x8002

# Ask a current primary master node. This must be the second message when connecting
# to a master node. Any -> M.
ASK_PRIMARY_MASTER = 0x0003

# Reply to Ask Primary Master. This message includes a list of known master nodes,
# to make sure that a peer has the same information. M -> Any.
ANSWER_PRIMARY_MASTER = 0x8003

# Announce a primary master node election. PM -> SM.
ANNOUNCE_PRIMARY_MASTER = 0x0004

# Force a re-election of a primary master node. M -> M.
REELECT_PRIMARY_MASTER = 0x0005

# Notify information about one or more nodes. Any -> PM, PM -> Any.
NOTIFY_NODE_INFORMATION = 0x0006

# Ask the last OID, the last TID and the last Partition Table ID that a storage node
# stores. Used to recover information. PM -> S.
ASK_LAST_IDS = 0x0007

# Reply to Ask Last IDs. S -> PM.
ANSWER_LAST_IDS = 0x8007

# Ask rows in a partition table that a storage node stores. Used to recover
# information. PM -> S.
ASK_PARTITION_TABLE = 0x0008

# Answer rows in a partition table. S -> PM.
ANSWER_PARTITION_TABLE = 0x8008

# Send rows in a partition table to update other nodes. PM -> S, C.
SEND_PARTITION_TABLE = 0x0009

# Notify a subset of a partition table. This is used to notify changes. PM -> S, C.
NOTIFY_PARTITION_CHANGES = 0x000a

# Tell a storage nodes to start an operation. Until a storage node receives this
# message, it must not serve client nodes. PM -> S.
START_OPERATION = 0x000b

# Tell a storage node to stop an operation. Once a storage node receives this message,
# it must not serve client nodes. PM -> S.
STOP_OPERATION = 0x000c

# Ask unfinished transactions' IDs. PM -> S.
ASK_UNFINISHED_TRANSACTIONS = 0x000d

# Answer unfinished transactions' IDs. S -> PM.
ANSWER_UNFINISHED_TRANSACTIONS = 0x800d

# Ask OIDs by a TID. PM -> S.
ASK_OIDS_BY_TID = 0x000e

# Answer OIDs by a TID. S -> PM.
ANSWER_OIDS_BY_TID = 0x800e

# Ask if an object is present. If not present, OID_NOT_FOUND should be returned. PM -> S.
ASK_OBJECT_PRESENT = 0x000f

# Answer that an object is present. PM -> S.
ANSWER_OBJECT_PRESENT = 0x800f

# Delete a transaction. PM -> S.
DELETE_TRANSACTION = 0x0010

# Commit a transaction. PM -> S.
COMMIT_TRANSACTION = 0x0011


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

VALID_NODE_TYPE_LIST = (MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE)

# Node states.
RUNNING_STATE = 0
TEMPORARILY_DOWN_STATE = 1
DOWN_STATE = 2
BROKEN_STATE = 3

VALID_NODE_STATE_LIST = (RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE)

# Partition cell states.
UP_TO_DATE_STATE = 0
OUT_OF_DATE_STATE = 1
FEEDING_STATE = 2
DISCARDED_STATE = 3

VALID_CELL_STATE_LIST = (UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, 
                         DISCARDED_STATE)

# Other constants.
INVALID_UUID = '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
INVALID_TID = '\0\0\0\0\0\0\0\0'
INVALID_SERIAL = '\0\0\0\0\0\0\0\0'
INVALID_OID = '\0\0\0\0\0\0\0\0'
INVALID_PTID = '\0\0\0\0\0\0\0\0'

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
        msg_id, msg_type, msg_len = unpack('!LHL', msg[:10])
        if msg_len > MAX_PACKET_SIZE:
            raise ProtocolError(cls(msg_id, msg_type), 
                                'message too big (%d)' % msg_len)
        if msg_len < MIN_PACKET_SIZE:
            raise ProtocolError(cls(msg_id, msg_type), 
                                'message too small (%d)' % msg_len)
        if len(msg) < msg_len:
            # Not enough.
            return None
        return cls(msg_id, msg_type, msg[10:msg_len])

    def __init__(self, msg_id = None, msg_type = None, body = None):
        self._id = msg_id
        self._type = msg_type
        self._body = body

    def getId(self):
        return self._id

    def getType(self):
        return self._type

    def __len__(self):
        return 10 + len(self._body)

    # Encoders.
    def encode(self):
        msg = pack('!LHL', self._id, self._type, 10 + len(self._body)) + self._body
        if len(msg) > MAX_PACKET_SIZE:
            raise ProtocolError(self, 'message too big (%d)' % len(msg))
        return msg

    __str__ = encode

    def error(self, msg_id, error_code, error_message):
        self._id = msg_id
        self._type = ERROR
        self._body = pack('!HL', error_code, len(error_message)) + error_message
        return self

    def protocolError(self, msg_id, error_message):
        return self.error(msg_id, PROTOCOL_ERROR_CODE, 'protocol error: ' + error_message)

    def internalError(self, msg_id, error_message):
        return self.error(msg_id, INTERNAL_ERROR_CODE, 'internal error: ' + error_message)

    def notReady(self, msg_id, error_message):
        return self.error(msg_id, NOT_READY, 'not ready: ' + error_message)

    def brokenNodeDisallowedError(self, msg_id, error_message):
        return self.error(msg_id, BROKEN_NODE_DISALLOWED_ERROR, 
                          'broken node disallowed error: ' + error_message)

    def ping(self, msg_id):
        self._id = msg_id
        self._type = PING
        self._body = ''
        return self

    def pong(self, msg_id):
        self._id = msg_id
        self._type = PONG
        self._body = ''
        return self

    def requestNodeIdentification(self, msg_id, node_type, uuid, ip_address, port, name):
        self._id = msg_id
        self._type = REQUEST_NODE_IDENTIFICATION
        self._body = pack('!LLH16s4sHL', PROTOCOL_VERSION[0], PROTOCOL_VERSION[1],
                          node_type, uuid, inet_aton(ip_address), port, len(name)) + name
        return self

    def acceptNodeIdentification(self, msg_id, node_type, uuid, ip_address, port):
        self._id = msg_id
        self._type = ACCEPT_NODE_IDENTIFICATION
        self._body = pack('!H16s4sH', node_type, uuid, inet_aton(ip_address), port)
        return self

    def askPrimaryMaster(self, msg_id):
        self._id = msg_id
        self._type = ASK_PRIMARY_MASTER
        self._body = ''
        return self

    def answerPrimaryMaster(self, msg_id, primary_uuid, known_master_list):
        self._id = msg_id
        self._type = ANSWER_PRIMARY_MASTER
        body = [primary_uuid, pack('!L', len(known_master_list))]
        for master in known_master_list:
            body.append(pack('!4sH16s', inet_aton(master[0]), master[1], master[2]))
        self._body = ''.join(body)
        return self

    def announcePrimaryMaster(self, msg_id):
        self._id = msg_id
        self._type = ANNOUNCE_PRIMARY_MASTER
        self._body = ''
        return self

    def reelectPrimaryMaster(self, msg_id):
        self._id = msg_id
        self._type = REELECT_PRIMARY_MASTER
        self._body = ''
        return self

    def notifyNodeInformation(self, msg_id, node_list):
        self._id = msg_id
        self._type = NOTIFY_NODE_INFORMATION
        body = [pack('!L', len(node_list))]
        for node_type, ip_address, port, uuid, state in node_list:
            body.append(pack('!H4sH16sH', node_type, inet_aton(ip_address), port,
                             uuid, state))
        self._body = ''.join(body)
        return self

    def askLastIDs(self, msg_id):
        self._id = msg_id
        self._type = ASK_LAST_IDS
        self._body = ''
        return self

    def answerLastIDs(self, msg_id, loid, ltid, lptid):
        self._id = msg_id
        self._type = ANSWER_LAST_IDS
        self._body = loid + ltid + lptid
        return self

    def askPartitionTable(self, msg_id, offset_list):
        self._id = msg_id
        self._type = ASK_PARTITION_TABLE
        body = [pack('!L', len(offset_list))]
        for offset in offset_list:
            body.append(pack('!L', offset))
        self._body = ''.join(body)
        return self

    def answerPartitionTable(self, msg_id, ptid, row_list):
        self._id = msg_id
        self._type = ANSWER_PARTITION_TABLE
        body = [pack('!8sL', ptid, len(row_list))]
        for offset, cell_list in row_list:
            body.append(pack('!LL', offset, len(cell_list)))
            for uuid, state in cell_list:
                body.append(pack('!16sH', uuid, state))
        self._body = ''.join(body)
        return self

    def sendPartitionTable(self, msg_id, ptid, row_list):
        self._id = msg_id
        self._type = SEND_PARTITION_TABLE
        body = [pack('!8sL', ptid, len(row_list))]
        for offset, cell_list in row_list:
            body.append(pack('!LL', offset, len(cell_list)))
            for uuid, state in cell_list:
                body.append(pack('!16sH', uuid, state))
        self._body = ''.join(body)
        return self

    def notifyPartitionChanges(self, msg_id, ptid, cell_list):
        self._id = msg_id
        self._type = NOTIFY_PARTITION_CHANGES
        body = [pack('!8sL', ptid, len(cell_list))]
        for offset, uuid, state in cell_list:
            body.append(pack('!L16sH', offset, uuid, state))
        self._body = ''.join(body)
        return self

    def startOperation(self, msg_id):
        self._id = msg_id
        self._type = START_OPERATION
        self._body = ''
        return self

    def stopOperation(self, msg_id):
        self._id = msg_id
        self._type = STOP_OPERATION
        self._body = ''
        return self

    def askUnfinishedTransactions(self, msg_id):
        self._id = msg_id
        self._type = ASK_UNFINISHED_TRANSACTIONS
        self._body = ''
        return self

    def answerUnfinishedTransactions(self, msg_id, tid_list):
        self._id = msg_id
        self._type = ANSWER_UNFINISHED_TRANSACTIONS
        body = [pack('!L', len(tid_list))]
        body.extend(tid_list)
        self._body = ''.join(body)
        return self

    def askOIDsByTID(self, msg_id, tid):
        self._id = msg_id
        self._type = ASK_OIDS_BY_TID
        self._body = tid
        return self

    def answerOIDsByTID(self, msg_id, oid_list, tid):
        self._id = msg_id
        self._type = ANSWER_OIDS_BY_TID
        body = [pack('!8sL', tid, len(oid_list))]
        body.extend(oid_list)
        self._body = ''.join(body)
        return self

    def askObjectPresent(self, msg_id, oid, tid):
        self._id = msg_id
        self._type = ASK_OBJECT_PRESENT
        self._body = oid + tid
        return self

    def answerObjectPresent(self, msg_id, oid, tid):
        self._id = msg_id
        self._type = ANSWER_OBJECT_PRESENT
        self._body = oid + tid
        return self

    def deleteTransaction(self, msg_id, tid):
        self._id = msg_id
        self._type = DELETE_TRANSACTION
        self._body = tid
        return self

    def commitTransaction(self, msg_id, tid):
        self._id = msg_id
        self._type = COMMIT_TRANSACTION
        self._body = tid
        return self

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
            major, minor, node_type, uuid, ip_address, port, size = unpack('!LLH16s4sHL',
                                                                           body[:36])
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
            node_type, uuid, ip_address, port = unpack('!H16s4sH', self._body)
            ip_address = inet_ntoa(ip_address)
        except:
            raise ProtocolError(self, 'invalid accept node identification')
        if node_type not in VALID_NODE_TYPE_LIST:
            raise ProtocolError(self, 'invalid node type %d' % node_type)
        return node_type, uuid, ip_address, port
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
            n = unpack('!L', self._body[:4])
            node_list = []
            for i in xrange(n):
                r = unpack('!H4sH16sH', self._body[4+i*26:30+i*26])
                node_type, ip_address, port, uuid, state = r
                ip_address = inet_ntoa(ip_address)
                if node_type not in VALID_NODE_TYPE_LIST:
                    raise ProtocolError(self, 'invalid node type %d' % node_type)
                if state not in VALID_NODE_STATE_LIST:
                    raise ProtocolError(self, 'invalid node state %d' % state)
                node_list.append((node_type, ip_address, port, uuid, state))
        except ProtocolError:
            raise
        except:
            raise ProtocolError(self, 'invalid answer node information')
        return node_list
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
            n = unpack('!L', self._body[:4])
            offset_list = []
            for i in xrange(n):
                offset = unpack('!L', self._body[4+i*4:8+i*4])
                offset_list.append(offset)
        except:
            raise ProtocolError(self, 'invalid ask partition table')
        return offset_list
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
                    cell = unpack('!16sH', self._body[index:index+18])
                    index += 18
                    cell_list.append(cell)
                row_list.append((offset, cell_list))
                del cell_list[:]
        except:
            raise ProtocolError(self, 'invalid answer partition table')
        return row_list
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
                    cell = unpack('!16sH', self._body[index:index+18])
                    index += 18
                    cell_list.append(cell)
                row_list.append((offset, cell_list))
                del cell_list[:]
        except:
            raise ProtocolError(self, 'invalid send partition table')
        return row_list
    decode_table[SEND_PARTITION_TABLE] = _decodeSendPartitionTable

    def _decodeNotifyPartitionChanges(self):
        try:
            ptid, n = unpack('!8sL', self._body[:12])
            for i in xrange(n):
                cell = unpack('!L16sH', self._body[12+i*22:34+i*22])
                cell_list.append(cell)
        except:
            raise ProtocolError(self, 'invalid notify partition changes')
        return cell_list
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
            n = unpack('!L', self._body[:4])
            tid_list = []
            for i in xrange(n):
                tid = unpack('8s', self._body[4+i*8:12+i*8])
                tid_list.append(tid)
        except:
            raise ProtocolError(self, 'invalid answer unfinished transactions')
        return tid_list
    decode_table[ANSWER_UNFINISHED_TRANSACTIONS] = _decodeAnswerUnfinishedTransactions

    def _decodeAskOIDsByTID(self):
        try:
            tid = unpack('8s', self._body)
        except:
            raise ProtocolError(self, 'invalid ask oids by tid')
        return tid
    decode_table[ASK_OIDS_BY_TID] = _decodeAskOIDsByTID

    def _decodeAnswerOIDsByTID(self):
        try:
            tid, n = unpack('!8sL', self._body[:12])
            oid_list = []
            for i in xrange(n):
                oid = unpack('8s', self._body[12+i*8:20+i*8])
                oid_list.append(oid)
        except:
            raise ProtocolError(self, 'invalid answer oids by tid')
        return oid_list, tid
    decode_table[ANSWER_OIDS_BY_TID] = _decodeAnswerOIDsByTID

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
            tid = unpack('8s', self._body)
        except:
            raise ProtocolError(self, 'invalid delete transaction')
        return tid
    decode_table[DELETE_TRANSACTION] = _decodeDeleteTransaction

    def _decodeCommitTransaction(self):
        try:
            tid = unpack('8s', self._body)
        except:
            raise ProtocolError(self, 'invalid commit transaction')
        return tid
    decode_table[COMMIT_TRANSACTION] = _decodeCommitTransaction
