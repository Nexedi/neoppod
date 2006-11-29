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
ERROR = 0x8000
REQUEST_NODE_IDENTIFICATION = 0x0001
ACCEPT_NODE_IDENTIFICATION = 0x8001
PING = 0x0002
PONG = 0x8002
ASK_PRIMARY_MASTER = 0x0003
ANSWER_PRIMARY_MASTER = 0x8003
ANNOUNCE_PRIMARY_MASTER = 0x0004
REELECT_PRIMARY_MASTER = 0x0005
NOTIFY_NODE_INFORMATION = 0x0006
START_OPERATION = 0x0007
STOP_OPERATION = 0x0008
ASK_FINISHING_TRANSACTIONS = 0x0009
ANSWER_FINISHING_TRANSACTIONS = 0x8009
FINISH_TRANSACTIONS = 0x000a

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
TEMPORARILY_DOWN_STATE = 2
DOWN_STATE = 3
BROKEN_STATE = 4

VALID_NODE_STATE_LIST = (RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE)

# Other constants.
INVALID_UUID = '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
INVALID_TID = '\0\0\0\0\0\0\0\0'
INVALID_SERIAL = '\0\0\0\0\0\0\0\0'
INVALID_OID = '\0\0\0\0\0\0\0\0'

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
        msg_id, msg_type, msg_len, reserved = unpack('!HHLH', msg[:10])
        if reserved != 0:
            raise ProtocolError(cls(msg_id, msg_type), 'reserved is non-zero')
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
        msg = pack('!HHLH', self._id, self._type, 10 + len(self._body), 0) + self._body
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
            primary_uuid, n = unpack('!16sL', self._body[:36])
            known_master_list = []
            for i in xrange(n):
                ip_address, port, uuid = unpack('!4sH16s', self._body[36+i*22:58+i*22])
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
