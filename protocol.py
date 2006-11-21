from struct import pack, unpack
from socket import inet_ntoa, inet_aton

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

class ProtocolError(Exception): pass

class Packet:
    """A packet."""

    _id = None
    _type = None
    _len = None

    @classmethod
    def parse(cls, msg):
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
        return cls(msg_id, msg_type, msg_len, msg[10:msg_len])

    def __init__(self, msg_id = None, msg_type = None, body = None):
        self._id = msg_id
        self._type = msg_type
        self._body = body

    def getId(self):
        return self._id

    def getType(self):
        return self._type

    def __len__(self):
        return len(self._body)

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

    def protocolError(self, msg_id, error_message):
        self.error(msg_id, PROTOCOL_ERROR_CODE, 'protocol error: ' + error_message)

    def internalError(self, msg_id, error_message):
        self.error(msg_id, INTERNAL_ERROR_CODE, 'internal error: ' + error_message)

    def ping(self, msg_id):
        self._id = msg_id
        self._type = PING
        self._body = ''

    def pong(self, msg_id):
        self._id = msg_id
        self._type = PONG
        self._body = ''

    def requestNodeIdentification(self, msg_id, node_type, uuid, ip_address, port, name):
        self._id = msg_id
        self._type = REQUEST_NODE_IDENTIFICATION
        self._body = pack('!LLH16s4sHL', protocol_version[0], protocol_version[1],
                          node_type, uuid, inet_aton(ip_address), port, len(name)) + name

    def acceptNodeIdentification(self, msg_id, node_type, uuid, ip_address, port):
        self._id = msg_id
        self._type = ACCEPT_NODE_IDENTIFICATION
        self._body = pack('!H16s4sH', node_type, uuid, inet_aton(ip_address), port)

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
        if (major, minor) != protocol_version:
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

