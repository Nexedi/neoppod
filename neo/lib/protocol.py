
# Copyright (C) 2006-2017  Nexedi SA
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

import struct, threading
from functools import partial

# The protocol version must be increased whenever upgrading a node may require
# to upgrade other nodes. It is encoded as a 4-bytes big-endian integer and
# the high order byte 0 is different from TLS Handshake (0x16).
PROTOCOL_VERSION = 6
ENCODED_VERSION = struct.pack('!L', PROTOCOL_VERSION)

RESPONSE_MASK = 0x8000

# Avoid some memory errors on corrupted data.
# Before we use msgpack, we limited the size of a whole packet. That's not
# possible anymore because the size is not known in advance. Packets bigger
# than the buffer size are possible (e.g. a huge list of small items) and for
# that we could compare the stream position (Unpacker.tell); it's not worth it.
UNPACK_BUFFER_SIZE = 0x4000000

@apply
def Unpacker():
    global registerExtType, packb
    from msgpack import ExtType, unpackb, Packer, Unpacker
    ext_type_dict = []
    kw = dict(use_bin_type=True)
    pack_ext = Packer(**kw).pack

    def registerExtType(getstate, make):
        code = len(ext_type_dict)
        ext_type_dict.append(lambda data: make(unpackb(data, use_list=False)))
        return lambda obj: ExtType(code, pack_ext(getstate(obj)))

    def default(obj):
        try:
            pack = obj._pack
        except AttributeError:
            assert type(obj) is not buffer
            return list(obj)
        return pack()
    lock = threading.Lock()
    pack = Packer(default, strict_types=True, **kw).pack
    def packb(obj):
        with lock: # in case that 'default' is called
            return pack(obj)

    return partial(Unpacker, use_list=False, max_buffer_size=UNPACK_BUFFER_SIZE,
        ext_hook=lambda code, data: ext_type_dict[code](data))

class Enum(tuple):

    class Item(int):
        __slots__ = '_name', '_enum', '_pack'
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
        pack = registerExtType(int, self.__getitem__)
        for item, name in zip(self, names):
            setattr(self, name, item)
            item._name = name
            item._enum = self
            item._pack = (lambda x: lambda: x)(pack(item))
        return self

    def __repr__(self):
        return "<Enum %s>" % self._name

# The order of extension type is important.
# Enum types first, sorted alphabetically.

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
def NodeStates():
    UNKNOWN
    DOWN
    RUNNING
    PENDING

@Enum
def NodeTypes():
    MASTER
    STORAGE
    CLIENT
    ADMIN

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
        Base class for any packet definition.
    """
    _ignore_when_closed = False
    _request = None
    _answer = None
    _code = None
    _id = None
    allow_dict = False
    nodelay = True
    poll_thread = False

    def __init__(self, *args):
        assert self._code is not None, "Packet class not registered"
        if dict in map(type, args) and not self.allow_dict:
            raise TypeError('disallowed dict')
        self._args = args

    def setId(self, value):
        self._id = value

    def getId(self):
        assert self._id is not None, "No identifier applied on the packet"
        return self._id

    def encode(self, packb=packb):
        """ Encode a packet as a string to send it over the network """
        r = packb((self._id, self._code, self._args))
        self.size = len(r)
        return r

    def __repr__(self):
        return '%s[%r]' % (self.__class__.__name__, self._id)

    def __eq__(self, other):
        """ Compare packets with their code instead of content """
        if other is None:
            return False
        assert isinstance(other, Packet)
        return self._code == other._code

    def isError(self):
        return self._code == RESPONSE_MASK

    def isResponse(self):
        return self._code & RESPONSE_MASK

    def getAnswerClass(self):
        return self._answer

    def ignoreOnClosedConnection(self):
        """
        Tells if this packet must be ignored when its connection is closed
        when it is handled.
        """
        return self._ignore_when_closed


class PacketRegistryFactory(dict):

    def __call__(self, name, base, d):
        for k, v in d.items():
            if isinstance(v, type) and issubclass(v, Packet):
                v.__name__ = k
                v.handler_method_name = k[0].lower() + k[1:]
        # this builds a "singleton"
        return type('PacketRegistry', base, d)(self)

    def register(self, doc, ignore_when_closed=None, request=False, error=False,
                       _base=(Packet,), **kw):
        """ Register a packet in the packet registry """
        code = len(self)
        if doc is None:
            self[code] = None
            return # None registered only to skip a code number (for compatibility)
        if error and not request:
            assert not code
            code = RESPONSE_MASK
        kw.update(__doc__=doc, _code=code)
        packet = type('', _base, kw)
        # register the request
        self[code] = packet
        if request:
            if ignore_when_closed is None:
                # By default, on a closed connection:
                # - request: ignore
                # - answer: keep
                # - notification: keep
                packet._ignore_when_closed = True
            else:
                assert ignore_when_closed is False
            if error:
                packet._answer = self[RESPONSE_MASK]
            else:
                # build a class for the answer
                code |= RESPONSE_MASK
                kw['_code'] = code
                answer = packet._answer = self[code] = type('', _base, kw)
                return packet, answer
        else:
            assert ignore_when_closed is None
        return packet


class Packets(dict):
    """
    Packet registry that checks packet code uniqueness and provides an index
    """
    __metaclass__ = PacketRegistryFactory()
    notify = __metaclass__.register
    request = partial(notify, request=True)

    Error = notify("""
        """, error=True)

    RequestIdentification, AcceptIdentification = request("""
        """, poll_thread=True)

    Ping, Pong = request("""
        """)

    CloseClient = notify("""
        """)

    AskPrimary, AnswerPrimary = request("""
        """)

    NotPrimaryMaster = notify("""
        """)

    NotifyNodeInformation = notify("""
        """)

    AskRecovery, AnswerRecovery = request("""
        """)

    AskLastIDs, AnswerLastIDs = request("""
        """)

    AskPartitionTable, AnswerPartitionTable = request("""
        """)

    SendPartitionTable = notify("""
        """)

    NotifyPartitionChanges = notify("""
        """)

    StartOperation = notify("""
        """)

    StopOperation = notify("""
        """)

    AskUnfinishedTransactions, AnswerUnfinishedTransactions = request("""
        """)

    AskLockedTransactions, AnswerLockedTransactions = request("""
        """, allow_dict=True)

    AskFinalTID, AnswerFinalTID = request("""
        """)

    ValidateTransaction = notify("""
        """)

    AskBeginTransaction, AnswerBeginTransaction = request("""
        """)

    FailedVote = request("""
        """, error=True)

    AskFinishTransaction, AnswerTransactionFinished = request("""
        """, ignore_when_closed=False, poll_thread=True)

    AskLockInformation, AnswerInformationLocked = request("""
        """, ignore_when_closed=False)

    InvalidateObjects = notify("""
        """)

    NotifyUnlockInformation = notify("""
        """)

    AskNewOIDs, AnswerNewOIDs = request("""
        """)

    NotifyDeadlock = notify("""
        """)

    AskRebaseTransaction, AnswerRebaseTransaction = request("""
        """)

    AskRebaseObject, AnswerRebaseObject = request("""
        """, data_path=(1, 0, 2, 0))

    AskStoreObject, AnswerStoreObject = request("""
        """, data_path=(0, 2))

    AbortTransaction = notify("""
        """)

    AskStoreTransaction, AnswerStoreTransaction = request("""
        """)

    AskVoteTransaction, AnswerVoteTransaction = request("""
        """)

    AskObject, AnswerObject = request("""
        """, data_path=(1, 3))

    AskTIDs, AnswerTIDs = request("""
        """)

    AskTransactionInformation, AnswerTransactionInformation = request("""
        """)

    AskObjectHistory, AnswerObjectHistory = request("""
        """)

    AskPartitionList, AnswerPartitionList = request("""
        """)

    AskNodeList, AnswerNodeList = request("""
        """)

    SetNodeState = request("""
        """, error=True, ignore_when_closed=False)

    AddPendingNodes = request("""
        """, error=True, ignore_when_closed=False)

    TweakPartitionTable = request("""
        """, error=True, ignore_when_closed=False)

    SetClusterState = request("""
        """,  error=True, ignore_when_closed=False)

    Repair = request("""
        """, error=True)

    NotifyRepair = notify("""
        """)

    NotifyClusterInformation = notify("""
        """)

    AskClusterState, AnswerClusterState = request("""
        """)

    AskObjectUndoSerial, AnswerObjectUndoSerial = request("""
        """, allow_dict=True)

    AskTIDsFrom, AnswerTIDsFrom = request("""
        """)

    AskPack, AnswerPack = request("""
        """, ignore_when_closed=False)

    CheckReplicas = request("""
        """, error=True, allow_dict=True)

    CheckPartition = notify("""
        """)

    AskCheckTIDRange, AnswerCheckTIDRange = request("""
        """)

    AskCheckSerialRange, AnswerCheckSerialRange = request("""
        """)

    NotifyPartitionCorrupted = notify("""
        """)

    NotifyReady = notify("""
        """)

    AskLastTransaction, AnswerLastTransaction = request("""
        """, poll_thread=True)

    AskCheckCurrentSerial, AnswerCheckCurrentSerial = request("""
        """)

    NotifyTransactionFinished = notify("""
        """)

    Replicate = notify("""
        """, allow_dict=True)

    NotifyReplicationDone = notify("""
        """)

    AskFetchTransactions, AnswerFetchTransactions = request("""
        """)

    AskFetchObjects, AnswerFetchObjects = request("""
        """, allow_dict=True)

    AddTransaction = notify("""
        """, nodelay=False)

    AddObject = notify("""
        """, nodelay=False, data_path=(0, 2))

    Truncate = request("""
        """, error=True)

    FlushLog = notify("""
        """)

    del notify, request


def Errors():
    registry_dict = {}
    handler_method_name_dict = {}
    Error = Packets.Error
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
        node_list = [(
                uuid_str(uuid), str(node_type),
                ('[%s]:%s' if ':' in addr[0] else '%s:%s')
                % addr if addr else '', str(state),
                str(id_timestamp and datetime.utcfromtimestamp(id_timestamp)))
            for node_type, addr, uuid, state, id_timestamp
                in sorted(node_list, key=_sort_key)]
        t = ''.join('%%-%us | ' % max(len(x[i]) for x in node_list)
                    for i in xrange(len(node_list[0]) - 1))
        return map((prefix + t + '%s').__mod__, node_list)
    return ()

Packets.NotifyNodeInformation._neolog = staticmethod(
    lambda timestamp, node_list:
    ((timestamp,), formatNodeList(node_list, ' ! ')))

Packets.Error._neolog = staticmethod(lambda *args: ((), ("%s (%s)" % args,)))
