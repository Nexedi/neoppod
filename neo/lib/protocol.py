
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

import threading
from functools import partial
from msgpack import packb

# For msgpack & Py2/ZODB5.
try:
    from zodbpickle import binary
except ImportError:
    class binary: pass # stub, binary should not be used

# The protocol version must be increased whenever upgrading a node may require
# to upgrade other nodes.
PROTOCOL_VERSION = 5
# By encoding the handshake packet with msgpack, the whole NEO stream can be
# decoded with msgpack. The first byte is 0x92, which is different from TLS
# Handshake (0x16).
HANDSHAKE_PACKET = packb(('NEO', PROTOCOL_VERSION))
# Used to distinguish non-NEO stream from version mismatch.
MAGIC_SIZE = len(HANDSHAKE_PACKET) - len(packb(PROTOCOL_VERSION))

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

    iterable_types = set, tuple
    def default(obj):
        if isinstance(obj, binary):
            return obj.__str__()
        try:
            pack = obj._pack
        except AttributeError:
            assert type(obj) in iterable_types, type(obj)
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
        names = func.__code__.co_names
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
    DENIED
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
    UNDO_PACK_ERROR

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


class Packet(object):
    """
        Base class for any packet definition.
    """
    _ignore_when_closed = False
    _request = None
    _answer = None
    _code = None
    _id = None
    nodelay = True
    poll_thread = False

    def __init__(self, *args):
        assert self._code is not None, "Packet class not registered"
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

    @classmethod
    def isError(cls):
        return cls._code == RESPONSE_MASK

    @classmethod
    def isResponse(cls):
        return cls._code & RESPONSE_MASK

    def getAnswerClass(self):
        return self._answer

    @classmethod
    def ignoreOnClosedConnection(cls):
        """
        Tells if this packet must be ignored when its connection is closed
        when it is handled.
        """
        return cls._ignore_when_closed


class PacketRegistryFactory(dict):

    _next_code = 0

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
        code = self._next_code
        assert code < RESPONSE_MASK
        self._next_code = code + 1
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
        Error is a special type of message, because this can be sent against
        any other message, even if such a message does not expect a reply
        usually.

        :nodes: * -> *
        """, error=True)

    RequestIdentification, AcceptIdentification = request("""
        Request a node identification. This must be the first packet for any
        connection.

        :nodes: * -> *
        """, poll_thread=True)

    Ping, Pong = request("""
        Empty request used as network barrier.

        :nodes: * -> *
        """)

    CloseClient = notify("""
        Tell peer that it can close the connection if it has finished with us.

        :nodes: * -> *
        """)

    AskPrimary, AnswerPrimary = request("""
        Ask node identier of the current primary master.

        :nodes: ctl -> A
        """)

    NotPrimaryMaster = notify("""
        Notify peer that I'm not the primary master. Attach any extra
        information to help the peer joining the cluster.

        :nodes: SM -> *
        """)

    NotifyNodeInformation = notify("""
        Notify information about one or more nodes.

        :nodes: M -> *
        """)

    AskRecovery, AnswerRecovery = request("""
        Ask storage nodes data needed by master to recover.
        Reused by `neoctl print ids`.

        :nodes: M -> S; ctl -> A -> M
        """)

    AskLastIDs, AnswerLastIDs = request("""
        Ask the last OID/TID so that a master can initialize its
        TransactionManager. Reused by `neoctl print ids`.

        :nodes: M -> S; ctl -> A -> M
        """)

    AskPartitionTable, AnswerPartitionTable = request("""
        Ask storage node the remaining data needed by master to recover.

        :nodes: M -> S
        """)

    SendPartitionTable = notify("""
        Send the full partition table to admin/client/storage nodes on
        connection.

        :nodes: M -> A, C, S
        """)

    NotifyPartitionChanges = notify("""
        Notify about changes in the partition table.

        :nodes: M -> *
        """)

    StartOperation = notify("""
        Tell a storage node to start operation. Before this message,
        it must only communicate with the primary master.

        :nodes: M -> S
        """)

    StopOperation = notify("""
        Notify that the cluster is not operational anymore.
        Any operation between nodes must be aborted.

        :nodes: M -> S, C
        """)

    AskUnfinishedTransactions, AnswerUnfinishedTransactions = request("""
        Ask unfinished transactions, which will be replicated
        when they're finished.

        :nodes: S -> M
        """)

    AskLockedTransactions, AnswerLockedTransactions = request("""
        Ask locked transactions to replay committed transactions
        that haven't been unlocked.

        :nodes: M -> S
        """)

    AskFinalTID, AnswerFinalTID = request("""
        Return final tid if ttid has been committed, to recover from certain
        failures during tpc_finish.

        :nodes: M -> S; C -> M, S
        """)

    ValidateTransaction = notify("""
        Do replay a committed transaction that was not unlocked.

        :nodes: M -> S
        """)

    AskBeginTransaction, AnswerBeginTransaction = request("""
        Ask to begin a new transaction. This maps to `tpc_begin`.

        :nodes: C -> M
        """)

    FailedVote = request("""
        Report storage nodes for which vote failed.
        True is returned if it's still possible to finish the transaction.

        :nodes: C -> M
        """, error=True)

    AskFinishTransaction, AnswerTransactionFinished = request("""
        Finish a transaction. Return the TID of the committed transaction.
        This maps to `tpc_finish`.

        :nodes: C -> M
        """, ignore_when_closed=False, poll_thread=True)

    AskLockInformation, AnswerInformationLocked = request("""
        Commit a transaction. The new data is read-locked.

        :nodes: M -> S
        """, ignore_when_closed=False)

    InvalidateObjects = notify("""
        Notify about a new transaction modifying objects,
        invalidating client caches.

        :nodes: M -> C
        """)

    NotifyUnlockInformation = notify("""
        Notify about a successfully committed transaction. The new data can be
        unlocked.

        :nodes: M -> S
        """)

    AskNewOIDs, AnswerNewOIDs = request("""
        Ask new OIDs to create objects.

        :nodes: C -> M
        """)

    NotifyDeadlock = notify("""
        A client acquired a write-lock before another one that has a smaller
        TTID, leading to a possible deadlock. In order to solve it, this asks
        the client with the greatest TTID to lock again if it can't vote.

        :nodes: S -> C
        """)

    AskRelockObject, AnswerRelockObject = request("""
        Relock an object change to solve a deadlock.

        :nodes: C -> S
        """, data_path=(1, 0, 2, 0))

    AskStoreObject, AnswerStoreObject = request("""
        Ask to create/modify an object. This maps to `store`.

        As for IStorage, 'serial' is ZERO_TID for new objects.

        :nodes: C -> S
        """, data_path=(0, 2))

    AbortTransaction = notify("""
        Abort a transaction. This maps to `tpc_abort`.

        :nodes: C -> S; C -> M -> S
        """)

    AskStoreTransaction, AnswerStoreTransaction = request("""
        Ask to store a transaction. Implies vote.

        :nodes: C -> S
        """)

    AskVoteTransaction, AnswerVoteTransaction = request("""
        Ask to vote a transaction.

        :nodes: C -> S
        """)

    AskObject, AnswerObject = request("""
        Ask a stored object by its OID, optionally at/before a specific tid.
        This maps to `load/loadBefore/loadSerial`.

        :nodes: C -> S
        """, data_path=(1, 3))

    AskTIDs, AnswerTIDs = request("""
        Ask for TIDs between a range of offsets. The order of TIDs is
        descending, and the range is [first, last). This maps to `undoLog`.

        :nodes: C -> S
        """)

    AskTransactionInformation, AnswerTransactionInformation = request("""
        Ask for transaction metadata.

        :nodes: C -> S
        """)

    AskObjectHistory, AnswerObjectHistory = request("""
        Ask history information for a given object. The order of serials is
        descending, and the range is [first, last]. This maps to `history`.

        :nodes: C -> S
        """)

    AskPartitionList, AnswerPartitionList = request("""
        Ask information about partitions.

        :nodes: ctl -> A
        """)

    AskNodeList, AnswerNodeList = request("""
        Ask information about nodes.

        :nodes: ctl -> A
        """)

    SetNodeState = request("""
        Change the state of a node.

        :nodes: ctl -> A -> M
        """, error=True, ignore_when_closed=False)

    AddPendingNodes = request("""
        Mark given pending nodes as running, for future inclusion when tweaking
        the partition table.

        :nodes: ctl -> A -> M
        """, error=True, ignore_when_closed=False)

    TweakPartitionTable, AnswerTweakPartitionTable = request("""
        Ask the master to balance the partition table, optionally excluding
        specific nodes in anticipation of removing them.

        :nodes: ctl -> A -> M
        """)

    SetNumReplicas = request("""
        Set the number of replicas.

        :nodes: ctl -> A -> M
        """, error=True, ignore_when_closed=False)

    SetClusterState = request("""
        Set the cluster state.

        :nodes: ctl -> A -> M
        """, error=True, ignore_when_closed=False)

    Repair = request("""
        Ask storage nodes to repair their databases.

        :nodes: ctl -> A -> M
        """, error=True)

    NotifyRepair = notify("""
        Repair is translated to this message, asking a specific storage node to
        repair its database.

        :nodes: M -> S
        """)

    NotifyClusterInformation = notify("""
        Notify about a cluster state change.

        :nodes: M -> *
        """)

    AskClusterState, AnswerClusterState = request("""
        Ask the state of the cluster

        :nodes: ctl -> A; A -> M
        """)

    AskObjectUndoSerial, AnswerObjectUndoSerial = request("""
        Ask storage the serial where object data is when undoing given
        transaction, for a list of OIDs.

        Answer a dict mapping oids to 3-tuples:
            current_serial (TID)
                The latest serial visible to the undoing transaction.
            undo_serial (TID)
                Where undone data is (tid at which data is before given undo).
            is_current (bool)
                If current_serial's data is current on storage.

        :nodes: C -> S
        """)

    AskTIDsFrom, AnswerTIDsFrom = request("""
        Ask for length TIDs starting at min_tid. The order of TIDs is ascending.
        Used by `iterator`.

        :nodes: C -> S
        """)

    AskOIDsFrom, AnswerOIDsFrom = request("""
        Iterate over non-deleted OIDs starting at min_oid.
        The order of OIDs is ascending.

        :nodes: C -> S
        """)

    WaitForPack, WaitedForPack = request("""
        Wait until pack given by tid is completed.

        :nodes: C -> M
        """)

    AskPackOrders, AnswerPackOrders = request("""
        Request list of pack orders excluding oldest completed ones.

        :nodes: M -> S; C, S -> M
        """)

    NotifyPackSigned = notify("""
        Send ids of pack orders to be processed. Also used to fix replicas
        that may have lost them.

        When a pack order is auto-approved, the master also notifies storage
        that store it, even though they're already notified via
        AskLockInformation. In addition to make the implementation simpler,
        storage nodes don't have to detect this case and it's slightly faster
        when there's no pack.

        :nodes: M -> S, backup
        """)

    NotifyPackCompleted = notify("""
        Notify the master node that partitions have been successfully
        packed up to the given ids.

        :nodes: S -> M
        """)

    CheckReplicas = request("""
        Ask the cluster to search for mismatches between replicas, metadata
        only, and optionally within a specific range. Reference nodes can be
        specified.

        :nodes: ctl -> A -> M
        """, error=True)

    CheckPartition = notify("""
        Ask a storage node to compare a partition with all other nodes.
        Like for CheckReplicas, only metadata are checked, optionally within a
        specific range. A reference node can be specified.

        :nodes: M -> S
        """)

    AskCheckTIDRange, AnswerCheckTIDRange = request("""
        Ask some stats about a range of transactions.
        Used to know if there are differences between a replicating node and
        reference node.

        :nodes: S -> S
        """)

    AskCheckSerialRange, AnswerCheckSerialRange = request("""
        Ask some stats about a range of object history.
        Used to know if there are differences between a replicating node and
        reference node.

        :nodes: S -> S
        """)

    NotifyPartitionCorrupted = notify("""
        Notify that mismatches were found while check replicas for a partition.

        :nodes: S -> M
        """)

    NotifyReady = notify("""
        Notify that we're ready to serve requests.

        :nodes: S -> M
        """)

    AskLastTransaction, AnswerLastTransaction = request("""
        Ask last committed TID.

        :nodes: C -> M; ctl -> A -> M
        """, poll_thread=True)

    AskCheckCurrentSerial, AnswerCheckCurrentSerial = request("""
        Check if given serial is current for the given oid, and lock it so that
        this state is not altered until transaction ends.
        This maps to `checkCurrentSerialInTransaction`.

        :nodes: C -> S
        """)

    NotifyTransactionFinished = notify("""
        Notify that a transaction blocking a replication is now finished.

        :nodes: M -> S
        """)

    Replicate = notify("""
        Notify a storage node to replicate partitions up to given 'tid'
        and from given sources.

        args: tid, upstream_name, {partition: address}
        - upstream_name: replicate from an upstream cluster
        - address: address of the source storage node, or None if there's
                   no new data up to 'tid' for the given partition

        :nodes: M -> S
        """)

    NotifyReplicationDone = notify("""
        Notify the master node that a partition has been successfully
        replicated from a storage to another.

        :nodes: S -> M
        """)

    AskFetchTransactions, AnswerFetchTransactions = request("""
        Ask a storage node to send all transaction data we don't have,
        and reply with the list of transactions we should not have.

        :nodes: S -> S
        """)

    AskFetchObjects, AnswerFetchObjects = request("""
        Ask a storage node to send object records we don't have,
        and reply with the list of records we should not have.

        :nodes: S -> S
        """)

    AddTransaction = notify("""
        Send metadata of a transaction to a node that does not have them.

        :nodes: S -> S
        """, nodelay=False)

    AddObject = notify("""
        Send an object record to a node that does not have it.

        :nodes: S -> S
        """, nodelay=False, data_path=(0, 2))

    Truncate = request("""
        Request DB to be truncated. Also used to leave backup mode.

        :nodes: ctl -> A -> M; M -> S
        """, error=True)

    FlushLog = notify("""
        Request all nodes to flush their logs.

        :nodes: ctl -> A -> M -> *
        """)

    AskMonitorInformation, AnswerMonitorInformation = request("""
        :nodes: ctl -> A
        """)

    NotifyMonitorInformation = notify("""
        :nodes: A -> A
        """)

    NotifyUpstreamAdmin = notify("""
        :nodes: M -> A
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
