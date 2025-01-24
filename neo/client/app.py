#
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

import heapq
import random
import time
from collections import defaultdict
from functools import partial
from neo import *

from ZODB._compat import dumps, loads, _protocol
from ZODB.POSException import (
    ConflictError, ReadConflictError, ReadOnlyError, UndoError)

from neo.lib import logging
from neo.lib.compress import decompress_list, getCompress
from neo.lib.protocol import NodeTypes, Packets, \
    INVALID_PARTITION, MAX_TID, ZERO_HASH, ZERO_OID, ZERO_TID
from neo.lib.util import makeChecksum, dump
from neo.lib.locking import Empty, Lock
from neo.lib.connection import MTClientConnection, ConnectionClosed
from neo.lib.exception import NodeNotReady
from . import TransactionMetaData
from .exception import (NEOStorageError, NEOStorageCreationUndoneError,
    NEOStorageReadRetry, NEOStorageNotFoundError, NEOStorageWrongChecksum,
    NEOPrimaryMasterLost)
from .handlers import storage, master
from neo.lib.threaded_app import ThreadedApplication
from .cache import ClientCache
from .transactions import TransactionContainer
from neo.lib.util import p64, u64, parseMasterList

CHECKED_SERIAL = object()

# How long before we might retry a connection to a node to which connection
# failed in the past.
MAX_FAILURE_AGE = 600

TXN_PACK_DESC = b'IStorage.pack'

try:
    from Signals.Signals import SignalHandler
except ImportError:
    SignalHandler = None
if SignalHandler:
    import signal
    SignalHandler.registerHandler(signal.SIGUSR2, logging.reopen)

class Application(ThreadedApplication):
    """The client node application."""

    # For tests only. Do not touch. We want tpc_finish to always recover when
    # the transaction is really committed, no matter for how long the master
    # is unreachable.
    max_reconnection_to_master = float('inf')
    # For tests only. See end of pack() method.
    wait_for_pack = False

    def __init__(self, master_nodes, name, compress=True, cache_size=None,
                 read_only=False, ignore_wrong_checksum=False, **kw):
        super(Application, self).__init__(parseMasterList(master_nodes),
                                          name, **kw)
        # Internal Attributes common to all thread
        self._db = None
        self.primary_master_node = None
        self.trying_master_node = None

        # no self-assigned NID, primary master will supply us one
        self._cache = ClientCache() if cache_size is None else \
                      ClientCache(max_size=cache_size)
        self._loading = defaultdict(lambda: (Lock(), []))
        self.new_oids = ()
        self.last_oid = ZERO_OID
        self.storage_event_handler = storage.StorageEventHandler(self)
        self.storage_bootstrap_handler = storage.StorageBootstrapHandler(self)
        self.storage_handler = storage.StorageAnswersHandler(self)
        self.primary_handler = master.PrimaryAnswersHandler(self)
        self.primary_bootstrap_handler = master.PrimaryBootstrapHandler(self)
        self.notifications_handler = master.PrimaryNotificationsHandler( self)
        self._txn_container = TransactionContainer()
        # Lock definition :
        # _oid_lock is used in order to not call multiple oid
        # generation at the same time
        lock = Lock()
        self._oid_lock_acquire = lock.acquire
        self._oid_lock_release = lock.release
        # _cache_lock is used for the client cache
        self._cache_lock = Lock()
        # _connecting_to_master_node is used to prevent simultaneous master
        # node connection attempts
        self._connecting_to_master_node = Lock()
        # same for storage nodes
        self._connecting_to_storage_node = Lock()
        self._node_failure_dict = {}
        self.compress = getCompress(compress)
        self.read_only = read_only
        self.ignore_wrong_checksum = ignore_wrong_checksum

    def __getattr__(self, attr):
        if attr in ('last_tid', 'pt'):
            self._getMasterConnection()
            # XXX: There's still a risk that we get disconnected from the
            #      master at this precise moment and for 'pt', we'd raise
            #      AttributeError. Should we catch it and loop until it
            #      succeeds?
        return self.__getattribute__(attr)

    def log(self):
        super(Application, self).log()
        logging.info("%r", self._cache)
        for txn_context in self.txn_contexts():
            logging.info("%r", txn_context)

    # do not iter lazily to avoid RuntimeError vs tpc_begin
    if six.PY2:
        @property
        def txn_contexts(self):
            return self._txn_container.values
    else:
        def txn_contexts(self):
            return list(self._txn_container.values())

    def _waitAnyMessage(self, queue, block=True):
        """
          Handle all pending packets.
          block
            If True (default), will block until at least one packet was
            received.
        """
        pending = self.dispatcher.pending
        get = queue.get
        _handlePacket = self._handlePacket
        while pending(queue):
            try:
                conn, packet, kw = get(block)
            except Empty:
                break
            block = False
            try:
                _handlePacket(conn, packet, kw)
            except (ConnectionClosed, NEOStorageReadRetry):
                # We also catch NEOStorageReadRetry for ObjectUndoSerial.
                pass

    def _waitAnyTransactionMessage(self, txn_context, block=True):
        """
        Just like _waitAnyMessage, but for per-transaction exchanges, rather
        than per-thread.
        """
        queue = txn_context.queue
        self.setHandlerData(txn_context)
        try:
            self._waitAnyMessage(queue, block=block)
        finally:
            # Don't leave access to thread context, even if a raise happens.
            self.setHandlerData(None)
        if txn_context.conflict_dict:
            self._handleConflicts(txn_context)

    def _askStorage(self, conn, packet, **kw):
        """ Send a request to a storage node and process its answer """
        return self._ask(conn, packet, handler=self.storage_handler, **kw)

    def _askPrimary(self, packet, **kw):
        """ Send a request to the primary master and process its answer """
        return self._ask(self._getMasterConnection(), packet,
            handler=self.primary_handler, **kw)

    def _getMasterConnection(self):
        """ Connect to the primary master node on demand """
        # For performance reasons, get 'master_conn' without locking.
        result = self.master_conn
        if result is None:
            # If not connected, 'master_conn' must be tested again while we have
            # the lock, to avoid concurrent threads reconnecting.
            with self._connecting_to_master_node:
                result = self.master_conn
                if result is None:
                    self.new_oids = ()
                    result = self.master_conn = self._connectToPrimaryNode()
        return result

    def _connectToPrimaryNode(self):
        """
            Lookup for the current primary master node
        """
        logging.debug('connecting to primary master...')
        self.start()
        index = -1
        fail_count = 0
        ask = self._ask
        handler = self.primary_bootstrap_handler
        conn = None
        try:
            while 1:
                self.ignore_invalidations = True
                # Get network connection to primary master
                while fail_count < self.max_reconnection_to_master:
                    self.nm.reset()
                    if self.primary_master_node is not None:
                        # If I know a primary master node, pinpoint it.
                        node = self.primary_master_node
                        self.primary_master_node = None
                    else:
                        # Otherwise, check one by one.
                        master_list = self.nm.getMasterList()
                        if not master_list:
                            # XXX: On shutdown, it already happened that this
                            #      list is empty, leading to ZeroDivisionError.
                            #      This looks a minor issue so let's wait to
                            #      have more information.
                            logging.error('%r', self.__dict__)
                        index = (index + 1) % len(master_list)
                        node = master_list[index]
                    # Connect to master
                    conn = MTClientConnection(self,
                        self.notifications_handler,
                        node=node,
                        dispatcher=self.dispatcher)
                    p = Packets.RequestIdentification(NodeTypes.CLIENT,
                        self.uuid, None, self.name, None,
                        {'read_only': True} if self.read_only else {})
                    try:
                        ask(conn, p, handler=handler)
                    except ConnectionClosed:
                        conn = None
                        fail_count += 1
                    else:
                        self.primary_master_node = node
                        break
                else:
                    raise NEOPrimaryMasterLost(
                        "Too many connection failures to the primary master")
                logging.info('Connected to %s', self.primary_master_node)
                try:
                    # Request identification and required informations to be
                    # operational. Might raise ConnectionClosed so that the new
                    # primary can be looked-up again.
                    logging.info('Initializing from master')
                    ask(conn, Packets.AskLastTransaction(), handler=handler)
                    if self.pt.operational():
                        break
                except ConnectionClosed:
                    conn = self.primary_master_node = None
                    logging.error('Connection to %s lost',
                                  self.trying_master_node)
                fail_count += 1
        except:
            if conn is not None:
                conn.close()
            raise
        logging.info("Connected and ready")
        return conn

    def getStorageConnection(self, node):
        conn = node._connection # XXX
        if node.isRunning() if conn is None else not node._identified:
            with self._connecting_to_storage_node:
                conn = node._connection # XXX
                if conn is None:
                    return self._connectToStorageNode(node)
        return conn

    def _connectToStorageNode(self, node):
        if self.master_conn is None:
            raise NEOPrimaryMasterLost
        conn = MTClientConnection(self, self.storage_event_handler, node,
                                  dispatcher=self.dispatcher)
        p = Packets.RequestIdentification(NodeTypes.CLIENT,
            self.uuid, None, self.name, self.id_timestamp, {})
        try:
            self._ask(conn, p, handler=self.storage_bootstrap_handler)
        except ConnectionClosed:
            logging.error('Connection to %r failed', node)
        except NodeNotReady:
            logging.info('%r not ready', node)
        else:
            logging.info('Connected %r', node)
            # Make sure this node will be considered for the next reads
            # even if there was a previous recent failure.
            self._node_failure_dict.pop(node.getUUID(), None)
            return conn
        self._node_failure_dict[node.getUUID()] = time.time() + MAX_FAILURE_AGE

    def getCellSortKey(self, cell, random=random.random):
        # Prefer a node that didn't fail recently.
        failure = self._node_failure_dict.get(cell.getUUID())
        if failure:
            if time.time() < failure:
                # Or order by date of connection failure.
                return failure
            # Do not use 'del' statement: we didn't lock, so another
            # thread might have removed uuid from _node_failure_dict.
            self._node_failure_dict.pop(cell.getUUID(), None)
        # A random one, connected or not, is a trivial and quite efficient way
        # to distribute the load evenly. On write accesses, a client connects
        # to all nodes of touched cells, but before that, or if a client is
        # specialized to only do read-only accesses, it should not limit
        # itself to only use the first connected nodes.
        return random()

    def registerDB(self, db, limit):
        self._db = db

    def getDB(self):
        return self._db

    def new_oid(self):
        """Get a new OID."""
        self._oid_lock_acquire()
        try:
            for oid in self.new_oids:
                break
            else:
                # Get new oid list from master node
                # we manage a list of oid here to prevent
                # from asking too many time new oid one by one
                # from master node
                self._askPrimary(Packets.AskNewOIDs(100))
                for oid in self.new_oids:
                    break
                else:
                    raise NEOStorageError('new_oid failed')
            self.last_oid = oid
            return oid
        finally:
            self._oid_lock_release()

    def getObjectCount(self):
        # return the last OID used, this is inaccurate
        return int(u64(self.last_oid))

    def _askStorageForRead(self, object_id, packet, askStorage=None):
        pt = self.pt
        # BBB: On Py2, it can be a subclass of bytes (binary from zodbpickle).
        if isinstance(object_id, bytes):
            object_id = pt.getPartition(object_id)
        if askStorage is None:
            askStorage = self._askStorage
        # Failure condition with minimal overhead: most of the time, only the
        # following line is executed. In case of storage errors, we retry each
        # node at least once, without looping forever.
        failed = 0
        while 1:
            cell_list = pt.getCellList(object_id, True)
            cell_list.sort(key=self.getCellSortKey)
            for cell in cell_list:
                node = cell.getNode()
                conn = self.getStorageConnection(node)
                if conn is not None:
                    try:
                        return askStorage(conn, packet)
                    except ConnectionClosed:
                        pass
                    except NEOStorageReadRetry as e:
                        if e.args[0]:
                            continue
                failed += 1
            if not pt.filled():
                raise NEOPrimaryMasterLost
            if len(cell_list) < failed: # too many failures
                raise NEOStorageError('no storage available')
            # Do not retry too quickly, for example
            # when there's an incoming PT update.
            self.sync()

    def load(self, oid, tid=None, before_tid=None):
        """
        Internal method which manage load, loadSerial and loadBefore.
        OID and TID (serial) parameters are expected packed.
        oid
            OID of object to get.
        tid
            If given, the exact serial at which OID is desired.
            before_tid should be None.
        before_tid
            If given, the excluded upper bound serial at which OID is desired.
            serial should be None.

        Return value: (3-tuple)
        - Object data (None if object creation was undone).
        - Serial of given data.
        - Next serial at which object exists, or None. Only set when tid
          parameter is not None.

        Exceptions:
            NEOStorageError
                technical problem
            NEOStorageNotFoundError
                object exists but no data satisfies given parameters
            NEOStorageDoesNotExistError
                object doesn't exist
            NEOStorageCreationUndoneError
                object existed, but its creation was undone

        Note that loadSerial is used during conflict resolution to load
        object's current version, which is not visible to us normally (it was
        committed after our snapshot was taken).
        """
        # TODO:
        # - rename parameters (here? and in handlers & packet definitions)
        acquired = False
        lock = self._cache_lock
        try:
            while 1:
                with lock:
                    if tid:
                        result = self._cache.load(oid, tid + b'*')
                        assert not result or result[1] == tid
                    else:
                        result = self._cache.load(oid, before_tid)
                    if result:
                        return result
                    u64(oid) # check type to not pollute _loading
                             #  or crash storage nodes
                    load_lock = self._loading[oid][0]
                    acquired = load_lock.acquire(0)
                # Several concurrent cache misses for the same oid are probably
                # for the same tid so we use a per-oid lock to avoid asking the
                # same data to the storage node.
                if acquired:
                    # The first thread does load from storage,
                    # and fills cache with the response.
                    break
                # The other threads wait for the first one to complete and
                # loop, possibly resulting in a new cache miss if a different
                # tid is actually wanted or if the data was too big.
                with load_lock:
                    pass
            # While the cache lock is released, an arbitrary number of
            # invalidations may be processed, for this oid or not. And at this
            # precise moment, if both tid and before_tid are None (which is
            # unlikely to happen with recent ZODB), self.last_tid can be any
            # new tid. Since we can get any serial from storage, fixing
            # next_tid requires to keep a list of all possible serials.

            # When not bound to a ZODB Connection, load() may be the
            # first method called and last_tid may still be None.
            # This happens, for example, when opening the DB.
            if not (tid or before_tid) and self.last_tid:
                # Do not get something more recent than the last invalidation
                # we got from master.
                before_tid = p64(u64(self.last_tid) + 1)
            data, tid, next_tid, _ = self._loadFromStorage(oid, tid, before_tid)
            with lock:
                loading = self._loading.pop(oid, None)
                if loading:
                    assert loading[0] is load_lock
                    if not next_tid:
                        for t in loading[1]:
                            if tid < t:
                                next_tid = t
                                break
                    self._cache.store(oid, data, tid, next_tid)
                # Else, we just reconnected to the master.
                load_lock.release()
        except:
            if acquired:
                with lock:
                    self._loading.pop(oid, None)
                    load_lock.release()
            raise
        return data, tid, next_tid

    def _loadFromStorage(self, oid, at_tid, before_tid):
        wrong_checksum = [] # Py3
        def askStorage(conn, packet):
            tid, next_tid, compression, checksum, data, data_tid \
                = self._askStorage(conn, packet)
            if data or checksum != ZERO_HASH:
                if checksum != makeChecksum(data):
                    logging.error('wrong checksum from %s for %s@%s',
                                  conn, dump(oid), dump(tid))
                    wrong_checksum.append((tid, next_tid, compression,
                                           checksum, data, data_tid))
                    raise NEOStorageReadRetry(False)
                return (decompress_list[compression](data),
                        tid, next_tid, data_tid)
            raise NEOStorageCreationUndoneError(dump(oid))
        try:
            return self._askStorageForRead(oid,
                Packets.AskObject(oid, at_tid, before_tid),
                askStorage)
        except NEOStorageError:
            if not wrong_checksum:
                raise
        tid, next_tid, compression, checksum, data, data_tid = \
            wrong_checksum[0]
        if self.ignore_wrong_checksum:
            try:
                data = decompress_list[compression](data)
            except Exception:
                data = b''
            return data, tid, next_tid, data_tid
        raise NEOStorageWrongChecksum(oid, tid)

    def tpc_begin(self, storage, transaction, tid=None, status=' '):
        """Begin a new transaction."""
        if self.read_only:
            raise ReadOnlyError
        # First get a transaction, only one is allowed at a time
        txn_context = self._txn_container.new(transaction)
        # use the given TID or request a new one to the master
        answer_ttid = self._askPrimary(Packets.AskBeginTransaction(tid))
        if answer_ttid is None:
            raise NEOStorageError('tpc_begin failed')
        assert tid in (None, answer_ttid), (tid, answer_ttid)
        txn_context.Storage = storage
        txn_context.ttid = answer_ttid

    def store(self, oid, serial, data, version, transaction):
        """Store object."""
        logging.debug('storing oid %s serial %s', dump(oid), dump(serial))
        if not serial: # BBB
            serial = ZERO_TID
        self._store(self._txn_container.get(transaction), oid, serial, data)

    def _store(self, txn_context, oid, serial, data, data_serial=None):
        ttid = txn_context.ttid
        if data is None:
            # This is some undo: either a no-data object (undoing object
            # creation) or a back-pointer to an earlier revision (going back to
            # an older object revision).
            compressed_data = b''
            compression = 0
            checksum = ZERO_HASH
            if data_serial is None:
                assert oid not in txn_context.resolved_dict, oid
                txn_context.delete_list.append(oid)
        else:
            size, compression, compressed_data = self.compress(data)
            checksum = makeChecksum(compressed_data)
            txn_context.data_size += size
        # Store object in tmp cache
        packet = Packets.AskStoreObject(oid, serial, compression,
            checksum, compressed_data, data_serial, ttid)
        txn_context.data_dict[oid] = data, serial, txn_context.write(
            self, packet, oid, oid=oid, serial=serial)

        while txn_context.data_size >= self._cache.max_size:
            self._waitAnyTransactionMessage(txn_context)
        self._waitAnyTransactionMessage(txn_context, False)

    def _handleConflicts(self, txn_context):
        data_dict = txn_context.data_dict
        pop_conflict = txn_context.conflict_dict.popitem
        resolved_dict = txn_context.resolved_dict
        tryToResolveConflict = txn_context.Storage.tryToResolveConflict
        while 1:
            # We iterate over conflict_dict, and clear it,
            # because new items may be added by calls to _store.
            try:
                oid, serial = pop_conflict()
            except KeyError:
                return
            data, old_serial, _ = data_dict.pop(oid)
            if data is CHECKED_SERIAL:
                raise ReadConflictError(oid=oid,
                    serials=(serial, old_serial))
            # data can be None if a conflict happens when undoing creation
            if data:
                txn_context.data_size -= len(data)
            if self.last_tid < serial:
                self.sync() # possible late invalidation (very rare)
            try:
                data = tryToResolveConflict(oid, serial, old_serial, data)
            except ConflictError:
                logging.info(
                    'Conflict resolution failed for %s@%s with %s',
                    dump(oid), dump(old_serial), dump(serial))
                # With recent ZODB, get_pickle_metadata (from ZODB.utils)
                # does not support empty values, so do not pass 'data'
                # in this case.
                raise ConflictError(oid=oid, serials=(serial, old_serial),
                                    data=data or None)
            logging.info(
                'Conflict resolution succeeded for %s@%s with %s',
                dump(oid), dump(old_serial), dump(serial))
            # Mark this conflict as resolved
            assert oid not in txn_context.delete_list, oid
            resolved_dict[oid] = serial
            # Try to store again
            self._store(txn_context, oid, serial, data)

    def _askStorageForWrite(self, txn_context, uuid, packet):
          conn = txn_context.conn_dict[uuid]
          try:
              return conn.ask(packet, queue=txn_context.queue)
          except AttributeError:
              if conn is not None:
                  raise
          except ConnectionClosed:
              txn_context.conn_dict[uuid] = None

    def waitResponses(self, queue):
        """Wait for all requests to be answered (or their connection to be
        detected as closed)"""
        pending = self.dispatcher.pending
        _waitAnyMessage = self._waitAnyMessage
        while pending(queue):
            _waitAnyMessage(queue)

    def waitStoreResponses(self, txn_context):
        queue = txn_context.queue
        pending = self.dispatcher.pending
        _waitAnyTransactionMessage = self._waitAnyTransactionMessage
        while pending(queue):
            _waitAnyTransactionMessage(txn_context)
        if txn_context.data_dict:
            raise NEOStorageError('could not store/check all oids')

    def tpc_vote(self, transaction):
        """Store current transaction."""
        txn_context = self._txn_container.get(transaction)
        self.waitStoreResponses(txn_context)
        txn_context.stored = True
        ttid = txn_context.ttid
        try: # TransactionMetaData (ZODB >= 5.6.0)
            ext = transaction.extension_bytes
        except AttributeError:
            ext = transaction._extension
            ext = dumps(ext, _protocol) if ext else b''
        # The type of user/description depends on the type of transaction:
        # - bytes if TransactionMetaData
        # - unicode if Transaction (transaction >= 2.0.3)
        user = transaction.user
        desc = transaction.description
        if not isinstance(user, bytes):
            user = user.encode('utf-8')
        if not isinstance(desc, bytes):
            desc = desc.encode('utf-8')
        packet = Packets.AskStoreTransaction(ttid, user,
            desc, ext, list(txn_context.cache_dict),
            txn_context.pack)
        queue = txn_context.queue
        conn_dict = txn_context.conn_dict
        # Ask in parallel all involved storage nodes to commit object metadata.
        # Nodes that store the transaction metadata get a special packet.
        trans_nodes = txn_context.write(self, packet, ttid)
        packet = Packets.AskVoteTransaction(ttid)
        for uuid in conn_dict:
            if uuid not in trans_nodes:
                self._askStorageForWrite(txn_context, uuid, packet)
        self.waitStoreResponses(txn_context)
        if None in six.itervalues(conn_dict): # unlikely
            # If some writes failed, we must first check whether
            # all oids have been locked by at least one node.
            failed = {node.getUUID(): node.isRunning()
                for node in self.nm.getStorageList()
                if conn_dict.get(node.getUUID(), 0) is None}
            if txn_context.lockless_dict:
                getCellList = self.pt.getCellList
                for offset, uuid_set in six.iteritems(txn_context.lockless_dict):
                    for cell in getCellList(offset):
                        uuid = cell.getUUID()
                        if not (uuid in failed or uuid in uuid_set):
                            break
                    else:
                        # Very unlikely. Trying to recover
                        # is not worth the complexity.
                        raise NEOStorageError(
                            'partition %s not fully write-locked' % offset)
            failed = [uuid for uuid, running in six.iteritems(failed) if running]
            # If there are running nodes for which some writes failed, ask the
            # master whether they can be disconnected while keeping the cluster
            # operational. If possible, this will happen during tpc_finish.
            if failed:
                try:
                    self._askPrimary(Packets.FailedVote(ttid, failed))
                except ConnectionClosed:
                    pass
        txn_context.voted = True
        # We must not go further if connection to master was lost since
        # tpc_begin, to lower the probability of failing during tpc_finish.
        # IDEA: We can improve in 2 opposite directions:
        #       - In the case of big transactions, it would be useful to
        #         also detect failures earlier.
        #       - If possible, recover from master failure.
        if txn_context.error:
            raise NEOStorageError(txn_context.error)
        return txn_context.resolved_dict

    def tpc_abort(self, transaction):
        """Abort current transaction."""
        txn_context = self._txn_container.pop(transaction)
        if txn_context is None:
            return
        # We want the involved nodes to abort a transaction after any
        # other packet sent by the client for this transaction. IOW, if we
        # already have a connection with a storage node, potentially with
        # a pending write, aborting only via the master may lead to a race
        # condition. The consequence would be that storage nodes lock oids
        # forever.
        p = Packets.AbortTransaction(txn_context.ttid, ())
        for conn in six.itervalues(txn_context.conn_dict):
            if conn is not None:
                try:
                    conn.send(p)
                except ConnectionClosed:
                    pass
        # Because we want to be sure that the involved nodes are notified,
        # we still have to send the full list to the master. Most of the
        # time, the storage nodes get 2 AbortTransaction packets, and the
        # second one is rarely useful. Another option would be that the
        # storage nodes keep a list of aborted transactions, but the
        # difficult part would be to avoid a memory leak.
        try:
            notify = self.master_conn.send
        except AttributeError:
            pass
        else:
            try:
                notify(Packets.AbortTransaction(txn_context.ttid,
                    list(txn_context.conn_dict)))
            except ConnectionClosed:
                pass
        # No need to flush queue, as it will be destroyed on return,
        # along with txn_context.
        self.dispatcher.forget_queue(txn_context.queue, flush_queue=False)

    def tpc_finish(self, transaction, f=None):
        """Finish current transaction

        To avoid inconsistencies between several databases involved in the
        same transaction, an IStorage implementation must do its best not to
        fail in tpc_finish. In particular, making a transaction permanent
        should ideally be as simple as switching a bit permanently.

        In NEO, all the data (with the exception of the tid, simply because
        it is not known yet) is already flushed on disk at the end on the vote.
        During tpc_finish, all nodes storing the transaction metadata are asked
        to commit by saving the new tid and flushing again: for SQL backends,
        it's just an UPDATE of 1 cell. At last, the metadata is moved to
        a final place so that the new transaction is readable, but this is
        something that can always be replayed (during the verification phase)
        if any failure happens.
        """
        txn_container = self._txn_container
        if not txn_container.get(transaction).voted:
            self.tpc_vote(transaction)
        txn_context = txn_container.pop(transaction)
        cache_dict = txn_context.cache_dict
        getPartition = self.pt.getPartition
        checked = set()
        for oid, data in list(cache_dict.items()) if six.PY3 else cache_dict.items():
            if data is CHECKED_SERIAL:
                del cache_dict[oid]
                checked.add(getPartition(oid))
        deleted = txn_context.delete_list
        if deleted:
            oids = set(cache_dict)
            oids.difference_update(deleted)
            deleted = list(map(getPartition, deleted))
        else:
            oids = list(cache_dict)
        ttid = txn_context.ttid
        p = Packets.AskFinishTransaction(ttid, oids, deleted, checked,
                                         txn_context.pack)
        try:
            tid = self._askPrimary(p, cache_dict=cache_dict, callback=f)
            assert tid
        except ConnectionClosed:
            tid = self._getFinalTID(ttid)
            if not tid:
                raise
        return tid

    def _getFinalTID(self, ttid):
        try:
            p = Packets.AskFinalTID(ttid)
            while 1:
                try:
                    tid = self._askPrimary(p)
                    break
                except ConnectionClosed:
                    pass
            if tid == MAX_TID:
                while 1:
                    try:
                        return self._askStorageForRead(ttid, p)
                    except NEOPrimaryMasterLost:
                        pass
            elif tid:
                return tid
        except Exception:
            logging.exception("Failed to get final tid for TXN %s",
                              dump(ttid))

    def undo(self, undone_tid, txn):
        txn_context = self._txn_container.get(txn)
        txn_info, txn_ext = self._getTransactionInformation(undone_tid)

        # Regroup objects per partition, to ask a minimum set of storage.
        partition_oid_dict = defaultdict(list)
        for oid in txn_info['oids']:
            partition_oid_dict[self.pt.getPartition(oid)].append(oid)

        # Ask storage the undo serial (serial at which object's previous data
        # is)
        getCellList = self.pt.getCellList
        getCellSortKey = self.getCellSortKey
        getConnForNode = self.getStorageConnection
        queue = self._thread_container.queue
        ttid = txn_context.ttid
        undo_object_tid_dict = {}
        snapshot_tid = p64(u64(self.last_tid) + 1)
        kw = {
            'queue': queue,
            'partition_oid_dict': partition_oid_dict,
            'undo_object_tid_dict': undo_object_tid_dict,
        }
        while 1:
            for partition, oid_list in six.iteritems(partition_oid_dict):
                cell_list = [cell
                    for cell in getCellList(partition, readable=True)
                    # Exclude nodes that may have missed previous resolved
                    # conflicts. For example, if a network failure happened
                    # only between the client and the storage, the latter would
                    # still be readable until we commit.
                    if txn_context.conn_dict.get(cell.getUUID(), 0) is not None]
                conn = getConnForNode(
                    min(cell_list, key=getCellSortKey).getNode())
                try:
                    conn.ask(Packets.AskObjectUndoSerial(ttid,
                        snapshot_tid, undone_tid, oid_list),
                        partition=partition, **kw)
                except AttributeError:
                    if conn is not None:
                        raise
                except ConnectionClosed:
                    pass

            # Wait for all AnswerObjectUndoSerial. We might get
            # OidNotFoundError, meaning that objects in transaction's oid_list
            # do not exist any longer. This is the symptom of a pack, so forbid
            # undoing transaction when it happens.
            try:
                self.waitResponses(queue)
            except NEOStorageNotFoundError:
                self.dispatcher.forget_queue(queue)
                raise UndoError('non-undoable transaction')

            if not partition_oid_dict:
                break
            # Do not retry too quickly, for example
            # when there's an incoming PT update.
            self.sync()

        # Send undo data to all storage nodes.
        for oid, (current_serial, undo_serial, is_current) in \
                six.iteritems(undo_object_tid_dict):
            if is_current:
                if undo_serial:
                    # The data are used:
                    # - by outdated cells that don't have them
                    # - if there's a conflict to resolve
                    # Otherwise, they're ignored.
                    # IDEA: So as an optimization, if all cells we're going to
                    #       write are readable, we could move the following
                    #       load to _handleConflicts and simply pass None here.
                    #       But evaluating such condition without race
                    #       condition is not easy:
                    #       1. The transaction context must have established
                    #          with all nodes that will be involved (e.g.
                    #          doable while processing partition_oid_dict).
                    #       2. The partition table must be up-to-date by
                    #          pinging the master (i.e. self.sync()).
                    #       3. At last, the PT can be looked up here.
                    try:
                        data = self.load(oid, undo_serial)[0]
                    except NEOStorageCreationUndoneError:
                        data = None
                else:
                    data = None
            else:
                # Serial being undone is not the latest version for this
                # object. This is an undo conflict, try to resolve it.
                try:
                    # Load the latest version we are supposed to see
                    if current_serial == ttid:
                        # XXX: see TODO below
                        data = txn_context.cache_dict[oid]
                    else:
                        data = self.load(oid, current_serial)[0]
                    # Load the version we were undoing to
                    undo_data = self.load(oid, undo_serial)[0]
                except NEOStorageNotFoundError:
                    raise UndoError('Object not found while resolving undo '
                        'conflict')
                # Resolve conflict
                try:
                    data = txn_context.Storage.tryToResolveConflict(
                        oid, current_serial, undone_tid, undo_data, data)
                except ConflictError:
                    raise UndoError('Some data were modified by a later ' \
                        'transaction', oid)
                undo_serial = None
                # TODO: The situation is similar to deadlock avoidance.
                #       Reenable the cache size limit to avoid OOM when there's
                #       a huge amount conflicting data, and get the data back
                #       from the storage when it's not in cache_dict anymore.
                txn_context.cache_size = - float('inf')
            self._store(txn_context, oid, current_serial, data, undo_serial)

        self.waitStoreResponses(txn_context)
        return None, list(undo_object_tid_dict)

    def _getTransactionInformation(self, tid):
        return self._askStorageForRead(tid,
            Packets.AskTransactionInformation(tid))

    def undoLog(self, first, last, filter=None, block=0):
        # XXX: undoLog is broken
        if last < 0:
            # See FileStorage.py for explanation
            last = first - last

        # First get a list of transactions from all storage nodes.
        # Each storage node will return TIDs only for UP_TO_DATE state and
        # FEEDING state cells
        queue = self._thread_container.queue
        packet = Packets.AskTIDs(first, last, INVALID_PARTITION)
        tid_set = set()
        for storage_node in self.pt.getNodeSet(True):
            conn = self.getStorageConnection(storage_node)
            if conn is None:
                continue
            conn.ask(packet, queue=queue, tid_set=tid_set)

        # Wait for answers from all storages.
        # TODO: Results are incomplete when readable cells move concurrently
        #       from one storage to another. We detect when this happens and
        #       retry.
        self.waitResponses(queue)

        # Reorder tids
        ordered_tids = sorted(tid_set, reverse=True)
        logging.debug("UndoLog tids %s", list(map(dump, ordered_tids)))
        # For each transaction, get info
        undo_info = []
        append = undo_info.append
        for tid in ordered_tids:
            (txn_info, txn_ext) = self._getTransactionInformation(tid)
            if filter is None or filter(txn_info):
                txn_info.pop('packed')
                txn_info.pop("oids")
                if txn_ext:
                    txn_info.update(loads(txn_ext))
                append(txn_info)
                if len(undo_info) >= last - first:
                    break
        # Check we return at least one element, otherwise call
        # again but extend offset
        if len(undo_info) == 0 and not block:
            undo_info = self.undoLog(first=first, last=last*5, filter=filter,
                    block=1)
        return undo_info

    def transactionLog(self, start, stop, limit):
        tid_list = []
        # request a tid list for each partition
        for offset in range(self.pt.getPartitions()):
            r = self._askStorageForRead(offset,
                Packets.AskTIDsFrom(start, stop, limit, offset))
            if r:
                tid_list = list(heapq.merge(tid_list, r))
                if len(tid_list) >= limit:
                    del tid_list[limit:]
                    stop = tid_list[-1]
        # request transactions informations
        txn_list = []
        append = txn_list.append
        tid = None
        for tid in tid_list:
            (txn_info, txn_ext) = self._getTransactionInformation(tid)
            txn_info['ext'] = loads(txn_ext) if txn_ext else {}
            append(txn_info)
        return (tid, txn_list)

    def oids(self, tid=None, min_oid=None, max_oid=None):
        if tid is None:
            tid = self.last_tid
        h = []
        def oids(offset, oid=min_oid or ZERO_OID):
            while True:
                oid, oid_list = self._askStorageForRead(offset,
                    Packets.AskOIDsFrom(offset, 1000, oid, tid))
                i = partial(next, iter(oid_list))
                try:
                    return [i(), i, offset, oid]
                except StopIteration:
                    if oid is None or None is not max_oid < oid:
                        return
        h = [x for x in map(oids, range(self.pt.getPartitions())) if x]
        heapq.heapify(h)
        heappop = partial(heapq.heappop, h)
        heappushpop = partial(heapq.heappushpop, h)
        while h:
            x = heappop()
            while True:
                oid = x[0]
                if None is not max_oid < oid:
                    return
                yield oid
                try:
                    x[0] = x[1]()
                except StopIteration:
                    oid = x[3]
                    if oid is None:
                        break
                    x = oids(x[2], oid)
                    if not x:
                        break
                x = heappushpop(x)

    def history(self, oid, size=1, filter=None):
        packet = Packets.AskObjectHistory(oid, 0, size)
        result = []
        # history_list is already sorted descending (by the storage)
        for serial, size in self._askStorageForRead(oid, packet):
                txn_info, txn_ext = self._getTransactionInformation(serial)
                # create history dict
                del txn_info['id']
                del txn_info['oids']
                del txn_info['packed']
                txn_info['tid'] = serial
                txn_info['version'] = ''
                txn_info['size'] = size
                if filter is None or filter(txn_info):
                    result.append(txn_info)
                if txn_ext:
                    txn_info.update(loads(txn_ext))
        return result

    def importFrom(self, storage, source):
        # TODO: The main difference with BaseStorage implementation is that
        #       preindex can't be filled with the result 'store' (tid only
        #       known after 'tpc_finish'. This method could be dropped if we
        #       implemented IStorageRestoreable (a wrapper around source would
        #       still be required for partial import).
        preindex = {}
        for transaction in source.iterator():
            tid = transaction.tid
            self.tpc_begin(storage, transaction, tid, transaction.status)
            for r in transaction:
                oid = r.oid
                try:
                    pre = preindex[oid]
                except KeyError:
                    try:
                        pre = self.load(oid)[1]
                    except NEOStorageNotFoundError:
                        pre = ZERO_TID
                self.store(oid, pre, r.data, r.version, transaction)
                preindex[oid] = tid
            conflicted = self.tpc_vote(transaction)
            assert not conflicted, conflicted
            real_tid = self.tpc_finish(transaction)
            assert real_tid == tid, (real_tid, tid)

    from .iterator import iterator

    def sync(self):
        self._askPrimary(Packets.Ping())

    def setPackOrder(self, transaction, tid, oids=None):
        self._txn_container.get(transaction).pack = oids and sorted(oids), tid

    def pack(self, tid, oids=None):
        if tid == ZERO_TID:
            return
        transaction = TransactionMetaData(description=TXN_PACK_DESC)
        try:
            self.tpc_begin(None, transaction)
            self.setPackOrder(transaction, tid, oids)
            tid = self.tpc_finish(transaction)
        except:
            self.tpc_abort(transaction)
            raise
        if not self.wait_for_pack:
            return
        # Waiting for pack to be finished is only needed
        # to make ZODB unit tests pass.
        self._askPrimary(Packets.WaitForPack(tid))
        # It should not be otherwise required (clients should be free to load
        # old data as long as it is available in cache, event if it was pruned
        # by a pack), so don't bother invalidating on other clients.
        with self._cache_lock:
            self._cache.clear()

    def getLastTID(self, oid):
        return self.load(oid)[1]

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._checkCurrentSerialInTransaction(
            self._txn_container.get(transaction), oid, serial)

    def _checkCurrentSerialInTransaction(self, txn_context, oid, serial):
        ttid = txn_context.ttid
        # ZODB.Connection performs calls 'checkCurrentSerialInTransaction'
        # after stores, and skips oids that have been successfully stored.
        assert oid not in txn_context.cache_dict, oid
        assert oid not in txn_context.data_dict, oid
        packet = Packets.AskCheckCurrentSerial(ttid, oid, serial)
        txn_context.data_dict[oid] = CHECKED_SERIAL, serial, txn_context.write(
            self, packet, oid, oid=oid, serial=serial)
        self._waitAnyTransactionMessage(txn_context, False)

