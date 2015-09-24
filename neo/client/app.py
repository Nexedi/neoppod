#
# Copyright (C) 2006-2015  Nexedi SA
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

from cPickle import dumps, loads
from zlib import compress, decompress
from random import shuffle
import heapq
import time
import weakref
from functools import partial

from ZODB.POSException import UndoError, StorageTransactionError, ConflictError
from ZODB.POSException import ReadConflictError
from ZODB.ConflictResolution import ResolvedSerial
from persistent.TimeStamp import TimeStamp

from neo.lib import logging
from neo.lib.protocol import NodeTypes, Packets, \
    INVALID_PARTITION, ZERO_HASH, ZERO_TID
from neo.lib.event import EventManager
from neo.lib.util import makeChecksum, dump
from neo.lib.locking import Empty, Lock, SimpleQueue
from neo.lib.connection import MTClientConnection, ConnectionClosed
from neo.lib.node import NodeManager
from .exception import NEOStorageError, NEOStorageCreationUndoneError
from .exception import NEOStorageNotFoundError
from .handlers import storage, master
from neo.lib.dispatcher import Dispatcher, ForgottenPacket
from neo.lib.threaded_app import ThreadedApplication
from .cache import ClientCache
from .pool import ConnectionPool
from neo.lib.util import p64, u64, parseMasterList
from neo.lib.debug import register as registerLiveDebugger

CHECKED_SERIAL = master.CHECKED_SERIAL

try:
    from Signals.Signals import SignalHandler
except ImportError:
    SignalHandler = None
if SignalHandler:
    import signal
    SignalHandler.registerHandler(signal.SIGUSR2, logging.reopen)


class TransactionContainer(dict):

    def pop(self, txn):
        return dict.pop(self, id(txn), None)

    def get(self, txn):
        try:
            return self[id(txn)]
        except KeyError:
            raise StorageTransactionError("unknown transaction %r" % txn)

    def new(self, txn):
        key = id(txn)
        if key in self:
            raise StorageTransactionError("commit of transaction %r"
                                          " already started" % txn)
        context = self[key] = {
            'queue': SimpleQueue(),
            'txn': txn,
            'ttid': None,
            'data_dict': {},
            'data_size': 0,
            'cache_dict': {},
            'cache_size': 0,
            'object_base_serial_dict': {},
            'object_serial_dict': {},
            'object_stored_counter_dict': {},
            'conflict_serial_dict': {},
            'resolved_conflict_serial_dict': {},
            'involved_nodes': set(),
        }
        return context


class Application(ThreadedApplication):
    """The client node application."""

    def __init__(self, master_nodes, name, compress=True, **kw):
        super(Application, self).__init__(parseMasterList(master_nodes),
                                          name, **kw)
        # Internal Attributes common to all thread
        self._db = None
        self.cp = ConnectionPool(self)
        self.primary_master_node = None
        self.trying_master_node = None

        # no self-assigned UUID, primary master will supply us one
        self._cache = ClientCache()
        self._loading_oid = None
        self.new_oid_list = ()
        self.last_oid = '\0' * 8
        self.last_tid = None
        self.storage_event_handler = storage.StorageEventHandler(self)
        self.storage_bootstrap_handler = storage.StorageBootstrapHandler(self)
        self.storage_handler = storage.StorageAnswersHandler(self)
        self.primary_handler = master.PrimaryAnswersHandler(self)
        self.primary_bootstrap_handler = master.PrimaryBootstrapHandler(self)
        self.notifications_handler = master.PrimaryNotificationsHandler( self)
        self._txn_container = TransactionContainer()
        # Lock definition :
        # _load_lock is used to make loading and storing atomic
        lock = Lock()
        self._load_lock_acquire = lock.acquire
        self._load_lock_release = lock.release
        # _oid_lock is used in order to not call multiple oid
        # generation at the same time
        lock = Lock()
        self._oid_lock_acquire = lock.acquire
        self._oid_lock_release = lock.release
        lock = Lock()
        # _cache_lock is used for the client cache
        self._cache_lock_acquire = lock.acquire
        self._cache_lock_release = lock.release
        # _connecting_to_master_node is used to prevent simultaneous master
        # node connection attemps
        self._connecting_to_master_node = Lock()
        self.compress = compress

    def __getattr__(self, attr):
        if attr == 'pt':
            self._getMasterConnection()
        return self.__getattribute__(attr)

    @property
    def txn_contexts(self):
        # do not iter lazily to avoid race condition
        return self._txn_container.values

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
            if packet is None or isinstance(packet, ForgottenPacket):
                # connection was closed or some packet was forgotten
                continue
            block = False
            try:
                _handlePacket(conn, packet, kw)
            except ConnectionClosed:
                pass

    def _waitAnyTransactionMessage(self, txn_context, block=True):
        """
        Just like _waitAnyMessage, but for per-transaction exchanges, rather
        than per-thread.
        """
        queue = txn_context['queue']
        self.setHandlerData(txn_context)
        try:
            self._waitAnyMessage(queue, block=block)
        finally:
            # Don't leave access to thread context, even if a raise happens.
            self.setHandlerData(None)

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
                    self.new_oid_list = ()
                    result = self.master_conn = self._connectToPrimaryNode()
        return result

    def _connectToPrimaryNode(self):
        """
            Lookup for the current primary master node
        """
        logging.debug('connecting to primary master...')
        self.start()
        index = -1
        ask = self._ask
        handler = self.primary_bootstrap_handler
        while 1:
            # Get network connection to primary master
            while 1:
                if self.primary_master_node is not None:
                    # If I know a primary master node, pinpoint it.
                    self.trying_master_node = self.primary_master_node
                    self.primary_master_node = None
                else:
                    # Otherwise, check one by one.
                    master_list = self.nm.getMasterList()
                    index = (index + 1) % len(master_list)
                    self.trying_master_node = master_list[index]
                # Connect to master
                conn = MTClientConnection(self,
                        self.notifications_handler,
                        node=self.trying_master_node,
                        dispatcher=self.dispatcher)
                # Query for primary master node
                if conn.getConnector() is None:
                    # This happens if a connection could not be established.
                    logging.error('Connection to master node %s failed',
                                  self.trying_master_node)
                    continue
                try:
                    ask(conn, Packets.RequestIdentification(
                            NodeTypes.CLIENT, self.uuid, None, self.name),
                        handler=handler)
                except ConnectionClosed:
                    continue
                # If we reached the primary master node, mark as connected
                if self.primary_master_node is not None and \
                   self.primary_master_node is self.trying_master_node:
                    break
            logging.info('Connected to %s', self.primary_master_node)
            try:
                # Request identification and required informations to be
                # operational. Might raise ConnectionClosed so that the new
                # primary can be looked-up again.
                logging.info('Initializing from master')
                ask(conn, Packets.AskNodeInformation(), handler=handler)
                ask(conn, Packets.AskPartitionTable(), handler=handler)
                ask(conn, Packets.AskLastTransaction(), handler=handler)
                if self.pt.operational():
                    break
            except ConnectionClosed:
                logging.error('Connection to %s lost', self.trying_master_node)
                self.primary_master_node = None
        logging.info("Connected and ready")
        return conn

    def registerDB(self, db, limit):
        self._db = db

    def getDB(self):
        return self._db

    def new_oid(self):
        """Get a new OID."""
        self._oid_lock_acquire()
        try:
            if not self.new_oid_list:
                # Get new oid list from master node
                # we manage a list of oid here to prevent
                # from asking too many time new oid one by one
                # from master node
                self._askPrimary(Packets.AskNewOIDs(100))
                if not self.new_oid_list:
                    raise NEOStorageError('new_oid failed')
            self.last_oid = oid = self.new_oid_list.pop()
            return oid
        finally:
            self._oid_lock_release()

    def getObjectCount(self):
        # return the last OID used, this is inaccurate
        return int(u64(self.last_oid))

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
        object's current version, which is not visible to us normaly (it was
        committed after our snapshot was taken).
        """
        # TODO:
        # - rename parameters (here? and in handlers & packet definitions)

        acquire = self._cache_lock_acquire
        release = self._cache_lock_release
        # XXX: Is it possible this giant lock ?
        #      See commit b77c946d67c9d7cc1e9ee9b15437568dee144aa4
        #      for a way to invalidate cache properly when several loads
        #      are done simultaneously.
        self._load_lock_acquire()
        try:
            acquire()
            try:
                result = self._loadFromCache(oid, tid, before_tid)
                if result:
                    return result
                self._loading_oid = oid
            finally:
                release()
            # When not bound to a ZODB Connection, load() may be the
            # first method called and last_tid may still be None.
            # This happens, for example, when opening the DB.
            if not (tid or before_tid) and self.last_tid:
                # Do not get something more recent than the last invalidation
                # we got from master.
                before_tid = p64(u64(self.last_tid) + 1)
            data, tid, next_tid, _ = self._loadFromStorage(oid, tid, before_tid)
            acquire()
            try:
                result = data, tid, (next_tid if self._loading_oid or next_tid
                                              else self._loading_invalidated)
                self._cache.store(oid, *result)
                return result
            finally:
                release()
        finally:
            self._load_lock_release()

    def _loadFromStorage(self, oid, at_tid, before_tid):
        packet = Packets.AskObject(oid, at_tid, before_tid)
        for node, conn in self.cp.iterateForObject(oid, readable=True):
            try:
                tid, next_tid, compression, checksum, data, data_tid \
                    = self._askStorage(conn, packet)
            except ConnectionClosed:
                continue

            if data or checksum != ZERO_HASH:
                if checksum != makeChecksum(data):
                    logging.error('wrong checksum from %s for oid %s',
                              conn, dump(oid))
                    continue
                return (decompress(data) if compression else data,
                        tid, next_tid, data_tid)
            raise NEOStorageCreationUndoneError(dump(oid))
        raise NEOStorageError("storage down or corrupted data")

    def _loadFromCache(self, oid, at_tid=None, before_tid=None):
        """
        Load from local cache, return None if not found.
        """
        if at_tid:
            result = self._cache.load(oid, at_tid + '*')
            assert not result or result[1] == at_tid
            return result
        return self._cache.load(oid, before_tid)

    def tpc_begin(self, transaction, tid=None, status=' '):
        """Begin a new transaction."""
        # First get a transaction, only one is allowed at a time
        txn_context = self._txn_container.new(transaction)
        # use the given TID or request a new one to the master
        answer_ttid = self._askPrimary(Packets.AskBeginTransaction(tid))
        if answer_ttid is None:
            raise NEOStorageError('tpc_begin failed')
        assert tid in (None, answer_ttid), (tid, answer_ttid)
        txn_context['ttid'] = answer_ttid

    def store(self, oid, serial, data, version, transaction):
        """Store object."""
        logging.debug('storing oid %s serial %s', dump(oid), dump(serial))
        self._store(self._txn_container.get(transaction), oid, serial, data)

    def _store(self, txn_context, oid, serial, data, data_serial=None,
            unlock=False):
        ttid = txn_context['ttid']
        if data is None:
            # This is some undo: either a no-data object (undoing object
            # creation) or a back-pointer to an earlier revision (going back to
            # an older object revision).
            compressed_data = ''
            compression = 0
            checksum = ZERO_HASH
        else:
            assert data_serial is None
            size = len(data)
            if self.compress:
                compressed_data = compress(data)
                if size < len(compressed_data):
                    compressed_data = data
                    compression = 0
                else:
                    compression = 1
            else:
                compression = 0
                compressed_data = data
            checksum = makeChecksum(compressed_data)
            txn_context['data_size'] += size
        on_timeout = partial(
            self.onStoreTimeout,
            txn_context=txn_context,
            oid=oid,
        )
        # Store object in tmp cache
        txn_context['data_dict'][oid] = data
        # Store data on each node
        txn_context['object_stored_counter_dict'][oid] = {}
        txn_context['object_base_serial_dict'].setdefault(oid, serial)
        txn_context['object_serial_dict'][oid] = serial
        queue = txn_context['queue']
        involved_nodes = txn_context['involved_nodes']
        add_involved_nodes = involved_nodes.add
        packet = Packets.AskStoreObject(oid, serial, compression,
            checksum, compressed_data, data_serial, ttid, unlock)
        for node, conn in self.cp.iterateForObject(oid):
            try:
                conn.ask(packet, on_timeout=on_timeout, queue=queue)
                add_involved_nodes(node)
            except ConnectionClosed:
                continue
        if not involved_nodes:
            raise NEOStorageError("Store failed")

        while txn_context['data_size'] >= self._cache._max_size:
            self._waitAnyTransactionMessage(txn_context)
        self._waitAnyTransactionMessage(txn_context, False)

    def onStoreTimeout(self, conn, msg_id, txn_context, oid):
        # NOTE: this method is called from poll thread, don't use
        #       thread-specific value !
        txn_context.setdefault('timeout_dict', {})[oid] = msg_id
        # Ask the storage if someone locks the object.
        # By sending a message with a smaller timeout,
        # the connection will be kept open.
        conn.ask(Packets.AskHasLock(txn_context['ttid'], oid),
                 timeout=5, queue=txn_context['queue'])

    def _handleConflicts(self, txn_context, tryToResolveConflict):
        result = []
        append = result.append
        # Check for conflicts
        data_dict = txn_context['data_dict']
        object_base_serial_dict = txn_context['object_base_serial_dict']
        object_serial_dict = txn_context['object_serial_dict']
        conflict_serial_dict = txn_context['conflict_serial_dict'].copy()
        txn_context['conflict_serial_dict'].clear()
        resolved_conflict_serial_dict = txn_context[
            'resolved_conflict_serial_dict']
        for oid, conflict_serial_set in conflict_serial_dict.iteritems():
            conflict_serial = max(conflict_serial_set)
            serial = object_serial_dict[oid]
            if ZERO_TID in conflict_serial_set:
              if 1:
                # XXX: disable deadlock avoidance code until it is fixed
                logging.info('Deadlock avoidance on %r:%r',
                    dump(oid), dump(serial))
                # 'data' parameter of ConflictError is only used to report the
                # class of the object. It doesn't matter if 'data' is None
                # because the transaction is too big.
                try:
                    data = data_dict[oid]
                except KeyError:
                    data = txn_context['cache_dict'][oid]
              else:
                # Storage refused us from taking object lock, to avoid a
                # possible deadlock. TID is actually used for some kind of
                # "locking priority": when a higher value has the lock,
                # this means we stored objects "too late", and we would
                # otherwise cause a deadlock.
                # To recover, we must ask storages to release locks we
                # hold (to let possibly-competing transactions acquire
                # them), and requeue our already-sent store requests.
                # XXX: currently, brute-force is implemented: we send
                # object data again.
                # WARNING: not maintained code
                logging.info('Deadlock avoidance triggered on %r:%r',
                    dump(oid), dump(serial))
                for store_oid, store_data in data_dict.iteritems():
                    store_serial = object_serial_dict[store_oid]
                    if store_data is CHECKED_SERIAL:
                        self._checkCurrentSerialInTransaction(txn_context,
                            store_oid, store_serial)
                    else:
                        if store_data is None:
                            # Some undo
                            logging.warning('Deadlock avoidance cannot reliably'
                                ' work with undo, this must be implemented.')
                            conflict_serial = ZERO_TID
                            break
                        self._store(txn_context, store_oid, store_serial,
                            store_data, unlock=True)
                else:
                    continue
            else:
                data = data_dict.pop(oid)
                if data is CHECKED_SERIAL:
                    raise ReadConflictError(oid=oid, serials=(conflict_serial,
                        serial))
                if data: # XXX: can 'data' be None ???
                    txn_context['data_size'] -= len(data)
                resolved_serial_set = resolved_conflict_serial_dict.setdefault(
                    oid, set())
                if resolved_serial_set and conflict_serial <= max(
                        resolved_serial_set):
                    # A later serial has already been resolved, skip.
                    resolved_serial_set.update(conflict_serial_set)
                    continue
                try:
                    new_data = tryToResolveConflict(oid, conflict_serial,
                        serial, data)
                except ConflictError:
                    logging.info('Conflict resolution failed for '
                        '%r:%r with %r', dump(oid), dump(serial),
                        dump(conflict_serial))
                else:
                    logging.info('Conflict resolution succeeded for '
                        '%r:%r with %r', dump(oid), dump(serial),
                        dump(conflict_serial))
                    # Mark this conflict as resolved
                    resolved_serial_set.update(conflict_serial_set)
                    # Base serial changes too, as we resolved a conflict
                    object_base_serial_dict[oid] = conflict_serial
                    # Try to store again
                    self._store(txn_context, oid, conflict_serial, new_data)
                    append(oid)
                    continue
            raise ConflictError(oid=oid, serials=(conflict_serial,
                serial), data=data)
        return result

    def waitResponses(self, queue):
        """Wait for all requests to be answered (or their connection to be
        detected as closed)"""
        pending = self.dispatcher.pending
        _waitAnyMessage = self._waitAnyMessage
        while pending(queue):
            _waitAnyMessage(queue)

    def waitStoreResponses(self, txn_context, tryToResolveConflict):
        result = []
        append = result.append
        resolved_oid_set = set()
        update = resolved_oid_set.update
        _handleConflicts = self._handleConflicts
        queue = txn_context['queue']
        conflict_serial_dict = txn_context['conflict_serial_dict']
        pending = self.dispatcher.pending
        _waitAnyTransactionMessage = self._waitAnyTransactionMessage
        while pending(queue) or conflict_serial_dict:
            # Note: handler data can be overwritten by _handleConflicts
            # so we must set it for each iteration.
            _waitAnyTransactionMessage(txn_context)
            if conflict_serial_dict:
                conflicts = _handleConflicts(txn_context,
                    tryToResolveConflict)
                if conflicts:
                    update(conflicts)

        # Check for never-stored objects, and update result for all others
        for oid, store_dict in \
                txn_context['object_stored_counter_dict'].iteritems():
            if not store_dict:
                logging.error('tpc_store failed')
                raise NEOStorageError('tpc_store failed')
            elif oid in resolved_oid_set:
                append((oid, ResolvedSerial))
        return result

    def tpc_vote(self, transaction, tryToResolveConflict):
        """Store current transaction."""
        txn_context = self._txn_container.get(transaction)
        result = self.waitStoreResponses(txn_context, tryToResolveConflict)

        ttid = txn_context['ttid']
        # Store data on each node
        assert not txn_context['data_dict'], txn_context
        packet = Packets.AskStoreTransaction(ttid, str(transaction.user),
            str(transaction.description), dumps(transaction._extension),
            txn_context['cache_dict'])
        add_involved_nodes = txn_context['involved_nodes'].add
        for node, conn in self.cp.iterateForObject(ttid):
            logging.debug("voting transaction %s on %s", dump(ttid),
                dump(conn.getUUID()))
            try:
                self._askStorage(conn, packet)
            except ConnectionClosed:
                continue
            add_involved_nodes(node)

        # check at least one storage node accepted
        if txn_context['involved_nodes']:
            txn_context['voted'] = None
            # We must not go further if connection to master was lost since
            # tpc_begin, to lower the probability of failing during tpc_finish.
            if 'error' in txn_context:
                raise NEOStorageError(txn_context['error'])
            return result
        logging.error('tpc_vote failed')
        raise NEOStorageError('tpc_vote failed')

    def tpc_abort(self, transaction):
        """Abort current transaction."""
        txn_context = self._txn_container.pop(transaction)
        if txn_context is None:
            return
        ttid = txn_context['ttid']
        p = Packets.AbortTransaction(ttid)
        getConnForNode = self.cp.getConnForNode
        # cancel transaction one all those nodes
        for node in txn_context['involved_nodes']:
            conn = getConnForNode(node)
            if conn is None:
                continue
            try:
                conn.notify(p)
            except:
                logging.exception('Exception in tpc_abort while notifying'
                    'storage node %r of abortion, ignoring.', conn)
        conn = self.master_conn
        if conn is not None:
            conn.notify(p)
        # We don't need to flush queue, as it won't be reused by future
        # transactions (deleted on next line & indexed by transaction object
        # instance).
        self.dispatcher.forget_queue(txn_context['queue'], flush_queue=False)

    def tpc_finish(self, transaction, tryToResolveConflict, f=None):
        """Finish current transaction."""
        txn_container = self._txn_container
        if 'voted' not in txn_container.get(transaction):
            self.tpc_vote(transaction, tryToResolveConflict)
        self._load_lock_acquire()
        try:
            # Call finish on master
            txn_context = txn_container.pop(transaction)
            cache_dict = txn_context['cache_dict']
            tid = self._askPrimary(Packets.AskFinishTransaction(
                txn_context['ttid'], cache_dict),
                cache_dict=cache_dict, callback=f)
            assert tid
            return tid
        finally:
            self._load_lock_release()

    def undo(self, undone_tid, txn, tryToResolveConflict):
        txn_context = self._txn_container.get(txn)
        txn_info, txn_ext = self._getTransactionInformation(undone_tid)
        txn_oid_list = txn_info['oids']

        # Regroup objects per partition, to ask a minimum set of storage.
        partition_oid_dict = {}
        for oid in txn_oid_list:
            partition = self.pt.getPartition(oid)
            try:
                oid_list = partition_oid_dict[partition]
            except KeyError:
                oid_list = partition_oid_dict[partition] = []
            oid_list.append(oid)

        # Ask storage the undo serial (serial at which object's previous data
        # is)
        getCellList = self.pt.getCellList
        getCellSortKey = self.cp.getCellSortKey
        getConnForCell = self.cp.getConnForCell
        queue = self._thread_container.queue
        ttid = txn_context['ttid']
        undo_object_tid_dict = {}
        snapshot_tid = p64(u64(self.last_tid) + 1)
        for partition, oid_list in partition_oid_dict.iteritems():
            cell_list = getCellList(partition, readable=True)
            # We do want to shuffle before getting one with the smallest
            # key, so that all cells with the same (smallest) key has
            # identical chance to be chosen.
            shuffle(cell_list)
            storage_conn = getConnForCell(min(cell_list, key=getCellSortKey))
            storage_conn.ask(Packets.AskObjectUndoSerial(ttid,
                snapshot_tid, undone_tid, oid_list),
                queue=queue, undo_object_tid_dict=undo_object_tid_dict)

        # Wait for all AnswerObjectUndoSerial. We might get OidNotFoundError,
        # meaning that objects in transaction's oid_list do not exist any
        # longer. This is the symptom of a pack, so forbid undoing transaction
        # when it happens.
        try:
            self.waitResponses(queue)
        except NEOStorageNotFoundError:
            self.dispatcher.forget_queue(queue)
            raise UndoError('non-undoable transaction')

        # Send undo data to all storage nodes.
        for oid in txn_oid_list:
            current_serial, undo_serial, is_current = undo_object_tid_dict[oid]
            if is_current:
                data = None
            else:
                # Serial being undone is not the latest version for this
                # object. This is an undo conflict, try to resolve it.
                try:
                    # Load the latest version we are supposed to see
                    data = self.load(oid, current_serial)[0]
                    # Load the version we were undoing to
                    undo_data = self.load(oid, undo_serial)[0]
                except NEOStorageNotFoundError:
                    raise UndoError('Object not found while resolving undo '
                        'conflict')
                # Resolve conflict
                try:
                    data = tryToResolveConflict(oid, current_serial,
                        undone_tid, undo_data, data)
                except ConflictError:
                    raise UndoError('Some data were modified by a later ' \
                        'transaction', oid)
                undo_serial = None
            self._store(txn_context, oid, current_serial, data, undo_serial)

        return None, txn_oid_list

    def _insertMetadata(self, txn_info, extension):
        for k, v in loads(extension).items():
            txn_info[k] = v

    def _getTransactionInformation(self, tid):
        packet = Packets.AskTransactionInformation(tid)
        for node, conn in self.cp.iterateForObject(tid, readable=True):
            try:
                txn_info, txn_ext = self._askStorage(conn, packet)
            except ConnectionClosed:
                continue
            except NEOStorageNotFoundError:
                # TID not found
                continue
            break
        else:
            raise NEOStorageError('Transaction %r not found' % (tid, ))
        return (txn_info, txn_ext)

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
            conn = self.cp.getConnForNode(storage_node)
            if conn is None:
                continue
            conn.ask(packet, queue=queue, tid_set=tid_set)

        # Wait for answers from all storages.
        self.waitResponses(queue)

        # Reorder tids
        ordered_tids = sorted(tid_set, reverse=True)
        logging.debug("UndoLog tids %s", map(dump, ordered_tids))
        # For each transaction, get info
        undo_info = []
        append = undo_info.append
        for tid in ordered_tids:
            (txn_info, txn_ext) = self._getTransactionInformation(tid)
            if filter is None or filter(txn_info):
                txn_info.pop('packed')
                txn_info.pop("oids")
                self._insertMetadata(txn_info, txn_ext)
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
        for offset in xrange(self.pt.getPartitions()):
            p = Packets.AskTIDsFrom(start, stop, limit, offset)
            for node, conn in self.cp.iterateForObject(offset, readable=True):
                try:
                    r = self._askStorage(conn, p)
                    break
                except ConnectionClosed:
                    pass
            else:
                raise NEOStorageError('transactionLog failed')
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
            txn_info['ext'] = loads(txn_ext)
            append(txn_info)
        return (tid, txn_list)

    def history(self, oid, size=1, filter=None):
        # Get history informations for object first
        packet = Packets.AskObjectHistory(oid, 0, size)
        for node, conn in self.cp.iterateForObject(oid, readable=True):
            try:
                history_list = self._askStorage(conn, packet)
            except ConnectionClosed:
                continue
            # Now that we have object informations, get txn informations
            result = []
            # history_list is already sorted descending (by the storage)
            for serial, size in history_list:
                txn_info, txn_ext = self._getTransactionInformation(serial)
                # create history dict
                txn_info.pop('id')
                txn_info.pop('oids')
                txn_info.pop('packed')
                txn_info['tid'] = serial
                txn_info['version'] = ''
                txn_info['size'] = size
                if filter is None or filter(txn_info):
                    result.append(txn_info)
                self._insertMetadata(txn_info, txn_ext)
            return result

    def importFrom(self, source, start, stop, tryToResolveConflict,
            preindex=None):
        # TODO: The main difference with BaseStorage implementation is that
        #       preindex can't be filled with the result 'store' (tid only
        #       known after 'tpc_finish'. This method could be dropped if we
        #       implemented IStorageRestoreable (a wrapper around source would
        #       still be required for partial import).
        if preindex is None:
            preindex = {}
        for transaction in source.iterator(start, stop):
            tid = transaction.tid
            self.tpc_begin(transaction, tid, transaction.status)
            for r in transaction:
                oid = r.oid
                pre = preindex.get(oid)
                self.store(oid, pre, r.data, r.version, transaction)
                preindex[oid] = tid
            conflicted = self.tpc_vote(transaction, tryToResolveConflict)
            assert not conflicted, conflicted
            real_tid = self.tpc_finish(transaction, tryToResolveConflict)
            assert real_tid == tid, (real_tid, tid)

    from .iterator import iterator

    def lastTransaction(self):
        self._askPrimary(Packets.AskLastTransaction())
        return self.last_tid

    def pack(self, t):
        tid = repr(TimeStamp(*time.gmtime(t)[:5] + (t % 60, )))
        if tid == ZERO_TID:
            raise NEOStorageError('Invalid pack time')
        self._askPrimary(Packets.AskPack(tid))
        # XXX: this is only needed to make ZODB unit tests pass.
        # It should not be otherwise required (clients should be free to load
        # old data as long as it is available in cache, event if it was pruned
        # by a pack), so don't bother invalidating on other clients.
        self._cache_lock_acquire()
        try:
            self._cache.clear()
        finally:
            self._cache_lock_release()

    def getLastTID(self, oid):
        return self.load(oid)[1]

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self._checkCurrentSerialInTransaction(
            self._txn_container.get(transaction), oid, serial)

    def _checkCurrentSerialInTransaction(self, txn_context, oid, serial):
        ttid = txn_context['ttid']
        txn_context['object_serial_dict'][oid] = serial
        # Placeholders
        queue = txn_context['queue']
        txn_context['object_stored_counter_dict'][oid] = {}
        # ZODB.Connection performs calls 'checkCurrentSerialInTransaction'
        # after stores, and skips oids that have been succeessfully stored.
        assert oid not in txn_context['cache_dict'], (oid, txn_context)
        txn_context['data_dict'].setdefault(oid, CHECKED_SERIAL)
        packet = Packets.AskCheckCurrentSerial(ttid, serial, oid)
        for node, conn in self.cp.iterateForObject(oid):
            try:
                conn.ask(packet, queue=queue)
            except ConnectionClosed:
                continue
        self._waitAnyTransactionMessage(txn_context, False)

