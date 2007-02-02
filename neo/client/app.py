import logging
import os
from time import time
from threading import Lock, local
from cPickle import dumps, loads
from zlib import compress, decompress
from Queue import Queue, Empty

from neo.client.mq import MQ
from neo.node import NodeManager, MasterNode, StorageNode
from neo.connection import ListeningConnection, ClientConnection
from neo.protocol import Packet, INVALID_UUID, INVALID_TID, \
        STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        TEMPORARILY_DOWN_STATE, \
        UP_TO_DATE_STATE, FEEDING_STATE, INVALID_SERIAL
from neo.client.handler import ClientEventHandler
from neo.client.NEOStorage import NEOStorageError, NEOStorageConflictError, \
        NEOStorageNotFoundError
from neo.client.multithreading import ThreadingMixIn
from neo.util import makeChecksum

from ZODB.POSException import UndoError, StorageTransactionError, ConflictError
from ZODB.utils import p64, u64, oid_repr

class ConnectionManager(object):
    """This class manage a pool of connection to storage node."""

    def __init__(self, storage, pool_size=25):
        self.storage = storage
        self.pool_size = 0
        self.max_pool_size = pool_size
        self.connection_dict = {}
        # define a lock in order to create one connection to
        # a storage node at a time to avoid multiple connection
        # to the same node
        l = Lock()
        self.connection_lock_acquire = l.acquire
        self.connection_lock_release = l.release

    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        addr = node.getNode().getServer()
        if addr is None:
            return None
        handler = ClientEventHandler(self.storage, self.storage.dispatcher)
        conn = ClientConnection(self.storage.em, handler, addr)
        msg_id = conn.getNextId()
        p = Packet()
        p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.storage.uuid, addr[0],
                                    addr[1], self.storage.name)
        self.storage.local_var.tmp_q = Queue(1)
        self.storage.queue.put((self.storage.local_var.tmp_q, msg_id, conn, p), True)
        self.storage.local_var.storage_node = None
        self.storage._waitMessage()
        if self.storage.storage_node == -1:
            # Connection failed, notify primary master node
            logging.error('Connection to storage node %s failed' %(addr,))
            conn = self.storage.master_conn
            msg_id = conn.getNextId()
            p = Packet()
            node_list = [(STORAGE_NODE_TYPE, addr[0], addr[1], node.getUUID(),
                         TEMPORARILY_DOWN_STATE),]
            p.notifyNodeInformation(msg_id, node_list)
            self.storage.queue.put((None, msg_id, conn, p), True)
            return None
        logging.info('connected to storage node %s' %(addr,))
        return conn

    def _dropConnection(self,):
        """Drop a connection."""
        pass

    def _createNodeConnection(self, node):
        """Create a connection to a given storage node."""
        self.connection_lock_acquire()
        try:
            # check dict again, maybe another thread
            # just created the connection
            if self.connection_dict.has_key(node.getUUID()):
                return self.connection_dict[node.getUUID()]
            if self.pool_size > self.max_pool_size:
                # must drop some unused connections
                self.dropConnection()
            conn = self._initNodeConnection(node)
            if conn is None:
                return None
            # add node to node manager
            if self.storage.nm.getNodeByServer(node.getServer()) is None:
                n = StorageNode(node.getServer())
                self.storage.nm.add(n)
            self.connection_dict[node.getUUID()] = conn
            return conn
        finally:
            self.connection_lock_release()

    def getConnForNode(self, node):
        """Return connection object to a given node
        If no connection exists, create a new one"""
        if self.connection_dict.has_key(node.getUUID()):
            # Already connected to node
            return self.connection_dict[node.getUUID()]
        else:
            # Create new connection to node
            return self._createNodeConnection(node)

    def removeConnection(self, node):
        """Explicitly remove connection when a node is broken."""
        if self.connection_dict.has_key(node.getUUID()):
            self.connection_dict.pop(node.getUUID())


class Application(ThreadingMixIn, object):
    """The client node application."""

    def __init__(self, master_nodes, name, em, dispatcher, message_queue,
                 request_queue, **kw):
        logging.basicConfig(level = logging.DEBUG)
        logging.debug('master node address are %s' %(master_nodes,))
        # Internal Attributes common to all thread
        self.name = name
        self.em = em
        self.dispatcher = dispatcher
        self.nm = NodeManager()
        self.cm = ConnectionManager(self)
        self.pt = None
        self.queue = message_queue
        self.request_queue = request_queue
        self.primary_master_node = None
        self.master_conn = None
        self.uuid = None
        self.mq_cache = MQ()
        self.new_oid_list = []
        self.ptid = None
        self.num_replicas = 0
        self.num_partitions = 0
        # Transaction specific variable
        self.tid = None
        self.txn = None
        self.txn_data_dict = {}
        self.txn_object_stored = 0
        self.txn_voted = 0
        self.txn_finished = 0
        # Internal attribute distinct between thread
        self.local_var = local()
        # Lock definition :
        # _return_lock is used to return data from thread to ZODB
        # _oid_lock is used in order to not call multiple oid
        # generation at the same time
        # _cache_lock is used for the client cache
        lock = Lock()
        self._return_lock_acquire = lock.acquire
        self._return_lock_release = lock.release
        lock = Lock()
        self._oid_lock_acquire = lock.acquire
        self._oid_lock_release = lock.release
        lock = Lock()
        self._cache_lock_acquire = lock.acquire
        self._cache_lock_release = lock.release
        # XXX Generate an UUID for self. For now, just use a random string.
        # Avoid an invalid UUID.
        if self.uuid is None:
            while 1:
                uuid = os.urandom(16)
                if uuid != INVALID_UUID:
                    break
            self.uuid = uuid
        # Connect to primary master node
        self.master_node_list = master_nodes.split(' ')
        while 1:
            self.node_not_ready = 0
            logging.info("trying to connect to primary master...")
            self.connectToPrimaryMasterNode()
            if not self.node_not_ready and self.pt.filled():
                # got a connection and partition table
                break
            else:
                # wait a bit before reasking
                t = time()
                while time() < t + 1:
                    pass
        logging.info("connected to primary master node")

    def _waitMessage(self,block=1):
        """Wait for a message returned by dispatcher in queues."""
        # First check if there are global messages and execute them
        global_message = None
        while 1:
            try:
                global_message = self.request_queue.get_nowait()
            except Empty:
                break
            if global_message is not None:
                global_message[0].handler.dispatch(global_message[0], global_message[1])
        # Next get messages we are waiting for
        message = None
        if block:
            message = self.local_var.tmp_q.get(True, None)
        else:
            # we don't want to block until we got a message
            try:
                message = self.local_var.tmp_q.get_nowait()
            except Empty:
                pass
        if message is not None:
            message[0].handler.dispatch(message[0], message[1])

    def connectToPrimaryMasterNode(self):
        """Connect to the primary master node."""
        addr, port = self.master_node_list[0].split(':')
        port = int(port)
        handler = ClientEventHandler(self, self.dispatcher)
        n = MasterNode(server = (addr, port))
        self.nm.add(n)

        # Connect to first master node defined and get primary master node
        self.local_var.tmp_q = Queue(1)
        if self.primary_master_node is None:
            conn = ClientConnection(self.em, handler, (addr, port))
            msg_id = conn.getNextId()
            p = Packet()
            p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid,
                                        addr, port, self.name)
            # send message to dispatcher
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
            self.primary_master_node = None
            self.node_not_ready = 0

            while 1:
                self._waitMessage(block=0)
                if self.primary_master_node == -1:
                    raise NEOStorageError("Unable to initialize connection to master node %s:%d" %(addr, port))
                if self.primary_master_node is not None:
                    break
                if self.node_not_ready:
                    # must wait
                    return
        logging.info('primary master node is %s' %(self.primary_master_node.server,))
        # Close connection if not already connected to primary master node
        if self.primary_master_node.getServer() !=  (addr, port):
            for conn in self.em.getConnectionList():
                conn.close()

            # Connect to primary master node
            conn = ClientConnection(self.em, handler, self.primary_master_node.server)
            msg_id = conn.getNextId()
            p = Packet()
            p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid,
                                        self.primary_master_node.server[0],
                                        self.primary_master_node.server[1] , self.name)
            # send message to dispatcher
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)

        self.master_conn = conn
        # Wait for primary master node information
        while 1:
            self._waitMessage(block=0)
            if self.pt.operational() or self.node_not_ready:
                break


    def new_oid(self):
        """Get a new OID."""
        self._oid_lock_acquire()
        try:
            if len(self.new_oid_list) == 0:
              # Get new oid list from master node
              # we manage a list of oid here to prevent
              # from asking too many time new oid one by one
              # from master node
              conn = self.master_conn
              msg_id = conn.getNextId()
              p = Packet()
              p.askNewOIDs(msg_id, 25)
              self.local_var.tmp_q = Queue(1)
              self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
              self._waitMessage()
              if len(self.new_oid_list) <= 0:
                  raise NEOStorageError('new_oid failed')
            return self.new_oid_list.pop()
        finally:
            self._oid_lock_release()


    def getSerial(self, oid):
        # Try in cache first
        self._cache_lock_acquire()
        try:
            if oid in self.mq_cache:
                return self.mq_cache[oid][0]
        finally:
            self._cache_lock_release()
        # history return serial, so use it
        hist = self.history(oid, length=1, object_only=1)
        if len(hist) == 0:
            raise NEOStorageNotFoundError()
        if hist[0] != oid:
            raise NEOStorageError('getSerial failed')
        return hist[1][0][0]


    def _load(self, oid, serial=INVALID_TID, tid=INVALID_TID, cache=0):
        """Internal method which manage load ,loadSerial and loadBefore."""
        partition_id = u64(oid) % self.num_partitions
        # Only used up to date node for retrieving object
        storage_node_list = [x for x in self.pt.getCellList(partition_id, True) \
                             if x.getState() == UP_TO_DATE_STATE]
        data = None

        # Store data on each node
        if len(storage_node_list) == 0:
            # FIXME must wait for cluster to be ready
            raise NEOStorageNotFoundError()
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askObject(msg_id, oid, serial, tid)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)

            # Wait for answer
            self.local_var.asked_object = 0
            # asked object retured value are :
            # -1 : oid not found
            # other : data
            self._waitMessage()
            if self.local_var.asked_object == -1:
                # OID not found
                # XXX either try with another node, either raise error here
                # for now try with another node
                continue

            # Check data
            noid, start_serial, end_serial, compression, checksum, data = self.local_var.asked_object
            if noid != oid:
                # Oops, try with next node
                logging.error('got wrong oid %s instead of %s from node %s' \
                              %(noid, oid, storage_node.getServer()))
                continue
            elif checksum != makeChecksum(data):
                # Check checksum.
                logging.error('wrong checksum from node %s for oid %s' \
                              %(storage_node.getServer(), oid))
                continue
            else:
                # Everything looks alright.
                break

        if self.local_var.asked_object == -1:
            # We didn't got any object from all storage node
            raise NEOStorageNotFoundError()

        # Uncompress data
        if compression:
            data = decompress(data)

        # Put in cache only when using load
        if cache:
            self._cache_lock_acquire()
            try:
                self.mq_cache[oid] = start_serial, data
            finally:
                self._cache_lock_release()
        if end_serial == INVALID_SERIAL:
            end_serial = None
        return loads(data), start_serial, end_serial


    def load(self, oid, version=None):
        """Load an object for a given oid."""
        # First try from cache
        self._cache_lock_acquire()
        try:
            if oid in self.mq_cache:
                return loads(self.mq_cache[oid][1]), self.mq_cache[oid][0]
        finally:
            self._cache_lock_release()
        # Otherwise get it from storage node
        return self._load(oid, cache=1)[:2]


    def loadSerial(self, oid, serial):
        """Load an object for a given oid and serial."""
        # Do not try in cache as it managed only up-to-date object
        return self._load(oid, serial)[:2], None


    def loadBefore(self, oid, tid):
        """Load an object for a given oid before tid committed."""
        # Do not try in cache as it managed only up-to-date object
        return self._load(oid, tid)


    def tpc_begin(self, transaction, tid=None, status=' '):
        """Begin a new transaction."""
        # First get a transaction, only one is allowed at a time
        if self.txn == transaction:
            # We already begin the same transaction
            return
        self.txn = transaction
        # Get a new transaction id if necessary
        if tid is None:
            self.tid = None
            conn = self.master_conn
            msg_id = conn.getNextId()
            p = Packet()
            p.askNewTID(msg_id)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
            # Wait for answer
            self._waitMessage()
            if self.tid is None:
                raise NEOStorageError('tpc_begin failed')
        else:
            self.tid = tid


    def store(self, oid, serial, data, version, transaction):
        """Store object."""
        if transaction is not self.txn:
            raise StorageTransactionError(self, transaction)
        if serial is None:
            serial = INVALID_SERIAL
        # Find which storage node to use
        partition_id = u64(oid) % self.num_partitions
        storage_node_list = self.pt.getCellList(partition_id, True)
        if len(storage_node_list) == 0:
            # FIXME must wait for cluster to be ready
            raise NEOStorageError
        # Store data on each node
        ddata = dumps(data)
        compressed_data = compress(ddata)
        checksum = makeChecksum(compressed_data)
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askStoreObject(msg_id, oid, serial, 1, checksum, compressed_data, self.tid)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)

            # Check we don't get any conflict
            self.txn_object_stored = 0
            self._waitMessage()
            if self.txn_object_stored[0] == -1:
                if self.txn_data_dict.has_key(oid):
                    # One storage already accept the object, is it normal ??
                    # remove from dict and raise ConflictError, don't care of
                    # previous node which already store data as it would be resent
                    # again if conflict is resolved or txn will be aborted
                    del self.txn_data_dict[oid]
                self.conflict_serial = self.txn_object_stored[1]
                raise NEOStorageConflictError

        # Store object in tmp cache
        noid, nserial = self.txn_object_stored
        self.txn_data_dict[oid] = ddata


    def tpc_vote(self, transaction):
        """Store current transaction."""
        if transaction is not self.txn:
            raise StorageTransactionError(self, transaction)
        user = transaction.user
        desc = transaction.description
        ext = dumps(transaction._extension)
        oid_list = self.txn_data_dict.keys()
        # Store data on each node
        partition_id = u64(self.tid) % self.num_partitions
        storage_node_list = self.pt.getCellList(partition_id, True)
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askStoreTransaction(msg_id, self.tid, user, desc, ext, oid_list)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
            self.txn_voted == 0
            self._waitMessage()
            if self.txn_voted != 1:
                raise NEOStorageError('tpc_vote failed')


    def _clear_txn(self):
        """Clear some transaction parameters."""
        self.tid = None
        self.txn = None
        self.txn_data_dict.clear()
        self.txn_voted = 0
        self.txn_finished = 0


    def tpc_abort(self, transaction):
        """Abort current transaction."""
        if transaction is not self.txn:
            return

        # Abort txn in node where objects were stored
        aborted_node = {}
        for oid in self.txn_data_dict.iterkeys():
            partition_id = u64(oid) % self.num_partitions
            storage_node_list = self.pt.getCellList(partition_id, True)
            for storage_node in storage_node_list:
                if not aborted_node.has_key(storage_node):
                    conn = self.cm.getConnForNode(storage_node)
                    if conn is None:
                        continue
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.abortTransaction(msg_id, self.tid)
                    self.queue.put((None, msg_id, conn, p), True)
                aborted_node[storage_node] = 1

        # Abort in nodes where transaction was stored
        partition_id = u64(self.tid) % self.num_partitions
        storage_node_list = self.pt.getCellList(partition_id, True)
        for storage_node in storage_node_list:
            if not aborted_node.has_key(storage_node):
                conn = self.cm.getConnForNode(storage_node)
                if conn is None:
                    continue
                msg_id = conn.getNextId()
                p = Packet()
                p.abortTransaction(msg_id, self.tid)
                self.queue.put((None, msg_id, conn, p), True)

        self._clear_txn()


    def tpc_finish(self, transaction, f=None):
        """Finish current transaction."""
        if self.txn is not transaction:
            return
        # Call function given by ZODB
        if f is not None:
            f(self.tid)
        # Call finish on master
        oid_list = self.txn_data_dict.keys()
        conn = self.master_conn
        msg_id = conn.getNextId()
        p = Packet()
        p.finishTransaction(msg_id, oid_list, self.tid)
        self.local_var.tmp_q = Queue(1)
        self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
        # Wait for answer
        self._waitMessage()
        if self.txn_finished != 1:
            raise NEOStorageError('tpc_finish failed')

        # Update cache
        self._cache_lock_acquire()
        try:
            for oid in self.txn_data_dict.iterkeys():
                ddata = self.txn_data_dict[oid]
                # Now serial is same as tid
                self.mq_cache[oid] = self.tid, ddata
        finally:
            self._cache_lock_release()
        self._clear_txn()
        return self.tid


    def undo(self, transaction_id, txn, wrapper):
        if transaction_id is not self.txn:
            raise StorageTransactionError(self, transaction_id)

        # First get transaction information from master node
        partition_id = u64(transaction_id) % self.num_partitions
        storage_node_list = self.pt.getCellList(partition_id, True)
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askTransactionInformation(msg_id, transaction_id)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
            # Wait for answer
            self.local_var.txn_info = 0
            self._waitMessage()
            if self.local_var.txn_info == -1:
                # Tid not found, try with next node
                continue
            elif isinstance(self.local_var.txn_info, {}):
                break
            else:
                raise NEOStorageError('undo failed')

        if self.local_var.txn_info == -1:
            raise NEOStorageError('undo failed')

        oid_list = self.local_var.txn_info['oids']
        # Second get object data from storage node using loadBefore
        data_dict = {}
        for oid in oid_list:
            try:
                data, start, end = self.loadBefore(oid, transaction_id)
            except NEOStorageNotFoundError:
                # Object created by transaction, so no previous record
                data_dict[oid] = None
                continue
            # end must be TID we are going to undone otherwise it means
            # a later transaction modify the object
            if end != transaction_id:
                raise UndoError("non-undoable transaction")
            data_dict[oid] = data
        # Third do transaction with old data
        self.tpc_begin(txn)

        for oid in data_dict.keys():
            data = data_dict[oid]
            try:
                self.store(oid, self.tid, data, None, txn)
            except NEOStorageConflictError, serial:
                if serial <= self.tid:
                    new_data = wrapper.tryToResolveConflict(oid, self.tid,
                                                            serial, data)
                    if new_data is not None:
                        self.store(oid, self.tid, new_data, None, txn)
                        continue
                raise ConflictError(oid = oid, serials = (self.tid, serial),
                                    data = data)

        self.tpc_vote(txn)
        self.tpc_finish(txn)


    def undoLog(self, first, last, filter=None):
        if last < 0:
            # See FileStorage.py for explanation
            last = first - last

        # First get list of transaction from all storage node
        storage_node_list = [x for x in self.pt.getNodeList() if x.getState() \
                             in (UP_TO_DATE_STATE, FEEDING_STATE)]
        self.local_var.node_tids = {}
        self.local_var.tmp_q = Queue(len(storage_node_list))
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askTIDs(msg_id, first, last)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)

        # Wait for answer from all storages
        while True:
            self._waitMessage()
            if len(self.local_var.node_tids) == len(storage_node_list):
                break

        # Reorder tids
        ordered_tids = []
        for tids in self.local_var.node_tids.values():
            ordered_tids.append(tids)
        # XXX do we need a special cmp function here ?
        ordered_tids.sort(reverse=True)

        # For each transaction, get info
        undo_info = []
        for tid in ordered_tids:
            partition_id = u64(tid) % self.num_partitions
            storage_node_list = self.pt.getCellList(partition_id, True)
            for storage_node in storage_node_list:
                conn = self.cm.getConnForNode(storage_node)
                if conn is None:
                    continue
                msg_id = conn.getNextId()
                p = Packet()
                p.askTransactionInformation(msg_id, tid)
                self.local_var.tmp_q = Queue(1)
                self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
                # Wait for answer
                self.local_var.txn_info = 0
                self._waitMessage()
                if self.local_var.txn_info == -1:
                    # TID not found, go on with next node
                    continue
                elif isinstance(self.local_var.txn_info, {}):
                    break

            # Filter result if needed
            if filter is not None:
                # Filter method return True if match
                if not filter(self.local_var.txn_info['description']):
                    continue

            # Append to returned list
            self.local_var.txn_info.pop("oids")
            undo_info.append(self.local_var.txn_info)
            if len(undo_info) >= last-first:
                break

        return undo_info


    def history(self, oid, version, length=1, filter=None, object_only=0):
        # Get history informations for object first
        partition_id = u64(oid) % self.num_partitions
        storage_node_list = [x for x in self.pt.getCellList(partition_id, True) \
                             if x.getState() == UP_TO_DATE_STATE]
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node)
            if conn is None:
                continue
            msg_id = conn.getNextId()
            p = Packet()
            p.askObjectHistory(msg_id, oid, length)
            self.local_var.tmp_q = Queue(1)
            self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
            self.local_var.history = None
            self._waitMessage()
            if self.local_var.history == -1:
                # Not found, go on with next node
                continue
            if self.local_var.history[0] != oid:
                # Got history for wrong oid
                continue
        if not isinstance(self.local_var.history, {}):
            raise NEOStorageError('history failed')
        if object_only:
            # Use by getSerial
            return self.local_var.history

        # Now that we have object informations, get txn informations
        history_list = []
        for serial, size in self.local_var.hisory[1]:
            partition_id = u64(serial) % self.num_partitions
            storage_node_list = self.pt.getCellList(partition_id, True)
            for storage_node in storage_node_list:
                conn = self.cm.getConnForNode(storage_node)
                if conn is None:
                    continue
                msg_id = conn.getNextId()
                p = Packet()
                p.askTransactionInformation(msg_id, serial)
                self.local_var.tmp_q = Queue(1)
                self.queue.put((self.local_var.tmp_q, msg_id, conn, p), True)
                # Wait for answer
                self.local_var.txn_info = None
                self._waitMessage()
                if self.local_var.txn_info == -1:
                    # TID not found
                    continue
                if isinstance(self.local_var.txn_info, {}):
                    break

            # create history dict
            self.txn_info.remove('id')
            self.txn_info.remove('oids')
            self.txn_info['serial'] = serial
            self.txn_info['version'] = None
            self.txn_info['size'] = size
            history_list.append(self.txn_info)

        return history_list

    def __del__(self):
        """Clear all connection."""
        # Due to bug in ZODB, close is not always called when shutting
        # down zope, so use __del__ to close connections
        for conn in self.em.getConnectionList():
            conn.close()
    close = __del__
