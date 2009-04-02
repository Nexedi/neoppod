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

import logging
import os
from threading import Lock, RLock, local
from cPickle import dumps, loads
from zlib import compress, decompress
from Queue import Queue, Empty
from random import shuffle
from time import sleep

from neo.client.mq import MQ
from neo.node import NodeManager, MasterNode, StorageNode
from neo.connection import MTClientConnection
from neo.protocol import Packet, INVALID_UUID, INVALID_TID, INVALID_PARTITION, \
        STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, \
        UP_TO_DATE_STATE, FEEDING_STATE, INVALID_SERIAL
from neo.client.handler import ClientEventHandler, ClientAnswerEventHandler
from neo.client.exception import NEOStorageError, NEOStorageConflictError, \
     NEOStorageNotFoundError
from neo.util import makeChecksum, dump
from neo.connector import getConnectorHandler
from neo.client.dispatcher import Dispatcher
from neo.client.poll import ThreadedPoll
from neo.event import EventManager

from ZODB.POSException import UndoError, StorageTransactionError, ConflictError
from ZODB.utils import p64, u64, oid_repr

class ConnectionPool(object):
    """This class manages a pool of connections to storage nodes."""

    def __init__(self, app, max_pool_size = 25):
        self.app = app
        self.max_pool_size = max_pool_size
        self.connection_dict = {}
        # Define a lock in order to create one connection to
        # a storage node at a time to avoid multiple connections
        # to the same node.
        l = RLock()
        self.connection_lock_acquire = l.acquire
        self.connection_lock_release = l.release

    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        addr = node.getNode().getServer()
        if addr is None:
            return None

        if node.getState() != RUNNING_STATE:
            return None

        app = self.app

        # Loop until a connection is obtained.
        while 1:
            logging.info('trying to connect to %s:%d', *addr)
            app.local_var.node_not_ready = 0
            conn = MTClientConnection(app.em, app.handler, addr,
                                      connector_handler=app.connector_handler)
            conn.lock()
            try:
                if conn.getConnector() is None:
                    # This happens, if a connection could not be established.
                    logging.error('Connection to storage node %s failed', addr)
                    return None

                msg_id = conn.getNextId()
                p = Packet()
                p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE,
                                            app.uuid, addr[0],
                                            addr[1], app.name)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                app.dispatcher.register(conn, msg_id, app.getQueue())
            finally:
                conn.unlock()

            try:
                app._waitMessage(conn, msg_id)
            except NEOStorageError:
                logging.error('Connection to storage node %s failed', addr)
                return None

            if app.local_var.node_not_ready:
                # Connection failed, notify primary master node
                logging.info('Storage node %s not ready', addr)
                return None
            else:
                logging.info('connected to storage node %s:%d', *addr)
                return conn

            sleep(1)

    def _dropConnections(self):
        """Drop connections."""
        for node_uuid, conn in self.connection_dict.items():
            # Drop first connection which looks not used
            conn.lock()
            try:
                if not conn.pending() and \
                        not self.app.dispatcher.registered(conn):
                    del self.connection_dict[conn.getUUID()]
                    conn.close()
                    logging.info('_dropConnections : connection to storage node %s:%d closed', 
                                 *(conn.getAddress()))
                    if len(self.connection_dict) <= self.max_pool_size:
                        break
            finally:
                conn.unlock()

    def _createNodeConnection(self, node):
        """Create a connection to a given storage node."""
        if len(self.connection_dict) > self.max_pool_size:
            # must drop some unused connections
            self._dropConnections()

        self.connection_lock_release()
        try:
            conn = self._initNodeConnection(node)
        finally:
            self.connection_lock_acquire()

        if conn is None:
            return None

        # add node to node manager
        if self.app.nm.getNodeByServer(node.getServer()) is None:
            n = StorageNode(node.getServer())
            self.app.nm.add(n)
        self.connection_dict[node.getUUID()] = conn
        conn.lock()
        return conn

    def getConnForNode(self, node):
        """Return a locked connection object to a given node
        If no connection exists, create a new one"""
        uuid = node.getUUID()
        self.connection_lock_acquire()
        try:
            try:
                conn = self.connection_dict[uuid]
                # Already connected to node
                conn.lock()
                return conn
            except KeyError:
                # Create new connection to node
                return self._createNodeConnection(node)
        finally:
            self.connection_lock_release()

    def removeConnection(self, node):
        """Explicitly remove connection when a node is broken."""
        self.connection_lock_acquire()
        try:
            try:
                del self.connection_dict[node.getUUID()]
            except KeyError:
                pass
        finally:
            self.connection_lock_release()


class Application(object):
    """The client node application."""

    def __init__(self, master_nodes, name, connector, **kw):
        logging.basicConfig(level = logging.DEBUG)
        logging.debug('master node address are %s' %(master_nodes,))
        em = EventManager()
        # Start polling thread
        self.poll_thread = ThreadedPoll(em)
        # Internal Attributes common to all thread
        self.name = name
        self.em = em
        self.connector_handler = getConnectorHandler(connector)
        self.dispatcher = Dispatcher()
        self.nm = NodeManager()
        self.cp = ConnectionPool(self)
        self.pt = None
        self.primary_master_node = None
        self.master_node_list = master_nodes.split(' ')
        self.master_conn = None
        self.uuid = None
        self.mq_cache = MQ()
        self.new_oid_list = []
        self.ptid = None
        self.num_replicas = 0
        self.num_partitions = 0
        self.handler = ClientEventHandler(self, self.dispatcher)
        self.answer_handler = ClientAnswerEventHandler(self, self.dispatcher)
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
        # _load_lock is used to make loading and storing atmic
        # _oid_lock is used in order to not call multiple oid
        # generation at the same time
        # _cache_lock is used for the client cache
        # _connecting_to_master_node is used to prevent simultaneous master
        # node connection attemps
        lock = Lock()
        self._load_lock_acquire = lock.acquire
        self._load_lock_release = lock.release
        lock = Lock()
        self._oid_lock_acquire = lock.acquire
        self._oid_lock_release = lock.release
        lock = Lock()
        self._cache_lock_acquire = lock.acquire
        self._cache_lock_release = lock.release
        lock = Lock()
        self._connecting_to_master_node_acquire = lock.acquire
        self._connecting_to_master_node_release = lock.release
        # XXX Generate an UUID for self. For now, just use a random string.
        # Avoid an invalid UUID.
        if self.uuid is None:
            while 1:
                uuid = os.urandom(16)
                if uuid != INVALID_UUID:
                    break
            self.uuid = uuid
        # Connect to master node
        self.connectToPrimaryMasterNode(self.connector_handler)

    def getQueue(self):
        try:
            return self.local_var.queue
        except AttributeError:
            self.local_var.queue = Queue(5)
            return self.local_var.queue

    def _waitMessage(self, target_conn = None, msg_id = None):
        """Wait for a message returned by the dispatcher in queues."""
        local_queue = self.getQueue()

        while 1:
            if msg_id is None:
                try:
                    conn, packet = local_queue.get_nowait()
                except Empty:
                    break
            else:
                conn, packet = local_queue.get()

            if packet is None:
                if conn is target_conn:
                    raise NEOStorageError('connection closed')
                else:
                    continue

            self.answer_handler.dispatch(conn, packet)

            if target_conn is conn and msg_id == packet.getId() \
                    and packet.getType() & 0x8000:
                break

    def registerDB(self, db, limit):
        self._db = db

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
                conn.lock()
                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askNewOIDs(msg_id, 25)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    self.dispatcher.register(conn, msg_id, self.getQueue())
                finally:
                    conn.unlock()

                self._waitMessage(conn, msg_id)
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
        hist = self.history(oid, length = 1, object_only = 1)
        if len(hist) == 0:
            raise NEOStorageNotFoundError()
        if hist[0] != oid:
            raise NEOStorageError('getSerial failed')
        return hist[1][0][0]


    def _load(self, oid, serial = INVALID_TID, tid = INVALID_TID, cache = 0):
        """Internal method which manage load ,loadSerial and loadBefore."""
        partition_id = u64(oid) % self.num_partitions

        self.local_var.asked_object = None
        while self.local_var.asked_object is None:
            cell_list = self.pt.getCellList(partition_id, True)
            if len(cell_list) == 0:
                sleep(1)
                continue

            shuffle(cell_list)
            self.local_var.asked_object = None
            for cell in cell_list:
                logging.debug('trying to load %s from %s',
                              dump(oid), dump(cell.getUUID()))
                conn = self.cp.getConnForNode(cell)
                if conn is None:
                    continue

                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askObject(msg_id, oid, serial, tid)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    self.dispatcher.register(conn, msg_id, self.getQueue())
                    self.local_var.asked_object = 0
                finally:
                    conn.unlock()

                self._waitMessage(conn, msg_id)
                if self.local_var.asked_object == -1:
                    # OID not found
                    break

                # Check data
                noid, start_serial, end_serial, compression, checksum, data \
                    = self.local_var.asked_object
                if noid != oid:
                    # Oops, try with next node
                    logging.error('got wrong oid %s instead of %s from node %s',
                                  noid, oid, cell.getServer())
                    continue
                elif checksum != makeChecksum(data):
                    # Check checksum.
                    logging.error('wrong checksum from node %s for oid %s',
                                  cell.getServer(), oid)
                    continue
                else:
                    # Everything looks alright.
                    break

        if self.local_var.asked_object == -1:
            # We didn't got any object from all storage node
            logging.debug('oid %s not found', dump(oid))
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
        self._load_lock_acquire()
        try:
            self._cache_lock_acquire()
            try:
                if oid in self.mq_cache:
                    logging.debug('load oid %s is cached', dump(oid))
                    return loads(self.mq_cache[oid][1]), self.mq_cache[oid][0]
            finally:
                self._cache_lock_release()
            # Otherwise get it from storage node
            return self._load(oid, cache=1)[:2]
        finally:
            self._load_lock_release()


    def loadSerial(self, oid, serial):
        """Load an object for a given oid and serial."""
        # Do not try in cache as it manages only up-to-date object
        logging.debug('loading %s at %s', dump(oid), dump(serial))
        return self._load(oid, serial=serial)[0]


    def loadBefore(self, oid, tid):
        """Load an object for a given oid before tid committed."""
        # Do not try in cache as it manages only up-to-date object
        if tid is None:
            tid = INVALID_TID
        logging.debug('loading %s before %s', dump(oid), dump(tid))
        data, start, end = self._load(oid, tid=tid)
        if end is None:
            # No previous version
            return None
        else:
            return data, start, end


    def tpc_begin(self, transaction, tid=None, status=' '):
        """Begin a new transaction."""
        # First get a transaction, only one is allowed at a time
        if self.txn == transaction:
            # We already begin the same transaction
            return
        # Get a new transaction id if necessary
        if tid is None:
            self.tid = None
            conn = self.master_conn
            if conn is None:
                raise NEOStorageError("Connection to master node failed")
            conn.lock()
            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askNewTID(msg_id)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                self.dispatcher.register(conn, msg_id, self.getQueue())
            finally:
                conn.unlock()
            # Wait for answer
            self._waitMessage(conn, msg_id)
            if self.tid is None:
                raise NEOStorageError('tpc_begin failed')
        else:
            self.tid = tid
        self.txn = transaction            


    def store(self, oid, serial, data, version, transaction):
        """Store object."""
        if transaction is not self.txn:
            raise StorageTransactionError(self, transaction)
        if serial is None:
            serial = INVALID_SERIAL
        logging.debug('storing oid %s serial %s',
                     dump(oid), dump(serial))
        # Find which storage node to use
        partition_id = u64(oid) % self.num_partitions
        cell_list = self.pt.getCellList(partition_id, True)
        if len(cell_list) == 0:
            # FIXME must wait for cluster to be ready
            raise NEOStorageError
        # Store data on each node
        ddata = dumps(data)
        compressed_data = compress(ddata)
        checksum = makeChecksum(compressed_data)
        for cell in cell_list:
            logging.info("storing object %s %s" %(cell.getServer(),cell.getState()))
            conn = self.cp.getConnForNode(cell)
            if conn is None:
                continue

            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askStoreObject(msg_id, oid, serial, 1,
                                 checksum, compressed_data, self.tid)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                self.dispatcher.register(conn, msg_id, self.getQueue())
                self.txn_object_stored = 0
            finally:
                conn.unlock()

            # Check we don't get any conflict
            self._waitMessage(conn, msg_id)
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

        return self.tid


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
        cell_list = self.pt.getCellList(partition_id, True)
        for cell in cell_list:
            logging.info("voting object %s %s" %(cell.getServer(), cell.getState()))
            conn = self.cp.getConnForNode(cell)
            if conn is None:
                continue

            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askStoreTransaction(msg_id, self.tid, user, desc, ext,
                                      oid_list)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                self.dispatcher.register(conn, msg_id, self.getQueue())
                self.txn_voted == 0
            finally:
                conn.unlock()

            self._waitMessage(conn, msg_id)
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
        aborted_node_set = set()
        for oid in self.txn_data_dict.iterkeys():
            partition_id = u64(oid) % self.num_partitions
            cell_list = self.pt.getCellList(partition_id, True)
            for cell in cell_list:
                if cell.getNode() not in aborted_node_set:
                    conn = self.cp.getConnForNode(cell)
                    if conn is None:
                        continue

                    try:
                        msg_id = conn.getNextId()
                        p = Packet()
                        p.abortTransaction(msg_id, self.tid)
                        conn.addPacket(p)
                    finally:
                        conn.unlock()

                    aborted_node_set.add(cell.getNode())

        # Abort in nodes where transaction was stored
        partition_id = u64(self.tid) % self.num_partitions
        cell_list = self.pt.getCellList(partition_id, True)
        for cell in cell_list:
            if cell.getNode() not in aborted_node_set:
                conn = self.cp.getConnForNode(cell)
                if conn is None:
                    continue

                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.abortTransaction(msg_id, self.tid)
                    conn.addPacket(p)
                finally:
                    conn.unlock()

                aborted_node_set.add(cell.getNode())

        # Abort the transaction in the primary master node.
        conn = self.master_conn
        conn.lock()
        try:
            conn.addPacket(Packet().abortTransaction(conn.getNextId(), self.tid))
        finally:
            conn.unlock()

        self._clear_txn()

    def tpc_finish(self, transaction, f=None):
        """Finish current transaction."""
        if self.txn is not transaction:
            return
        self._load_lock_acquire()
        try:
            # Call function given by ZODB
            if f is not None:
                f(self.tid)

            # Call finish on master
            oid_list = self.txn_data_dict.keys()
            conn = self.master_conn
            conn.lock()
            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.finishTransaction(msg_id, oid_list, self.tid)
                conn.addPacket(p)
                conn.expectMessage(msg_id, additional_timeout = 300)
                self.dispatcher.register(conn, msg_id, self.getQueue())
            finally:
                conn.unlock()

            # Wait for answer
            self._waitMessage(conn, msg_id)
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
        finally:
            self._load_lock_release()


    def undo(self, transaction_id, txn, wrapper):
        if txn is not self.txn:
            raise StorageTransactionError(self, transaction_id)

        # First get transaction information from a storage node.
        partition_id = u64(transaction_id) % self.num_partitions
        cell_list = self.pt.getCellList(partition_id, True)
        shuffle(cell_list)
        for cell in cell_list:
            conn = self.cp.getConnForNode(cell)
            if conn is None:
                continue

            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askTransactionInformation(msg_id, transaction_id)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                self.dispatcher.register(conn, msg_id, self.getQueue())
                self.local_var.txn_info = 0
            finally:
                conn.unlock()

            # Wait for answer
            self._waitMessage(conn, msg_id)
            if self.local_var.txn_info == -1:
                # Tid not found, try with next node
                continue
            elif isinstance(self.local_var.txn_info, dict):
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
        oid_list = data_dict.keys()
        for oid in oid_list:
            data = data_dict[oid]
            try:
                self.store(oid, transaction_id, data, None, txn)
            except NEOStorageConflictError, serial:
                if serial <= self.tid:
                    new_data = wrapper.tryToResolveConflict(oid, self.tid,
                                                            serial, data)
                    if new_data is not None:
                        self.store(oid, self.tid, new_data, None, txn)
                        continue
                raise ConflictError(oid = oid, serials = (self.tid, serial),
                                    data = data)
        return self.tid, oid_list

    def undoLog(self, first, last, filter=None, block=0):
        if last < 0:
            # See FileStorage.py for explanation
            last = first - last

        # First get a list of transactions from all storage nodes.
        storage_node_list = [x for x in self.pt.getNodeList() if x.getState() \
                             in (UP_TO_DATE_STATE, FEEDING_STATE)]
        self.local_var.node_tids = {}
        for storage_node in storage_node_list:
            conn = self.cp.getConnForNode(storage_node)
            if conn is None:
                continue

            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askTIDs(msg_id, first, last, INVALID_PARTITION)
                conn.addPacket(p)
            finally:
                conn.unlock()

        # Wait for answers from all storages.
        # FIXME this is a busy loop.
        while True:
            self._waitMessage()
            if len(self.local_var.node_tids.keys()) == len(storage_node_list):
                break

        # Reorder tids
        ordered_tids = []
        for tids in self.local_var.node_tids.values():
            ordered_tids.extend(tids)
        # XXX do we need a special cmp function here ?
        ordered_tids.sort(reverse=True)
        logging.info("UndoLog, tids %s", ordered_tids)
        # For each transaction, get info
        undo_info = []
        for tid in ordered_tids:
            partition_id = u64(tid) % self.num_partitions
            cell_list = self.pt.getCellList(partition_id, True)
            shuffle(cell_list)
            for cell in cell_list:
                conn = self.cp.getConnForNode(storage_node)
                if conn is None:
                    continue

                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askTransactionInformation(msg_id, tid)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    self.dispatcher.register(conn, msg_id, self.getQueue())
                    self.local_var.txn_info = 0
                finally:
                    conn.unlock()

                # Wait for answer
                self._waitMessage(conn, msg_id)
                if self.local_var.txn_info == -1:
                    # TID not found, go on with next node
                    continue
                elif isinstance(self.local_var.txn_info, dict):
                    break

            if self.local_var.txn_info == -1:
                # TID not found at all
                continue

            # Filter result if needed
            if filter is not None:
                # Filter method return True if match
                if not filter(self.local_var.txn_info):
                    continue

            # Append to returned list
            self.local_var.txn_info.pop("oids")
            undo_info.append(self.local_var.txn_info)
            if len(undo_info) >= last - first:
                break
        # Check we return at least one element, otherwise call
        # again but extend offset
        if len(undo_info) == 0 and not block:
            undo_info = self.undoLog(first=first, last=last*5, filter=filter, block=1)
        return undo_info


    def history(self, oid, version, length=1, filter=None, object_only=0):
        # Get history informations for object first
        partition_id = u64(oid) % self.num_partitions
        cell_list = self.pt.getCellList(partition_id, True)
        shuffle(cell_list)

        for cell in cell_list:
            conn = self.cp.getConnForNode(cell)
            if conn is None:
                continue

            try:
                msg_id = conn.getNextId()
                p = Packet()
                p.askObjectHistory(msg_id, oid, 0, length)
                conn.addPacket(p)
                conn.expectMessage(msg_id)
                self.dispatcher.register(conn, msg_id, self.getQueue())
                self.local_var.history = None
            finally:
                conn.unlock()

            self._waitMessage(conn, msg_id)
            if self.local_var.history == -1:
                # Not found, go on with next node
                continue
            if self.local_var.history[0] != oid:
                # Got history for wrong oid
                continue

        if not isinstance(self.local_var.history, dict):
            raise NEOStorageError('history failed')
        if object_only:
            # Use by getSerial
            return self.local_var.history

        # Now that we have object informations, get txn informations
        history_list = []
        for serial, size in self.local_var.hisory[1]:
            partition_id = u64(serial) % self.num_partitions
            cell_list = self.pt.getCellList(partition_id, True)
            shuffle(cell_list)

            for cell in cell_list:
                conn = self.cp.getConnForNode(cell)
                if conn is None:
                    continue

                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askTransactionInformation(msg_id, serial)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    self.dispatcher.register(conn, msg_id, self.getQueue())
                    self.local_var.txn_info = None
                finally:
                    conn.unlock()

                # Wait for answer
                self._waitMessage(conn, msg_id)
                if self.local_var.txn_info == -1:
                    # TID not found
                    continue
                if isinstance(self.local_var.txn_info, dict):
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
        # TODO: Stop polling thread here.
        # Due to bug in ZODB, close is not always called when shutting
        # down zope, so use __del__ to close connections
        for conn in self.em.getConnectionList():
            conn.close()
    close = __del__

    def sync(self):
        self._waitMessage()

    def connectToPrimaryMasterNode(self, connector_handler):
        """Connect to a primary master node.
        This can be called either at bootstrap or when
        client got disconnected during process"""
        # Indicate we are trying to connect to avoid multiple try a time
        acquired = self._connecting_to_master_node_acquire(0)
        if acquired:
            try:
                if self.pt is not None:
                    self.pt.clear()
                master_index = 0
                conn = None
                # Make application execute remaining message if any
                self._waitMessage()
                while 1:
                    self.local_var.node_not_ready = 0
                    if self.primary_master_node is None:
                        # Try with master node defined in config
                        try:
                            addr, port = self.master_node_list[master_index].split(':')                        
                        except IndexError:
                            master_index = 0
                            addr, port = self.master_node_list[master_index].split(':')
                        port = int(port)
                    else:
                        addr, port = self.primary_master_node.getServer()
                    # Request Node Identification
                    conn = MTClientConnection(self.em, self.handler, (addr, port), connector_handler=connector_handler)
                    if self.nm.getNodeByServer((addr, port)) is None:
                        n = MasterNode(server = (addr, port))
                        self.nm.add(n)

                    conn.lock()
                    try:
                        msg_id = conn.getNextId()
                        p = Packet()
                        p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid,
                                                    '0.0.0.0', 0, self.name)

                        # Send message
                        conn.addPacket(p)
                        conn.expectMessage(msg_id)
                        self.dispatcher.register(conn, msg_id, self.getQueue())
                    finally:
                        conn.unlock()

                    # Wait for answer
                    while 1:
                        self._waitMessage()
                        # Now check result
                        if self.primary_master_node is not None:
                            if self.primary_master_node == -1:
                                # Connection failed, try with another master node
                                self.primary_master_node = None
                                master_index += 1
                                break
                            elif self.primary_master_node.getServer() != (addr, port):
                                # Master node changed, connect to new one
                                break
                            elif self.local_var.node_not_ready:
                                # Wait a bit and reask again
                                break
                            elif self.pt is not None and self.pt.operational():
                                # Connected to primary master node
                                break
                    if self.pt is not None and self.pt.operational():
                        # Connected to primary master node and got all informations
                        break
                    sleep(1)

                logging.info("connected to primary master node %s:%d" % self.primary_master_node.getServer())
                self.master_conn = conn
            finally:
                self._connecting_to_master_node_release()
