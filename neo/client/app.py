import logging
import os
from time import time
from threading import Lock, Condition
from cPickle import dumps, loads
from zlib import compress, alder32, decompress

from neo.client.mq import MQ
from neo.node import NodeManager, MasterNode
from neo.event import EventManager
from neo.connection import ListeningConnection, ClientConnection
from neo.protocol import Packet, INVALID_UUID, CLIENT_NODE_TYPE, UP_TO_DATE_STATE
from neo.client.master import MasterEventHandler
from neo.client.NEOStorage import NEOStorageConflictError, NEOStorageKeyError

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
                
    def _initNodeConnection(self, addr):
        """Init a connection to a given storage node."""        
        handler = StorageEventHandler(self.storage)
        conn = ClientConnection(self.storage.em, handler, addr)
        msg_id = conn.getNextId()
        p = Packet()
        p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid, addr[0],
                                    addr[1], 'main')
        conn.expectMessage(msg_id)
        while 1:
            self.em.poll(1)
            if self.storage_node is not None:
                break
        logging.debug('connected to a storage node %s' %(addr,))
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
            conn = self._initNodeConnection(node.getServer())
            # add node to node manager
            if not self.storage.nm.hasNode(node):
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
            

class Application(object):
    """The client node application."""

    def __init__(self, master_addr, master_port, **kw):
        logging.basicConfig(level = logging.DEBUG)
        logging.debug('master node address is %s, port is %d' %(master_addr, master_port))

        # Internal Attributes
        self.em = EventManager()
        self.nm = NodeManager()
        self.cm = ConnectionManager(self)        
        self.pt = None
        self.primary_master_node = None
        self.master_conn = None
        self.uuid = None
        self.mq_cache = MQ()
        self.new_oid_list = [] # List of new oid for ZODB
        self.txn = None # The current transaction
        self.tid = None # The current transaction id
        self.txn_finished = 0 # Flag to know when transaction finished on master
        self.txn_stored = 0 # Flag to knwo when transaction has well been stored
        self.loaded_object = None # Current data of the object we are loading
        # object_stored is used to know if storage node
        # accepted the object or raised a conflict
        # 0 : no answer yet
        # 1 : ok
        # 2 : conflict
        self.object_stored = 0
        # Lock definition :
        # _oid_lock is used in order to not call multiple oid
        # generation at the same time
        # _txn_lock lock the entire transaction process, it is acquire
        # at tpc begin and release at tpc_finish or tpc_abort
        # _cache_lock is used for the client cache
        # _load_lock is acquire to protect self.loaded_object used in event
        # handler when retrieving object from storage node
        # _undo_log_lock is used when retrieving undo information
        lock = Lock()
        self._oid_lock_acquire = lock.acquire
        self._oid_lock_release = lock.release                
        lock = Lock()
        self._txn_lock_acquire = lock.acquire
        self._txn_lock_release = lock.release
        lock = Lock()
        self._cache_lock_acquire = lock.acquire
        self._cache_lock_release = lock.release
        lock = Lock()
        self._load_lock_acquire = lock.acquire
        self._load_lock_release = lock.release
        lock = Lock()
        self._undo_log_lock_acquire = lock.acquire
        self._undo_log_lock_release = lock.release

        # XXX Generate an UUID for self. For now, just use a random string.
        # Avoid an invalid UUID.
        if self.uuid is None:
            while 1:
                uuid = os.urandom(16)
                if uuid != INVALID_UUID:
                    break
            self.uuid = uuid

        # Connect to primary master node
        defined_master_addr = (master_addr, master_port)
        while 1:
            self.node_not_ready = 0
            logging.debug("trying to connect to primary master...")                            
            self.connectToPrimaryMasterNode(defined_master_addr)
            if not self.node_not_ready and self.pt.filled():
                # got a connection and partition table
                break
            else:
                # wait a bit before reasking
                t = time()
                while time() < t + 1:
                    pass
        logging.info("connected to primary master node")

    def connectToPrimaryMasterNode(self, defined_master_addr):
          """Connect to the primary master node."""
          handler = MasterEventHandler(self)
          n = MasterNode(server = defined_master_addr)
          self.nm.add(n)

          # Connect to defined master node and get primary master node
          if self.primary_master_node is None:
              conn = ClientConnection(self.em, handler, defined_master_addr)
              msg_id = conn.getNextId()
              p = Packet()
              p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid,
                                          defined_master_addr[0],
                                          defined_master_addr[1], 'main')
              conn.addPacket(p)
              conn.expectMessage(msg_id)
              while 1:
                  self.em.poll(1)
                  if self.primary_master_node is not None:
                      break
                  if self.node_not_ready:
                      # must wait
                      return

          logging.debug('primary master node is %s' %(self.primary_master_node.server,))
          # Close connection if not already connected to primary master node
          if self.primary_master_node.server !=  defined_master_addr:
              for conn in self.em.getConnectionList():
                  if not isinstance(conn, ListeningConnection):
                      conn.close()

              # Connect to primary master node
              conn = ClientConnection(self.em, handler, self.primary_master_node.server)
              msg_id = conn.getNextId()
              p = Packet()
              p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, self.uuid,
                                          self.primary_master_node.server[0],
                                          self.primary_master_node.server[1] , 'main')
              conn.addPacket(p)
              conn.expectMessage(msg_id)
          self.master_conn = conn
          # Wait for primary master node information
          while 1:
              self.em.poll(1)
              if self.pt.filled()  or self.node_not_ready:
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
              conn.getNextId()
              p = Packet()
              p.askNewOIDList(msg_id)
              conn.addPacket(p)
              conn.expectMessage(msg_id)
              # Wait for answer
              while 1:
                  self.em.poll(1)
                  if len(self.new_oid_list) > 0:
                      break
            return sellf.new_oid_list.pop()
        finally:
            self._oid_lock_release()

    def _load(self, oid, serial=""):
        """Internal method which manage load and loadSerial."""
        partition_id = oid % self.num_paritions
        # Only used up to date node for retrieving object
        storage_node_list = [x for x in self.pt.getCellList(partition_id, True) \
                             if x.getState() == UP_TO_DATE_STATE]
        self._load_lock_acquire()
        data = None
        # Store data on each node
        for storage_node in storage_node_list:
            conn = self.cm.getConnForNode(storage_node.getUUID())
            conn.getNextId()
            p = Packet()
            p.askObjectByOID(msg_id, oid, serial)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            # Wait for answer            
            self.loaded_object = None
            try:
                while 1:
                    self.em.poll(1)
                    if self.loaded_object is not None:
                        break
                if self.loaded_object == -1:
                    # OID not found
                    continue
                # Copy object data here to release lock as soon as possible
                noid, serial, compressed, crc, data = self.loaded_object
            finally:
                self._load_lock_release()
            # Check data here
            if noid != oid:
                # Oops, try with next node
                logging.error('got wrong oid %s instead of %s from node %s' \
                              %(noid, oid, storage_node.getServer()))
                # Reacquire lock and try again
                self._load_lock_acquire()
                continue
            elif compressed and crc != alder32(data):
                # Check crc if we use compression
                logging.error('wrong crc from node %s for oid %s' \
                              %(storage_node.getServer(), oid))
                # Reacquire lock and try again
                self._load_lock_acquire()
                continue
            else:
                break
        if data is None:
            # We didn't got any object from storage node
            raise NEOStorageNotFoundError()
        # Uncompress data
        if compressed:
            data = decompressed(data)
        try:
            # Put object into cache
            self.cache_lock_acquire()
            self.cache[oid] = serial, data
            return loads(data), serial
        finally:
            self.cache_lock_release()


    def load(self, oid, version=None):
        """Load an object for a given oid."""
        # First try from cache
        self._cache_lock_acquire = lock.acquire
        try:
            if oid in self.cache:
                return loads(self.cache[oid][1]), self.cache[oid][0]
        finally:
            self._cache_lock_release = lock.release
        # Otherwise get it from storage node
        return self._load(oid)

    def loadSerial(self, oid, serial):
        """Load an object for a given oid."""
        # Do not try in cache as it managed only up-to-date object
        return self._load(oid, serial), None
            
    def lastTransaction(self):
        # does not seem to be used
        return
    
    def tpc_begin(self, transaction, tid=None, status=' '):
        """Begin a new transaction."""
        # First get a transaction, only one is allowed at a time
        self._txn_lock_acquire()
        if self.txn == transaction:
            # Wa have already began the same transaction
            return
        self.txn = transaction
        # Init list of oid used in this transaction
        self.txn_oid_list = []
        # Get a new transaction id if necessary
        if tid is None:
            self.tid = None
            conn = self.master_conn
            conn.getNextId()
            p = Packet()
            p.askNewTID(msg_id)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            # Wait for answer    
            while 1:
                self.em.poll(1)
                if self.tid is not None:
                    break
        else:
            self.tid = tid

    def store(self, oid, serial, data, version, transaction):
        """Store object."""
        if transaction is not self.txn:
            raise POSException.StorageTransactionError(self, transaction)
        # Find which storage node to use
        partition_id = oid % self.num_paritions
        storage_node_list = self.pt.getCellList(partition_id, True)
        # Store data on each node
        for storage_node in storage_node_list:
            conn = self.getConnForNode(storage_node.getUUID())
            conn.getNextId()
            p = Packet()
            # Compres data with zlib
            compressed_data = compress(dumps(data))
            crc = alder32(compressed_data)
            p.askStoreObject(msg_id, oid, serial, 1, crc, compressed_data, self.tid)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            # Wait for answer
            self.object_stored = 0
            while 1:
                self.em.poll(1)
                if self.object_stored == 1:
                    self.txn_oid_list.append(oid)
                    break
                elif self.object.stored == 2:
                    # Conflict, removed oid from list
                    try:
                        self.txn_oid_list.remove(oid)
                    except ValueError:
                        # Oid wasn't already stored in list
                        pass
                    raise NEOStorageConflictError()
            
    def tpc_vote(self, transaction):
        """Store current transaction."""
        if transaction is not self.txn:
            raise POSException.StorageTransactionError(self, transaction)
        user = transaction.user
        desc = transaction.description
        ext = transaction._extension # XXX Need a dump ?
        partition_id = self.tid % self.num_paritions
        storage_node_list = self.pt.getCellList(partition_id, True)
        # Store data on each node
        for storage_node in storage_node_list:
            conn = self.getConnForNode(storage_node.getUUID())
            conn.getNextId()
            p = Packet()
            p.askStoreTransaction(msg_id, self.tid, user, desc, ext, oid_list)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            self.txn_stored == 0
            while 1:
                self.em.poll(1)
                if self.txn_stored:
                    break

    def _clear_txn(self):
        """Clear some transaction parameter and release lock."""
        self.txn = None
        self._txn_lock_release()
        
    def tpc_abort(self, transaction):
        """Abort current transaction."""
        if transaction is not self.txn:
            return
        try:
            # Abort transaction on each node used for it
            # In node where objects were stored
            aborted_node = {} 
            for oid in self.txn_oid_list:
                partition_id = oid % self.num_paritions
                storage_node_list = self.pt.getCellList(partition_id, True)
                for storage_node in storage_node_list:
                    if not aborted_node.has_key(storage_node):
                        conn = self.getConnForNode(storage_node.getUUID())
                        conn.getNextId()
                        p = Packet()
                        p.abortTransaction(msg_id, self.tid)
                        conn.addPacket(p)
                    aborted_node[storage_node] = 1
            # In nodes where transaction was stored
            partition_id = self.tid % self.num_paritions
            storage_node_list = self.pt.getCellList(partition_id, True)
            for storage_node in storage_node_list:
                if not aborted_node.has_key(storage_node):
                    conn = self.getConnForNode(storage_node.getUUID())
                    conn.getNextId()
                    p = Packet()
                    p.abortTransaction(msg_id, self.tid)
                    conn.addPacket(p)
        finally:
            self._clear_txn()
            
    def tpc_finish(self, transaction, f=None):
        """Finish current transaction."""
        if self.txn is not transaction:
            return
        try:
            # Call function given by ZODB
            if f is not None:
              f()
            # Call finish on master
            conn = self.master_conn
            conn.getNextId()
            p = Packet()
            p.finishTransaction(msg_id, self.oid_list, self.tid)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            # Wait for answer
            self.txn_finished = 0
            while 1:
                self.em.poll(1)
                if self.txn_finished:
                    break
            # XXX must update cache here...        
            # Release transaction
            return self.tid
        finally:
            self._clear_txn()

    def loadBefore(self, oid, tid):
        raise NotImplementedError

    def undo(self, transaction_id, txn):
        if transaction is not self.txn:
            raise POSException.StorageTransactionError(self, transaction)
        # First abort on primary master node
        # Second abort on storage node
        # Then invalidate cache
        return tid, oid_list
        
    def undoLog(self, first, last, filter):
        # First get list of transaction from master node
        self.undo_log_lock_acquire()
        try:
            conn = self.master_conn
            conn.getNextId()
            p = Packet()
            p.getTIDList(msg_id, first, last)
            conn.addPacket(p)
            conn.expectMessage(msg_id)
            # Wait for answer
            self.undo_tid_list = None
            while 1:
                self.em.poll(1)
                # must take care of order here
                if self.undo_tid_list is not None:
                    break
            # For each transaction, get info
            undo_txn_list = []
            for tid in undo_tid_list:
                partition_id = tid % self.num_paritions
                storage_node_list = self.pt.getCellList(partition_id, True)
                for storage_node in storage_node_list:
                    conn = self.getConnForNode(storage_node.getUUID())
                    conn.getNextId()
                    p = Packet()
                    p.getTransactionInformation(msg_id, tid)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    # Wait for answer
                    self.undo_txn_info = None
                    while 1:
                        self.em.poll(1)
                        if self.undo_txn_info is not None:
                            break
                undo_txn_list.append(self.undo_txn_info)
            return undo_txn_dict
        finally:
            self.undo_log_lock_release()
            
    def __del__(self):
        """Clear all connection."""
        # Due to bug in ZODB, close is not always called when shutting
        # down zope, so use __del__ to close connections
        for conn in self.em.getConnectionList():
            conn.close()
    close = __del__
