#
# Copyright (C) 2011-2012  Nexedi SA
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

import os, random, socket, sys, tempfile, threading, time, types, weakref
import traceback
from collections import deque
from itertools import count
from functools import wraps
from zlib import decompress
from mock import Mock
import transaction, ZODB
import neo.admin.app, neo.master.app, neo.storage.app
import neo.client.app, neo.neoctl.app
from neo.client import Storage
from neo.lib import bootstrap, logging
from neo.lib.connection import BaseConnection, Connection
from neo.lib.connector import SocketConnector, \
    ConnectorConnectionRefusedException, ConnectorTryAgainException
from neo.lib.event import EventManager
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, NodeTypes, \
    UUID_NAMESPACES, INVALID_UUID
from neo.lib.util import SOCKET_CONNECTORS_DICT, parseMasterList, p64
from .. import NeoTestBase, getTempDirectory, setupMySQLdb, \
    ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, DB_PREFIX, DB_USER

BIND = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE], 0
LOCAL_IP = socket.inet_pton(ADDRESS_TYPE, IP_VERSION_FORMAT_DICT[ADDRESS_TYPE])


class Serialized(object):

    @classmethod
    def init(cls):
        cls._global_lock = threading.Lock()
        cls._global_lock.acquire()
        cls._lock_list = deque()
        cls._lock_lock = threading.Lock()
        cls._pdb = False
        cls.pending = 0

    @classmethod
    def release(cls, lock=None, wake_other=True, stop=None):
        """Suspend lock owner and resume first suspended thread"""
        if lock is None:
            lock = cls._global_lock
            if stop: # XXX: we should fix ClusterStates.STOPPING
                cls.pending = frozenset(stop)
            else:
                cls.pending = 0
        try:
            sys._getframe(1).f_trace.im_self.set_continue()
            cls._pdb = True
        except AttributeError:
            pass
        q = cls._lock_list
        l = cls._lock_lock
        l.acquire()
        try:
            q.append(lock)
            if wake_other:
                q.popleft().release()
        finally:
            l.release()

    @classmethod
    def acquire(cls, lock=None):
        """Suspend all threads except lock owner"""
        if lock is None:
            lock = cls._global_lock
        lock.acquire()
        pending = cls.pending # XXX: getattr once to avoid race conditions
        if type(pending) is frozenset: # XXX
            if lock is cls._global_lock:
                cls.pending = 0
            elif threading.currentThread() in pending:
                sys.exit()
        if cls._pdb:
            cls._pdb = False
            try:
                sys.stdout.write(threading.currentThread().node_name)
            except AttributeError:
                pass
            pdb(1)

    @classmethod
    def tic(cls, lock=None):
        # switch to another thread
        # (the following calls are not supposed to be debugged into)
        cls.release(lock); cls.acquire(lock)

    @classmethod
    def background(cls):
        with cls._lock_lock:
            if cls._lock_list:
                cls._lock_list.popleft().release()

class SerializedEventManager(EventManager):

    _lock = None
    _timeout = 0

    @classmethod
    def decorate(cls, func):
        def decorator(*args, **kw):
            try:
                EventManager.__init__ = types.MethodType(
                    cls.__init__.im_func, None, EventManager)
                return func(*args, **kw)
            finally:
                EventManager.__init__ = types.MethodType(
                    cls._super__init__.im_func, None, EventManager)
        return wraps(func)(decorator)

    _super__init__ = EventManager.__init__.im_func

    def __init__(self):
        cls = self.__class__
        assert cls is EventManager
        self.__class__ = SerializedEventManager
        self._super__init__()

    def _poll(self, timeout=1):
        if self._pending_processing:
            assert timeout <= 0
        elif 0 == self._timeout == timeout == Serialized.pending == len(
            self.writer_set):
            return
        else:
            if self.writer_set and Serialized.pending == 0:
                Serialized.pending = 1
            # Jump to another thread before polling, so that when a message is
            # sent on the network, one can debug immediately the receiving part.
            # XXX: Unfortunately, this means we have a useless full-cycle
            #      before the first message is sent.
            # TODO: Detect where a message is sent to jump immediately to nodes
            #       that will do something.
            Serialized.tic(self._lock)
            if timeout != 0:
                timeout = self._timeout
                if timeout != 0 and Serialized.pending == 1:
                    Serialized.pending = timeout = 0
        EventManager._poll(self, timeout)

    def addReader(self, conn):
        EventManager.addReader(self, conn)
        if type(Serialized.pending) is not frozenset:
            Serialized.pending = 1


class Node(object):

    def filterConnection(self, *peers):
        addr = lambda c: c and (c.accepted_from or c.getAddress())
        addr_set = set(addr(c.connector) for peer in peers
            for c in peer.em.connection_dict.itervalues()
            if isinstance(c, Connection))
        addr_set.discard(None)
        conn_list = (c for c in self.em.connection_dict.itervalues()
            if isinstance(c, Connection) and addr(c.connector) in addr_set)
        return ConnectionFilter(*conn_list)

class ServerNode(Node):

    _server_class_dict = {}

    class __metaclass__(type):
        def __init__(cls, name, bases, d):
            type.__init__(cls, name, bases, d)
            if Node not in bases and threading.Thread not in cls.__mro__:
                cls.__bases__ = bases + (threading.Thread,)
                cls.node_type = getattr(NodeTypes, name[:-11].upper())
                cls._node_list = []
                cls._virtual_ip = socket.inet_ntop(ADDRESS_TYPE,
                    LOCAL_IP[:-1] + chr(2 + len(cls._server_class_dict)))
                cls._server_class_dict[cls._virtual_ip] = cls

    @staticmethod
    def resetPorts():
        for cls in ServerNode._server_class_dict.itervalues():
            del cls._node_list[:]

    @classmethod
    def newAddress(cls):
        address = cls._virtual_ip, len(cls._node_list)
        cls._node_list.append(None)
        return address

    @classmethod
    def resolv(cls, address):
        try:
            cls = cls._server_class_dict[address[0]]
        except KeyError:
            return address
        return cls._node_list[address[1]].getListeningAddress()

    @SerializedEventManager.decorate
    def __init__(self, cluster=None, address=None, **kw):
        if not address:
            address = self.newAddress()
        if cluster is None:
            master_nodes = kw['master_nodes']
            name = kw['name']
        else:
            master_nodes = kw.get('master_nodes', cluster.master_nodes)
            name = kw.get('name', cluster.name)
        port = address[1]
        self._node_list[port] = weakref.proxy(self)
        self._init_args = init_args = kw.copy()
        init_args['cluster'] = cluster
        init_args['address'] = address
        threading.Thread.__init__(self)
        self.daemon = True
        self.node_name = '%s_%u' % (self.node_type, port)
        kw.update(getCluster=name, getBind=address,
                  getMasters=parseMasterList(master_nodes, address))
        super(ServerNode, self).__init__(Mock(kw))

    def getVirtualAddress(self):
        return self._init_args['address']

    def resetNode(self):
        assert not self.isAlive()
        kw = self._init_args
        self.__dict__.clear()
        self.__init__(**kw)

    def start(self):
        Serialized.pending = 1
        self.em._lock = l = threading.Lock()
        l.acquire()
        Serialized.release(l, wake_other=0)
        threading.Thread.start(self)

    def run(self):
        try:
            Serialized.acquire(self.em._lock)
            super(ServerNode, self).run()
        finally:
            self._afterRun()
            logging.debug('stopping %r', self)
            Serialized.background()

    def _afterRun(self):
        try:
            self.listening_conn.close()
        except AttributeError:
            pass

    def stop(self):
        try:
            Serialized.release(stop=(self,))
            self.join()
        finally:
            Serialized.acquire()

    def getListeningAddress(self):
        try:
            return self.listening_conn.getAddress()
        except AttributeError:
            raise ConnectorConnectionRefusedException

class AdminApplication(ServerNode, neo.admin.app.Application):
    pass

class MasterApplication(ServerNode, neo.master.app.Application):
    pass

class StorageApplication(ServerNode, neo.storage.app.Application):

    def resetNode(self, clear_database=False):
        self._init_args['getReset'] = clear_database
        dm = self.dm
        super(StorageApplication, self).resetNode()
        if dm and not clear_database:
            self.dm = dm

    def _afterRun(self):
        super(StorageApplication, self)._afterRun()
        try:
            self.dm.close()
            self.dm = None
        except StandardError: # AttributeError & ProgrammingError
            pass

    def switchTables(self):
        with self.dm as q:
            for table in ('trans', 'obj'):
                q('ALTER TABLE %s RENAME TO tmp' % table)
                q('ALTER TABLE t%s RENAME TO %s' % (table, table))
                q('ALTER TABLE tmp RENAME TO t%s' % table)

    def getDataLockInfo(self):
        dm = self.dm
        checksum_dict = dict(dm.query("SELECT id, hash FROM data"))
        assert set(dm._uncommitted_data).issubset(checksum_dict)
        get = dm._uncommitted_data.get
        return dict((str(v), get(k, 0)) for k, v in checksum_dict.iteritems())

class ClientApplication(Node, neo.client.app.Application):

    @SerializedEventManager.decorate
    def __init__(self, master_nodes, name):
        super(ClientApplication, self).__init__(master_nodes, name)
        self.em._lock = threading.Lock()

    def setPoll(self, master=False):
        if master:
            self.em._timeout = 1
            if not self.em._lock.acquire(0):
                Serialized.background()
        else:
            Serialized.release(wake_other=0); Serialized.acquire()
            self.em._timeout = 0

    def __del__(self):
        try:
            super(ClientApplication, self).__del__()
        finally:
            if self.poll_thread.isAlive():
                Serialized.background()
    close = __del__

    def filterConnection(self, *peers):
        conn_list = []
        for peer in peers:
            if isinstance(peer, MasterApplication):
                conn = self._getMasterConnection()
            else:
                assert isinstance(peer, StorageApplication)
                conn = self.cp.getConnForNode(self.nm.getByUUID(peer.uuid))
            conn_list.append(conn)
        return ConnectionFilter(*conn_list)

class NeoCTL(neo.neoctl.app.NeoCTL):

    @SerializedEventManager.decorate
    def __init__(self, *args, **kw):
        super(NeoCTL, self).__init__(*args, **kw)
        self.em._timeout = -1


class LoggerThreadName(str):

    def __new__(cls, default='TEST'):
        return str.__new__(cls, default)

    def __getattribute__(self, attr):
        return getattr(str(self), attr)

    def __hash__(self):
        return id(self)

    def __str__(self):
        try:
            return threading.currentThread().node_name
        except AttributeError:
            return str.__str__(self)


class Patch(object):

    def __init__(self, patched, **patch):
        (name, patch), = patch.iteritems()
        wrapped = getattr(patched, name)
        wrapper = lambda *args, **kw: patch(wrapped, *args, **kw)
        orig = patched.__dict__.get(name)
        setattr(patched, name, wraps(wrapped)(wrapper))
        if orig is None:
            self._revert = lambda: delattr(patched, name)
        else:
            self._revert = lambda: setattr(patched, name, orig)

    def __del__(self):
        self._revert()


class ConnectionFilter(object):

    filtered_count = 0

    def __init__(self, *conns):
        self.filter_dict = {}
        self.lock = threading.Lock()
        self.conn_list = [(conn, self._patch(conn)) for conn in conns]

    def _patch(self, conn):
        assert '_addPacket' not in conn.__dict__, "already patched"
        lock = self.lock
        filter_dict = self.filter_dict
        orig = conn.__class__._addPacket
        queue = deque()
        def _addPacket(packet):
            lock.acquire()
            try:
                if not queue:
                    for filter in filter_dict:
                        if filter(conn, packet):
                            self.filtered_count += 1
                            break
                    else:
                        return orig(conn, packet)
                queue.append(packet)
            finally:
                lock.release()
        conn._addPacket = _addPacket
        return queue

    def __call__(self, revert=1):
        with self.lock:
            self.filter_dict.clear()
            self._retry()
            if revert:
                for conn, queue in self.conn_list:
                    assert not queue
                    del conn._addPacket
                del self.conn_list[:]

    def _retry(self):
        for conn, queue in self.conn_list:
            while queue:
                packet = queue.popleft()
                for filter in self.filter_dict:
                    if filter(conn, packet):
                        queue.appendleft(packet)
                        break
                else:
                    conn.__class__._addPacket(conn, packet)
                    continue
                break

    def clear(self):
        self(0)

    def add(self, filter, *patches):
        with self.lock:
            self.filter_dict[filter] = patches

    def remove(self, *filters):
        with self.lock:
            for filter in filters:
                del self.filter_dict[filter]
            self._retry()

    def __contains__(self, filter):
        return filter in self.filter_dict

class NEOCluster(object):

    BaseConnection_checkTimeout = staticmethod(BaseConnection.checkTimeout)
    SocketConnector_makeClientConnection = staticmethod(
        SocketConnector.makeClientConnection)
    SocketConnector_makeListeningConnection = staticmethod(
        SocketConnector.makeListeningConnection)
    SocketConnector_receive = staticmethod(SocketConnector.receive)
    SocketConnector_send = staticmethod(SocketConnector.send)
    _patch_count = 0
    _resource_dict = weakref.WeakValueDictionary()

    def _allocate(self, resource, new):
        result = resource, new()
        while result in self._resource_dict:
            result = resource, new()
        self._resource_dict[result] = self
        return result[1]

    @staticmethod
    def _patch():
        cls = NEOCluster
        cls._patch_count += 1
        if cls._patch_count > 1:
            return
        def makeClientConnection(self, addr):
            real_addr = ServerNode.resolv(addr)
            try:
                return cls.SocketConnector_makeClientConnection(self, real_addr)
            finally:
                self.remote_addr = addr
        def send(self, msg):
            result = cls.SocketConnector_send(self, msg)
            if type(Serialized.pending) is not frozenset:
                Serialized.pending = 1
            return result
        def receive(self):
            # If the peer sent an entire packet, make sure we read it entirely,
            # otherwise Serialize.pending would be reset to 0.
            data = ''
            try:
                while True:
                    d = cls.SocketConnector_receive(self)
                    if not d:
                        return data
                    data += d
            except ConnectorTryAgainException:
                if data:
                    return data
                raise
        # TODO: 'sleep' should 'tic' in a smart way, so that storages can be
        #       safely started even if the cluster isn't.
        bootstrap.sleep = lambda seconds: None
        BaseConnection.checkTimeout = lambda self, t: None
        SocketConnector.makeClientConnection = makeClientConnection
        SocketConnector.makeListeningConnection = lambda self, addr: \
            cls.SocketConnector_makeListeningConnection(self, BIND)
        SocketConnector.receive = receive
        SocketConnector.send = send
        Serialized.init()

    @staticmethod
    def _unpatch():
        cls = NEOCluster
        assert cls._patch_count > 0
        cls._patch_count -= 1
        if cls._patch_count:
            return
        bootstrap.sleep = time.sleep
        BaseConnection.checkTimeout = cls.BaseConnection_checkTimeout
        SocketConnector.makeClientConnection = \
            cls.SocketConnector_makeClientConnection
        SocketConnector.makeListeningConnection = \
            cls.SocketConnector_makeListeningConnection
        SocketConnector.receive = cls.SocketConnector_receive
        SocketConnector.send = cls.SocketConnector_send

    def __init__(self, master_count=1, partitions=1, replicas=0, upstream=None,
                       adapter=os.getenv('NEO_TESTS_ADAPTER', 'SQLite'),
                       storage_count=None, db_list=None, clear_databases=True,
                       db_user=DB_USER, db_password=''):
        self.name = 'neo_%s' % self._allocate('name',
            lambda: random.randint(0, 100))
        master_list = [MasterApplication.newAddress()
                       for _ in xrange(master_count)]
        self.master_nodes = ' '.join('%s:%s' % x for x in master_list)
        weak_self = weakref.proxy(self)
        kw = dict(cluster=weak_self, getReplicas=replicas, getAdapter=adapter,
                  getPartitions=partitions, getReset=clear_databases)
        if upstream is not None:
          self.upstream = weakref.proxy(upstream)
          kw.update(getUpstreamCluster=upstream.name,
                    getUpstreamMasters=parseMasterList(upstream.master_nodes))
        self.master_list = [MasterApplication(address=x, **kw)
                            for x in master_list]
        if db_list is None:
            if storage_count is None:
                storage_count = replicas + 1
            index = count().next
            db_list = ['%s%u' % (DB_PREFIX, self._allocate('db', index))
                       for _ in xrange(storage_count)]
        if adapter == 'MySQL':
            setupMySQLdb(db_list, db_user, db_password, clear_databases)
            db = '%s:%s@%%s' % (db_user, db_password)
        elif adapter == 'SQLite':
            db = os.path.join(getTempDirectory(), '%s.sqlite')
        else:
            assert False, adapter
        self.storage_list = [StorageApplication(getDatabase=db % x, **kw)
                             for x in db_list]
        self.admin_list = [AdminApplication(**kw)]
        self.client = ClientApplication(name=self.name,
            master_nodes=self.master_nodes)
        self.neoctl = NeoCTL(self.admin.getVirtualAddress())

    # A few shortcuts that work when there's only 1 master/storage/admin
    @property
    def master(self):
        master, = self.master_list
        return master
    @property
    def storage(self):
        storage, = self.storage_list
        return storage
    @property
    def admin(self):
        admin, = self.admin_list
        return admin
    ###

    @property
    def primary_master(self):
        master, = [master for master in self.master_list if master.primary]
        return master

    def reset(self, clear_database=False):
        for node_type in 'master', 'storage', 'admin':
            kw = {}
            if node_type == 'storage':
                kw['clear_database'] = clear_database
            for node in getattr(self, node_type + '_list'):
                node.resetNode(**kw)
        self.client = ClientApplication(name=self.name,
            master_nodes=self.master_nodes)
        self.neoctl = NeoCTL(self.admin.getVirtualAddress())

    def start(self, storage_list=None, fast_startup=False):
        self._patch()
        for node_type in 'master', 'admin':
            for node in getattr(self, node_type + '_list'):
                node.start()
        self.tic()
        if fast_startup:
            self._startCluster()
        if storage_list is None:
            storage_list = self.storage_list
        for node in storage_list:
            node.start()
        self.tic()
        if not fast_startup:
            self._startCluster()
            self.tic()
        state = self.neoctl.getClusterState()
        assert state in (ClusterStates.RUNNING, ClusterStates.BACKINGUP), state
        self.enableStorageList(storage_list)

    def _startCluster(self):
        try:
            self.neoctl.startCluster()
        except RuntimeError:
            self.tic()
            if self.neoctl.getClusterState() not in (
                      ClusterStates.BACKINGUP,
                      ClusterStates.RUNNING,
                      ClusterStates.VERIFYING,
                  ):
                raise

    def enableStorageList(self, storage_list):
        self.neoctl.enableStorageList([x.uuid for x in storage_list])
        self.tic()
        for node in storage_list:
            assert self.getNodeState(node) == NodeStates.RUNNING

    @property
    def db(self):
        try:
            return self._db
        except AttributeError:
            self._db = db = ZODB.DB(storage=self.getZODBStorage())
            return db

    def stop(self):
        if hasattr(self, '_db') and self.client.em._timeout == 0:
            self.client.setPoll(True)
        self.__dict__.pop('_db', self.client).close()
        #self.neoctl.setClusterState(ClusterStates.STOPPING) # TODO
        try:
            Serialized.release(stop=
                self.admin_list + self.storage_list + self.master_list)
            for node_type in 'admin', 'storage', 'master':
                for node in getattr(self, node_type + '_list'):
                    if node.isAlive():
                        node.join()
        finally:
            Serialized.acquire()
        self._unpatch()

    @staticmethod
    def tic(force=False):
        # XXX: Should we automatically switch client in slave mode if it isn't ?
        logging.info('tic ...')
        if force:
            Serialized.tic()
            logging.info('forced tic')
        while Serialized.pending:
            Serialized.tic()
            logging.info('tic')

    def getNodeState(self, node):
        uuid = node.uuid
        for node in self.neoctl.getNodeList(node.node_type):
            if node[2] == uuid:
                return node[3]

    def getOudatedCells(self):
        return [cell for row in self.neoctl.getPartitionRowList()[1]
                     for cell in row[1]
                     if cell[1] == CellStates.OUT_OF_DATE]

    def getZODBStorage(self, **kw):
        # automatically put client in master mode
        if self.client.em._timeout == 0:
            self.client.setPoll(True)
        return Storage.Storage(None, self.name, _app=self.client, **kw)

    def importZODB(self, dummy_zodb=None, random=random):
        if dummy_zodb is None:
            from ..stat_zodb import PROD1
            dummy_zodb = PROD1(random)
        preindex = {}
        as_storage = dummy_zodb.as_storage
        return lambda count: self.getZODBStorage().importFrom(
            as_storage(count), preindex=preindex)

    def populate(self, transaction_list, tid=lambda i: p64(i+1),
                                         oid=lambda i: p64(i+1)):
        storage = self.getZODBStorage()
        tid_dict = {}
        for i, oid_list in enumerate(transaction_list):
            txn = transaction.Transaction()
            storage.tpc_begin(txn, tid(i))
            for o in oid_list:
                storage.store(oid(o), tid_dict.get(o), repr((i, o)), '', txn)
            storage.tpc_vote(txn)
            i = storage.tpc_finish(txn)
            for o in oid_list:
                tid_dict[o] = i

    def getTransaction(self):
        txn = transaction.TransactionManager()
        return txn, self.db.open(transaction_manager=txn)

    def __del__(self, __print_exc=traceback.print_exc):
        try:
            self.neoctl.close()
            for node_type in 'admin', 'storage', 'master':
                for node in getattr(self, node_type + '_list'):
                    node.close()
            self.client.em.close()
        except:
            __print_exc()
            raise

    def extraCellSortKey(self, key):
        return Patch(self.client.cp, _getCellSortKey=lambda orig, *args:
            (orig(*args), key(*args)))


class NEOThreadedTest(NeoTestBase):

    def setupLog(self):
        log_file = os.path.join(getTempDirectory(), self.id() + '.log')
        logging.setup(log_file)
        return LoggerThreadName()

    def _tearDown(self, success):
        super(NEOThreadedTest, self)._tearDown(success)
        ServerNode.resetPorts()
        if success:
            q = logging.db.execute
            q("UPDATE packet SET body=NULL")
            q("VACUUM")

    def getUnpickler(self, conn):
        reader = conn._reader
        def unpickler(data, compression=False):
            if compression:
                data = decompress(data)
            obj = reader.getGhost(data)
            reader.setGhostState(obj, data)
            return obj
        return unpickler

    class newThread(threading.Thread):

        def __init__(self, func, *args, **kw):
            threading.Thread.__init__(self)
            self.__target = func, args, kw
            self.daemon = True
            self.start()

        def run(self):
            try:
                apply(*self.__target)
                self.__exc_info = None
            except:
                self.__exc_info = sys.exc_info()

        def join(self, timeout=None):
            threading.Thread.join(self, timeout)
            if not self.isAlive() and self.__exc_info:
                etype, value, tb = self.__exc_info
                del self.__exc_info
                raise etype, value, tb


def predictable_random(seed=None):
    # Because we have 2 running threads when client works, we can't
    # patch neo.client.pool (and cluster should have 1 storage).
    from neo.master import backup_app
    from neo.master.handlers import administration
    from neo.storage import replicator
    def decorator(wrapped):
        def wrapper(*args, **kw):
            s = repr(time.time()) if seed is None else seed
            logging.info("using seed %r", s)
            r = random.Random(s)
            try:
                def getNewUUID(self, uuid, address, node_type):
                    if node_type == NodeTypes.CLIENT:
                        return super(MasterApplication, self).getNewUUID(uuid,
                            address, node_type)
                    while uuid is None or uuid == self.uuid:
                        node = self.nm.getByUUID(uuid)
                        if node is None or node.getAddress() in (None, addr):
                            uuid = UUID_NAMESPACES[node_type] + ''.join(
                                chr(r.randrange(256)) for _ in xrange(15))
                    return uuid
                MasterApplication.getNewUUID = getNewUUID

                administration.random = backup_app.random = replicator.random \
                    = r
                return wrapped(*args, **kw)
            finally:
                del MasterApplication.getNewUUID
                administration.random = backup_app.random = replicator.random \
                    = random
        return wraps(wrapped)(wrapper)
    return decorator
