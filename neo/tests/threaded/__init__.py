#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import os, random, socket, sys, tempfile, threading, time, types, weakref
from collections import deque
from functools import wraps
from zlib import decompress
from mock import Mock
import transaction, ZODB
import neo.admin.app, neo.master.app, neo.storage.app
import neo.client.app, neo.neoctl.app
from neo.client import Storage
from neo.lib import bootstrap, setupLog
from neo.lib.connection import BaseConnection, Connection
from neo.lib.connector import SocketConnector, \
    ConnectorConnectionRefusedException
from neo.lib.event import EventManager
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, NodeTypes
from neo.lib.util import SOCKET_CONNECTORS_DICT, parseMasterList
from neo.tests import NeoTestBase, getTempDirectory, setupMySQLdb, \
    ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, DB_PREFIX, DB_USER

BIND = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE], 0
LOCAL_IP = socket.inet_pton(ADDRESS_TYPE, IP_VERSION_FORMAT_DICT[ADDRESS_TYPE])
SERVER_TYPE = ['master', 'storage', 'admin']
VIRTUAL_IP = [socket.inet_ntop(ADDRESS_TYPE, LOCAL_IP[:-1] + chr(2 + i))
              for i in xrange(len(SERVER_TYPE))]

def getVirtualIp(server_type):
    return VIRTUAL_IP[SERVER_TYPE.index(server_type)]


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
    def release(cls, lock=None, wake_other=True, stop=False):
        """Suspend lock owner and resume first suspended thread"""
        if lock is None:
            lock = cls._global_lock
            if stop: # XXX: we should fix ClusterStates.STOPPING
                cls.pending = None
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
        if cls.pending is None: # XXX
            if lock is cls._global_lock:
                cls.pending = 0
            else:
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
        cls._lock_lock.acquire()
        try:
            if cls._lock_list:
                cls._lock_list.popleft().release()
        finally:
            cls._lock_lock.release()

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
            assert not timeout
        elif 0 == self._timeout == timeout == Serialized.pending == len(
            self.writer_set):
            return
        else:
            if self.writer_set and Serialized.pending is not None:
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
                if timeout != 0 and Serialized.pending:
                    Serialized.pending = timeout = 0
        EventManager._poll(self, timeout)


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

    class __metaclass__(type):
        def __init__(cls, name, bases, d):
            type.__init__(cls, name, bases, d)
            if Node not in bases and threading.Thread not in cls.__mro__:
                cls.__bases__ = bases + (threading.Thread,)

    @SerializedEventManager.decorate
    def __init__(self, cluster, address, **kw):
        self._init_args = (cluster, address), dict(kw)
        threading.Thread.__init__(self)
        self.setDaemon(True)
        h, p = address
        self.node_type = getattr(NodeTypes,
            SERVER_TYPE[VIRTUAL_IP.index(h)].upper())
        self.node_name = '%s_%u' % (self.node_type, p)
        kw.update(getCluster=cluster.name, getBind=address,
                  getMasters=parseMasterList(cluster.master_nodes, address))
        super(ServerNode, self).__init__(Mock(kw))

    def resetNode(self):
        assert not self.isAlive()
        args, kw = self._init_args
        kw['getUUID'] = self.uuid
        self.__dict__.clear()
        self.__init__(*args, **kw)

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
            neo.lib.logging.debug('stopping %r', self)
            Serialized.background()

    def _afterRun(self):
        try:
            self.listening_conn.close()
        except AttributeError:
            pass

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
        self._init_args[1]['getReset'] = clear_database
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
        adapter = self._init_args[1]['getAdapter']
        dm = self.dm
        if adapter == 'BTree':
            dm._obj, dm._tobj = dm._tobj, dm._obj
            dm._trans, dm._ttrans = dm._ttrans, dm._trans
            uncommitted_data = dm._uncommitted_data
            for checksum, (_, _, index) in dm._data.iteritems():
                uncommitted_data[checksum] = len(index)
                index.clear()
        elif adapter == 'MySQL':
            q = dm.query
            dm.begin()
            for table in ('trans', 'obj'):
                q('RENAME TABLE %s to tmp' % table)
                q('RENAME TABLE t%s to %s' % (table, table))
                q('RENAME TABLE tmp to t%s' % table)
            dm.commit()
        else:
            assert False

    def getDataLockInfo(self):
        adapter = self._init_args[1]['getAdapter']
        dm = self.dm
        if adapter == 'BTree':
            checksum_list = dm._data
        elif adapter == 'MySQL':
            checksum_list = [x for x, in dm.query("SELECT hash FROM data")]
        else:
            assert False
        assert set(dm._uncommitted_data).issubset(checksum_list)
        return dict((x, dm._uncommitted_data.get(x, 0)) for x in checksum_list)

class ClientApplication(Node, neo.client.app.Application):

    @SerializedEventManager.decorate
    def __init__(self, cluster):
        super(ClientApplication, self).__init__(
            cluster.master_nodes, cluster.name)
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
    def __init__(self, cluster, address=(getVirtualIp('admin'), 0)):
        self._cluster = cluster
        super(NeoCTL, self).__init__(address)
        self.em._timeout = None

    server = property(lambda self: self._cluster.resolv(self._server),
                      lambda self, address: setattr(self, '_server', address))


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

    def __init__(self, *conns):
        self.filter_dict = {}
        self.lock = threading.Lock()
        self.conn_list = [(conn, self._patch(conn)) for conn in conns]

    def _patch(self, conn):
        assert '_addPacket' not in conn.__dict__
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
                            break
                    else:
                        return orig(conn, packet)
                queue.append(packet)
            finally:
                lock.release()
        conn._addPacket = _addPacket
        return queue

    def __call__(self, revert=1):
        self.lock.acquire()
        try:
            self.filter_dict.clear()
            self._retry()
            if revert:
                for conn, queue in self.conn_list:
                    assert not queue
                    del conn._addPacket
                del self.conn_list[:]
        finally:
            self.lock.release()

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
        self.lock.acquire()
        try:
            self.filter_dict[filter] = patches
        finally:
            self.lock.release()

    def remove(self, *filters):
        self.lock.acquire()
        try:
            for filter in filters:
                del self.filter_dict[filter]
            self._retry()
        finally:
            self.lock.release()

    def __contains__(self, filter):
        return filter in self.filter_dict

class NEOCluster(object):

    BaseConnection_checkTimeout = staticmethod(BaseConnection.checkTimeout)
    SocketConnector_makeClientConnection = staticmethod(
        SocketConnector.makeClientConnection)
    SocketConnector_makeListeningConnection = staticmethod(
        SocketConnector.makeListeningConnection)
    SocketConnector_send = staticmethod(SocketConnector.send)
    Storage__init__ = staticmethod(Storage.__init__)

    _patched = threading.Lock()

    def _patch(cluster):
        cls = cluster.__class__
        if not cls._patched.acquire(0):
            raise RuntimeError("Can't run several cluster at the same time")
        def makeClientConnection(self, addr):
            try:
                real_addr = cluster.resolv(addr)
                return cls.SocketConnector_makeClientConnection(self, real_addr)
            finally:
                self.remote_addr = addr
        def send(self, msg):
            result = cls.SocketConnector_send(self, msg)
            if Serialized.pending is not None:
                Serialized.pending = 1
            return result
        # TODO: 'sleep' should 'tic' in a smart way, so that storages can be
        #       safely started even if the cluster isn't.
        bootstrap.sleep = lambda seconds: None
        BaseConnection.checkTimeout = lambda self, t: None
        SocketConnector.makeClientConnection = makeClientConnection
        SocketConnector.makeListeningConnection = lambda self, addr: \
            cls.SocketConnector_makeListeningConnection(self, BIND)
        SocketConnector.send = send
        Storage.setupLog = lambda *args, **kw: None

    @classmethod
    def _unpatch(cls):
        bootstrap.sleep = time.sleep
        BaseConnection.checkTimeout = cls.BaseConnection_checkTimeout
        SocketConnector.makeClientConnection = \
            cls.SocketConnector_makeClientConnection
        SocketConnector.makeListeningConnection = \
            cls.SocketConnector_makeListeningConnection
        SocketConnector.send = cls.SocketConnector_send
        Storage.setupLog = setupLog
        cls._patched.release()

    def __init__(self, master_count=1, partitions=1, replicas=0,
                       adapter=os.getenv('NEO_TESTS_ADAPTER', 'BTree'),
                       storage_count=None, db_list=None, clear_databases=True,
                       db_user=DB_USER, db_password='', verbose=None):
        if verbose is not None:
            temp_dir = os.getenv('TEMP') or \
                os.path.join(tempfile.gettempdir(), 'neo_tests')
            os.path.exists(temp_dir) or os.makedirs(temp_dir)
            log_file = tempfile.mkstemp('.log', '', temp_dir)[1]
            print 'Logging to %r' % log_file
            setupLog(LoggerThreadName(), log_file, verbose)
        self.name = 'neo_%s' % random.randint(0, 100)
        ip = getVirtualIp('master')
        self.master_nodes = ' '.join('%s:%s' % (ip, i)
                                     for i in xrange(master_count))
        weak_self = weakref.proxy(self)
        kw = dict(cluster=weak_self, getReplicas=replicas, getAdapter=adapter,
                  getPartitions=partitions, getReset=clear_databases)
        self.master_list = [MasterApplication(address=(ip, i), **kw)
                            for i in xrange(master_count)]
        ip = getVirtualIp('storage')
        if db_list is None:
            if storage_count is None:
                storage_count = replicas + 1
            db_list = ['%s%u' % (DB_PREFIX, i) for i in xrange(storage_count)]
        setupMySQLdb(db_list, db_user, db_password, clear_databases)
        db = '%s:%s@%%s' % (db_user, db_password)
        self.storage_list = [StorageApplication(address=(ip, i),
                                                getDatabase=db % x, **kw)
                             for i, x in enumerate(db_list)]
        ip = getVirtualIp('admin')
        self.admin_list = [AdminApplication(address=(ip, 0), **kw)]
        self.client = ClientApplication(weak_self)
        self.neoctl = NeoCTL(weak_self)

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

    def resolv(self, addr):
        host, port = addr
        try:
            attr = SERVER_TYPE[VIRTUAL_IP.index(host)] + '_list'
        except ValueError:
            return addr
        return getattr(self, attr)[port].getListeningAddress()

    def reset(self, clear_database=False):
        for node_type in SERVER_TYPE:
            kw = {}
            if node_type == 'storage':
                kw['clear_database'] = clear_database
            for node in getattr(self, node_type + '_list'):
                node.resetNode(**kw)
        self.client = ClientApplication(self)
        self.neoctl = NeoCTL(weakref.proxy(self))

    def start(self, storage_list=None, fast_startup=True):
        self._patch()
        Serialized.init()
        for node_type in 'master', 'admin':
            for node in getattr(self, node_type + '_list'):
                node.start()
        self.tic()
        if fast_startup:
            self.neoctl.startCluster()
        if storage_list is None:
            storage_list = self.storage_list
        for node in storage_list:
            node.start()
        self.tic()
        if not fast_startup:
            self.neoctl.startCluster()
            self.tic()
        assert self.neoctl.getClusterState() == ClusterStates.RUNNING
        self.enableStorageList(storage_list)

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
        self.__dict__.pop('_db', self.client).close()
        #self.neoctl.setClusterState(ClusterStates.STOPPING) # TODO
        try:
            Serialized.release(stop=1)
            for node_type in SERVER_TYPE[::-1]:
                for node in getattr(self, node_type + '_list'):
                    if node.isAlive():
                        node.join()
        finally:
            Serialized.acquire()
        self._unpatch()

    def tic(self, force=False):
        if force:
            Serialized.tic()
        while Serialized.pending:
            Serialized.tic()

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

    def getTransaction(self):
        txn = transaction.TransactionManager()
        return txn, self.db.open(transaction_manager=txn)

    def __del__(self):
        self.neoctl.close()
        for node_type in 'admin', 'storage', 'master':
            for node in getattr(self, node_type + '_list'):
                node.close()
        self.client.em.close()

    def extraCellSortKey(self, key):
        return Patch(self.client.cp, _getCellSortKey=lambda orig, *args:
            (orig(*args), key(*args)))


class NEOThreadedTest(NeoTestBase):

    def setupLog(self):
        log_file = os.path.join(getTempDirectory(), self.id() + '.log')
        setupLog(LoggerThreadName(), log_file, True)

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
            self.setDaemon(True)
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
