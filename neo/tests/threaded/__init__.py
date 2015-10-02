#
# Copyright (C) 2011-2015  Nexedi SA
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

# XXX: Consider using ClusterStates.STOPPING to stop clusters

import os, random, select, socket, sys, tempfile, threading, time, weakref
import traceback
from collections import deque
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
from itertools import count
from functools import wraps
from zlib import decompress
from mock import Mock
import transaction, ZODB
import neo.admin.app, neo.master.app, neo.storage.app
import neo.client.app, neo.neoctl.app
from neo.client import Storage
from neo.lib import logging
from neo.lib.connection import BaseConnection, Connection
from neo.lib.connector import SocketConnector, ConnectorException
from neo.lib.locking import SimpleQueue
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, NodeTypes
from neo.lib.util import cached_property, parseMasterList, p64
from .. import NeoTestBase, Patch, getTempDirectory, setupMySQLdb, \
    ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, DB_PREFIX, DB_USER

BIND = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE], 0
LOCAL_IP = socket.inet_pton(ADDRESS_TYPE, IP_VERSION_FORMAT_DICT[ADDRESS_TYPE])


class FairLock(deque):
    """Same as a threading.Lock except that waiting threads are queued, so that
    the first one waiting for the lock is the first to get it. This is useful
    when several concurrent threads fight for the same resource in loop:
    the owner could give too little time for other to get a chance to acquire,
    blocking them for a long time with bad luck.
    """

    def __enter__(self, _allocate_lock=threading.Lock):
        me = _allocate_lock()
        me.acquire()
        self.append(me)
        other = self[0]
        while me is not other:
            with other:
                other = self[0]

    def __exit__(self, t, v, tb):
        self.popleft().release()


class Serialized(object):
    """
    "Threaded" tests run all nodes in the same process as the test itself,
    and threads are scheduled by this class, which mainly provides 2 features:
    - more determinism, by minimizing the number of active threads, and
      switching them in a round-robin;
    - tic() method to wait only the necessary time for the cluster to be idle.

    The basic concept is that each thread has a lock that always gets acquired
    by itself. The following pattern is used to yield the processor to the next
    thread:
        release(); acquire()
    It should be noted that this is not atomic, i.e. all other threads
    sometimes complete before a thread tries to acquire its lock: in order that
    the previous thread does not fail by releasing an un-acquired lock,
    we actually use Semaphores instead of Locks.

    The epoll object of each node is hooked so that thread switching happens
    before polling for network activity. An extra epoll object is used to
    detect which node has a readable epoll object.
    """
    check_timeout = False

    @classmethod
    def init(cls):
        cls._busy = set()
        cls._busy_cond = threading.Condition(threading.Lock())
        cls._epoll = select.epoll()
        cls._pdb = None
        cls._sched_lock = threading.Semaphore(0)
        cls._tic_lock = FairLock()
        cls._fd_dict = {}

    @classmethod
    def idle(cls, owner):
        with cls._busy_cond:
            cls._busy.discard(owner)
            cls._busy_cond.notify_all()

    @classmethod
    def stop(cls):
        assert not cls._fd_dict, cls._fd_dict
        del(cls._busy, cls._busy_cond, cls._epoll, cls._fd_dict,
            cls._pdb, cls._sched_lock, cls._tic_lock)

    @classmethod
    def _sort_key(cls, fd_event):
        return -cls._fd_dict[fd_event[0]]._last

    @classmethod
    @contextmanager
    def pdb(cls):
        try:
            cls._pdb = sys._getframe(2).f_trace.im_self
            cls._pdb.set_continue()
        except AttributeError:
            pass
        yield
        p = cls._pdb
        if p is not None:
            cls._pdb = None
            t = threading.currentThread()
            p.stdout.write(getattr(t, 'node_name', t.name))
            p.set_trace(sys._getframe(3))

    @classmethod
    def tic(cls, step=-1, check_timeout=()):
        # If you're in a pdb here, 'n' switches to another thread
        # (the following lines are not supposed to be debugged into)
        with cls._tic_lock, cls.pdb():
            f = sys._getframe(1)
            try:
                logging.info('tic (%s:%u) ...',
                    f.f_code.co_filename, f.f_lineno)
            finally:
                del f
            if cls._busy:
                with cls._busy_cond:
                    while cls._busy:
                        cls._busy_cond.wait()
            for app in check_timeout:
                app.em.epoll.check_timeout = True
                app.em.wakeup()
                del app
            while step:
                event_list = cls._epoll.poll(0)
                if not event_list:
                    break
                step -= 1
                event_list.sort(key=cls._sort_key)
                next_lock = cls._sched_lock
                for fd, event in event_list:
                    self = cls._fd_dict[fd]
                    self._release_next = next_lock.release
                    next_lock = self._lock
                del self
                next_lock.release()
                cls._sched_lock.acquire()

    def __init__(self, app, busy=True):
        self._epoll = app.em.epoll
        app.em.epoll = self
        # XXX: It may have been initialized before the SimpleQueue is patched.
        thread_container = getattr(app, '_thread_container', None)
        thread_container is None or thread_container.__init__()
        if busy:
            self._busy.add(self) # block tic until app waits for polling

    def __getattr__(self, attr):
        if attr in ('close', 'modify', 'register', 'unregister'):
            return getattr(self._epoll, attr)
        return self.__getattribute__(attr)

    def poll(self, timeout):
        if self.check_timeout:
            assert timeout >= 0, (self, timeout)
            del self.check_timeout
        elif timeout:
            with self.pdb(): # same as in tic()
                release = self._release_next
                self._release_next = None
                release()
                self._lock.acquire()
                self._last = time.time()
        return self._epoll.poll(timeout)

    def _release_next(self):
        self._last = time.time()
        self._lock = threading.Semaphore(0)
        fd = self._epoll.fileno()
        cls = self.__class__
        cls._fd_dict[fd] = self
        cls._epoll.register(fd)
        cls.idle(self)

    def exit(self):
        fd = self._epoll.fileno()
        cls = self.__class__
        if cls._fd_dict.pop(fd, None) is None:
            cls.idle(self)
        else:
            cls._epoll.unregister(fd)
            self._release_next()

class TestSerialized(Serialized):

    def __init__(*args):
        Serialized.__init__(busy=False, *args)

    def poll(self, timeout):
        if timeout:
            while 1:
                r = self._epoll.poll(0)
                if r:
                    return r
                Serialized.tic(step=1)
        return self._epoll.poll(timeout)


class Node(object):

    def getConnectionList(self, *peers):
        addr = lambda c: c and (c.addr if c.is_server else c.getAddress())
        addr_set = {addr(c.connector) for peer in peers
            for c in peer.em.connection_dict.itervalues()
            if isinstance(c, Connection)}
        addr_set.discard(None)
        return (c for c in self.em.connection_dict.itervalues()
            if isinstance(c, Connection) and addr(c.connector) in addr_set)

    def filterConnection(self, *peers):
        return ConnectionFilter(self.getConnectionList(*peers))

class ServerNode(Node):

    _server_class_dict = {}

    class __metaclass__(type):
        def __init__(cls, name, bases, d):
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
        assert not self.is_alive()
        kw = self._init_args
        self.close()
        self.__init__(**kw)

    def start(self):
        Serialized(self)
        threading.Thread.start(self)

    def run(self):
        try:
            super(ServerNode, self).run()
        finally:
            self._afterRun()
            logging.debug('stopping %r', self)
            self.em.epoll.exit()

    def _afterRun(self):
        try:
            self.listening_conn.close()
        except AttributeError:
            pass

    def getListeningAddress(self):
        try:
            return self.listening_conn.getAddress()
        except AttributeError:
            raise ConnectorException

class AdminApplication(ServerNode, neo.admin.app.Application):
    pass

class MasterApplication(ServerNode, neo.master.app.Application):
    pass

class StorageApplication(ServerNode, neo.storage.app.Application):

    dm = type('', (), {'close': lambda self: None})()

    def resetNode(self, clear_database=False):
        self._init_args['getReset'] = clear_database
        super(StorageApplication, self).resetNode()

    def _afterRun(self):
        super(StorageApplication, self)._afterRun()
        try:
            self.dm.close()
            del self.dm
        except StandardError: # AttributeError & ProgrammingError
            pass

    def getAdapter(self):
        return self._init_args['getAdapter']

    def switchTables(self):
        q = self.dm.query
        for table in 'trans', 'obj':
            q('ALTER TABLE %s RENAME TO tmp' % table)
            q('ALTER TABLE t%s RENAME TO %s' % (table, table))
            q('ALTER TABLE tmp RENAME TO t%s' % table)

    def getDataLockInfo(self):
        dm = self.dm
        index = tuple(dm.query("SELECT id, hash, compression FROM data"))
        assert set(dm._uncommitted_data).issubset(x[0] for x in index)
        get = dm._uncommitted_data.get
        return {(str(h), c & 0x7f): get(i, 0) for i, h, c in index}

    def sqlCount(self, table):
        (r,), = self.dm.query("SELECT COUNT(*) FROM " + table)
        return r

class ClientApplication(Node, neo.client.app.Application):

    def __init__(self, master_nodes, name, **kw):
        super(ClientApplication, self).__init__(master_nodes, name, **kw)
        self.poll_thread.node_name = name

    def _run(self):
        try:
            super(ClientApplication, self)._run()
        finally:
            self.em.epoll.exit()

    def start(self):
        isinstance(self.em.epoll, Serialized) or Serialized(self)
        super(ClientApplication, self).start()

    def getConnectionList(self, *peers):
        for peer in peers:
            if isinstance(peer, MasterApplication):
                conn = self._getMasterConnection()
            else:
                assert isinstance(peer, StorageApplication)
                conn = self.cp.getConnForNode(self.nm.getByUUID(peer.uuid))
            yield conn

class NeoCTL(neo.neoctl.app.NeoCTL):

    def __init__(self, *args, **kw):
        super(NeoCTL, self).__init__(*args, **kw)
        TestSerialized(self)


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


class ConnectionFilter(object):

    filtered_count = 0
    filter_list = []
    filter_queue = weakref.WeakKeyDictionary()
    lock = threading.Lock()
    _addPacket = Connection._addPacket

    @contextmanager
    def __new__(cls, conn_list=()):
        self = object.__new__(cls)
        self.filter_dict = {}
        self.conn_list = frozenset(conn_list)
        if not cls.filter_list:
            def _addPacket(conn, packet):
                with cls.lock:
                    try:
                        queue = cls.filter_queue[conn]
                    except KeyError:
                        for self in cls.filter_list:
                            if self(conn, packet):
                                self.filtered_count += 1
                                break
                        else:
                            return cls._addPacket(conn, packet)
                        cls.filter_queue[conn] = queue = deque()
                    p = packet.__new__(packet.__class__)
                    p.__dict__.update(packet.__dict__)
                    queue.append(p)
            Connection._addPacket = _addPacket
        try:
            cls.filter_list.append(self)
            yield self
        finally:
            del cls.filter_list[-1:]
            if not cls.filter_list:
                Connection._addPacket = cls._addPacket.im_func
        with cls.lock:
            cls._retry()

    def __call__(self, conn, packet):
        if not self.conn_list or conn in self.conn_list:
            for filter in self.filter_dict:
                if filter(conn, packet):
                    return True
        return False

    @classmethod
    def _retry(cls):
        for conn, queue in cls.filter_queue.items():
            while queue:
                packet = queue.popleft()
                for self in cls.filter_list:
                    if self(conn, packet):
                        queue.appendleft(packet)
                        break
                else:
                    cls._addPacket(conn, packet)
                    continue
                break
            else:
                del cls.filter_queue[conn]

    def add(self, filter, *patches):
        with self.lock:
            self.filter_dict[filter] = patches
            for p in patches:
                p.apply()

    def remove(self, *filters):
        with self.lock:
            for filter in filters:
                del self.filter_dict[filter]
            self._retry()

    def discard(self, *filters):
        try:
            self.remove(*filters)
        except KeyError:
            pass

    def __contains__(self, filter):
        return filter in self.filter_dict

class NEOCluster(object):

    SSL = None

    def __init__(orig, self): # temporary definition for SimpleQueue patch
        orig(self)
        lock = self._lock
        def _lock(blocking=True):
            if blocking:
                while not lock(False):
                    Serialized.tic(step=1)
                return True
            return lock(False)
        self._lock = _lock
    _patches = (
        Patch(BaseConnection, getTimeout=lambda orig, self: None),
        Patch(SimpleQueue, __init__=__init__),
        Patch(SocketConnector, CONNECT_LIMIT=0),
        Patch(SocketConnector, _bind=lambda orig, self, addr: orig(self, BIND)),
        Patch(SocketConnector, _connect = lambda orig, self, addr:
            orig(self, ServerNode.resolv(addr))))
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
        for patch in cls._patches:
            patch.apply()
        Serialized.init()

    @staticmethod
    def _unpatch():
        cls = NEOCluster
        assert cls._patch_count > 0
        cls._patch_count -= 1
        if cls._patch_count:
            return
        for patch in cls._patches:
            patch.revert()
        Serialized.stop()

    def __init__(self, master_count=1, partitions=1, replicas=0, upstream=None,
                       adapter=os.getenv('NEO_TESTS_ADAPTER', 'SQLite'),
                       storage_count=None, db_list=None, clear_databases=True,
                       db_user=DB_USER, db_password='', compress=True,
                       importer=None, autostart=None):
        self.name = 'neo_%s' % self._allocate('name',
            lambda: random.randint(0, 100))
        self.compress = compress
        master_list = [MasterApplication.newAddress()
                       for _ in xrange(master_count)]
        self.master_nodes = ' '.join('%s:%s' % x for x in master_list)
        weak_self = weakref.proxy(self)
        kw = dict(cluster=weak_self, getReplicas=replicas, getAdapter=adapter,
                  getPartitions=partitions, getReset=clear_databases,
                  getSSL=self.SSL)
        if upstream is not None:
            self.upstream = weakref.proxy(upstream)
            kw.update(getUpstreamCluster=upstream.name,
                getUpstreamMasters=parseMasterList(upstream.master_nodes))
        self.master_list = [MasterApplication(getAutostart=autostart,
                                              address=x, **kw)
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
        if importer:
            cfg = SafeConfigParser()
            cfg.add_section("neo")
            cfg.set("neo", "adapter", adapter)
            cfg.set("neo", "database", db % tuple(db_list))
            for name, zodb in importer:
                cfg.add_section(name)
                for x in zodb.iteritems():
                    cfg.set(name, *x)
            db = os.path.join(getTempDirectory(), '%s.conf')
            with open(db % tuple(db_list), "w") as f:
                cfg.write(f)
            kw["getAdapter"] = "Importer"
        self.storage_list = [StorageApplication(getDatabase=db % x, **kw)
                             for x in db_list]
        self.admin_list = [AdminApplication(**kw)]
        self.neoctl = NeoCTL(self.admin.getVirtualAddress(), ssl=self.SSL)

    def __repr__(self):
        return "<%s(%s) at 0x%x>" % (self.__class__.__name__,
                                     self.name, id(self))

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
        self.neoctl.close()
        self.neoctl = NeoCTL(self.admin.getVirtualAddress(), ssl=self.SSL)

    def start(self, storage_list=None, fast_startup=False):
        self._patch()
        for node_type in 'master', 'admin':
            for node in getattr(self, node_type + '_list'):
                node.start()
        Serialized.tic()
        if fast_startup:
            self.startCluster()
        if storage_list is None:
            storage_list = self.storage_list
        for node in storage_list:
            node.start()
        Serialized.tic()
        if not fast_startup:
            self.startCluster()
            Serialized.tic()
        state = self.neoctl.getClusterState()
        assert state in (ClusterStates.RUNNING, ClusterStates.BACKINGUP), state
        self.enableStorageList(storage_list)

    def newClient(self):
        return ClientApplication(name=self.name, master_nodes=self.master_nodes,
                                 compress=self.compress, ssl=self.SSL)

    @cached_property
    def client(self):
        client = self.newClient()
        # Make sure client won't be reused after it was closed.
        def close():
            client = self.client
            del self.client, client.close
            client.close()
        client.close = close
        return client

    @cached_property
    def db(self):
        return ZODB.DB(storage=self.getZODBStorage())

    def startCluster(self):
        try:
            self.neoctl.startCluster()
        except RuntimeError:
            Serialized.tic()
            if self.neoctl.getClusterState() not in (
                      ClusterStates.BACKINGUP,
                      ClusterStates.RUNNING,
                      ClusterStates.VERIFYING,
                  ):
                raise

    def enableStorageList(self, storage_list):
        self.neoctl.enableStorageList([x.uuid for x in storage_list])
        Serialized.tic()
        for node in storage_list:
            assert self.getNodeState(node) == NodeStates.RUNNING

    def join(self, thread_list, timeout=5):
        timeout += time.time()
        while thread_list:
            assert time.time() < timeout
            Serialized.tic()
            thread_list = [t for t in thread_list if t.is_alive()]

    def stop(self):
        logging.debug("stopping %s", self)
        client = self.__dict__.get("client")
        client is None or self.__dict__.pop("db", client).close()
        node_list = self.admin_list + self.storage_list + self.master_list
        for node in node_list:
            node.em.wakeup(True)
        try:
            node_list.append(client.poll_thread)
        except AttributeError: # client is None or thread is already stopped
            pass
        self.join(node_list)
        logging.debug("stopped %s", self)
        self._unpatch()

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
        except:
            __print_exc()
            raise

    def extraCellSortKey(self, key):
        return Patch(self.client.cp, getCellSortKey=lambda orig, cell:
            (orig(cell), key(cell)))


class NEOThreadedTest(NeoTestBase):

    def setupLog(self):
        log_file = os.path.join(getTempDirectory(), self.id() + '.log')
        logging.setup(log_file)
        return LoggerThreadName()

    def _tearDown(self, success):
        super(NEOThreadedTest, self)._tearDown(success)
        ServerNode.resetPorts()
        if success:
            with logging as db:
                db.execute("UPDATE packet SET body=NULL")
                db.execute("VACUUM")

    tic = Serialized.tic

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
            if not self.is_alive() and self.__exc_info:
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
                administration.random = backup_app.random = replicator.random \
                    = r
                return wrapped(*args, **kw)
            finally:
                administration.random = backup_app.random = replicator.random \
                    = random
        return wraps(wrapped)(wrapper)
    return decorator
