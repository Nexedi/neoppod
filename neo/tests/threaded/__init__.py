#
# Copyright (C) 2011-2017  Nexedi SA
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

import os, random, select, socket, sys, tempfile
import thread, threading, time, traceback, weakref
from collections import deque
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
from itertools import count
from functools import partial, wraps
from zlib import decompress
from ..mock import Mock
import transaction, ZODB
import neo.admin.app, neo.master.app, neo.storage.app
import neo.client.app, neo.neoctl.app
from neo.client import Storage
from neo.lib import logging
from neo.lib.connection import BaseConnection, \
    ClientConnection, Connection, ConnectionClosed, ListeningConnection
from neo.lib.connector import SocketConnector, ConnectorException
from neo.lib.handler import EventHandler
from neo.lib.locking import SimpleQueue
from neo.lib.protocol import ClusterStates, Enum, NodeStates, NodeTypes, Packets
from neo.lib.util import cached_property, parseMasterList, p64
from .. import NeoTestBase, Patch, getTempDirectory, setupMySQLdb, \
    ADDRESS_TYPE, IP_VERSION_FORMAT_DICT, DB_PREFIX, DB_SOCKET, DB_USER

BIND = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE], 0
LOCAL_IP = socket.inet_pton(ADDRESS_TYPE, IP_VERSION_FORMAT_DICT[ADDRESS_TYPE])
TIC_LOOP = xrange(1000)


class LockLock(object):
    """Double lock used as synchronisation point between 2 threads

    Used to wait that a slave thread has reached a specific location, and to
    keep it suspended there. It resumes on __exit__
    """

    def __init__(self):
        self._l = threading.Lock(), threading.Lock()

    def __call__(self):
        """Define synchronisation point for both threads"""
        if self._owner == thread.get_ident():
            self._l[0].acquire()
        else:
            self._l[0].release()
            self._l[1].acquire()

    def __enter__(self):
        self._owner = thread.get_ident()
        for l in self._l:
            l.acquire(0)
        return self

    def __exit__(self, t, v, tb):
        try:
            self._l[1].release()
        except thread.error:
            pass


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
        assert not cls._fd_dict, ("file descriptor leak (%r)\nThis may happen"
            " when a test fails, in which case you can see the real exception"
            " by disabling this one." % cls._fd_dict)
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
    def tic(cls, step=-1, check_timeout=(), quiet=False,
            # BUG: We overuse epoll as a way to know if there are pending
            #      network messages. Sometimes, and this is more visible with
            #      a single-core CPU, other threads are still busy and haven't
            #      sent anything yet on the network. This causes tic() to
            #      return prematurely. Passing a non-zero value is a hack.
            timeout=0):
        # If you're in a pdb here, 'n' switches to another thread
        # (the following lines are not supposed to be debugged into)
        with cls._tic_lock, cls.pdb():
            if not quiet:
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
                event_list = cls._epoll.poll(timeout)
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
            for x in TIC_LOOP:
                r = self._epoll.poll(0)
                if r:
                    return r
                Serialized.tic(step=1, timeout=.001)
            raise Exception("tic is looping forever")
        return self._epoll.poll(timeout)


class Node(object):

    @staticmethod
    def convertInitArgs(**kw):
        return {'get' + k.capitalize(): v for k, v in kw.iteritems()}

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
            master_nodes = ()
            name = kw.get('name', 'test')
        else:
            master_nodes = cluster.master_nodes
            name = kw.get('name', cluster.name)
        port = address[1]
        if address is not BIND:
            self._node_list[port] = weakref.proxy(self)
        self._init_args = init_args = kw.copy()
        init_args['cluster'] = cluster
        init_args['address'] = address
        threading.Thread.__init__(self)
        self.daemon = True
        self.node_name = '%s_%u' % (self.node_type, port)
        kw.update(getCluster=name, getBind=address,
            getMasters=master_nodes and parseMasterList(master_nodes))
        super(ServerNode, self).__init__(Mock(kw))

    def getVirtualAddress(self):
        return self._init_args['address']

    def resetNode(self, **kw):
        assert not self.is_alive()
        kw = self.convertInitArgs(**kw)
        init_args = self._init_args
        init_args['getReset'] = False
        assert set(kw).issubset(init_args), (kw, init_args)
        init_args.update(kw)
        self.close()
        self.__init__(**init_args)

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
            self.listening_conn = None
        except AttributeError:
            pass

    def getListeningAddress(self):
        try:
            return self.listening_conn.getAddress()
        except AttributeError:
            raise ConnectorException

    def stop(self):
        self.em.wakeup(thread.exit)

class AdminApplication(ServerNode, neo.admin.app.Application):
    pass

class MasterApplication(ServerNode, neo.master.app.Application):
    pass

class StorageApplication(ServerNode, neo.storage.app.Application):

    dm = type('', (), {'close': lambda self: None})()

    def _afterRun(self):
        super(StorageApplication, self)._afterRun()
        try:
            self.dm.close()
            del self.dm
        except StandardError: # AttributeError & ProgrammingError
            pass
        if self.master_conn:
            self.master_conn.close()

    def getAdapter(self):
        return self._init_args['getAdapter']

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

    max_reconnection_to_master = 10

    def __init__(self, master_nodes, name, **kw):
        super(ClientApplication, self).__init__(master_nodes, name, **kw)
        self.poll_thread.node_name = name
        # Smaller cache to speed up tests that checks behaviour when it's too
        # small. See also NEOCluster.cache_size
        self._cache._max_size //= 1024

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

    def extraCellSortKey(self, key):
        return Patch(self.cp, getCellSortKey=lambda orig, cell:
            (orig(cell, lambda: key(cell)), random.random()))

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
    filter_queue = weakref.WeakKeyDictionary() # XXX: see the end of __new__
    lock = threading.RLock()
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
                            if self._test(conn, packet):
                                self.filtered_count += 1
                                break
                        else:
                            return cls._addPacket(conn, packet)
                        cls.filter_queue[conn] = queue = deque()
                    p = packet.__class__
                    logging.debug("queued %s#0x%04x for %s",
                                  p.__name__, packet.getId(), conn)
                    p = packet.__new__(p)
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
            # Retry even in case of exception, at least to avoid leaks in
            # filter_queue. Sometimes, WeakKeyDictionary only does the job
            # only an explicit call to gc.collect.
            with cls.lock:
                cls._retry()

    def _test(self, conn, packet):
        if not self.conn_list or conn in self.conn_list:
            for filter in self.filter_dict:
                if filter(conn, packet):
                    return True
        return False

    @classmethod
    def retry(cls):
        with cls.lock:
            cls._retry()

    @classmethod
    def _retry(cls):
        for conn, queue in cls.filter_queue.items():
            while queue:
                packet = queue.popleft()
                for self in cls.filter_list:
                    if self._test(conn, packet):
                        queue.appendleft(packet)
                        break
                else:
                    if conn.isClosed():
                        queue.clear()
                    else:
                        # Use the thread that created the packet to reinject it,
                        # to avoid a race condition on Connector.queued.
                        conn.em.wakeup(lambda conn=conn, packet=packet:
                            conn.isClosed() or cls._addPacket(conn, packet))
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
                for p in self.filter_dict.pop(filter):
                    p.revert()
            self._retry()

    def discard(self, *filters):
        try:
            self.remove(*filters)
        except KeyError:
            pass

    def __contains__(self, filter):
        return filter in self.filter_dict

    def byPacket(self, packet_type, *args):
        patches = []
        other = []
        for x in args:
            (patches if isinstance(x, Patch) else other).append(x)
        def delay(conn, packet):
            return isinstance(packet, packet_type) and False not in (
                callback(conn) for callback in other)
        self.add(delay, *patches)
        return delay

    def __getattr__(self, attr):
        if attr.startswith('delay'):
            return partial(self.byPacket, getattr(Packets, attr[5:]))
        return self.__getattribute__(attr)

class NEOCluster(object):

    SSL = None

    def __init__(orig, self): # temporary definition for SimpleQueue patch
        orig(self)
        lock = self._lock
        def _lock(blocking=True):
            if blocking:
                logging.info('<SimpleQueue>._lock.acquire()')
                for i in TIC_LOOP:
                    if lock(False):
                        return True
                    Serialized.tic(step=1, quiet=True, timeout=.001)
                raise Exception("tic is looping forever")
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

    started = False

    def __init__(self, master_count=1, partitions=1, replicas=0, upstream=None,
                       adapter=os.getenv('NEO_TESTS_ADAPTER', 'SQLite'),
                       storage_count=None, db_list=None, clear_databases=True,
                       db_user=DB_USER, db_password='', compress=True,
                       importer=None, autostart=None, dedup=False):
        self.name = 'neo_%s' % self._allocate('name',
            lambda: random.randint(0, 100))
        self.compress = compress
        self.num_partitions = partitions
        master_list = [MasterApplication.newAddress()
                       for _ in xrange(master_count)]
        self.master_nodes = ' '.join('%s:%s' % x for x in master_list)
        kw = Node.convertInitArgs(replicas=replicas, adapter=adapter,
            partitions=partitions, reset=clear_databases, dedup=dedup)
        kw['cluster'] = weak_self = weakref.proxy(self)
        kw['getSSL'] = self.SSL
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
            db = '%s:%s@%%s%s' % (db_user, db_password, DB_SOCKET)
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

    # More handy shortcuts for tests
    @property
    def backup_tid(self):
        return self.neoctl.getRecovery()[1]

    @property
    def last_tid(self):
        return self.primary_master.getLastTransaction()

    @property
    def primary_master(self):
        master, = [master for master in self.master_list if master.primary]
        return master

    @property
    def cache_size(self):
        return self.client._cache._max_size
    ###

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.stop(None)

    def start(self, storage_list=None, master_list=None, recovering=False):
        self.started = True
        self._patch()
        self.neoctl = NeoCTL(self.admin.getVirtualAddress(), ssl=self.SSL)
        for node in self.master_list if master_list is None else master_list:
            node.start()
        for node in self.admin_list:
            node.start()
        Serialized.tic()
        if storage_list is None:
            storage_list = self.storage_list
        for node in storage_list:
            node.start()
        Serialized.tic()
        if recovering:
            expected_state = ClusterStates.RECOVERING
        else:
            self.startCluster()
            Serialized.tic()
            expected_state = ClusterStates.RUNNING, ClusterStates.BACKINGUP
        self.checkStarted(expected_state, storage_list)

    def checkStarted(self, expected_state, storage_list=None):
        if isinstance(expected_state, Enum.Item):
            expected_state = expected_state,
        state = self.neoctl.getClusterState()
        assert state in expected_state, state
        expected_state = (NodeStates.PENDING
            if state == ClusterStates.RECOVERING
            else NodeStates.RUNNING)
        for node in self.storage_list if storage_list is None else storage_list:
            state = self.getNodeState(node)
            assert state == expected_state, (node, state)

    def stop(self, clear_database=False, __print_exc=traceback.print_exc, **kw):
        if self.started:
            del self.started
            logging.debug("stopping %s", self)
            client = self.__dict__.get("client")
            client is None or self.__dict__.pop("db", client).close()
            node_list = self.admin_list + self.storage_list + self.master_list
            for node in node_list:
                node.stop()
            try:
                node_list.append(client.poll_thread)
            except AttributeError: # client is None or thread is already stopped
                pass
            self.join(node_list)
            self.neoctl.close()
            del self.neoctl
            logging.debug("stopped %s", self)
            self._unpatch()
        if clear_database is None:
            try:
                for node_type in 'admin', 'storage', 'master':
                    for node in getattr(self, node_type + '_list'):
                        node.close()
            except:
                __print_exc()
                raise
        else:
            for node_type in 'master', 'storage', 'admin':
                reset_kw = kw.copy()
                if node_type == 'storage':
                    reset_kw['reset'] = clear_database
                for node in getattr(self, node_type + '_list'):
                    node.resetNode(**reset_kw)

    def _newClient(self):
        return ClientApplication(name=self.name, master_nodes=self.master_nodes,
                                 compress=self.compress, ssl=self.SSL)

    @contextmanager
    def newClient(self, with_db=False):
        x = self._newClient()
        try:
            t = x.poll_thread
            closed = []
            if with_db:
                x = ZODB.DB(storage=self.getZODBStorage(client=x))
            else:
                # XXX: Do nothing if finally if the caller already closed it.
                x.close = lambda: closed.append(x.__class__.close(x))
            yield x
        finally:
            closed or x.close()
            self.join((t,))

    @cached_property
    def client(self):
        client = self._newClient()
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
            state = self.getNodeState(node)
            assert state == NodeStates.RUNNING, state

    def join(self, thread_list, timeout=5):
        timeout += time.time()
        while thread_list:
            # Map with repr before that threads become unprintable.
            assert time.time() < timeout, map(repr, thread_list)
            Serialized.tic(timeout=.001)
            thread_list = [t for t in thread_list if t.is_alive()]

    def getNodeState(self, node):
        uuid = node.uuid
        for node in self.neoctl.getNodeList(node.node_type):
            if node[2] == uuid:
                return node[3]

    def getOutdatedCells(self):
        # Ask the admin instead of the primary master to check that it is
        # notified of every change.
        return [(i, cell.getUUID())
            for i, row in enumerate(self.admin.pt.partition_list)
            for cell in row
            if not cell.isReadable()]

    def getZODBStorage(self, **kw):
        kw['_app'] = kw.pop('client', self.client)
        return Storage.Storage(None, self.name, **kw)

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

    def getTransaction(self, db=None):
        txn = transaction.TransactionManager()
        return txn, (self.db if db is None else db).open(txn)

    def moduloTID(self, partition):
        """Force generation of TIDs that will be stored in given partition"""
        partition = p64(partition)
        master = self.primary_master
        return Patch(master.tm, _nextTID=lambda orig, *args:
            orig(*args) if args else orig(partition, master.pt.getPartitions()))

    def sortStorageList(self):
        """Sort storages so that storage_list[i] has partition i for all i"""
        pt = [{x.getUUID() for x in x}
            for x in self.primary_master.pt.partition_list]
        r = []
        x = [iter(pt[0])]
        try:
            while 1:
                try:
                    r.append(next(x[-1]))
                except StopIteration:
                    del r[-1], x[-1]
                else:
                    x.append(iter(pt[len(r)].difference(r)))
        except IndexError:
            assert len(r) == len(self.storage_list)
        x = {x.uuid: x for x in self.storage_list}
        self.storage_list[:] = (x[r] for r in r)
        return self.storage_list

class NEOThreadedTest(NeoTestBase):

    __run_count = {}

    def setupLog(self):
        test_id = self.id()
        i = self.__run_count.get(test_id, 0)
        self.__run_count[test_id] = 1 + i
        if i:
            test_id += '-%s' % i
        logging.setup(os.path.join(getTempDirectory(), test_id + '.log'))
        return LoggerThreadName()

    def _tearDown(self, success):
        super(NEOThreadedTest, self)._tearDown(success)
        ServerNode.resetPorts()
        if success and logging._max_size is not None:
            with logging as db:
                db.execute("UPDATE packet SET body=NULL")
                db.execute("VACUUM")

    tic = Serialized.tic

    @contextmanager
    def getLoopbackConnection(self):
        app = MasterApplication(address=BIND,
            getSSL=NEOCluster.SSL, getReplicas=0, getPartitions=1)
        try:
            handler = EventHandler(app)
            app.listening_conn = ListeningConnection(app, handler, app.server)
            yield ClientConnection(app, handler, app.nm.createMaster(
                address=app.listening_conn.getAddress(), uuid=app.uuid))
        finally:
            app.close()

    def getUnpickler(self, conn):
        reader = conn._reader
        def unpickler(data, compression=False):
            if compression:
                data = decompress(data)
            obj = reader.getGhost(data)
            reader.setGhostState(obj, data)
            return obj
        return unpickler

    class newPausedThread(threading.Thread):

        def __init__(self, func, *args, **kw):
            threading.Thread.__init__(self)
            self.__target = func, args, kw
            self.daemon = True

        def run(self):
            try:
                apply(*self.__target)
                self.__exc_info = None
            except:
                self.__exc_info = sys.exc_info()
                if self.__exc_info[0] is NEOThreadedTest.failureException:
                    traceback.print_exception(*self.__exc_info)

        def join(self, timeout=None):
            threading.Thread.join(self, timeout)
            if not self.is_alive() and self.__exc_info:
                etype, value, tb = self.__exc_info
                del self.__exc_info
                raise etype, value, tb

    class newThread(newPausedThread):

        def __init__(self, *args, **kw):
            NEOThreadedTest.newPausedThread.__init__(self, *args, **kw)
            self.start()

    def commitWithStorageFailure(self, client, txn):
        with Patch(client, _getFinalTID=lambda *_: None):
            self.assertRaises(ConnectionClosed, txn.commit)

    def assertPartitionTable(self, cluster, expected, pt_node=None):
        index = [x.uuid for x in cluster.storage_list].index
        super(NEOThreadedTest, self).assertPartitionTable(
            (pt_node or cluster.admin).pt, expected,
            lambda x: index(x.getUUID()))

    @staticmethod
    def noConnection(jar, storage):
        return Patch(jar.db().storage.app.cp, getConnForNode=lambda orig, node:
            None if node.getUUID() == storage.uuid else orig(node))

    @staticmethod
    def readCurrent(ob):
        ob._p_activate()
        ob._p_jar.readCurrent(ob)


class ThreadId(list):

    def __call__(self):
        try:
            return self.index(thread.get_ident())
        except ValueError:
            i = len(self)
            self.append(thread.get_ident())
            return i


@apply
class RandomConflictDict(dict):
    # One must not depend on how Python iterates over dict keys, because this
    # is implementation-defined behaviour. This patch makes sure of that when
    # resolving conflicts.

    def __new__(cls):
        from neo.client.transactions import Transaction
        def __init__(orig, self, *args):
            orig(self, *args)
            assert self.conflict_dict == {}
            self.conflict_dict = dict.__new__(cls)
        return Patch(Transaction, __init__=__init__)

    def popitem(self):
        try:
            k = random.choice(list(self))
        except IndexError:
            raise KeyError
        return k, self.pop(k)


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

def with_cluster(start_cluster=True, **cluster_kw):
    def decorator(wrapped):
        def wrapper(self, *args, **kw):
            with NEOCluster(**cluster_kw) as cluster:
                if start_cluster:
                    cluster.start()
                return wrapped(self, cluster, *args, **kw)
        return wraps(wrapped)(wrapper)
    return decorator
