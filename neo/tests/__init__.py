#
# Copyright (C) 2009-2019  Nexedi SA
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

from __future__ import print_function
import __builtin__
import errno
import functools
import gc
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import unittest
import weakref
import transaction

from contextlib import closing, contextmanager
from ConfigParser import SafeConfigParser
from cStringIO import StringIO
try:
    from ZODB._compat import Unpickler
except ImportError:
    from cPickle import Unpickler
from functools import wraps
from inspect import isclass
from itertools import islice
from mock import MagicMock, Mock, NonCallableMock
from neo.lib import debug, event, logging
from neo.lib.protocol import NodeTypes, Packet, Packets, UUID_NAMESPACES
from neo.lib.util import cached_property
from neo.storage.database.manager import DatabaseManager
from time import time, sleep
from struct import pack, unpack
from unittest.case import _ExpectedFailure, _UnexpectedSuccess
try:
    from transaction.interfaces import IDataManager
    from ZODB.utils import newTid
    from ZODB.ConflictResolution import PersistentReferenceFactory
except ImportError:
    pass

def expectedFailure(exception=AssertionError):
    def decorator(func):
        def wrapper(*args, **kw):
            try:
                func(*args, **kw)
            except exception as e:
                # XXX: passing sys.exc_info() causes deadlocks
                raise _ExpectedFailure((type(e), None, None))
            raise _UnexpectedSuccess
        return wraps(func)(wrapper)
    if callable(exception) and not isinstance(exception, type):
        func = exception
        exception = Exception
        return decorator(func)
    return decorator

DB_PREFIX = os.getenv('NEO_DB_PREFIX', 'test_neo')
DB_ADMIN = os.getenv('NEO_DB_ADMIN', 'root')
DB_PASSWD = os.getenv('NEO_DB_PASSWD', '')
DB_USER = os.getenv('NEO_DB_USER', 'test')
DB_SOCKET = os.getenv('NEO_DB_SOCKET', '')
DB_INSTALL = os.getenv('NEO_DB_INSTALL', 'mysql_install_db')
DB_MYSQLD = os.getenv('NEO_DB_MYSQLD', '/usr/sbin/mysqld')
DB_MYCNF = os.getenv('NEO_DB_MYCNF')

adapter = os.getenv('NEO_TESTS_ADAPTER')
if adapter:
    from neo.storage.database import getAdapterKlass
    if getAdapterKlass(adapter).__name__ == 'MySQLDatabaseManager':
        os.environ['NEO_TESTS_ADAPTER'] = 'MySQL'

IP_VERSION_FORMAT_DICT = {
    socket.AF_INET:  '127.0.0.1',
    socket.AF_INET6: '::1',
}

ADDRESS_TYPE = socket.AF_INET

SSL = os.path.dirname(__file__) + os.sep
SSL = SSL + "ca.crt", SSL + "node.crt", SSL + "node.key"

logging.default_root_handler.handle = lambda record: None

debug.register()

def MockObject(name=None, **methods):
    return NonCallableMock(name=name, **{
        k + '.return_value': v
        for k, v in methods.iteritems()
    })

def buildUrlFromString(address):
    try:
        socket.inet_pton(socket.AF_INET6, address)
        address = '[%s]' % address
    except Exception:
        pass
    return address

def getTempDirectory():
    """get the current temp directory or a new one"""
    try:
        temp_dir = os.environ['TEMP']
    except KeyError:
        neo_dir = os.path.join(tempfile.gettempdir(), 'neo_tests')
        while True:
            temp_name = repr(time())
            temp_dir = os.path.join(neo_dir, temp_name)
            try:
                os.makedirs(temp_dir)
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        last = os.path.join(neo_dir, "last")
        try:
            os.remove(last)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        os.symlink(temp_name, last)
        os.environ['TEMP'] = temp_dir
        print('Using temp directory', temp_dir)
    return temp_dir

def setupMySQL(db_list, clear_databases=True):
    if mysql_pool:
        return mysql_pool.setup(db_list, clear_databases)
    from neo.storage.database.mysql import \
        Connection, OperationalError, BAD_DB_ERROR
    user = DB_USER
    password = ''
    kw = {'unix_socket': os.path.expanduser(DB_SOCKET)} if DB_SOCKET else {}
    # BBB: passwd is deprecated favour of password since 1.3.8
    with closing(Connection(user=DB_ADMIN, passwd=DB_PASSWD, **kw)) as conn:
        for database in db_list:
            try:
                conn.select_db(database)
                if not clear_databases:
                    continue
                conn.query('DROP DATABASE `%s`' % database)
            except OperationalError as e:
                if e.args[0] != BAD_DB_ERROR:
                    raise
                conn.query('GRANT ALL ON `%s`.* TO "%s"@"localhost" IDENTIFIED'
                           ' BY "%s"' % (database, user, password))
            conn.query('CREATE DATABASE `%s`' % database)
    return '{}:{}@%s{}'.format(user, password, DB_SOCKET).__mod__

class MySQLPool(object):

    def __init__(self, pool_dir=None):
        self._args = {}
        self._mysqld_dict = {}
        if not pool_dir:
            pool_dir = getTempDirectory()
        self._base = pool_dir + os.sep
        self._sock_template = os.path.join(pool_dir, '%s', 'mysql.sock')

    def __del__(self):
        self.kill(*self._mysqld_dict)

    def setup(self, db_list, clear_databases):
        from neo.storage.database.mysql import Connection
        start_list = set(db_list).difference(self._mysqld_dict)
        if start_list:
            start_list = sorted(start_list)
            x = []
            with open(os.devnull, 'wb') as f:
                for db in start_list:
                    base = self._base + db
                    datadir = os.path.join(base, 'datadir')
                    sock = self._sock_template % db
                    tmpdir = os.path.join(base, 'tmp')
                    args = [DB_INSTALL,
                        '--defaults-file=' + DB_MYCNF,
                        '--datadir=' + datadir,
                        '--socket=' + sock,
                        '--tmpdir=' + tmpdir,
                        '--log_error=' + os.path.join(base, 'error.log')]
                    if os.path.exists(datadir):
                        try:
                            os.remove(sock)
                        except OSError as e:
                            if e.errno != errno.ENOENT:
                                raise
                    else:
                        os.makedirs(tmpdir)
                        x.append(subprocess.Popen(args,
                            stdout=f, stderr=subprocess.STDOUT))
                    args[0] = DB_MYSQLD
                    self._args[db] = args
            for x in x:
                x = x.wait()
                if x:
                    raise subprocess.CalledProcessError(x, DB_INSTALL)
            self.start(*start_list)
            for db in start_list:
                sock = self._sock_template % db
                p = self._mysqld_dict[db]
                while not os.path.exists(sock):
                    sleep(1)
                    x = p.poll()
                    if x is not None:
                        raise subprocess.CalledProcessError(x, DB_MYSQLD)
        for db in db_list:
            with closing(Connection(unix_socket=self._sock_template % db,
                                    user='root')) as db:
                if clear_databases:
                    db.query('DROP DATABASE IF EXISTS neo')
                db.query('CREATE DATABASE IF NOT EXISTS neo')
        return ('root@neo' + self._sock_template).__mod__

    def start(self, *db, **kw):
        assert set(db).isdisjoint(self._mysqld_dict)
        for db in db:
            self._mysqld_dict[db] = subprocess.Popen(self._args[db], **kw)

    def kill(self, *db):
        processes = []
        for db in db:
            p = self._mysqld_dict.pop(db)
            processes.append(p)
            p.kill()
        for p in processes:
            p.wait()

mysql_pool = MySQLPool() if DB_MYCNF else None


def ImporterConfigParser(adapter, zodb, **kw):
    cfg = SafeConfigParser()
    cfg.add_section("neo")
    cfg.set("neo", "adapter", adapter)
    for x in kw.iteritems():
        cfg.set("neo", *x)
    for name, zodb in zodb:
        cfg.add_section(name)
        for x in zodb.iteritems():
            cfg.set(name, *x)
    return cfg

class NeoTestBase(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        logging.name = self.setupLog()
        unittest.TestCase.setUp(self)

    def setupLog(self):
        test_case, logging.name = self.id().rsplit('.', 1)
        logging.setup(os.path.join(getTempDirectory(), test_case + '.log'))

    def tearDown(self):
        assert self.tearDown.im_func is NeoTestBase.tearDown.im_func
        self._tearDown(sys._getframe(1).f_locals['success'])
        assert not gc.garbage, gc.garbage
        # XXX: I tried the following line to avoid random freezes on PyPy...
        gc.collect()

    def _tearDown(self, success):
        # Kill all unfinished transactions for next test.
        # Note we don't even abort them because it may require a valid
        # connection to a master node (see Storage.sync()).
        transaction.manager.__init__()
        if logging._max_size is not None:
            logging.flush()

    class failureException(AssertionError):
        def __init__(self, msg=None):
            logging.error(msg)
            AssertionError.__init__(self, msg)

    failIfEqual = failUnlessEqual = assertEquals = assertNotEquals = None

    assert issubclass(Mock, NonCallableMock)

    def assertNotEqual(self, first, second, msg=None):
        assert not (isinstance(first, NonCallableMock) or
                    isinstance(second, NonCallableMock)), \
          "Mock objects can't be compared with '==' or '!='"
        return super(NeoTestBase, self).assertNotEqual(first, second, msg=msg)

    def assertEqual(self, first, second, msg=None):
        assert not (isinstance(first, NonCallableMock) or
                    isinstance(second, NonCallableMock)), \
          "Mock objects can't be compared with '==' or '!='"
        return super(NeoTestBase, self).assertEqual(first, second, msg=msg)

    def assertPartitionTable(self, pt, expected, key=None):
        self.assertEqual(
            expected if isinstance(expected, str) else '|'.join(expected),
            '|'.join(pt._formatRows(sorted(pt.count_dict, key=key))))

    @contextmanager
    def expectedFailure(self, exception=AssertionError, regex=None):
        with self.assertRaisesRegexp(exception, regex) as cm:
            yield
            raise _UnexpectedSuccess
        # XXX: passing sys.exc_info() causes deadlocks
        raise _ExpectedFailure((type(cm.exception), None, None))

class NeoUnitTestBase(NeoTestBase):
    """ Base class for neo tests, implements common checks """

    local_ip = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE]

    def setUp(self):
        self.uuid_dict = {}
        NeoTestBase.setUp(self)

    @cached_property
    def nm(self):
        from neo.lib import node
        return node.NodeManager()

    def createStorage(self, *args):
        return self.nm.createStorage(**dict(zip(
            ('address', 'uuid', 'state'), args)))

    def prepareDatabase(self, number, prefix=DB_PREFIX):
        """ create empty databases """
        adapter = os.getenv('NEO_TESTS_ADAPTER', 'MySQL')
        if adapter == 'MySQL':
            db_template = setupMySQL(
                [prefix + str(i) for i in xrange(number)])
            self.db_template = lambda i: db_template(prefix + str(i))
        elif adapter == 'SQLite':
            self.db_template = os.path.join(getTempDirectory(),
                                       prefix + '%s.sqlite').__mod__
            for i in xrange(number):
                try:
                    os.remove(self.db_template(i))
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise
        else:
            assert False, adapter

    def getMasterConfiguration(self, cluster='main', master_number=2,
            replicas=2, partitions=1009, uuid=None):
        masters = [(self.local_ip, 10010 + i) for i in xrange(master_number)]
        return {
                'cluster': cluster,
                'bind': masters[0],
                'masters': masters,
                'replicas': replicas,
                'partitions': partitions,
                'uuid': uuid,
        }

    def getStorageConfiguration(self, cluster='main', master_number=2,
            index=0, prefix=DB_PREFIX, uuid=None):
        assert 0 < master_number < 10
        masters = [(self.local_ip, 10010 + i) for i in xrange(master_number)]
        adapter = os.getenv('NEO_TESTS_ADAPTER', 'MySQL')
        return {
                'cluster': cluster,
                'bind': (self.local_ip, 10020 + index),
                'masters': masters,
                'database': self.db_template(index),
                'uuid': uuid,
                'adapter': adapter,
                'wait': 0,
        }

    def getNewUUID(self, node_type):
        """
            Retuns a 16-bytes UUID according to namespace 'prefix'
        """
        if node_type is None:
            node_type = random.choice(NodeTypes)
        self.uuid_dict[node_type] = uuid = 1 + self.uuid_dict.get(node_type, 0)
        return uuid + (UUID_NAMESPACES[node_type] << 24)

    def getClientUUID(self):
        return self.getNewUUID(NodeTypes.CLIENT)

    def getMasterUUID(self):
        return self.getNewUUID(NodeTypes.MASTER)

    def getStorageUUID(self):
        return self.getNewUUID(NodeTypes.STORAGE)

    def getAdminUUID(self):
        return self.getNewUUID(NodeTypes.ADMIN)

    def getNextTID(self, ltid=None):
        return newTid(ltid)

    def getFakeApplication(self, **kw):
        return NonCallableMock(name='FakeApplication', _handlers={}, **kw)

    def getFakeConnector(self, descriptor=None):
        return MockObject('FakeConnector',
            getDescriptor=descriptor,
            getAddress=('', 0),
        )

    def getFakeConnection(self, uuid=None, address=('127.0.0.1', 10000),
            is_server=False, connector=None, peer_id=None):
        if connector is None:
            connector = self.getFakeConnector()
        conn = MockObject('FakeConnection',
            getUUID=uuid,
            getAddress=address,
            isServer=is_server,
            getConnector=connector,
            getPeerId=peer_id,
            isClosed=False,
        )
        conn.connecting = False
        return conn

    def checkAborted(self, conn):
        """ Ensure the connection was aborted """
        m = conn.abort
        m.assert_called_once()
        m.reset_mock()

    def checkClosed(self, conn):
        """ Ensure the connection was closed """
        m = conn.close
        m.close.assert_called_once()
        m.reset_mock()

    def checkNoPacketSent(self, conn):
        """ check if no packet were sent """
        conn.send.assert_not_called()
        conn.answer.assert_not_called()
        conn.ask.assert_not_called()

    # in check(Ask|Answer|Notify)Packet we return the packet so it can be used
    # in tests if more accurate checks are required

    def checkErrorPacket(self, conn):
        """ Check if an error packet was answered """
        return self.checkAnswerPacket(conn, Packets.Error)

    def checkAskPacket(self, conn, packet_type):
        """ Check if an ask-packet with the right type is sent """
        m = conn.ask
        m.assert_called_once()
        packet = m.call_args.args[0]
        self.assertIsInstance(packet, packet_type)
        m.reset_mock()
        return packet

    def checkAnswerPacket(self, conn, packet_type):
        """ Check if an answer-packet with the right type is sent """
        m = conn.answer
        m.assert_called_once()
        packet = m.call_args.args[0]
        self.assertIsInstance(packet, packet_type)
        m.reset_mock()
        return packet

    def checkNotifyPacket(self, conn, packet_type):
        """ Check if a notify-packet with the right type is sent """
        m = conn.send
        m.assert_called_once()
        packet = m.call_args.args[0]
        self.assertIsInstance(packet, packet_type)
        m.reset_mock()
        return packet


class TransactionalResource(object):

    class _sortKey(object):

        def __init__(self, last):
            self._last = last

        def __cmp__(self, other):
            assert type(self) is not type(other), other
            return 1 if self._last else -1

    def __init__(self, txn, last, **kw):
        self.sortKey = lambda: self._sortKey(last)
        for k in kw:
            assert callable(IDataManager.get(k)), k
        self.__dict__.update(kw)
        txn.get().join(self)

    def __call__(self, func):
        name = func.__name__
        assert callable(IDataManager.get(name)), name
        setattr(self, name, func)
        return func

    def __getattr__(self, attr):
        if callable(IDataManager.get(attr)):
            return lambda *_: None
        return self.__getattribute__(attr)

try:
    from ZODB.Connection import TransactionMetaData
except ImportError: # BBB: ZODB < 5
    def getTransactionMetaData(txn, conn):
        return txn
else:
    def getTransactionMetaData(txn, conn):
        return txn.data(conn)


class Patch(object):
    """
    Patch attributes and revert later automatically.

    Usage:

      with Patch(someObject, [new,] attrToPatch=newValue) as patch:
        [... code that runs with patches ...]
      [... code that runs without patch ...]

      The 'new' positional parameter defaults to False and it must be equal to
         not hasattr(someObject, 'attrToPatch')
      It is an assertion to detect when a Patch is obsolete.

      ' as patch' is optional: 'patch.revert()' can be used to revert patches
      in the middle of the 'with' clause.

    Or:

      patch = Patch(...)
      patch.apply()

      In this case, patches are automatically reverted when 'patch' is deleted.

    For patched callables, the new one receives the original value as first
    argument if 'new' is True.

    Alternative usage:

      @Patch(someObject)
      def funcToPatch(orig, ...):
        ...
      ...
      funcToPatch.revert()

      The decorator applies the patch immediately.
    """

    applied = False

    def __new__(cls, patched, *args, **patch):
        if patch:
            return object.__new__(cls)
        def patch(func):
            self = cls(patched, *args, **{func.__name__: func})
            self.apply()
            return self
        return patch

    def __init__(self, patched, *args, **patch):
        new, = args or (0,)
        (name, patch), = patch.iteritems()
        self._patched = patched
        self._name = name
        try:
            wrapped = getattr(patched, name)
        except AttributeError:
            assert new, (patched, name)
        else:
            assert not new, (patched, name)
            if callable(patch):
                  func = patch
                  patch = lambda *args, **kw: func(wrapped, *args, **kw)
                  if callable(wrapped):
                      patch = wraps(wrapped)(patch)
        self._patch = patch
        try:
            orig = patched.__dict__[name]
        except KeyError:
            if new or isclass(patched):
                self._revert = lambda: delattr(patched, name)
                return
            orig = getattr(patched, name)
        self._revert = lambda: setattr(patched, name, orig)

    def apply(self):
        assert not self.applied
        setattr(self._patched, self._name, self._patch)
        self.applied = True

    def revert(self):
        del self.applied
        self._revert()

    def __del__(self):
        if self.applied:
            self.revert()

    def __enter__(self):
        self.apply()
        return weakref.proxy(self)

    def __exit__(self, t, v, tb):
        self.__del__()

def consume(iterator, n):
    """Advance the iterator n-steps ahead and returns the last consumed item"""
    return next(islice(iterator, n-1, n))

def unpickle_state(data):
    unpickler = Unpickler(StringIO(data))
    unpickler.persistent_load = PersistentReferenceFactory().persistent_load
    unpickler.load() # skip the class tuple
    return unpickler.load()

__builtin__.pdb = lambda depth=0: \
    debug.getPdb().set_trace(sys._getframe(depth+1))
