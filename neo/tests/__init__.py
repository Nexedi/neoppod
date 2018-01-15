#
# Copyright (C) 2009-2017  Nexedi SA
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

import __builtin__
import errno
import functools
import gc
import os
import random
import socket
import sys
import tempfile
import unittest
import weakref
import MySQLdb
import transaction

from cStringIO import StringIO
from cPickle import Unpickler
from functools import wraps
from inspect import isclass
from .mock import Mock
from neo.lib import debug, logging, protocol
from neo.lib.protocol import NodeTypes, Packets, UUID_NAMESPACES
from neo.lib.util import cached_property
from time import time
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
            except exception, e:
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

IP_VERSION_FORMAT_DICT = {
    socket.AF_INET:  '127.0.0.1',
    socket.AF_INET6: '::1',
}

ADDRESS_TYPE = socket.AF_INET

SSL = os.path.dirname(__file__) + os.sep
SSL = SSL + "ca.crt", SSL + "node.crt", SSL + "node.key"

logging.default_root_handler.handle = lambda record: None

debug.register()
# prevent "signal only works in main thread" errors in subprocesses
debug.register = lambda on_log=None: None

def mockDefaultValue(name, function):
    def method(self, *args, **kw):
        if name in self.mockReturnValues:
            return self.__getattr__(name)(*args, **kw)
        return function(self, *args, **kw)
    method.__name__ = name
    setattr(Mock, name, method)

mockDefaultValue('__nonzero__', lambda self: self.__len__() != 0)
mockDefaultValue('__repr__', lambda self:
    '<%s object at 0x%x>' % (self.__class__.__name__, id(self)))
mockDefaultValue('__str__', repr)

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
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
        last = os.path.join(neo_dir, "last")
        try:
            os.remove(last)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        os.symlink(temp_name, last)
        os.environ['TEMP'] = temp_dir
        print 'Using temp directory %r.' % temp_dir
    return temp_dir

def setupMySQLdb(db_list, user=DB_USER, password='', clear_databases=True):
    from MySQLdb.constants.ER import BAD_DB_ERROR
    kw = {'unix_socket': os.path.expanduser(DB_SOCKET)} if DB_SOCKET else {}
    conn = MySQLdb.connect(user=DB_ADMIN, passwd=DB_PASSWD, **kw)
    cursor = conn.cursor()
    for database in db_list:
        try:
            conn.select_db(database)
            if not clear_databases:
                continue
            cursor.execute('DROP DATABASE `%s`' % database)
        except MySQLdb.OperationalError, (code, _):
            if code != BAD_DB_ERROR:
                raise
            cursor.execute('GRANT ALL ON `%s`.* TO "%s"@"localhost" IDENTIFIED'
                           ' BY "%s"' % (database, user, password))
        cursor.execute('CREATE DATABASE `%s`' % database)
    cursor.close()
    conn.commit()
    conn.close()

class NeoTestBase(unittest.TestCase):

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

    def _tearDown(self, success):
        # Kill all unfinished transactions for next test.
        # Note we don't even abort them because it may require a valid
        # connection to a master node (see Storage.sync()).
        transaction.manager.__init__()

    class failureException(AssertionError):
        def __init__(self, msg=None):
            logging.error(msg)
            AssertionError.__init__(self, msg)

    failIfEqual = failUnlessEqual = assertEquals = assertNotEquals = None

    def assertNotEqual(self, first, second, msg=None):
        assert not (isinstance(first, Mock) or isinstance(second, Mock)), \
          "Mock objects can't be compared with '==' or '!='"
        return super(NeoTestBase, self).assertNotEqual(first, second, msg=msg)

    def assertEqual(self, first, second, msg=None):
        assert not (isinstance(first, Mock) or isinstance(second, Mock)), \
          "Mock objects can't be compared with '==' or '!='"
        return super(NeoTestBase, self).assertEqual(first, second, msg=msg)

    def assertPartitionTable(self, pt, expected, key=None):
        self.assertEqual(
            expected if isinstance(expected, str) else '|'.join(expected),
            '|'.join(pt._formatRows(sorted(pt.count_dict, key=key))))

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
            setupMySQLdb([prefix + str(i) for i in xrange(number)])
        elif adapter == 'SQLite':
            temp_dir = getTempDirectory()
            for i in xrange(number):
                try:
                    os.remove(os.path.join(temp_dir,
                        '%s%s.sqlite' % (prefix, i)))
                except OSError, e:
                    if e.errno != errno.ENOENT:
                        raise
        else:
            assert False, adapter

    def getMasterConfiguration(self, cluster='main', master_number=2,
            replicas=2, partitions=1009, uuid=None):
        assert master_number >= 1 and master_number <= 10
        masters = ([(self.local_ip, 10010 + i)
                    for i in xrange(master_number)])
        return Mock({
                'getCluster': cluster,
                'getBind': masters[0],
                'getMasters': masters,
                'getReplicas': replicas,
                'getPartitions': partitions,
                'getUUID': uuid,
        })

    def getStorageConfiguration(self, cluster='main', master_number=2,
            index=0, prefix=DB_PREFIX, uuid=None):
        assert master_number >= 1 and master_number <= 10
        assert index >= 0 and index <= 9
        masters = [(buildUrlFromString(self.local_ip),
                     10010 + i) for i in xrange(master_number)]
        adapter = os.getenv('NEO_TESTS_ADAPTER', 'MySQL')
        if adapter == 'MySQL':
            db = '%s@%s%s%s' % (DB_USER, prefix, index, DB_SOCKET)
        elif adapter == 'SQLite':
            db = os.path.join(getTempDirectory(), 'test_neo%s.sqlite' % index)
        else:
            assert False, adapter
        return Mock({
                'getCluster': cluster,
                'getBind': (masters[0], 10020 + index),
                'getMasters': masters,
                'getDatabase': db,
                'getUUID': uuid,
                'getReset': False,
                'getAdapter': adapter,
        })

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

    def getFakeConnector(self, descriptor=None):
        return Mock({
            '__repr__': 'FakeConnector',
            'getDescriptor': descriptor,
            'getAddress': ('', 0),
        })

    def getFakeConnection(self, uuid=None, address=('127.0.0.1', 10000),
            is_server=False, connector=None, peer_id=None):
        if connector is None:
            connector = self.getFakeConnector()
        conn = Mock({
            'getUUID': uuid,
            'getAddress': address,
            'isServer': is_server,
            '__repr__': 'FakeConnection',
            '__nonzero__': 0,
            'getConnector': connector,
            'getPeerId': peer_id,
        })
        conn.mockAddReturnValues(__hash__ = id(conn))
        conn.connecting = False
        return conn

    def checkProtocolErrorRaised(self, method, *args, **kwargs):
        """ Check if the ProtocolError exception was raised """
        self.assertRaises(protocol.ProtocolError, method, *args, **kwargs)

    def checkAborted(self, conn):
        """ Ensure the connection was aborted """
        self.assertEqual(len(conn.mockGetNamedCalls('abort')), 1)

    def checkClosed(self, conn):
        """ Ensure the connection was closed """
        self.assertEqual(len(conn.mockGetNamedCalls('close')), 1)

    def _checkNoPacketSend(self, conn, method_id):
        self.assertEqual([], conn.mockGetNamedCalls(method_id))

    def checkNoPacketSent(self, conn):
        """ check if no packet were sent """
        self._checkNoPacketSend(conn, 'send')
        self._checkNoPacketSend(conn, 'answer')
        self._checkNoPacketSend(conn, 'ask')

    # in check(Ask|Answer|Notify)Packet we return the packet so it can be used
    # in tests if more accurate checks are required

    def checkErrorPacket(self, conn):
        """ Check if an error packet was answered """
        calls = conn.mockGetNamedCalls("answer")
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), Packets.Error)
        return packet

    def checkAskPacket(self, conn, packet_type):
        """ Check if an ask-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('ask')
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
        return packet

    def checkAnswerPacket(self, conn, packet_type):
        """ Check if an answer-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('answer')
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
        return packet

    def checkNotifyPacket(self, conn, packet_type, packet_number=0):
        """ Check if a notify-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('send')
        packet = calls.pop(packet_number).getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
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


def unpickle_state(data):
    unpickler = Unpickler(StringIO(data))
    unpickler.persistent_load = PersistentReferenceFactory().persistent_load
    unpickler.load() # skip the class tuple
    return unpickler.load()

__builtin__.pdb = lambda depth=0: \
    debug.getPdb().set_trace(sys._getframe(depth+1))
