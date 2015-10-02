#
# Copyright (C) 2009-2015  Nexedi SA
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

from functools import wraps
from mock import Mock
from neo.lib import debug, logging, protocol
from neo.lib.protocol import NodeTypes, Packets, UUID_NAMESPACES
from time import time
from struct import pack, unpack
from unittest.case import _ExpectedFailure, _UnexpectedSuccess
try:
    from ZODB.utils import newTid
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

IP_VERSION_FORMAT_DICT = {
    socket.AF_INET:  '127.0.0.1',
    socket.AF_INET6: '::1',
}

ADDRESS_TYPE = socket.AF_INET

SSL = os.path.dirname(__file__) + os.sep
SSL = SSL + "ca.crt", SSL + "node.crt", SSL + "node.key"

logging.default_root_handler.handle = lambda record: None
logging.backlog(None, 1<<20)

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
    conn = MySQLdb.Connect(user=DB_ADMIN, passwd=DB_PASSWD)
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

class NeoUnitTestBase(NeoTestBase):
    """ Base class for neo tests, implements common checks """

    local_ip = IP_VERSION_FORMAT_DICT[ADDRESS_TYPE]

    def setUp(self):
        self.uuid_dict = {}
        NeoTestBase.setUp(self)

    def prepareDatabase(self, number, prefix=DB_PREFIX):
        """ create empty databases """
        adapter = os.getenv('NEO_TESTS_ADAPTER', 'MySQL')
        if adapter == 'MySQL':
            setupMySQLdb([prefix + str(i) for i in xrange(number)])
        elif adapter == 'SQLite':
            temp_dir = getTempDirectory()
            for i in xrange(number):
                try:
                    os.remove(os.path.join(temp_dir, 'test_neo%s.sqlite' % i))
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
            db = '%s@%s%s' % (DB_USER, prefix, index)
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

    def getPTID(self, i=None):
        """ Return an integer PTID """
        if i is None:
            return random.randint(1, 2**64)
        return i

    def getOID(self, i=None):
        """ Return a 8-bytes OID """
        if i is None:
            return os.urandom(8)
        return pack('!Q', i)

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

    def checkUnexpectedPacketRaised(self, method, *args, **kwargs):
        """ Check if the UnexpectedPacketError exception wxas raised """
        self.assertRaises(protocol.UnexpectedPacketError, method, *args, **kwargs)

    def checkIdenficationRequired(self, method, *args, **kwargs):
        """ Check is the identification_required decorator is applied """
        self.checkUnexpectedPacketRaised(method, *args, **kwargs)

    def checkBrokenNodeDisallowedErrorRaised(self, method, *args, **kwargs):
        """ Check if the BrokenNodeDisallowedError exception wxas raised """
        self.assertRaises(protocol.BrokenNodeDisallowedError, method, *args, **kwargs)

    def checkNotReadyErrorRaised(self, method, *args, **kwargs):
        """ Check if the NotReadyError exception wxas raised """
        self.assertRaises(protocol.NotReadyError, method, *args, **kwargs)

    def checkAborted(self, conn):
        """ Ensure the connection was aborted """
        self.assertEqual(len(conn.mockGetNamedCalls('abort')), 1)

    def checkNotAborted(self, conn):
        """ Ensure the connection was not aborted """
        self.assertEqual(len(conn.mockGetNamedCalls('abort')), 0)

    def checkClosed(self, conn):
        """ Ensure the connection was closed """
        self.assertEqual(len(conn.mockGetNamedCalls('close')), 1)

    def checkNotClosed(self, conn):
        """ Ensure the connection was not closed """
        self.assertEqual(len(conn.mockGetNamedCalls('close')), 0)

    def _checkNoPacketSend(self, conn, method_id):
        call_list = conn.mockGetNamedCalls(method_id)
        self.assertEqual(len(call_list), 0, call_list)

    def checkNoPacketSent(self, conn, check_notify=True, check_answer=True,
            check_ask=True):
        """ check if no packet were sent """
        if check_notify:
            self._checkNoPacketSend(conn, 'notify')
        if check_answer:
            self._checkNoPacketSend(conn, 'answer')
        if check_ask:
            self._checkNoPacketSend(conn, 'ask')

    def checkNoUUIDSet(self, conn):
        """ ensure no UUID was set on the connection """
        self.assertEqual(len(conn.mockGetNamedCalls('setUUID')), 0)

    def checkUUIDSet(self, conn, uuid=None, check_intermediate=True):
        """ ensure UUID was set on the connection """
        calls = conn.mockGetNamedCalls('setUUID')
        found_uuid = calls.pop().getParam(0)
        if check_intermediate:
            for call in calls:
                self.assertEqual(found_uuid, call.getParam(0))
        if uuid is not None:
            self.assertEqual(found_uuid, uuid)

    # in check(Ask|Answer|Notify)Packet we return the packet so it can be used
    # in tests if more accurates checks are required

    def checkErrorPacket(self, conn, decode=False):
        """ Check if an error packet was answered """
        calls = conn.mockGetNamedCalls("answer")
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), Packets.Error)
        if decode:
            return packet.decode()
        return packet

    def checkAskPacket(self, conn, packet_type, decode=False):
        """ Check if an ask-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('ask')
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
        if decode:
            return packet.decode()
        return packet

    def checkAnswerPacket(self, conn, packet_type, decode=False):
        """ Check if an answer-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('answer')
        self.assertEqual(len(calls), 1)
        packet = calls.pop().getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
        if decode:
            return packet.decode()
        return packet

    def checkNotifyPacket(self, conn, packet_type, packet_number=0, decode=False):
        """ Check if a notify-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('notify')
        packet = calls.pop(packet_number).getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEqual(type(packet), packet_type)
        if decode:
            return packet.decode()
        return packet

    def checkNotify(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.Notify, **kw)

    def checkNotifyNodeInformation(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyNodeInformation, **kw)

    def checkSendPartitionTable(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.SendPartitionTable, **kw)

    def checkStartOperation(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.StartOperation, **kw)

    def checkInvalidateObjects(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.InvalidateObjects, **kw)

    def checkAbortTransaction(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.AbortTransaction, **kw)

    def checkNotifyLastOID(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyLastOID, **kw)

    def checkAnswerTransactionFinished(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerTransactionFinished, **kw)

    def checkAnswerInformationLocked(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerInformationLocked, **kw)

    def checkAskLockInformation(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskLockInformation, **kw)

    def checkNotifyUnlockInformation(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyUnlockInformation, **kw)

    def checkNotifyTransactionFinished(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyTransactionFinished, **kw)

    def checkRequestIdentification(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.RequestIdentification, **kw)

    def checkAskPrimary(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskPrimary)

    def checkAskUnfinishedTransactions(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskUnfinishedTransactions)

    def checkAskTransactionInformation(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskTransactionInformation, **kw)

    def checkAskObjectPresent(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskObjectPresent, **kw)

    def checkAskObject(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskObject, **kw)

    def checkAskStoreObject(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskStoreObject, **kw)

    def checkAskStoreTransaction(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskStoreTransaction, **kw)

    def checkAskFinishTransaction(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskFinishTransaction, **kw)

    def checkAskNewTid(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskBeginTransaction, **kw)

    def checkAskLastIDs(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.AskLastIDs, **kw)

    def checkAcceptIdentification(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AcceptIdentification, **kw)

    def checkAnswerPrimary(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerPrimary, **kw)

    def checkAnswerLastIDs(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerLastIDs, **kw)

    def checkAnswerUnfinishedTransactions(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerUnfinishedTransactions, **kw)

    def checkAnswerObject(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObject, **kw)

    def checkAnswerTransactionInformation(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerTransactionInformation, **kw)

    def checkAnswerBeginTransaction(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerBeginTransaction, **kw)

    def checkAnswerTids(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerTIDs, **kw)

    def checkAnswerTidsFrom(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerTIDsFrom, **kw)

    def checkAnswerObjectHistory(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObjectHistory, **kw)

    def checkAnswerStoreTransaction(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerStoreTransaction, **kw)

    def checkAnswerStoreObject(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerStoreObject, **kw)

    def checkAnswerPartitionTable(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerPartitionTable, **kw)

    def checkAnswerObjectPresent(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObjectPresent, **kw)


class Patch(object):

    applied = False

    def __init__(self, patched, **patch):
        (name, patch), = patch.iteritems()
        self._patched = patched
        self._name = name
        if callable(patch):
            wrapped = getattr(patched, name, None)
            func = patch
            patch = lambda *args, **kw: func(wrapped, *args, **kw)
            if callable(wrapped):
                patch = wraps(wrapped)(patch)
        self._patch = patch
        try:
            orig = patched.__dict__[name]
            self._revert = lambda: setattr(patched, name, orig)
        except KeyError:
            self._revert = lambda: delattr(patched, name)

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


__builtin__.pdb = lambda depth=0: \
    debug.getPdb().set_trace(sys._getframe(depth+1))
