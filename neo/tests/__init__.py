#
# Copyright (C) 2009-2010  Nexedi SA
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

import __builtin__
import os
import random
import socket
import sys
import tempfile
import unittest
import MySQLdb
import neo
import transaction

from mock import Mock
from neo.lib import debug, logger, protocol, setupLog
from neo.lib.protocol import Packets
from neo.lib.util import getAddressType
from time import time, gmtime
from struct import pack, unpack

DB_PREFIX = os.getenv('NEO_DB_PREFIX', 'test_neo')
DB_ADMIN = os.getenv('NEO_DB_ADMIN', 'root')
DB_PASSWD = os.getenv('NEO_DB_PASSWD', '')
DB_USER = os.getenv('NEO_DB_USER', 'test')

IP_VERSION_FORMAT_DICT = {
    socket.AF_INET:  '127.0.0.1',
    socket.AF_INET6: '::1',
}

ADDRESS_TYPE = socket.AF_INET

debug.ENABLED = True
debug.register()
# prevent "signal only works in main thread" errors in subprocesses
debug.ENABLED = False

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
            temp_dir = os.path.join(neo_dir, repr(time()))
            try:
                os.makedirs(temp_dir)
                break
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
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
        logger.PACKET_LOGGER.enable(True)
        sys.stdout.write(' * %s ' % (self.id(), ))
        sys.stdout.flush()
        self.setupLog()
        unittest.TestCase.setUp(self)

    def setupLog(self):
        test_case, test_method = self.id().rsplit('.', 1)
        log_file = os.path.join(getTempDirectory(), test_case + '.log')
        setupLog(test_method, log_file, True)

    def tearDown(self):
        # Kill all unfinished transactions for next test.
        # Note we don't even abort them because it may require a valid
        # connection to a master node (see Storage.sync()).
        transaction.manager.__init__()
        unittest.TestCase.tearDown(self)
        sys.stdout.write('\n')
        sys.stdout.flush()

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

    def prepareDatabase(self, number, prefix='test_neo'):
        """ create empties databases """
        setupMySQLdb(['%s%u' % (prefix, i) for i in xrange(number)])

    def getMasterConfiguration(self, cluster='main', master_number=2,
            replicas=2, partitions=1009, uuid=None):
        assert master_number >= 1 and master_number <= 10
        masters = ([(self.local_ip, 10010 + i)
                    for i in xrange(master_number)])
        return Mock({
                'getCluster': cluster,
                'getBind': masters[0],
                'getMasters': (masters, getAddressType((
                        self.local_ip, 0))),
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
        database = '%s@%s%s' % (DB_USER, prefix, index)
        return Mock({
                'getCluster': cluster,
                'getName': 'storage',
                'getBind': (masters[0], 10020 + index),
                'getMasters': (masters, getAddressType((
                        self.local_ip, 0))),
                'getDatabase': database,
                'getUUID': uuid,
                'getReset': False,
                'getAdapter': 'MySQL',
        })

    def _makeUUID(self, prefix):
        """
            Retuns a 16-bytes UUID according to namespace 'prefix'
        """
        assert len(prefix) == 1
        uuid = protocol.INVALID_UUID
        while uuid[1:] == protocol.INVALID_UUID[1:]:
            uuid = prefix + os.urandom(15)
        return uuid

    def getNewUUID(self):
        return self._makeUUID('\0')

    def getClientUUID(self):
        return self._makeUUID('C')

    def getMasterUUID(self):
        return self._makeUUID('M')

    def getStorageUUID(self):
        return self._makeUUID('S')

    def getAdminUUID(self):
        return self._makeUUID('A')

    def getNextTID(self, ltid=None):
        tm = time()
        gmt = gmtime(tm)
        upper = ((((gmt.tm_year - 1900) * 12 + gmt.tm_mon - 1) * 31 \
                  + gmt.tm_mday - 1) * 24 + gmt.tm_hour) * 60 + gmt.tm_min
        lower = int((gmt.tm_sec % 60 + (tm - int(tm))) / (60.0 / 65536.0 / 65536.0))
        tid = pack('!LL', upper, lower)
        if ltid is not None and tid <= ltid:
            upper, lower = unpack('!LL', self._last_tid)
            if lower == 0xffffffff:
                # This should not happen usually.
                from datetime import timedelta, datetime
                d = datetime(gmt.tm_year, gmt.tm_mon, gmt.tm_mday,
                             gmt.tm_hour, gmt.tm_min) \
                        + timedelta(0, 60)
                upper = ((((d.year - 1900) * 12 + d.month - 1) * 31 \
                          + d.day - 1) * 24 + d.hour) * 60 + d.minute
                lower = 0
            else:
                lower += 1
            tid = pack('!LL', upper, lower)
        return tid

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

    def getTwoIDs(self):
        """ Return a tuple of two sorted UUIDs """
        # generate two ptid, first is lower
        uuids = self.getNewUUID(), self.getNewUUID()
        return min(uuids), max(uuids)

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
        return Mock({
            'getUUID': uuid,
            'getAddress': address,
            'isServer': is_server,
            '__repr__': 'FakeConnection',
            '__nonzero__': 0,
            'getConnector': connector,
            'getPeerId': peer_id,
        })

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

    def checkUUIDSet(self, conn, uuid=None):
        """ ensure no UUID was set on the connection """
        calls = conn.mockGetNamedCalls('setUUID')
        self.assertEqual(len(calls), 1)
        call = calls.pop()
        if uuid is not None:
            self.assertEqual(call.getParam(0), uuid)

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
            return protocol.decode_table[type(packet)](packet._body)
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

    def checkAnswerObjectHistoryFrom(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObjectHistoryFrom, **kw)

    def checkAnswerStoreTransaction(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerStoreTransaction, **kw)

    def checkAnswerStoreObject(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerStoreObject, **kw)

    def checkAnswerOids(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerOIDs, **kw)

    def checkAnswerPartitionTable(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerPartitionTable, **kw)

    def checkAnswerObjectPresent(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObjectPresent, **kw)

connector_cpt = 0

class DoNothingConnector(Mock):
    def __init__(self, s=None):
        neo.lib.logging.info("initializing connector")
        global connector_cpt
        self.desc = connector_cpt
        connector_cpt += 1
        self.packet_cpt = 0
        Mock.__init__(self)

    def getAddress(self):
        return self.addr

    def makeClientConnection(self, addr):
        self.addr = addr

    def makeListeningConnection(self, addr):
        self.addr = addr

    def getDescriptor(self):
        return self.desc


__builtin__.pdb = lambda depth=0: \
    debug.getPdb().set_trace(sys._getframe(depth+1))
