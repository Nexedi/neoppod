#
# Copyright (C) 2009  Nexedi SA
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

import os
import unittest
import tempfile
import MySQLdb
from neo import logging
from mock import Mock
from neo import protocol
from neo.protocol import Packets

DB_PREFIX = 'test_neo_'
DB_ADMIN = 'root'
DB_PASSWD = None
DB_USER = 'test'

def getNewUUID():
    """ Return a valid UUID """
    uuid = protocol.INVALID_UUID
    while uuid == protocol.INVALID_UUID:
        uuid = os.urandom(16)
    return uuid

class NeoTestBase(unittest.TestCase):
    """ Base class for neo tests, implements common checks """

    def prepareDatabase(self, number, admin=DB_ADMIN, password=DB_PASSWD,
            user=DB_USER, prefix=DB_PREFIX):
        """ create empties databases """
        # SQL connection
        connect_arg_dict = {'user': admin}
        if password is not None:
            connect_arg_dict['passwd'] = password
        sql_connection = MySQLdb.Connect(**connect_arg_dict)
        cursor = sql_connection.cursor()
        # drop and create each database
        for i in xrange(number):
            database = "%s%d" % (prefix, i)
            cursor.execute('DROP DATABASE IF EXISTS %s' % (database, ))
            cursor.execute('CREATE DATABASE %s' % (database, ))
            cursor.execute('GRANT ALL ON %s.* TO "%s"@"localhost" IDENTIFIED BY ""' %
                (database, user))
        cursor.close()
        sql_connection.close()

    def getMasterConfiguration(self, cluster='main', master_number=2,
            replicas=2, partitions=1009, uuid=None):
        assert master_number >= 1 and master_number <= 10
        masters = [('127.0.0.1', 10010 + i) for i in xrange(master_number)]
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
        masters = [('127.0.0.1', 10010 + i) for i in xrange(master_number)]
        database = '%s@%s%s' % (DB_USER, prefix, index)
        return Mock({
                'getCluster': cluster,
                'getName': 'storage',
                'getBind': ('127.0.0.1', 10020 + index),
                'getMasters': masters,
                'getDatabase': database,
                'getUUID': uuid,
                'getReset': False,
                'getAdapter': 'MySQL',
        })

    # XXX: according to changes with namespaced UUIDs, it would be better to
    # implement get<NodeType>UUID() methods
    def getNewUUID(self):
        self.uuid = getNewUUID()
        return self.uuid

    def getTwoIDs(self):
        """ Return a tuple of two sorted UUIDs """
        # generate two ptid, first is lower
        uuids = self.getNewUUID(), self.getNewUUID()
        return min(uuids), max(uuids)

    def getFakeConnection(self, uuid=None, address=('127.0.0.1', 10000)):
        return Mock({
            'getUUID': uuid,
            'getAddress': address,
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
        self.assertEquals(len(conn.mockGetNamedCalls('abort')), 1)

    def checkNotAborted(self, conn):
        """ Ensure the connection was not aborted """
        self.assertEquals(len(conn.mockGetNamedCalls('abort')), 0)

    def checkClosed(self, conn):
        """ Ensure the connection was closed """
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 1)

    def checkNotClosed(self, conn):
        """ Ensure the connection was not closed """
        self.assertEquals(len(conn.mockGetNamedCalls('close')), 0)

    def checkNoPacketSent(self, conn):
        """ check if no packet were sent """
        self.assertEquals(len(conn.mockGetNamedCalls('notify')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('answer')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('ask')), 0)

    def checkNoUUIDSet(self, conn):
        """ ensure no UUID was set on the connection """
        self.assertEquals(len(conn.mockGetNamedCalls('setUUID')), 0)

    def checkUUIDSet(self, conn, uuid=None):
        """ ensure no UUID was set on the connection """
        calls = conn.mockGetNamedCalls('setUUID')
        self.assertEquals(len(calls), 1)
        if uuid is not None:
            self.assertEquals(calls[0].getParam(0), uuid)

    # in check(Ask|Answer|Notify)Packet we return the packet so it can be used
    # in tests if more accurates checks are required

    def checkErrorPacket(self, conn, decode=False):
        """ Check if an error packet was answered """
        calls = conn.mockGetNamedCalls("answer")
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), Packets.Error)
        if decode:
            return packet.decode()
            return protocol.decode_table[packet.getType()](packet._body)
        return packet

    def checkAskPacket(self, conn, packet_type, decode=False):
        """ Check if an ask-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('ask')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        if decode:
            return packet.decode()
        return packet

    def checkAnswerPacket(self, conn, packet_type, answered_packet=None, decode=False):
        """ Check if an answer-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('answer')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        if answered_packet is not None:
            msg_id = calls[0].getParam(1)
            self.assertEqual(msg_id, answered_packet.getId())
        if decode:
            return packet.decode()
        return packet

    def checkNotifyPacket(self, conn, packet_type, packet_number=0, decode=False):
        """ Check if a notify-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('notify')
        self.assertTrue(len(calls) > packet_number)
        packet = calls[packet_number].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        if decode:
            return packet.decode()
        return packet

    def checkNotifyNodeInformation(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyNodeInformation, **kw)

    def checkSendPartitionTable(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.SendPartitionTable, **kw)

    def checkStartOperation(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.StartOperation, **kw)

    def checkNotifyTransactionFinished(self, conn, **kw):
        return self.checkNotifyPacket(conn, Packets.NotifyTransactionFinished, **kw)

    def checkNotifyInformationLocked(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.NotifyInformationLocked, **kw)

    def checkLockInformation(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.LockInformation, **kw)

    def checkUnlockInformation(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.UnlockInformation, **kw)

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

    def checkFinishTransaction(self, conn, **kw):
        return self.checkAskPacket(conn, Packets.FinishTransaction, **kw)

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

    def checkAnswerTids(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerTIDs, **kw)

    def checkAnswerObjectHistory(self, conn, **kw):
        return self.checkAnswerPacket(conn, Packets.AnswerObjectHistory, **kw)

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
        logging.info("initializing connector")
        self.desc = globals()['connector_cpt']
        globals()['connector_cpt'] = globals()['connector_cpt']+ 1
        self.packet_cpt = 0
        Mock.__init__(self)

    def getAddress(self):
        return self.addr

    def makeClientConnection(self, addr):
        self.addr = addr

    def getDescriptor(self):
        return self.desc


class TestElectionConnector(DoNothingConnector):

    def receive(self):
        """ simulate behavior of election """
        if self.packet_cpt == 0:
            # first : identify
            logging.info("in patched analyse / IDENTIFICATION")
            p = protocol.Packet()
            self.uuid = getNewUUID()
            p.AcceptIdentification(1, NodeType.MASTER, self.uuid,
                 self.getAddress()[0], self.getAddress()[1], 1009, 2)
            self.packet_cpt += 1
            return p.encode()
        elif self.packet_cpt == 1:
            # second : answer primary master nodes
            logging.info("in patched analyse / ANSWER PM")
            p = protocol.Packet()
            p.answerPrimary(2, protocol.INVALID_UUID, [])
            self.packet_cpt += 1
            return p.encode()
        else:
            # then do nothing
            from neo.connector import ConnectorTryAgainException
            raise ConnectorTryAgainException

