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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest, os
from mock import Mock
from neo import protocol

class NeoTestBase(unittest.TestCase):
    """ Base class for neo tests, implements common checks """

    # XXX: according to changes with namespaced UUIDs, it would be better to 
    # implement get<NodeType>UUID() methods 
    def getNewUUID(self):
        """ Return a valid UUID """
        uuid = protocol.INVALID_UUID
        while uuid == protocol.INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def getTwoIDs(self):
        """ Return a tuple of two sorted UUIDs """
        # generate two ptid, first is lower
        ptids = self.getNewUUID(), self.getNewUUID()
        return min(ptids), max(ptids)
        ptid = min(ptids)

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
        self.assertEquals(packet.getType(), protocol.ERROR)
        if decode:
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
            return protocol.decode_table[packet.getType()](packet._body)
        return packet

    def checkAnswerPacket(self, conn, packet_type, answered_packet=None, decode=False):
        """ Check if an answer-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('answer')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        if answered_packet is not None:
            a_packet = calls[0].getParam(1)
            self.assertEquals(a_packet, answered_packet)
            self.assertEquals(a_packet.getId(), answered_packet.getId())
        if decode:
            return protocol.decode_table[packet.getType()](packet._body)
        return packet

    def checkNotifyPacket(self, conn, packet_type, packet_number=0, decode=False):
        """ Check if a notify-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('notify')
        self.assertTrue(len(calls) > packet_number)
        packet = calls[packet_number].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        if decode:
            return protocol.decode_table[packet.getType()](packet._body)
        return packet

    def checkNotifyNodeInformation(self, conn, **kw):
        return self.checkNotifyPacket(conn, protocol.NOTIFY_NODE_INFORMATION, **kw)

    def checkSendPartitionTable(self, conn, **kw):
        return self.checkNotifyPacket(conn, protocol.SEND_PARTITION_TABLE, **kw)

    def checkStartOperation(self, conn, **kw):
        return self.checkNotifyPacket(conn, protocol.START_OPERATION, **kw)

    def checkNotifyTransactionFinished(self, conn, **kw):
        return self.checkNotifyPacket(conn, protocol.NOTIFY_TRANSACTION_FINISHED, **kw)

    def checkNotifyInformationLocked(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.NOTIFY_INFORMATION_LOCKED, **kw)

    def checkLockInformation(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.LOCK_INFORMATION, **kw)

    def checkUnlockInformation(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.UNLOCK_INFORMATION, **kw)

    def checkRequestNodeIdentification(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.REQUEST_NODE_IDENTIFICATION, **kw)

    def checkAskPrimaryMaster(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.ASK_PRIMARY_MASTER)

    def checkAskUnfinishedTransactions(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.ASK_UNFINISHED_TRANSACTIONS)

    def checkAskTransactionInformation(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.ASK_TRANSACTION_INFORMATION, **kw)

    def checkAskObjectPresent(self, conn, **kw):
        return self.checkAskPacket(conn, protocol.ASK_OBJECT_PRESENT, **kw)

    def checkAcceptNodeIdentification(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ACCEPT_NODE_IDENTIFICATION, **kw)

    def checkAnswerPrimaryMaster(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_PRIMARY_MASTER, **kw)

    def checkAnswerLastIDs(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_LAST_IDS, **kw)

    def checkAnswerUnfinishedTransactions(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_UNFINISHED_TRANSACTIONS, **kw)

    def checkAnswerObject(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_OBJECT, **kw)

    def checkAnswerTransactionInformation(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_TRANSACTION_INFORMATION, **kw)

    def checkAnswerTids(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_TIDS, **kw)

    def checkAnswerObjectHistory(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_OBJECT_HISTORY, **kw)

    def checkAnswerStoreTransaction(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_STORE_TRANSACTION, **kw)

    def checkAnswerStoreObject(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_STORE_OBJECT, **kw)

    def checkAnswerOids(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_OIDS, **kw)

    def checkAnswerPartitionTable(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_PARTITION_TABLE, **kw)

    def checkAnswerObjectPresent(self, conn, **kw):
        return self.checkAnswerPacket(conn, protocol.ANSWER_OBJECT_PRESENT, **kw)
