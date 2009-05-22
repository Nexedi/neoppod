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

    # XXX: according to changes with namespaced UUIDs, it whould be better to 
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

    def checkNoPacketSent(self, conn):
        # no packet should be sent
        self.assertEquals(len(conn.mockGetNamedCalls('notify')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('answer')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('ask')), 0)

    # in check(Ask|Answer|Notify)Packet we return the packet so it can be used
    # in tests if more accurates checks are required

    def checkAskPacket(self, conn, packet_type):
        """ Check if an ask-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('ask')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        return packet

    def checkAnswerPacket(self, conn, packet_type, answered_packet=None):
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
        return packet

    def checkNotifyPacket(self, conn, packet_type, packet_number=0):
        """ Check if a notify-packet with the right type is sent """
        calls = conn.mockGetNamedCalls('notify')
        self.assertTrue(len(calls) > packet_number)
        packet = calls[packet_number].getParam(0)
        self.assertTrue(isinstance(packet, protocol.Packet))
        self.assertEquals(packet.getType(), packet_type)
        return packet

    def checkNotifyNodeInformation(self, conn, packet_number=0):
        """ Check Notify Node Information message has been send"""
        return self.checkNotifyPacket(conn, protocol.NOTIFY_NODE_INFORMATION,
                packet_number)

    def checkSendPartitionTable(self, conn, packet_number=0):
        """ Check partition table has been send"""
        return self.checkNotifyPacket(conn, protocol.SEND_PARTITION_TABLE, 
                packet_number)

    def checkStartOperation(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        return self.checkNotifyPacket(conn, protocol.START_OPERATION, 
                packet_number)

    def checkNotifyTransactionFinished(self, conn, packet_number=0):
        """ Check notifyTransactionFinished message has been send"""
        return self.checkNotifyPacket(conn, protocol.NOTIFY_TRANSACTION_FINISHED,
                packet_number)

    def checkLockInformation(self, conn):
        """ Check lockInformation message has been send"""
        return self.checkAskPacket(conn, protocol.LOCK_INFORMATION)

    def checkUnlockInformation(self, conn):
        """ Check unlockInformation message has been send"""
        return self.checkAskPacket(conn, protocol.UNLOCK_INFORMATION)

    def checkAcceptNodeIdentification(self, conn, answered_packet=None):
        """ Check Accept Node Identification has been answered """
        return self.checkAnswerPacket(conn, protocol.ACCEPT_NODE_IDENTIFICATION, 
                answered_packet)

    def checkAnswerPrimaryMaster(self, conn, answered_packet=None):
        """ Check Answer primaty master message has been send"""
        return self.checkAnswerPacket(conn, protocol.ANSWER_PRIMARY_MASTER,
                answered_packet)

    def checkAnswerLastIDs(self, conn, packet_number=0):
        """ Check answerLastIDs message has been send"""
        return self.checkAnswerPacket(conn, protocol.ANSWER_LAST_IDS)

    def checkAnswerUnfinishedTransactions(self, conn, packet_number=0):
        """ Check answerUnfinishedTransactions message has been send"""
        return self.checkAnswerPacket(conn,
                protocol.ANSWER_UNFINISHED_TRANSACTIONS)

