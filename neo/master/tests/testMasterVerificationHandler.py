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

import os
import unittest
import logging
from mock import Mock
from struct import pack, unpack
import neo
from neo.tests.base import NeoTestBase
from neo.protocol import Packet, INVALID_UUID
from neo.master.verification import VerificationEventHandler
from neo.master.app import Application
from neo import protocol
from neo.protocol import ERROR, ANNOUNCE_PRIMARY_MASTER, \
    NOTIFY_NODE_INFORMATION, ANSWER_LAST_IDS, ANSWER_PARTITION_TABLE, \
     ANSWER_UNFINISHED_TRANSACTIONS, ANSWER_OBJECT_PRESENT, \
     ANSWER_TRANSACTION_INFORMATION, OID_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import OperationFailure, ElectionFailure, VerificationFailure     
from neo.node import MasterNode, StorageNode
from neo.master.tests.connector import DoNothingConnector
from neo.connection import ClientConnection


class MasterVerificationTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config = self.getConfigFile(master_number=2)
        self.app = Application(config, "master1")
        self.app.pt.clear()
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.verification = VerificationEventHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        self.app.asking_uuid_dict = {}
        self.app.unfinished_tid_set = set()
        for node in self.app.nm.getMasterNodeList():
            self.app.unconnected_master_node_set.add(node.getServer())
            node.setState(RUNNING_STATE)

        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10011
        self.master_address = ('127.0.0.1', self.master_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    def identifyToMasterNode(self, node_type=STORAGE_NODE_TYPE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        uuid = self.getNewUUID()
        args = (node_type, uuid, ip, port, self.app.name)
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = self.getFakeConnection()
        self.verification.handleRequestNodeIdentification(conn, packet, *args)
        self.app.nm.getNodeByServer((ip, port)).setState(RUNNING_STATE)
        self.checkAcceptNodeIdentification(conn)
        return uuid

    # Tests
    def test_01_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.connectionClosed(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_02_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.timeoutExpired(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_03_peerBroken(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.peerBroken(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), BROKEN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_04_handleRequestNodeIdentification(self):
        verification = self.verification
        uuid = self.getNewUUID()
        args = ( MASTER_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, "INVALID_NAME")
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = self.getFakeConnection()
        self.checkProtocolErrorRaised(
                verification.handleRequestNodeIdentification,
                conn, packet=packet, 
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name="INVALID_NAME",)
        # test connection from a client node, rejectet
        uuid = self.getNewUUID()
        conn = self.getFakeConnection()
        self.checkNotReadyErrorRaised(
                verification.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=CLIENT_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.client_port,
                name=self.app.name,)

        # 1. unknown storage node with known address, must be rejected
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkProtocolErrorRaised(
                verification.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=STORAGE_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.master_port,
                name=self.app.name,)

        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)

        # 2. unknown master node with known address, will be accepted
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)

        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkUUIDSet(conn, uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkAcceptNodeIdentification(conn)

        # 3. unknown master node with known address but different uuid, will be replaced
        old_uuid = uuid
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getUUID(), old_uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkProtocolErrorRaised(
                verification.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.master_port,
                name=self.app.name,)

        # 4. unknown master node with known address but different uuid and broken state, will be accepted
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        node.setState(DOWN_STATE)
        self.assertEqual(node.getState(), DOWN_STATE)
        self.assertEqual(node.getUUID(), old_uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)

        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkAcceptNodeIdentification(conn)

        # 5. known by uuid, but different address
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.2',
                                                port=self.master_port,
                                                name=self.app.name,)
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.checkAcceptNodeIdentification(conn)
        # a new uuid is sent
        call = conn.mockGetNamedCalls('answer')[0]
        body = call.getParam(0)._body
        new_uuid = body[:-16]
        self.assertNotEquals(new_uuid, uuid)

        # 6.known by uuid, but different address and non running state
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        node.setState(DOWN_STATE)
        self.assertEqual(node.getState(), DOWN_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.checkProtocolErrorRaised(
                verification.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.2',
                port=self.master_port,
                name=self.app.name,)

        # 7. known node but broken
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), DOWN_STATE)
        node.setState(BROKEN_STATE)
        self.assertEqual(node.getState(), BROKEN_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.checkBrokenNodeDisallowedErrorRaised(
                verification.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.master_port,
                name=self.app.name,)

        # 8. known node but down
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), BROKEN_STATE)
        node.setState(DOWN_STATE)
        self.assertEqual(node.getState(), DOWN_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)

        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkAcceptNodeIdentification(conn)

        # 9. New node
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, ('127.0.0.3', self.master_port))
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.3',
                                                port=self.master_port,
                                                name=self.app.name,)

        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 3)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkAcceptNodeIdentification(conn)
        

    def test_05_handleAskPrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleAskPrimaryMaster(conn, packet)        
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn)
        self.checkNotifyNodeInformation(conn)
        # if storage node, expect messages
        uuid = self.identifyToMasterNode(STORAGE_NODE_TYPE, port=self.storage_port)
        packet = protocol.askPrimaryMaster()
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleAskPrimaryMaster(conn, packet)        
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn)
        self.checkNotifyNodeInformation(conn, packet_number=0)
        self.checkSendPartitionTable(conn, packet_number=1)
        self.checkSendPartitionTable(conn, packet_number=2)

    def test_06_handleAnnouncePrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANNOUNCE_PRIMARY_MASTER)
        # No uuid
        conn = self.getFakeConnection(None, self.master_address)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkIdenficationRequired(verification.handleAnnouncePrimaryMaster, conn, packet)
        # announce
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.primary, None)
        self.assertEqual(self.app.primary_master_node, None)
        self.assertRaises(ElectionFailure, verification.handleAnnouncePrimaryMaster, conn, packet)        

    def test_07_handleReelectPrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        # No uuid
        conn = self.getFakeConnection(None, self.master_address)
        self.assertRaises(ElectionFailure, verification.handleReelectPrimaryMaster, conn, packet)

    def test_08_handleNotifyNodeInformation(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.master_address)
        node_list = []
        self.checkIdenficationRequired(verification.handleNotifyNodeInformation, conn, packet, node_list)
        # tell about a client node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.client_port, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)

        # tell the master node about itself, if running must do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, RUNNING_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))

        # tell the master node about itself, if down must raise
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        self.assertRaises(RuntimeError, verification.handleNotifyNodeInformation, conn, packet, node_list)

        # tell about an unknown storage node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)

        # tell about a known node but different address
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.2', self.master_port, uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)

        # tell about a known node
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), DOWN_STATE)

    def test_09_handleAnswerLastIDs(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.pt.getID()
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(verification.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.pt.getID())
        # do not care if master node call it
        master_uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        node_list = []
        self.checkUnexpectedPacketRaised(verification.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.pt.getID())
        # send information which are later to what PMN knows, this must raise
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        oid = unpack('!Q', loid)[0]
        new_oid = pack('!Q', oid + 1)
        upper, lower = unpack('!LL', ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.failUnless(new_ptid > self.app.pt.getID())
        self.failUnless(new_oid > self.app.loid)
        self.failUnless(new_tid > self.app.ltid)
        self.assertRaises(VerificationFailure, verification.handleAnswerLastIDs, conn, packet, new_oid, new_tid, new_ptid)
        self.assertNotEquals(new_oid, self.app.loid)
        self.assertNotEquals(new_tid, self.app.ltid)
        self.assertNotEquals(new_ptid, self.app.pt.getID())

    def test_10_handleAnswerPartitionTable(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANSWER_PARTITION_TABLE, )
        conn = self.getFakeConnection(uuid, self.master_address)
        verification.handleAnswerPartitionTable(conn, packet, None, [])
        self.assertEqual(len(conn.mockGetAllCalls()), 0)

    def test_11_handleAnswerUnfinishedTransactions(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_UNFINISHED_TRANSACTIONS)
        # reject when no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(verification.handleAnswerUnfinishedTransactions, conn, packet, [])
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        self.checkUnexpectedPacketRaised(verification.handleAnswerUnfinishedTransactions, conn, packet, [])
        # do nothing
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_tid_set), 0)
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        verification.handleAnswerUnfinishedTransactions(conn, packet, [new_tid])
        self.assertEquals(len(self.app.unfinished_tid_set), 0)        
        # update dict
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_tid_set), 0)
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        verification.handleAnswerUnfinishedTransactions(conn, packet, [new_tid,])
        self.assertTrue(self.app.asking_uuid_dict[uuid])
        self.assertEquals(len(self.app.unfinished_tid_set), 1)
        self.assertTrue(new_tid in self.app.unfinished_tid_set)


    def test_12_handleAnswerTransactionInformation(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_TRANSACTION_INFORMATION)
        # reject when no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(verification.handleAnswerTransactionInformation, conn, packet, None, None, None, None, None)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.storage_address)
        self.checkUnexpectedPacketRaised(verification.handleAnswerTransactionInformation, conn, packet, None, None, None, None, None)
        # do nothing, as unfinished_oid_set is None
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = False
        self.app.unfinished_oid_set  = None
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        oid = unpack('!Q', self.app.loid)[0]
        new_oid = pack('!Q', oid + 1)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(self.app.unfinished_oid_set, None)        
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = set()
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 1)
        self.assertTrue(new_oid in self.app.unfinished_oid_set)
        # do not work as oid is diff
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 1)
        old_oid = new_oid
        oid = unpack('!Q', old_oid)[0]
        new_oid = pack('!Q', oid + 1)
        self.assertNotEqual(new_oid, old_oid)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(self.app.unfinished_oid_set, None)

    def test_13_handleTidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=TID_NOT_FOUND_CODE)
        # reject when no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(verification.handleTidNotFound, conn, packet, [])
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        self.checkUnexpectedPacketRaised(verification.handleTidNotFound, conn, packet, [])
        # do nothing as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = []
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleTidNotFound(conn, packet, "msg")
        self.assertNotEqual(self.app.unfinished_oid_set, None)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.app.unfinished_oid_set  = []
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleTidNotFound(conn, packet, "msg")
        self.assertEqual(self.app.unfinished_oid_set, None)
        
    def test_14_handleAnswerObjectPresent(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_OBJECT_PRESENT)
        # reject when no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(verification.handleAnswerObjectPresent, conn, packet, None, None)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        self.checkUnexpectedPacketRaised(verification.handleAnswerObjectPresent, conn, packet, None, None)
        # do nothing as asking_uuid_dict is True
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        oid = unpack('!Q', self.app.loid)[0]
        new_oid = pack('!Q', oid + 1)
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleAnswerObjectPresent(conn, packet, new_oid, new_tid)
        # do work
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertFalse(self.app.asking_uuid_dict[uuid])
        verification.handleAnswerObjectPresent(conn, packet, new_oid, new_tid)
        self.assertTrue(self.app.asking_uuid_dict[uuid])
        
    def test_15_handleOidNotFound(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=OID_NOT_FOUND_CODE)
        # reject when no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(verification.handleOidNotFound, conn, packet, [])
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        self.checkUnexpectedPacketRaised(verification.handleOidNotFound, conn, packet, [])
        # do nothinf as asking_uuid_dict is True
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.object_present = True
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertTrue(self.app.object_present)
        # do work as asking_uuid_dict is False
        conn = self.getFakeConnection(uuid, self.storage_address)
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertFalse(self.app.asking_uuid_dict[uuid ])
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertFalse(self.app.object_present)
        self.assertTrue(self.app.asking_uuid_dict[uuid ])
    
if __name__ == '__main__':
    unittest.main()

