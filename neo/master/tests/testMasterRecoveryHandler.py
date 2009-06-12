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
from neo.tests.base import NeoTestBase
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.recovery import RecoveryEventHandler
from neo.master.app import Application
from neo.protocol import ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
     PING, PONG, ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, ANNOUNCE_PRIMARY_MASTER, \
     REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION, START_OPERATION, \
     STOP_OPERATION, ASK_LAST_IDS, ANSWER_LAST_IDS, ASK_PARTITION_TABLE, \
     ANSWER_PARTITION_TABLE, SEND_PARTITION_TABLE, NOTIFY_PARTITION_CHANGES, \
     ASK_UNFINISHED_TRANSACTIONS, ANSWER_UNFINISHED_TRANSACTIONS, \
     ASK_OBJECT_PRESENT, ANSWER_OBJECT_PRESENT, \
     DELETE_TRANSACTION, COMMIT_TRANSACTION, ASK_NEW_TID, ANSWER_NEW_TID, \
     FINISH_TRANSACTION, NOTIFY_TRANSACTION_FINISHED, LOCK_INFORMATION, \
     NOTIFY_INFORMATION_LOCKED, INVALIDATE_OBJECTS, UNLOCK_INFORMATION, \
     ASK_NEW_OIDS, ANSWER_NEW_OIDS, ASK_STORE_OBJECT, ANSWER_STORE_OBJECT, \
     ABORT_TRANSACTION, ASK_STORE_TRANSACTION, ANSWER_STORE_TRANSACTION, \
     ASK_OBJECT, ANSWER_OBJECT, ASK_TIDS, ANSWER_TIDS, ASK_TRANSACTION_INFORMATION, \
     ANSWER_TRANSACTION_INFORMATION, ASK_OBJECT_HISTORY, ANSWER_OBJECT_HISTORY, \
     ASK_OIDS, ANSWER_OIDS, \
     NOT_READY_CODE, OID_NOT_FOUND_CODE, SERIAL_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
     PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE, \
     INTERNAL_ERROR_CODE, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import OperationFailure, ElectionFailure     
from neo.node import MasterNode, StorageNode
from neo.master.tests.connector import DoNothingConnector
from neo.connection import ClientConnection

class MasterRecoveryTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config = self.getConfigFile()
        self.app = Application(config, "master1")        
        self.app.pt.clear()
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.recovery = RecoveryEventHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
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
        args = (node_type, uuid, ip, port,self.app.name)
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = Mock({})
        self.recovery.handleRequestNodeIdentification(conn, packet, *args)
        self.checkAcceptNodeIdentification(conn)
        return uuid

    # Tests
    def test_01_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.recovery.connectionClosed(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)                

    def test_02_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.recovery.timeoutExpired(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)                


    def test_03_peerBroken(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.recovery.peerBroken(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), BROKEN_STATE)                

    def test_04_handleRequestNodeIdentification(self):
        recovery = self.recovery
        uuid = self.getNewUUID()
        args = (MASTER_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, "INVALID_NAME")
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = Mock({})
        self.checkProtocolErrorRaised(
                recovery.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name="INVALID_NAME",)
        # test connection from a client node, rejectet
        uuid = self.getNewUUID()
        conn = Mock({})
        self.checkNotReadyErrorRaised(
                recovery.handleRequestNodeIdentification,
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
                recovery.handleRequestNodeIdentification,
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
        recovery.handleRequestNodeIdentification(conn,
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
                recovery.handleRequestNodeIdentification,
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
        recovery.handleRequestNodeIdentification(conn,
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
        known_uuid = uuid

        # 5. known by uuid, but different address -> conflict / new master
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        recovery.handleRequestNodeIdentification(conn,
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

        # 6.known by uuid, but different address and non running state -> conflict
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
                recovery.handleRequestNodeIdentification,
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
                recovery.handleRequestNodeIdentification,
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
        recovery.handleRequestNodeIdentification(conn,
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
        recovery.handleRequestNodeIdentification(conn,
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
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        recovery.handleAskPrimaryMaster(conn, packet)        
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn)
        self.checkNotifyNodeInformation(conn)
        # if storage node, expect message

        uuid = self.identifyToMasterNode(STORAGE_NODE_TYPE, port=self.storage_port)
        packet = protocol.askPrimaryMaster()
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        recovery.handleAskPrimaryMaster(conn, packet)        
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn)
        self.checkNotifyNodeInformation(conn)
        self.checkAskLastIDs(conn)


    def test_06_handleAnnouncePrimaryMaster(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANNOUNCE_PRIMARY_MASTER)
        # No uuid
        conn = self.getFakeConnection(None, self.master_address)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkIdenficationRequired(recovery.handleAnnouncePrimaryMaster, conn, packet)
        # announce
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.primary, None)
        self.assertEqual(self.app.primary_master_node, None)
        self.assertRaises(ElectionFailure, recovery.handleAnnouncePrimaryMaster, conn, packet)        


    def test_07_handleReelectPrimaryMaster(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        # No uuid
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertRaises(ElectionFailure, recovery.handleReelectPrimaryMaster, conn, packet)


    def test_08_handleNotifyNodeInformation(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.master_address)
        node_list = []
        self.checkIdenficationRequired(recovery.handleNotifyNodeInformation, conn, packet, node_list)
        # tell about a client node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.client_port, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)
        recovery.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)

        # tell the master node about itself, if running must do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, RUNNING_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        recovery.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))

        # tell the master node about itself, if down must raise
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        self.assertRaises(RuntimeError, recovery.handleNotifyNodeInformation, conn, packet, node_list)

        # tell about an unknown storage node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)
        recovery.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)

        # tell about a known node but different address
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.2', self.master_port, uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)
        recovery.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)

        # tell about a known node
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)
        recovery.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), DOWN_STATE)
        

    def test_09_handleAnswerLastIDs(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.lptid
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_port)
        node_list = []
        self.checkIdenficationRequired(recovery.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # do not care if master node call it
        master_uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = self.getFakeConnection(master_uuid, self.master_address)
        node_list = []
        self.checkUnexpectedPacketRaised(recovery.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # send information which are later to what PMN knows, this must update target node
        conn = self.getFakeConnection(uuid, self.storage_port)
        node_list = []
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        oid = unpack('!Q', loid)[0]
        new_oid = pack('!Q', oid + 1)
        upper, lower = unpack('!LL', ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.failUnless(new_ptid > self.app.lptid)
        self.failUnless(new_oid > self.app.loid)
        self.failUnless(new_tid > self.app.ltid)
        self.assertEquals(self.app.target_uuid, None)
        recovery.handleAnswerLastIDs(conn, packet, new_oid, new_tid, new_ptid)
        self.assertEquals(new_oid, self.app.loid)
        self.assertEquals(new_tid, self.app.ltid)
        self.assertEquals(new_ptid, self.app.lptid)
        self.assertEquals(self.app.target_uuid,uuid)


    def test_10_handleAnswerPartitionTable(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANSWER_PARTITION_TABLE)
        # No uuid
        conn = self.getFakeConnection(None, self.master_address)
        self.checkIdenficationRequired(recovery.handleAnswerPartitionTable, conn, packet, None, [])
        # not a storage node
        conn = self.getFakeConnection(uuid, self.master_address)
        self.checkUnexpectedPacketRaised(recovery.handleAnswerPartitionTable, conn, packet, None, [])
        # not from target node, ignore
        uuid = self.identifyToMasterNode(STORAGE_NODE_TYPE, port=self.storage_port)
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertNotEquals(self.app.target_uuid, uuid)
        offset = 1
        cell_list = [(offset, uuid, UP_TO_DATE_STATE)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        recovery.handleAnswerPartitionTable(conn, packet, None, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        # from target node, taken into account
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertNotEquals(self.app.target_uuid, uuid)
        self.app.target_uuid = uuid
        self.assertEquals(self.app.target_uuid, uuid)
        offset = 1
        cell_list = [(offset, ((uuid, UP_TO_DATE_STATE,),),)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        recovery.handleAnswerPartitionTable(conn, packet, None, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, UP_TO_DATE_STATE)
        # give a bad offset, must send error
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertEquals(self.app.target_uuid, uuid)
        offset = 1000000
        self.assertFalse(self.app.pt.hasOffset(offset))
        cell_list = [(offset, ((uuid, DOWN_STATE,),),)]
        self.checkUnexpectedPacketRaised(recovery.handleAnswerPartitionTable, conn, packet, None, cell_list)
        
    
if __name__ == '__main__':
    unittest.main()

