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
from neo.tests import NeoTestBase
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.handlers.recovery import RecoveryHandler
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
from neo.tests import DoNothingConnector
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
        self.recovery = RecoveryHandler(self.app)
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

    def test_08_handleNotifyNodeInformation(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
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
        lptid = self.app.pt.getID()
        # send information which are later to what PMN knows, this must update target node
        conn = self.getFakeConnection(uuid, self.storage_port)
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
        self.assertEquals(self.app.target_uuid, None)
        recovery.handleAnswerLastIDs(conn, packet, new_oid, new_tid, new_ptid)
        self.assertEquals(new_oid, self.app.loid)
        self.assertEquals(new_tid, self.app.ltid)
        self.assertEquals(new_ptid, self.app.pt.getID())
        self.assertEquals(self.app.target_uuid,uuid)


    def test_10_handleAnswerPartitionTable(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANSWER_PARTITION_TABLE)
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

