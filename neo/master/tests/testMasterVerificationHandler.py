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
from tempfile import mkstemp
from mock import Mock
from struct import pack, unpack
from neo.protocol import Packet, INVALID_UUID
from neo.master.verification import VerificationEventHandler
from neo.master.app import Application
from neo import protocol
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
from neo.exception import OperationFailure, ElectionFailure, VerificationFailure     
from neo.node import MasterNode, StorageNode
from neo.master.tests.connector import DoNothingConnector
from neo.connection import ClientConnection


class MasterVerificationeTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config_file_text = """# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: 127.0.0.1:10010 127.0.0.1:10011
# The number of replicas.
replicas: 2
# The number of partitions.
partitions: 1009
# The name of this cluster.
name: main
# The user name for the database.
user: neo
# The password for the database.
password: neo
connector : SocketConnector
# The first master.
[mastertest]
server: 127.0.0.1:10010

# The first storage.
[storage1]
database: neotest1
server: 127.0.0.1:10020

# The second storage.
[storage2]
database: neotest2
server: 127.0.0.1:10021

# The third storage.
[storage3]
database: neotest3
server: 127.0.0.1:10022

# The fourth storage.
[storage4]
database: neotest4
server: 127.0.0.1:10023
"""
        tmp_id, self.tmp_path = mkstemp()
        tmp_file = os.fdopen(tmp_id, "w+b")
        tmp_file.write(config_file_text)
        tmp_file.close()
        self.app = Application(self.tmp_path, "mastertest")        
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
        
    def tearDown(self):
        # Delete tmp file
        os.remove(self.tmp_path)

    def checkCalledAcceptNodeIdentification(self, conn, packet_number=0):
        """ Check Accept Node Identification has been send"""
        self.assertEquals(len(conn.mockGetNamedCalls("answer")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        call = conn.mockGetNamedCalls("answer")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ACCEPT_NODE_IDENTIFICATION)

    # Common methods
    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

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
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        self.verification.handleRequestNodeIdentification(conn, packet, *args)
        self.checkCalledAcceptNodeIdentification(conn)
        return uuid

    # Method to test the kind of packet returned in answer
    def checkCalledRequestNodeIdentification(self, conn, packet_number=0):
        """ Check Request Node Identification has been send"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 1)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), REQUEST_NODE_IDENTIFICATION)

        
    def checkCalledAskPrimaryMaster(self, conn, packet_number=0):
        """ Check ask primary master has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(),ASK_PRIMARY_MASTER)

    def checkCalledAbort(self, conn, packet_number=0):
        """Check the abort method has been called and an error packet has been sent"""
        # sometimes we answer an error, sometimes we just send it
        send_calls_len = len(conn.mockGetNamedCalls("send"))
        answer_calls_len = len(conn.mockGetNamedCalls('answer'))
        self.assertEquals(send_calls_len + answer_calls_len, 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 1)
        if send_calls_len == 1:
            call = conn.mockGetNamedCalls("send")[packet_number]
        else:
            call = conn.mockGetNamedCalls("answer")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ERROR)

    def checkCalledNotifyNodeInformation(self, conn, packet_number=0):
        """ Check Notify Node Information message has been send"""
        call = conn.mockGetNamedCalls("notify")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), NOTIFY_NODE_INFORMATION)

    def checkCalledAnswerPrimaryMaster(self, conn, packet_number=0):
        """ Check Answer primaty master message has been send"""
        call = conn.mockGetNamedCalls("answer")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_PRIMARY_MASTER)

    def checkCalledAnswerLastIDs(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("answer")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_LAST_IDS)
        return packet._decodeAnswerLastIDs()

    def checkCalledSendPartitionTable(self, conn, packet_number=0):
        """ Check partition table has been send"""
        call = conn.mockGetNamedCalls("notify")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), SEND_PARTITION_TABLE)

    # Tests
    def test_01_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.connectionClosed(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_02_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.timeoutExpired(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_03_peerBroken(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.verification.peerBroken(conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), BROKEN_STATE)                
        # test a storage, must raise as cluster no longer op
        uuid = self.identifyToMasterNode()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)        
        self.assertRaises(VerificationFailure, self.verification.connectionClosed,conn)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_04_handleRequestNodeIdentification(self):
        verification = self.verification
        uuid = self.getNewUUID()
        args = ( MASTER_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, "INVALID_NAME")
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = Mock({"addPacket" : None, "abort" : None})
        verification.handleRequestNodeIdentification(conn, packet=packet, 
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name="INVALID_NAME",)
        self.checkCalledAbort(conn)        
        # test connection from a client node, rejectet
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=CLIENT_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.client_port,
                                                name=self.app.name,)
        self.checkCalledAbort(conn)

        # 1. unknown storage node with known address, must be rejected
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)

        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkCalledAbort(conn)

        # 2. unknown master node with known address, will be accepted
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
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
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkCalledAcceptNodeIdentification(conn)

        # 3. unknown master node with known address but different uuid, will be replaced
        old_uuid = uuid
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getUUID(), old_uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)
        self.checkCalledAbort(conn)

        # 4. unknown master node with known address but different uuid and broken state, will be accepted
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
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
        self.checkCalledAcceptNodeIdentification(conn)

        # 5. known by uuid, but different address
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
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
        self.checkCalledAcceptNodeIdentification(conn)
        # a new uuid is sent
        call = conn.mockGetNamedCalls('answer')[0]
        body = call.getParam(0)._body
        new_uuid = body[:-16]
        self.assertNotEquals(new_uuid, uuid)

        # 6.known by uuid, but different address and non running state
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), RUNNING_STATE)
        node.setState(DOWN_STATE)
        self.assertEqual(node.getState(), DOWN_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.2',
                                                port=self.master_port,
                                                name=self.app.name,)
        self.checkCalledAbort(conn)

        # 7. known node but broken
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertNotEqual(self.app.nm.getNodeByUUID(conn.getUUID()), None)
        self.assertNotEqual(self.app.nm.getNodeByServer(conn.getAddress()), None)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self.assertEqual(node.getState(), DOWN_STATE)
        node.setState(BROKEN_STATE)
        self.assertEqual(node.getState(), BROKEN_STATE)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        verification.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)

        self.checkCalledAbort(conn)

        # 8. known node but down
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
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
        self.checkCalledAcceptNodeIdentification(conn)

        # 9. New node
        uuid = self.getNewUUID()
        conn = Mock({"addPacket" : None,
                     "abort" : None,
                     "expectMessage" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.3", self.master_port)})
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
        self.checkCalledAcceptNodeIdentification(conn)
        

    def test_05_handleAskPrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleAskPrimaryMaster(conn, packet)        
        self.assertEquals(len(conn.mockGetNamedCalls("answer")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("notify")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.checkCalledAnswerPrimaryMaster(conn, 0)
        self.checkCalledNotifyNodeInformation(conn, 0)
        # if storage node, expect messages
        uuid = self.identifyToMasterNode(STORAGE_NODE_TYPE, port=self.storage_port)
        packet = protocol.askPrimaryMaster()
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleAskPrimaryMaster(conn, packet)        
        self.assertEquals(len(conn.mockGetNamedCalls("answer")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("notify")), 3)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.checkCalledAnswerPrimaryMaster(conn, 0)
        self.checkCalledNotifyNodeInformation(conn, 0)
        self.checkCalledSendPartitionTable(conn, 1)
        self.checkCalledSendPartitionTable(conn, 2)

    def test_06_handleAnnouncePrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANNOUNCE_PRIMARY_MASTER)
        # No uuid
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        verification.handleAnnouncePrimaryMaster(conn, packet)
        self.checkCalledAbort(conn)
        # announce
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.primary, None)
        self.assertEqual(self.app.primary_master_node, None)
        self.assertRaises(ElectionFailure, verification.handleAnnouncePrimaryMaster, conn, packet)        

    def test_07_handleReelectPrimaryMaster(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        # No uuid
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertRaises(ElectionFailure, verification.handleReelectPrimaryMaster, conn, packet)

    def test_08_handleNotifyNodeInformation(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        node_list = []
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        self.checkCalledAbort(conn)
        # tell about a client node, do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.client_port, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)

        # tell the master node about itself, if running must do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, RUNNING_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))

        # tell the master node about itself, if down must raise
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port-1, self.app.uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        self.assertRaises(RuntimeError, verification.handleNotifyNodeInformation, conn, packet, node_list)

        # tell about an unknown storage node, do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)

        # tell about a known node but different address
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.2', self.master_port, uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)
        verification.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), RUNNING_STATE)

        # tell about a known node
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
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
        lptid = self.app.lptid
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        verification.handleAnswerLastIDs(conn, packet, None, None, None)
        self.checkCalledAbort(conn)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # do not care if master node call it
        master_uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        node_list = []
        verification.handleAnswerLastIDs(conn, packet, None, None, None)
        self.checkCalledAbort(conn)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # send information which are later to what PMN knows, this must raise
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        self.assertRaises(VerificationFailure, verification.handleAnswerLastIDs, conn, packet, new_oid, new_tid, new_ptid)
        self.assertNotEquals(new_oid, self.app.loid)
        self.assertNotEquals(new_tid, self.app.ltid)
        self.assertNotEquals(new_ptid, self.app.lptid)

    def test_10_handleAnswerPartitionTable(self):
        verification = self.verification
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANSWER_PARTITION_TABLE, )
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleAnswerPartitionTable(conn, packet, None, [])
        self.assertEqual(len(conn.mockGetAllCalls()), 0)

    def test_11_handleAnswerUnfinishedTransactions(self):
        verification = self.verification
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_UNFINISHED_TRANSACTIONS)
        # reject when no uuid
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        verification.handleAnswerUnfinishedTransactions(conn, packet, [])
        self.checkCalledAbort(conn)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"addPacket" : None,
                     "getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleAnswerUnfinishedTransactions(conn, packet, [])
        self.checkCalledAbort(conn)
        # do nothing
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_tid_set), 0)
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        verification.handleAnswerUnfinishedTransactions(conn, packet, [new_tid])
        self.assertEquals(len(self.app.unfinished_tid_set), 0)        
        # update dict
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        verification.handleAnswerTransactionInformation(conn, packet, None, None, None, None, None)
        self.checkCalledAbort(conn)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"addPacket" : None,
                     "getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleAnswerTransactionInformation(conn, packet, None, None, None, None, None)
        self.checkCalledAbort(conn)
        # do nothing, as unfinished_oid_set is None
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = set()
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        # do work
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        self.assertEquals(len(self.app.unfinished_oid_set), 0)
        verification.handleAnswerTransactionInformation(conn, packet, new_tid,
                                                        "user", "desc", "ext", [new_oid,])
        self.assertEquals(len(self.app.unfinished_oid_set), 1)
        self.assertTrue(new_oid in self.app.unfinished_oid_set)
        # do not work as oid is diff
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        verification.handleTidNotFound(conn, packet, [])
        self.checkCalledAbort(conn)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"addPacket" : None,
                     "getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleTidNotFound(conn, packet, [])
        self.checkCalledAbort(conn)
        # do nothinf as asking_uuid_dict is True
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.unfinished_oid_set  = []
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleTidNotFound(conn, packet, "msg")
        self.assertNotEqual(self.app.unfinished_oid_set, None)
        # do work as asking_uuid_dict is False
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        verification.handleAnswerObjectPresent(conn, packet, None, None)
        self.checkCalledAbort(conn)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"addPacket" : None,
                     "getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleAnswerObjectPresent(conn, packet, None, None)
        self.checkCalledAbort(conn)
        # do nothing as asking_uuid_dict is True
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        oid = unpack('!Q', self.app.loid)[0]
        new_oid = pack('!Q', oid + 1)
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.assertTrue(self.app.asking_uuid_dict.has_key(uuid))
        verification.handleAnswerObjectPresent(conn, packet, new_oid, new_tid)
        # do work
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
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
        conn = Mock({"addPacket" : None,
                     "getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        verification.handleOidNotFound(conn, packet, [])
        self.checkCalledAbort(conn)
        # reject master node
        master_uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"addPacket" : None,
                     "getUUID" : master_uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        verification.handleOidNotFound(conn, packet, [])
        self.checkCalledAbort(conn)
        # do nothinf as asking_uuid_dict is True
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 0)
        self.app.asking_uuid_dict[uuid]  = True
        self.app.object_present = True
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertTrue(self.app.object_present)
        # do work as asking_uuid_dict is False
        conn = Mock({"addPacket" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(len(self.app.asking_uuid_dict), 1)
        self.app.asking_uuid_dict[uuid]  = False
        self.assertFalse(self.app.asking_uuid_dict[uuid ])
        self.assertTrue(self.app.object_present)
        verification.handleOidNotFound(conn, packet, "msg")
        self.assertFalse(self.app.object_present)
        self.assertTrue(self.app.asking_uuid_dict[uuid ])
    
if __name__ == '__main__':
    unittest.main()

