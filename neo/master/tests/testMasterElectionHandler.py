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
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.election import ElectionEventHandler
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

# patch connection so that we can register _addPacket messages
# in mock object
def _addPacket(self, packet):
    if self.connector is not None:
        self.connector._addPacket(packet)
def expectMessage(self, packet):
    if self.connector is not None:
        self.connector.expectMessage(packet)

ClientConnection._addPacket = _addPacket
ClientConnection.expectMessage = expectMessage


class MasterElectionTests(unittest.TestCase):

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
        self.app.em = Mock({"getConnectionList" : []})
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.election = ElectionEventHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
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
        conn = Mock({"_addPacket" : None, "abort" : None, "expectMessage" : None,
                     "isServerConnection" : True})
        self.election.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=node_type,
                                                uuid=uuid,
                                                ip_address=ip,
                                                port=port,
                                                name=self.app.name,)
        self.checkCalledAcceptNodeIdentification(conn)
        return uuid

    # Method to test the kind of packet returned in answer
    def checkCalledRequestNodeIdentification(self, conn, packet_number=0):
        """ Check Request Node Identification has been send"""
        self.assertEquals(len(conn.mockGetNamedCalls("ask")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        call = conn.mockGetNamedCalls("ask")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), REQUEST_NODE_IDENTIFICATION)

        
    def checkCalledAskPrimaryMaster(self, conn, packet_number=0):
        """ Check ask primary master has been send"""
        call = conn.mockGetNamedCalls("_addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(),ASK_PRIMARY_MASTER)

    def checkCalledAnswerPrimaryMaster(self, conn, packet_number=0):
        """ Check Answer primaty master message has been send"""
        call = conn.mockGetNamedCalls("answer")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_PRIMARY_MASTER)

    # Tests
    def test_01_connectionStarted(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.election.connectionStarted(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        

    def test_02_connectionCompleted(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.election.connectionCompleted(conn)
        self.checkCalledRequestNodeIdentification(conn)
    

    def test_03_connectionFailed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.election.connectionStarted(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)
        self.election.connectionFailed(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_04_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)
        self.election.connectionClosed(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_05_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)
        self.election.timeoutExpired(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), TEMPORARILY_DOWN_STATE)

    def test_06_peerBroken1(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)
        self.election.peerBroken(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), DOWN_STATE)

    def test_06_peerBroken2(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        # Without a client connection
        conn = Mock({"getUUID" : uuid,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port),})
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.election.connectionStarted(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)
        self.election.peerBroken(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), BROKEN_STATE)

    def test_07_packetReceived(self):
        uuid = self.identifyToMasterNode(node_type=MASTER_NODE_TYPE, port=self.master_port)
        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE, uuid,
                       "127.0.0.1", self.master_port, 1009, 2, self.app.uuid)

        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        node = self.app.nm.getNodeByServer(conn.getAddress())
        node.setState(DOWN_STATE)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), DOWN_STATE)
        self.election.packetReceived(conn, p)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getState(), RUNNING_STATE)

    def test_08_handleAcceptNodeIdentification1(self):
        # test with storage node, must be rejected
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        args = (MASTER_NODE_TYPE, uuid, '127.0.0.1', self.master_port,
                self.app.num_partitions, self.app.num_replicas, self.app.uuid)
        p = protocol.acceptNodeIdentification(*args)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getUUID(), None)
        self.assertEqual(conn.getUUID(), None)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.election.handleAcceptNodeIdentification(conn, p, STORAGE_NODE_TYPE,
                                                     uuid, "127.0.0.1", self.master_port,
                                                     self.app.num_partitions, 
                                                     self.app.num_replicas,
                                                     self.app.uuid
                                                     )
        self.assertEqual(conn.getConnector(), None)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        
    def test_08_handleAcceptNodeIdentification2(self):
        # test with bad address, must be rejected
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        args = (MASTER_NODE_TYPE, uuid, '127.0.0.1', self.master_port,
                self.app.num_partitions, self.app.num_replicas, self.app.uuid)
        p = protocol.acceptNodeIdentification(*args)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getUUID(), None)
        self.assertEqual(conn.getUUID(), None)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.election.handleAcceptNodeIdentification(conn, p, STORAGE_NODE_TYPE,
                                                     uuid, "127.0.0.2", self.master_port,
                                                     self.app.num_partitions, 
                                                     self.app.num_replicas,
                                                     self.app.uuid)
        self.assertEqual(conn.getConnector(), None)

    def test_08_handleAcceptNodeIdentification3(self):
        # test with master node, must be ok
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        args = (MASTER_NODE_TYPE, uuid, '127.0.0.1', self.master_port,
                self.app.num_partitions, self.app.num_replicas, self.app.uuid)
        p = protocol.acceptNodeIdentification(*args)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getUUID(), None)
        self.assertEqual(conn.getUUID(), None)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)

        self.election.handleAcceptNodeIdentification(conn, p, MASTER_NODE_TYPE,
                                                     uuid, "127.0.0.1", self.master_port,
                                                     self.app.num_partitions, 
                                                     self.app.num_replicas,
                                                     self.app.uuid)
        self.assertEqual(self.app.nm.getNodeByServer(conn.getAddress()).getUUID(), uuid)
        self.assertEqual(conn.getUUID(), uuid)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),2)
        self.checkCalledAskPrimaryMaster(conn.getConnector(), 1)
        

    def test_09_handleAnswerPrimaryMaster1(self):
        # test with master node and greater uuid
        uuid = self.getNewUUID()
        while uuid < self.app.uuid:
            uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        conn.setUUID(uuid)
        p = protocol.askPrimaryMaster()
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.election.handleAnswerPrimaryMaster(conn, p, INVALID_UUID, [])
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(self.app.primary, False)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)


    def test_09_handleAnswerPrimaryMaster2(self):
        # test with master node and lesser uuid
        uuid = self.getNewUUID()
        while uuid > self.app.uuid:
            uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        conn.setUUID(uuid)
        p = protocol.askPrimaryMaster()
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.election.handleAnswerPrimaryMaster(conn, p, INVALID_UUID, [])
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(self.app.primary, None)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)


    def test_09_handleAnswerPrimaryMaster3(self):
        # test with master node and given uuid for PMN
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        conn.setUUID(uuid)
        p = protocol.askPrimaryMaster()
        self.app.nm.add(MasterNode(("127.0.0.1", self.master_port), uuid))
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.assertEqual(self.app.primary_master_node, None)
        self.election.handleAnswerPrimaryMaster(conn, p, uuid, [])
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertNotEqual(self.app.primary_master_node, None)
        self.assertEqual(self.app.primary, False)


    def test_09_handleAnswerPrimaryMaster4(self):
        # test with master node and unknown uuid for PMN
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        conn.setUUID(uuid)
        p = protocol.askPrimaryMaster()
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(self.app.primary_master_node, None)
        self.election.handleAnswerPrimaryMaster(conn, p, uuid, [])
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertEqual(self.app.primary_master_node, None)
        self.assertEqual(self.app.primary, None)


    def test_09_handleAnswerPrimaryMaster5(self):
        # test with master node and new uuid for PMN
        uuid = self.getNewUUID()
        conn = ClientConnection(self.app.em, self.election, addr = ("127.0.0.1", self.master_port),
                                connector_handler = DoNothingConnector)
        conn.setUUID(uuid)
        p = protocol.askPrimaryMaster()
        self.app.nm.add(MasterNode(("127.0.0.1", self.master_port), uuid))
        self.assertEqual(len(self.app.unconnected_master_node_set), 0)
        self.assertEqual(len(self.app.negotiating_master_node_set), 1)
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.assertEqual(self.app.primary_master_node, None)
        master_uuid = self.getNewUUID()
        self.election.handleAnswerPrimaryMaster(conn, p, master_uuid, [("127.0.0.1", self.master_port+1, master_uuid),])
        self.assertEqual(len(conn.getConnector().mockGetNamedCalls("_addPacket")),1)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 3)
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        self.assertNotEqual(self.app.primary_master_node, None)
        self.assertEqual(self.app.primary, False)
        # Now tell it's another node which is primary, it must raise 
        self.assertRaises(ElectionFailure, self.election.handleAnswerPrimaryMaster, conn, p, uuid, [])

        
    def test_10_handleRequestNodeIdentification(self):
        election = self.election
        uuid = self.getNewUUID()
        args = (MASTER_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, 'INVALID_NAME')
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = Mock({"_addPacket" : None, "abort" : None,
                     "isServerConnection" : True})
        self.checkProtocolErrorRaised(
                election.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name="INVALID_NAME",)
        # test connection of a storage node
        conn = Mock({"_addPacket" : None, "abort" : None, "expectMessage" : None,
                    "isServerConnection" : True})
        self.checkNotReadyErrorRaised(
                election.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=STORAGE_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name=self.app.name,)

        # known node
        conn = Mock({"_addPacket" : None, "abort" : None, "expectMessage" : None,
                    "isServerConnection" : True})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        node = self.app.nm.getMasterNodeList()[0]
        self.assertEqual(node.getUUID(), None)
        self.assertEqual(node.getState(), RUNNING_STATE)
        election.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port,
                                                name=self.app.name,)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.checkCalledAcceptNodeIdentification(conn)
        # unknown node
        conn = Mock({"_addPacket" : None, "abort" : None, "expectMessage" : None,
                    "isServerConnection" : True})
        new_uuid = self.getNewUUID()
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.assertEqual(len(self.app.unconnected_master_node_set), 1)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        election.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=MASTER_NODE_TYPE,
                                                uuid=new_uuid,
                                                ip_address='127.0.0.1',
                                                port=self.master_port+1,
                                                name=self.app.name,)
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 2)
        self.checkCalledAcceptNodeIdentification(conn)
        self.assertEqual(len(self.app.unconnected_master_node_set), 2)
        self.assertEqual(len(self.app.negotiating_master_node_set), 0)
        # broken node
        conn = Mock({"_addPacket" : None, "abort" : None, "expectMessage" : None,
                    "isServerConnection" : True})
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port+1))
        self.assertEqual(node.getUUID(), new_uuid)
        self.assertEqual(node.getState(), RUNNING_STATE)
        node.setState(BROKEN_STATE)
        self.assertEqual(node.getState(), BROKEN_STATE)
        self.checkBrokenNodeDisallowedErrorRaised(
                election.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=MASTER_NODE_TYPE,
                uuid=new_uuid,
                ip_address='127.0.0.1',
                port=self.master_port+1,
                name=self.app.name,)        


    def test_11_handleAskPrimaryMaster(self):
        election = self.election
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        conn = Mock({"_addPacket" : None,
                     "getUUID" : uuid,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        election.handleAskPrimaryMaster(conn, packet)        
        self.assertEquals(len(conn.mockGetNamedCalls("answer")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.checkCalledAnswerPrimaryMaster(conn, 0)

    def test_12_handleAnnouncePrimaryMaster(self):
        election = self.election
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=ANNOUNCE_PRIMARY_MASTER)
        # No uuid
        conn = Mock({"_addPacket" : None,
                     "getUUID" : None,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(len(self.app.nm.getMasterNodeList()), 1)
        self.checkIdenficationRequired(election.handleAnnouncePrimaryMaster, conn, packet)
        # announce
        conn = Mock({"_addPacket" : None,
                     "getUUID" : uuid,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertEqual(self.app.primary, None)
        self.assertEqual(self.app.primary_master_node, None)
        election.handleAnnouncePrimaryMaster(conn, packet)        
        self.assertEqual(self.app.primary, False)
        self.assertNotEqual(self.app.primary_master_node, None)
        # set current as primary, and announce another, must raise
        conn = Mock({"_addPacket" : None,
                     "getUUID" : uuid,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.app.primary = True
        self.assertEqual(self.app.primary, True)
        self.assertRaises(ElectionFailure, election.handleAnnouncePrimaryMaster, conn, packet)        
        

    def test_13_handleReelectPrimaryMaster(self):
        election = self.election
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = protocol.askPrimaryMaster()
        # No uuid
        conn = Mock({"_addPacket" : None,
                     "getUUID" : None,
                     "isServerConnection" : True,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.assertRaises(ElectionFailure, election.handleReelectPrimaryMaster, conn, packet)

    def test_14_handleNotifyNodeInformation(self):
        election = self.election
        uuid = self.identifyToMasterNode(MASTER_NODE_TYPE, port=self.master_port)
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        node_list = []
        self.checkIdenficationRequired(election.handleNotifyNodeInformation, conn, packet, node_list)
        # tell the master node about itself, must do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.app.uuid, DOWN_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        election.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        # tell about a storage node, do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)
        election.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getStorageNodeList()), 0)
        # tell about a client node, do nothing
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.master_port - 1, self.getNewUUID(), DOWN_STATE),]
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)
        election.handleNotifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getClientNodeList()), 0)
        # tell about another master node
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port + 1, self.getNewUUID(), RUNNING_STATE),]
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port+1))
        self.assertEqual(node, None)
        election.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port+1))
        self.assertNotEqual(node, None)
        self.assertEqual(node.getServer(), ("127.0.0.1", self.master_port+1))
        self.assertEqual(node.getState(), RUNNING_STATE)
        # tell that node is down
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port + 1, self.getNewUUID(), DOWN_STATE),]
        election.handleNotifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getNodeByServer(("127.0.0.1", self.master_port+1))
        self.assertNotEqual(node, None)
        self.assertEqual(node.getServer(), ("127.0.0.1", self.master_port+1))
        self.assertEqual(node.getState(), DOWN_STATE)

    
if __name__ == '__main__':
    unittest.main()

