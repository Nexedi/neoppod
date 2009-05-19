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
from neo.master.service import ServiceEventHandler
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

class MasterServiceTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config_file_text = """# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: 127.0.0.1:10010
# The number of replicas.
replicas: 1
# The number of partitions.
partitions: 1009
# The name of this cluster.
name: main
# The user name for the database.
user: neo
# The password for the database.
password: neo

connector: SocketConnector

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
        self.app.lptid = pack('!Q', 1)
        self.app.em = Mock({"getConnectionList" : []})
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.service = ServiceEventHandler(self.app)
        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10010
        
    def tearDown(self):
        # Delete tmp file
        os.remove(self.tmp_path)

    # Method to test the kind of packet returned in answer
    def checkCalledAbort(self, conn, packet_number=0):
        """Check the abort method has been called and an error packet has been sent"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1) # XXX required here ????
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 0)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ERROR)

    def checkCalledAcceptNodeIdentification(self, conn, packet_number=0):
        """ Check Accept Node Identification has been send"""
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.assertEquals(len(conn.mockGetNamedCalls("expectMessage")), 1)
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ACCEPT_NODE_IDENTIFICATION)

    def checkCalledNotifyNodeInformation(self, conn, packet_number=0):
        """ Check Notify Node Information message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), NOTIFY_NODE_INFORMATION)

    def checkCalledAnswerPrimaryMaster(self, conn, packet_number=0):
        """ Check Answer primaty master message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_PRIMARY_MASTER)

    def checkCalledSendPartitionTable(self, conn, packet_number=0):
        """ Check partition table has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), SEND_PARTITION_TABLE)

    def checkCalledStartOperation(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), START_OPERATION)

    def checkCalledLockInformation(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), LOCK_INFORMATION)

    def checkCalledUnlockInformation(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), UNLOCK_INFORMATION)

    def checkCalledNotifyTransactionFinished(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), NOTIFY_TRANSACTION_FINISHED)

    def checkCalledAnswerLastIDs(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_LAST_IDS)
        return packet._decodeAnswerLastIDs()

    def checkCalledAnswerUnfinishedTransactions(self, conn, packet_number=0):
        """ Check start operation message has been send"""
        call = conn.mockGetNamedCalls("addPacket")[packet_number]
        packet = call.getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), ANSWER_UNFINISHED_TRANSACTIONS)
        return packet._decodeAnswerUnfinishedTransactions()

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
        packet = protocol.requestNodeIdentification(1, *args)
        # test alien cluster
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        self.service.handleRequestNodeIdentification(conn, packet, *args)
        self.checkCalledAcceptNodeIdentification(conn)
        return uuid

    # Tests
    def test_01_handleRequestNodeIdentification(self):
        service = self.service
        uuid = self.getNewUUID()
        args = (STORAGE_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, 'INVALID_NAME')
        packet = protocol.requestNodeIdentification(1, *args)
        # test alien cluster
        conn = Mock({"addPacket" : None, "abort" : None})
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn, packet, *args)
        self.checkCalledAbort(conn)        
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 0)
        self.assertEquals(self.app.lptid, ptid)
        
        # test connection of a storage node
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkCalledAcceptNodeIdentification(conn)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 1)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        self.failUnless(self.app.lptid > ptid)

        # send message again for the same storage node, MN must recognize it
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkCalledAcceptNodeIdentification(conn)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # No change of partition table
        self.assertEquals(self.app.lptid, ptid)
        
        # send message again for the same storage node but different uuid
        # must be rejected as SN is considered as running
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        new_uuid = self.getNewUUID()

        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=new_uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkCalledAbort(conn)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # No change of partition table
        self.assertEquals(self.app.lptid, ptid)

        # same test, but set SN as not running before
        # this new node must replaced the old one
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), RUNNING_STATE)
        sn.setState(TEMPORARILY_DOWN_STATE)
        self.assertEquals(sn.getState(), TEMPORARILY_DOWN_STATE)
        
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=new_uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkCalledAcceptNodeIdentification(conn)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 1)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), new_uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # Partition table changed
        self.failUnless(self.app.lptid > ptid)        
        uuid = new_uuid
        
        # send message again for the same storage node but different address
        # A new UUID should be send and the node is added to the storage node list
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.2',
                                                port=10022,
                                                name=self.app.name,)
        self.checkCalledAcceptNodeIdentification(conn)
        call = conn.mockGetNamedCalls('addPacket')[0]
        new_uuid = call.getParam(0)._body[-16:]
        self.assertNotEquals(uuid, new_uuid)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 2)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        sn = self.app.nm.getStorageNodeList()[1]
        self.assertEquals(sn.getServer(), ('127.0.0.2', 10022))
        self.assertEquals(sn.getUUID(), new_uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # Partition table changed
        self.failUnless(self.app.lptid > ptid)        

        # mark the node as broken and request identification, this must be forbidden
        conn = Mock({"addPacket" : None, "abort" : None, "expectMessage" : None})
        ptid = self.app.lptid
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), RUNNING_STATE)
        sn.setState(BROKEN_STATE)
        self.assertEquals(sn.getState(), BROKEN_STATE)
        
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkCalledAbort(conn)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 2)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), BROKEN_STATE)
        # No change of partition table
        self.assertEqual(self.app.lptid, ptid)


    def test_02_handleAskPrimaryMaster(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = protocol.askPrimaryMaster(msg_id=2)
        # test answer to a storage node
        conn = Mock({"addPacket" : None,
                     "answerPrimaryMaster" : None,
                     "notifyNodeInformation" : None,
                     "sendPartitionTable" : None,
                     "getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAskPrimaryMaster(conn, packet)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 5)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.checkCalledAnswerPrimaryMaster(conn, 0)
        self.checkCalledNotifyNodeInformation(conn, 1)
        self.checkCalledSendPartitionTable(conn, 2)
        self.checkCalledSendPartitionTable(conn, 3)
        self.checkCalledStartOperation(conn, 4)

        # Same but identify as a client node, must not get start operation message
        uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=11021)
        packet = protocol.askPrimaryMaster(msg_id=2)
        conn = Mock({"addPacket" : None, "abort" : None, "answerPrimaryMaster" : None,
                     "notifyNodeInformation" : None, "sendPartitionTable" : None,
                     "getUUID" : uuid, "getAddress" : ("127.0.0.1", 11021)})
        service.handleAskPrimaryMaster(conn, packet)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 4)
        self.assertEquals(len(conn.mockGetNamedCalls("abort")), 0)
        self.checkCalledAnswerPrimaryMaster(conn, 0)
        self.checkCalledNotifyNodeInformation(conn, 1)
        self.checkCalledSendPartitionTable(conn, 2)
        self.checkCalledSendPartitionTable(conn, 3)

    def test_03_handleAnnouncePrimaryMaster(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=3, msg_type=ANNOUNCE_PRIMARY_MASTER)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        # must do a relection
        self.assertRaises(ElectionFailure, service.handleAnnouncePrimaryMaster, conn, packet)
        # if no uuid in conn, no reelection done
        conn = Mock({"getUUID" : None,
                    "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAnnouncePrimaryMaster(conn, packet)
        self.checkCalledAbort(conn)


    def test_04_handleReelectPrimaryMaster(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=4, msg_type=REELECT_PRIMARY_MASTER)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        # must do a relection
        self.assertRaises(ElectionFailure, service.handleReelectPrimaryMaster, conn, packet)


    def test_05_handleNotifyNodeInformation(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=5, msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleNotifyNodeInformation(conn, packet, node_list)
        self.checkCalledAbort(conn)
        # tell the master node that is not running any longer, it must raises
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, self.app.uuid, DOWN_STATE),]
        self.assertRaises(RuntimeError, service.handleNotifyNodeInformation, conn, packet, node_list)
        # tell the master node that it's running, nothing change
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, self.app.uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a client node, don't care
        new_uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.client_port, new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about an unknown node, don't care
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', 11010, new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a known node but with bad address, don't care
        self.app.nm.add(StorageNode(("127.0.0.1", 11011), self.getNewUUID()))
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', 11012, uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is running, as PMN already know it, nothing is done
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is temp down, must be taken into account
        ptid = self.app.lptid
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, TEMPORARILY_DOWN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), TEMPORARILY_DOWN_STATE)
        self.assertEquals(ptid, self.app.lptid)
        # notify node is broken, must be taken into account and partition must changed
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})                
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), BROKEN_STATE)
        self.failUnless(ptid < self.app.lptid)

    def test_06_handleAnswerLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=5, msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.lptid
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleAnswerLastIDs(conn, packet, None, None, None)
        self.checkCalledAbort(conn)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # do not care if client node call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        node_list = []
        service.handleAnswerLastIDs(conn, packet, None, None, None)
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
        self.failUnless(new_ptid > self.app.lptid)
        self.assertRaises(OperationFailure, service.handleAnswerLastIDs, conn, packet, None, None, new_ptid)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        

    def test_07_handleAskNewTID(self):        
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=5, msg_type=ASK_NEW_TID)
        ltid = self.app.ltid
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleAskNewTID(conn, packet)
        self.checkCalledAbort(conn)
        self.assertEquals(ltid, self.app.ltid)
        # do not care if storage node call it
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        self.assertRaises(OperationFailure, service.handleAskNewTID, conn, packet)
        self.checkCalledAbort(conn)
        self.assertEquals(ltid, self.app.ltid)
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        service.handleAskNewTID(conn, packet)
        self.failUnless(ltid < self.app.ltid)
        self.assertEquals(len(self.app.finishing_transaction_dict), 1)
        tid = self.app.finishing_transaction_dict.keys()[0]
        self.assertEquals(tid, self.app.ltid)
        

    def test_08_handleAskNewOIDs(self):        
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=5, msg_type=ASK_NEW_OIDS)
        loid = self.app.loid
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleAskNewOIDs(conn, packet, 1)
        self.checkCalledAbort(conn)
        self.assertEquals(loid, self.app.loid)
        # do not care if storage node call it
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        self.assertRaises(OperationFailure, service.handleAskNewOIDs, conn, packet, 1)
        self.checkCalledAbort(conn)
        self.assertEquals(loid, self.app.loid)
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        service.handleAskNewOIDs(conn, packet, 1)
        self.failUnless(loid < self.app.loid)

    def test_09_handleFinishTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=9, msg_type=FINISH_TRANSACTION)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleFinishTransaction(conn, packet, [], None, )
        self.checkCalledAbort(conn)
        # do not care if storage node call it
        storage_conn = conn = Mock({"getUUID" : uuid,
                                    "getAddress" : ("127.0.0.1", self.storage_port),
                                    "getSockect" : uuid})
        node_list = []
        self.assertRaises(OperationFailure, service.handleFinishTransaction, conn, packet, [], None)
        self.checkCalledAbort(conn)
        # give an older tid than the PMN known, must abort
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        oid_list = []
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        service.handleFinishTransaction(conn, packet, oid_list, new_tid)
        self.checkCalledAbort(conn)
        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        storage_uuid = self.identifyToMasterNode()
        storage_conn = Mock({"getUUID" : storage_uuid,
                             "getAddress" : ("127.0.0.1", self.storage_port),
                             "getSockect" : 50,
                             "getDescriptor" : 50})        
        self.assertNotEquals(uuid, client_uuid)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        service.handleAskNewTID(conn, packet)
        oid_list = []
        tid = self.app.ltid
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn]})
        service.handleFinishTransaction(conn, packet, oid_list, tid)
        self.checkCalledLockInformation(storage_conn)
        self.assertEquals(len(storage_conn.mockGetNamedCalls("expectMessage")), 1)
        self.assertEquals(len(self.app.finishing_transaction_dict), 1)
        apptid = self.app.finishing_transaction_dict.keys()[0]
        self.assertEquals(tid, apptid)
        txn = self.app.finishing_transaction_dict.values()[0]
        self.assertEquals(len(txn.getOIDList()), 0)
        self.assertEquals(len(txn.getUUIDSet()), 1)
        self.assertEquals(txn.getMessageId(), 9)

    def test_10_handleNotifyInformationLocked(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=10, msg_type=NOTIFY_INFORMATION_LOCKED)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleNotifyInformationLocked(conn, packet, None, )
        self.checkCalledAbort(conn)
        # do not care if client node call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=11021)
        client = conn = Mock({"getUUID" : client_uuid,
                              "getAddress" : ("127.0.0.1", 11021),
                              "getSockect" : client_uuid})
        service.handleNotifyInformationLocked(conn, packet, None)
        self.checkCalledAbort(conn)
        # give an older tid than the PMN known, must abort
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        oid_list = []
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.assertRaises(OperationFailure, service.handleNotifyInformationLocked, conn, packet, new_tid)
        self.checkCalledAbort(conn)

        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        storage_uuid_1 = self.identifyToMasterNode()
        storage_uuid_2 = self.identifyToMasterNode(port=10022)
        storage_conn_1 = Mock({"getUUID" : storage_uuid_1,
                             "getAddress" : ("127.0.0.1", self.storage_port),
                             "getSockect" : 1,
                             "getDescriptor" : 1})        
        storage_conn_2 = Mock({"getUUID" : storage_uuid_2,
                               "getAddress" : ("127.0.0.1", 10022),
                               "getSockect" : 2,
                               "getDescriptor" : 2})        
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "getSockect" : 3,
                     "getDescriptor" : 3})
        service.handleAskNewTID(conn, packet)
        # clean mock object
        conn.mockCalledMethods = {}
        conn.mockAllCalledMethods = []
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn_1, storage_conn_2]})
        oid_list = []
        tid = self.app.ltid
        service.handleFinishTransaction(conn, packet, oid_list, tid)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)
        self.checkCalledLockInformation(storage_conn_1)
        self.checkCalledLockInformation(storage_conn_2)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())
        self.assertEquals(len(storage_conn_1.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(storage_conn_2.mockGetNamedCalls("addPacket")), 1)
        
        service.handleNotifyInformationLocked(storage_conn_1, packet, tid)
        self.assertEquals(len(storage_conn_1.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(storage_conn_2.mockGetNamedCalls("addPacket")), 1)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 0)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())

        print "notify"
        service.handleNotifyInformationLocked(storage_conn_2, packet, tid)
        self.checkCalledNotifyTransactionFinished(conn)
        self.assertEquals(len(storage_conn_1.mockGetNamedCalls("addPacket")), 2)
        self.assertEquals(len(storage_conn_2.mockGetNamedCalls("addPacket")), 2)
        self.assertEquals(len(conn.mockGetNamedCalls("addPacket")), 1)
        self.checkCalledLockInformation(storage_conn_1)
        self.checkCalledLockInformation(storage_conn_2)
        

    def test_11_handleAbortTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=11, msg_type=ABORT_TRANSACTION)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        service.handleAbortTransaction(conn, packet, None, )
        self.checkCalledAbort(conn)
        # do not answer if not a client
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        node_list = []
        self.assertRaises(OperationFailure, service.handleAbortTransaction, conn, packet, None)
        self.checkCalledAbort(conn)
        # give a bad tid, must not failed, just ignored it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})

        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)
        service.handleAbortTransaction(conn, packet, None)
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)        
        # give a known tid
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        tid = self.app.ltid
        self.app.finishing_transaction_dict[tid] = None
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 1)
        service.handleAbortTransaction(conn, packet, tid)
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)


    def test_12_handleAskLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=11, msg_type=ASK_LAST_IDS)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAskLastIDs(conn, packet )
        self.checkCalledAbort(conn)
        # give a uuid
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        ptid = self.app.lptid
        tid = self.app.ltid
        oid = self.app.loid
        service.handleAskLastIDs(conn, packet)
        loid, ltid, lptid = self.checkCalledAnswerLastIDs(conn)
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)
        

    def test_13_handleAskUnfinishedTransactions(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=11, msg_type=ASK_UNFINISHED_TRANSACTIONS)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAskUnfinishedTransactions(conn, packet)
        self.checkCalledAbort(conn)
        # give a uuid
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAskUnfinishedTransactions(conn, packet)
        tid_list = self.checkCalledAnswerUnfinishedTransactions(conn)[0]
        self.assertEqual(len(tid_list), 0)
        # create some transaction
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})        
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleAskUnfinishedTransactions(conn, packet)
        tid_list = self.checkCalledAnswerUnfinishedTransactions(conn)[0]
        self.assertEqual(len(tid_list), 3)
        

    def test_14_handleNotifyPartitionChanges(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_id=11, msg_type=NOTIFY_PARTITION_CHANGES)
        # do not answer if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        service.handleNotifyPartitionChanges(conn, packet, None, None)
        self.checkCalledAbort(conn)
        # do not answer if not a storage node
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port=self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        service.handleNotifyPartitionChanges(conn, packet, None, None)

        # send a bad state, must not be take into account
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        storage_uuid = self.identifyToMasterNode(port=self.storage_port+1)
        offset = 1
        cell_list = [(offset, uuid, FEEDING_STATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        service.handleNotifyPartitionChanges(conn, packet, self.app.lptid, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)

        # send for another node, must not be take into account
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        offset = 1
        cell_list = [(offset, storage_uuid, UP_TO_DATE_STATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        service.handleNotifyPartitionChanges(conn, packet, self.app.lptid, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)

        # send for itself, must be taken into account
        # and the feeding node must be removed
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        cell_list = [(offset, uuid, UP_TO_DATE_STATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, OUT_OF_DATE_STATE)
        # mark the second storage node as feeding and say we are up to date
        # second node must go to discarded state and first one to up to date state
        self.app.pt.setCell(offset, self.app.nm.getNodeByUUID(storage_uuid), FEEDING_STATE)
        cell_list = [(offset, uuid, UP_TO_DATE_STATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            if cell == storage_uuid:
                self.assertEquals(state, FEEDING_STATE)
            else:
                self.assertEquals(state, OUT_OF_DATE_STATE)        
        lptid = self.app.lptid
        service.handleNotifyPartitionChanges(conn, packet, self.app.lptid, cell_list)
        self.failUnless(lptid < self.app.lptid)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            if cell == uuid:
                self.assertEquals(state, UP_TO_DATE_STATE)
            else:
                self.assertEquals(state, DISCARDED_STATE)
        

    def test_15_peerBroken(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        # add a second storage node and then declare it as broken
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = Mock({"getUUID" : storage_uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port+1)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.peerBroken, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        lptid = self.app.lptid
        packet = Packet(msg_id=15, msg_type=ASK_NEW_TID)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.peerBroken(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.lptid)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)
        

    def test_16_timeoutExpired(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = Mock({"getUUID" : storage_uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port+1)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.timeoutExpired, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        lptid = self.app.lptid
        packet = Packet(msg_id=15, msg_type=ASK_NEW_TID)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.timeoutExpired(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.lptid)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)


    def test_17_connectionClosed(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = Mock({"getUUID" : None,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE)
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = Mock({"getUUID" : storage_uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port+1)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.storage_port)})
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.connectionClosed, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = Mock({"getUUID" : client_uuid,
                     "getAddress" : ("127.0.0.1", self.client_port)})
        lptid = self.app.lptid
        packet = Packet(msg_id=15, msg_type=ASK_NEW_TID)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.connectionClosed(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.lptid)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)




if __name__ == '__main__':
    unittest.main()

