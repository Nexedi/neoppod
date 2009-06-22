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
import neo.master
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.service import ServiceEventHandler
from neo.master.app import Application
from neo.protocol import ERROR, PING, PONG, ANNOUNCE_PRIMARY_MASTER, \
     REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION,  \
     ASK_LAST_IDS, ANSWER_LAST_IDS, NOTIFY_PARTITION_CHANGES, \
     ASK_UNFINISHED_TRANSACTIONS, ASK_NEW_TID, FINISH_TRANSACTION, \
     NOTIFY_INFORMATION_LOCKED, ASK_NEW_OIDS, ABORT_TRANSACTION, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import OperationFailure, ElectionFailure     
from neo.node import MasterNode, StorageNode

class MasterServiceTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config = self.getConfigFile(master_number=1, replicas=1)
        self.app = Application(config, "master1")
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
        self.master_address = ('127.0.0.1', self.master_port)
        self.client_address = ('127.0.0.1', self.client_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

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
        self.service.handleRequestNodeIdentification(conn, packet, *args)
        self.checkAcceptNodeIdentification(conn, answered_packet=packet)
        return uuid

    # Tests
    def test_01_handleRequestNodeIdentification(self):
        service = self.service
        uuid = self.getNewUUID()
        args = (STORAGE_NODE_TYPE, uuid, '127.0.0.1', self.storage_port, 'INVALID_NAME')
        packet = protocol.requestNodeIdentification(*args)
        # test alien cluster
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        self.checkProtocolErrorRaised(service.handleRequestNodeIdentification, conn, packet, *args)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 0)
        self.assertEquals(self.app.lptid, ptid)
        
        # test connection of a storage node
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkAcceptNodeIdentification(conn, answered_packet=packet)
        self.assertEquals(len(self.app.nm.getStorageNodeList()), 1)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        self.failUnless(self.app.lptid > ptid)

        # send message again for the same storage node, MN must recognize it
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.1',
                                                port=self.storage_port,
                                                name=self.app.name,)
        self.checkAcceptNodeIdentification(conn, answered_packet=packet)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # No change of partition table
        self.assertEquals(self.app.lptid, ptid)
        
        # send message again for the same storage node but different uuid
        # must be rejected as SN is considered as running
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        new_uuid = self.getNewUUID()

        self.checkProtocolErrorRaised(
                service.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=STORAGE_NODE_TYPE,
                uuid=new_uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name=self.app.name,)
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getServer(), ('127.0.0.1', self.storage_port))
        self.assertEquals(sn.getUUID(), uuid)
        self.assertEquals(sn.getState(), RUNNING_STATE)
        # No change of partition table
        self.assertEquals(self.app.lptid, ptid)

        # same test, but set SN as not running before
        # this new node must replaced the old one
        conn = self.getFakeConnection()
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
        self.checkAcceptNodeIdentification(conn, answered_packet=packet)
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
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        service.handleRequestNodeIdentification(conn,
                                                packet=packet,
                                                node_type=STORAGE_NODE_TYPE,
                                                uuid=uuid,
                                                ip_address='127.0.0.2',
                                                port=10022,
                                                name=self.app.name,)
        self.checkAcceptNodeIdentification(conn, answered_packet=packet)
        call = conn.mockGetNamedCalls('answer')[0]
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
        conn = self.getFakeConnection()
        ptid = self.app.lptid
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), RUNNING_STATE)
        sn.setState(BROKEN_STATE)
        self.assertEquals(sn.getState(), BROKEN_STATE)
        
        self.checkBrokenNodeDisallowedErrorRaised(
                service.handleRequestNodeIdentification,
                conn,
                packet=packet,
                node_type=STORAGE_NODE_TYPE,
                uuid=uuid,
                ip_address='127.0.0.1',
                port=self.storage_port,
                name=self.app.name,)
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
        packet = protocol.askPrimaryMaster()
        # test answer to a storage node
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.handleAskPrimaryMaster(conn, packet)
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn, answered_packet=packet)
        self.checkNotifyNodeInformation(conn, packet_number=0)
        self.checkSendPartitionTable(conn, packet_number=1)
        self.checkSendPartitionTable(conn, packet_number=2)
        self.checkStartOperation(conn, packet_number=3)

        # Same but identify as a client node, must not get start operation message
        uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=11021)
        packet = protocol.askPrimaryMaster()
        conn = self.getFakeConnection(uuid, ("127.0.0.1", 11021))
        service.handleAskPrimaryMaster(conn, packet)
        self.checkNotAborted(conn)
        self.checkAnswerPrimaryMaster(conn, answered_packet=packet)
        self.checkNotifyNodeInformation(conn, packet_number=0)
        self.checkSendPartitionTable(conn, packet_number=1)
        self.checkSendPartitionTable(conn, packet_number=2)

    def test_03_handleAnnouncePrimaryMaster(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANNOUNCE_PRIMARY_MASTER)
        conn = self.getFakeConnection(uuid, self.storage_address)
        # must do a relection
        self.assertRaises(ElectionFailure, service.handleAnnouncePrimaryMaster, conn, packet)
        # if no uuid in conn, no reelection done
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(service.handleAnnouncePrimaryMaster, conn, packet)


    def test_04_handleReelectPrimaryMaster(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=REELECT_PRIMARY_MASTER)
        conn = self.getFakeConnection(uuid, self.storage_address)
        # must do a relection
        self.assertRaises(ElectionFailure, service.handleReelectPrimaryMaster, conn, packet)


    def test_05_handleNotifyNodeInformation(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleNotifyNodeInformation, conn, packet, node_list)
        # tell the master node that is not running any longer, it must raises
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, self.app.uuid, DOWN_STATE),]
        self.assertRaises(RuntimeError, service.handleNotifyNodeInformation, conn, packet, node_list)
        # tell the master node that it's running, nothing change
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(MASTER_NODE_TYPE, '127.0.0.1', self.master_port, self.app.uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a client node, don't care
        new_uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(CLIENT_NODE_TYPE, '127.0.0.1', self.client_port, new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about an unknown node, don't care
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', 11010, new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a known node but with bad address, don't care
        self.app.nm.add(StorageNode(("127.0.0.1", 11011), self.getNewUUID()))
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', 11012, uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is running, as PMN already know it, nothing is done
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is temp down, must be taken into account
        ptid = self.app.lptid
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, TEMPORARILY_DOWN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), TEMPORARILY_DOWN_STATE)
        self.assertEquals(ptid, self.app.lptid)
        # notify node is broken, must be taken into account and partition must changed
        conn = self.getFakeConnection(uuid, self.storage_address)
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
        packet = Packet(msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.lptid
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # do not care if client node call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.lptid)
        # send information which are later to what PMN knows, this must raise
        conn = self.getFakeConnection(uuid, self.storage_address)
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
        packet = Packet(msg_type=ASK_NEW_TID)
        ltid = self.app.ltid
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleAskNewTID, conn, packet)
        self.assertEquals(ltid, self.app.ltid)
        # do not care if storage node call it
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleAskNewTID, conn, packet)
        self.assertEquals(ltid, self.app.ltid)
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskNewTID(conn, packet)
        self.failUnless(ltid < self.app.ltid)
        self.assertEquals(len(self.app.finishing_transaction_dict), 1)
        tid = self.app.finishing_transaction_dict.keys()[0]
        self.assertEquals(tid, self.app.ltid)
        

    def test_08_handleAskNewOIDs(self):        
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_NEW_OIDS)
        loid = self.app.loid
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleAskNewOIDs, conn, packet, 1)
        self.assertEquals(loid, self.app.loid)
        # do not care if storage node call it
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleAskNewOIDs, conn, packet, 1)
        self.assertEquals(loid, self.app.loid)
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskNewOIDs(conn, packet, 1)
        self.failUnless(loid < self.app.loid)

    def test_09_handleFinishTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=FINISH_TRANSACTION)
        packet.setId(9)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleFinishTransaction, conn, packet, [], None, )
        # do not care if storage node call it
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleFinishTransaction, conn, packet, [], None)
        # give an older tid than the PMN known, must abort
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        oid_list = []
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.checkUnexpectedPacketRaised(service.handleFinishTransaction, conn, packet, oid_list, new_tid)
        old_node = self.app.nm.getNodeByUUID(uuid)
        self.app.nm.remove(old_node)
        self.app.pt.dropNode(old_node)

        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        storage_uuid = self.identifyToMasterNode()
        storage_conn = self.getFakeConnection(storage_uuid, self.storage_address)
        self.assertNotEquals(uuid, client_uuid)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskNewTID(conn, packet)
        oid_list = []
        tid = self.app.ltid
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn]})
        service.handleFinishTransaction(conn, packet, oid_list, tid)
        self.checkLockInformation(storage_conn)
        self.assertEquals(len(self.app.finishing_transaction_dict), 1)
        apptid = self.app.finishing_transaction_dict.keys()[0]
        self.assertEquals(tid, apptid)
        txn = self.app.finishing_transaction_dict.values()[0]
        self.assertEquals(len(txn.getOIDList()), 0)
        self.assertEquals(len(txn.getUUIDSet()), 1)
        self.assertEquals(txn.getMessageId(), 9)

    def test_10_handleNotifyInformationLocked(self):
        service = self.service
        uuid = self.identifyToMasterNode(port=10020)
        packet = Packet(msg_type=NOTIFY_INFORMATION_LOCKED)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleNotifyInformationLocked, conn, packet, None, )
        # do not care if client node call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=11021)
        client = self.getFakeConnection(client_uuid, ('127.0.0.1', 11020))
        self.checkUnexpectedPacketRaised(service.handleNotifyInformationLocked, conn, packet, None)
        # give an older tid than the PMN known, must abort
        conn = self.getFakeConnection(uuid, self.storage_address)
        oid_list = []
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.checkUnexpectedPacketRaised(service.handleNotifyInformationLocked, conn, packet, new_tid)
        old_node = self.app.nm.getNodeByUUID(uuid)
        # job done through dispatch -> peerBroken
        self.app.nm.remove(old_node)
        self.app.pt.dropNode(old_node)

        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        storage_uuid_1 = self.identifyToMasterNode()
        storage_uuid_2 = self.identifyToMasterNode(port=10022)
        storage_conn_1 = self.getFakeConnection(storage_uuid_1, ("127.0.0.1", self.storage_port))
        storage_conn_2 = self.getFakeConnection(storage_uuid_2, ("127.0.0.1", 10022))
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskNewTID(conn, packet)
        # clean mock object
        conn.mockCalledMethods = {}
        conn.mockAllCalledMethods = []
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn_1, storage_conn_2]})
        oid_list = []
        tid = self.app.ltid
        service.handleFinishTransaction(conn, packet, oid_list, tid)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())
        service.handleNotifyInformationLocked(storage_conn_1, packet, tid)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())
        service.handleNotifyInformationLocked(storage_conn_2, packet, tid)
        self.checkNotifyTransactionFinished(conn)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        

    def test_11_handleAbortTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ABORT_TRANSACTION)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        node_list = []
        self.checkIdenficationRequired(service.handleAbortTransaction, conn, packet, None, )
        # do not answer if not a client
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleAbortTransaction, conn, packet, None)
        # give a bad tid, must not failed, just ignored it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)
        service.handleAbortTransaction(conn, packet, None)
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)        
        # give a known tid
        conn = self.getFakeConnection(client_uuid, self.client_address)
        tid = self.app.ltid
        self.app.finishing_transaction_dict[tid] = None
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 1)
        service.handleAbortTransaction(conn, packet, tid)
        self.assertEqual(len(self.app.finishing_transaction_dict.keys()), 0)


    def test_12_handleAskLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_LAST_IDS)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(service.handleAskLastIDs, conn, packet )
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        ptid = self.app.lptid
        tid = self.app.ltid
        oid = self.app.loid
        service.handleAskLastIDs(conn, packet)
        packet = self.checkAnswerLastIDs(conn, answered_packet=packet)
        loid, ltid, lptid = protocol._decodeAnswerLastIDs(packet._body)
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)
        

    def test_13_handleAskUnfinishedTransactions(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_UNFINISHED_TRANSACTIONS)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(service.handleAskUnfinishedTransactions, 
                conn, packet)
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.handleAskUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        tid_list = protocol._decodeAnswerUnfinishedTransactions(packet._body)[0]
        self.assertEqual(len(tid_list), 0)
        # create some transaction
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        service.handleAskNewTID(conn, packet)
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.handleAskUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        tid_list = protocol._decodeAnswerUnfinishedTransactions(packet._body)[0]
        self.assertEqual(len(tid_list), 3)
        

    def test_14_handleNotifyPartitionChanges(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=NOTIFY_PARTITION_CHANGES)
        # do not answer if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.checkIdenficationRequired(service.handleNotifyPartitionChanges,
                conn, packet, None, None)
        # do not answer if not a storage node
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.checkUnexpectedPacketRaised(service.handleNotifyPartitionChanges, 
                conn, packet, None, None)

        # send a bad state, must not be take into account
        conn = self.getFakeConnection(uuid, self.storage_address)
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
        conn = self.getFakeConnection(uuid, self.storage_address)
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
        conn = self.getFakeConnection(uuid, self.storage_address)
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
        conn = self.getFakeConnection(None, self.storage_address)
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
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.peerBroken, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.lptid
        packet = Packet(msg_type=ASK_NEW_TID)
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
        conn = self.getFakeConnection(None, self.storage_address)
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
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.timeoutExpired, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.lptid
        packet = Packet(msg_type=ASK_NEW_TID)
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
        conn = self.getFakeConnection(None, self.storage_address)
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
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.lptid
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.connectionClosed, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.lptid)
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.lptid
        packet = Packet(msg_type=ASK_NEW_TID)
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

