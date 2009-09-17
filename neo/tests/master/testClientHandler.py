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
from neo import logging
from mock import Mock
from struct import pack, unpack
from neo.tests import NeoTestBase
import neo.master
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.handlers.client import ClientServiceHandler
from neo.master.app import Application
from neo.protocol import ERROR, PING, PONG, ANNOUNCE_PRIMARY_MASTER, \
     REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION,  \
     ASK_LAST_IDS, ANSWER_LAST_IDS, NOTIFY_PARTITION_CHANGES, \
     ASK_UNFINISHED_TRANSACTIONS, ASK_BEGIN_TRANSACTION, FINISH_TRANSACTION, \
     NOTIFY_INFORMATION_LOCKED, ASK_NEW_OIDS, ABORT_TRANSACTION, \
     STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, MASTER_NODE_TYPE, \
     RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
     UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.exception import OperationFailure, ElectionFailure     
from neo.node import MasterNode, StorageNode

class MasterClientHandlerTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(**config)
        self.app.pt.clear()
        self.app.pt.setID(pack('!Q', 1))
        self.app.em = Mock({"getConnectionList" : []})
        self.app.loid = '\0' * 8
        self.app.ltid = '\0' * 8
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.service = ClientServiceHandler(self.app)
        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.master_port = 10010
        self.master_address = ('127.0.0.1', self.master_port)
        self.client_address = ('127.0.0.1', self.client_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        # register the storage
        kw = {'uuid':self.getNewUUID(), 'server': self.master_address}
        self.app.nm.add(StorageNode(**kw))
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    def getLastUUID(self):
        return self.uuid

    def identifyToMasterNode(self, node_type=STORAGE_NODE_TYPE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN """
        # register the master itself
        uuid = self.getNewUUID()
        self.app.nm.add(MasterNode(uuid, (ip, port)))
        return uuid

    # Tests
    def test_05_handleNotifyNodeInformation(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
        # tell the master node that is not running any longer, it must raises
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(MASTER_NODE_TYPE, ('127.0.0.1', self.master_port), self.app.uuid, DOWN_STATE),]
        self.assertRaises(RuntimeError, service.handleNotifyNodeInformation, conn, packet, node_list)
        # tell the master node that it's running, nothing change
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(MASTER_NODE_TYPE, ('127.0.0.1', self.master_port), self.app.uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a client node, don't care
        new_uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(CLIENT_NODE_TYPE, ('127.0.0.1', self.client_port), new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about an unknown node, don't care
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, ('127.0.0.1', 11010), new_uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a known node but with bad address, don't care
        self.app.nm.add(StorageNode(("127.0.0.1", 11011), self.getNewUUID()))
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, ('127.0.0.1', 11012), uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is running, as PMN already know it, nothing is done
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, ('127.0.0.1', self.storage_port), uuid, RUNNING_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is temp down, must be taken into account
        ptid = self.app.pt.getID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, ('127.0.0.1', self.storage_port), uuid, TEMPORARILY_DOWN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), TEMPORARILY_DOWN_STATE)
        self.assertEquals(ptid, self.app.pt.getID())
        # notify node is broken, must be taken into account and partition must changed
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, ('127.0.0.1', self.storage_port), uuid, BROKEN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), BROKEN_STATE)
        self.failUnless(ptid < self.app.pt.getID())

    
    def test_06_handleAnswerLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ANSWER_LAST_IDS)
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.pt.getID()
        # do not care if client node call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        node_list = []
        self.checkUnexpectedPacketRaised(service.handleAnswerLastIDs, conn, packet, None, None, None)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.pt.getID())
        # send information which are later to what PMN knows, this must raise
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        self.failUnless(new_ptid > self.app.pt.getID())
        self.assertRaises(OperationFailure, service.handleAnswerLastIDs, conn, packet, None, None, new_ptid)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.pt.getID())
        

    def test_07_handleAskBeginTransaction(self):        
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_BEGIN_TRANSACTION)
        ltid = self.app.ltid
        # client call it
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE, port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.handleAskBeginTransaction(conn, packet)
        self.failUnless(ltid < self.app.ltid)
        self.assertEquals(len(self.app.finishing_transaction_dict), 1)
        tid = self.app.finishing_transaction_dict.keys()[0]
        self.assertEquals(tid, self.app.ltid)
        

    def test_08_handleAskNewOIDs(self):        
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_NEW_OIDS)
        loid = self.app.loid
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
        service.handleAskBeginTransaction(conn, packet)
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


    def test_11_handleAbortTransaction(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ABORT_TRANSACTION)
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
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        ptid = self.app.pt.getID()
        self.app.ltid = '\1' * 8
        self.app.loid = '\1' * 8
        service.handleAskLastIDs(conn, packet)
        packet = self.checkAnswerLastIDs(conn, answered_packet=packet)
        loid, ltid, lptid = protocol._decodeAnswerLastIDs(packet._body)
        self.assertEqual(loid, self.app.loid)
        self.assertEqual(ltid, self.app.ltid)
        self.assertEqual(lptid, ptid)
        

    def test_13_handleAskUnfinishedTransactions(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_UNFINISHED_TRANSACTIONS)
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
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.handleAskUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        tid_list = protocol._decodeAnswerUnfinishedTransactions(packet._body)[0]
        self.assertEqual(len(tid_list), 3)
        

    def test_15_peerBroken(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # add a second storage node and then declare it as broken
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.peerBroken, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), BROKEN_STATE) 
        self.failUnless(lptid < self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = Packet(msg_type=ASK_BEGIN_TRANSACTION)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.peerBroken(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)
        

    def test_16_timeoutExpired(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.timeoutExpired, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = Packet(msg_type=ASK_BEGIN_TRANSACTION)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.timeoutExpired(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)


    def test_17_connectionClosed(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageNodeList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), RUNNING_STATE)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getNodeByUUID(storage_uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), RUNNING_STATE) 
        self.assertRaises(OperationFailure, service.connectionClosed, conn)
        self.assertEquals(self.app.nm.getNodeByUUID(uuid).getState(), TEMPORARILY_DOWN_STATE) 
        self.assertEquals(lptid, self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=CLIENT_NODE_TYPE,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = Packet(msg_type=ASK_BEGIN_TRANSACTION)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        service.handleAskBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid).getState(), RUNNING_STATE)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.connectionClosed(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getNodeByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)




if __name__ == '__main__':
    unittest.main()

