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
import neo.master
from neo import protocol
from neo.protocol import Packet, INVALID_UUID
from neo.master.handlers.storage import StorageServiceHandler
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

class MasterStorageHandlerTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config = self.getConfigFile(master_number=1, replicas=1)
        self.app = Application(config, "master1")
        self.app.pt.clear()
        self.app.pt.setID(pack('!Q', 1))
        self.app.em = Mock({"getConnectionList" : []})
        self.app.finishing_transaction_dict = {}
        for server in self.app.master_node_list:
            self.app.nm.add(MasterNode(server = server))
        self.service = StorageServiceHandler(self.app)
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
        return uuid


    def test_05_handleNotifyNodeInformation(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=NOTIFY_NODE_INFORMATION)
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
        ptid = self.app.pt.getID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, TEMPORARILY_DOWN_STATE),]
        service.handleNotifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageNodeList()[0]
        self.assertEquals(sn.getState(), TEMPORARILY_DOWN_STATE)
        self.assertEquals(ptid, self.app.pt.getID())
        # notify node is broken, must be taken into account and partition must changed
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(STORAGE_NODE_TYPE, '127.0.0.1', self.storage_port, uuid, BROKEN_STATE),]
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
        
    def test_10_handleNotifyInformationLocked(self):
        service = self.service
        uuid = self.identifyToMasterNode(port=10020)
        packet = Packet(msg_type=NOTIFY_INFORMATION_LOCKED)
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
        service.handleAskBeginTransaction(conn, packet)
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
        

    def test_12_handleAskLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=ASK_LAST_IDS)
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        ptid = self.app.pt.getID()
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
        

    def test_14_handleNotifyPartitionChanges(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packet(msg_type=NOTIFY_PARTITION_CHANGES)
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
        service.handleNotifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
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
        service.handleNotifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
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
        lptid = self.app.pt.getID()
        service.handleNotifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
        self.failUnless(lptid < self.app.pt.getID())
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

