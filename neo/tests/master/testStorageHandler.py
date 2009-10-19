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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import unittest
from neo import logging
from mock import Mock
from struct import pack, unpack
from neo.tests import NeoTestBase
from neo import protocol
from neo.protocol import Packet, Packets
from neo.protocol import NodeTypes, NodeStates, CellStates
from neo.master.handlers.storage import StorageServiceHandler
from neo.master.app import Application
from neo.exception import OperationFailure

class MasterStorageHandlerTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.pt.clear()
        self.app.pt.setID(pack('!Q', 1))
        self.app.em = Mock({"getConnectionList" : []})
        self.app.finishing_transaction_dict = {}
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
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

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        uuid = self.getNewUUID()
        return uuid


    def test_05_notifyNodeInformation(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.NotifyNodeInformation()
        # tell the master node that is not running any longer, it must raises
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.1', self.master_port,
            self.app.uuid, NodeStates.DOWN),]
        self.assertRaises(RuntimeError, service.notifyNodeInformation, conn, packet, node_list)
        # tell the master node that it's running, nothing change
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.1', self.master_port,
            self.app.uuid, NodeStates.RUNNING),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a client node, don't care
        new_uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.CLIENT, '127.0.0.1', self.client_port, new_uuid,
            NodeStates.BROKEN),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about an unknown node, don't care
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', 11010, new_uuid,
            NodeStates.BROKEN),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify about a known node but with bad address, don't care
        self.app.nm.createStorage(
            address=("127.0.0.1", 11011), 
            uuid=self.getNewUUID(),
        )
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', 11012, uuid, NodeStates.BROKEN),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is running, as PMN already know it, nothing is done
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', self.storage_port, uuid,
            NodeStates.RUNNING),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        # notify node is temp down, must be taken into account
        ptid = self.app.pt.getID()
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', self.storage_port, uuid,
            NodeStates.TEMPORARILY_DOWN),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageList()[0]
        self.assertEquals(sn.getState(), NodeStates.TEMPORARILY_DOWN)
        self.assertEquals(ptid, self.app.pt.getID())
        # notify node is broken, must be taken into account and partition must changed
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', self.storage_port, uuid,
            NodeStates.BROKEN),]
        service.notifyNodeInformation(conn, packet, node_list)
        for call in conn.mockGetAllCalls():
            self.assertEquals(call.getName(), "getUUID")
        sn = self.app.nm.getStorageList()[0]
        self.assertEquals(sn.getState(), CellStates.BROKEN)
        self.failUnless(ptid < self.app.pt.getID())

    def test_06_answerLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AnswerLastIDs()
        loid = self.app.loid
        ltid = self.app.ltid
        lptid = self.app.pt.getID()
        # send information which are later to what PMN knows, this must raise
        conn = self.getFakeConnection(uuid, self.storage_address)
        node_list = []
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        self.failUnless(new_ptid > self.app.pt.getID())
        self.assertRaises(OperationFailure, service.answerLastIDs, conn, packet, None, None, new_ptid)
        self.assertEquals(loid, self.app.loid)
        self.assertEquals(ltid, self.app.ltid)
        self.assertEquals(lptid, self.app.pt.getID())
        
    def test_10_notifyInformationLocked(self):
        service = self.service
        uuid = self.identifyToMasterNode(port=10020)
        packet = Packets.NotifyInformationLocked()
        # give an older tid than the PMN known, must abort
        conn = self.getFakeConnection(uuid, self.storage_address)
        oid_list = []
        upper, lower = unpack('!LL', self.app.ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.checkUnexpectedPacketRaised(service.notifyInformationLocked, conn, packet, new_tid)
        old_node = self.app.nm.getByUUID(uuid)
        # job done through dispatch -> peerBroken
        self.app.nm.remove(old_node)
        self.app.pt.dropNode(old_node)

        # do the right job
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        storage_uuid_1 = self.identifyToMasterNode()
        storage_uuid_2 = self.identifyToMasterNode(port=10022)
        storage_conn_1 = self.getFakeConnection(storage_uuid_1, ("127.0.0.1", self.storage_port))
        storage_conn_2 = self.getFakeConnection(storage_uuid_2, ("127.0.0.1", 10022))
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askBeginTransaction(conn, packet)
        # clean mock object
        conn.mockCalledMethods = {}
        conn.mockAllCalledMethods = []
        self.app.em = Mock({"getConnectionList" : [conn, storage_conn_1, storage_conn_2]})
        oid_list = []
        tid = self.app.ltid
        service.finishTransaction(conn, packet, oid_list, tid)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())
        service.notifyInformationLocked(storage_conn_1, packet, tid)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        self.assertFalse(self.app.finishing_transaction_dict.values()[0].allLocked())
        service.notifyInformationLocked(storage_conn_2, packet, tid)
        self.checkNotifyTransactionFinished(conn)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
        

    def test_12_askLastIDs(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AskLastIDs()
        packet.setId(0)
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        ptid = self.app.pt.getID()
        tid = self.app.ltid
        oid = self.app.loid
        service.askLastIDs(conn, packet)
        packet = self.checkAnswerLastIDs(conn, answered_packet=packet)
        loid, ltid, lptid = packet.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)
        

    def test_13_askUnfinishedTransactions(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.AskUnfinishedTransactions()
        packet.setId(0)
        # give a uuid
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.askUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        tid_list = packet.decode()
        self.assertEqual(len(tid_list), 0)
        # create some transaction
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        conn = self.getFakeConnection(uuid, self.storage_address)
        service.askUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        tid_list = protocol._decodeAnswerUnfinishedTransactions(packet._body)[0]
        self.assertEqual(len(tid_list), 3)
        

    def test_14_notifyPartitionChanges(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        packet = Packets.NotifyPartitionChanges()
        # do not answer if not a storage node
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        self.checkUnexpectedPacketRaised(service.notifyPartitionChanges, 
                conn, packet, None, None)

        # send a bad state, must not be take into account
        conn = self.getFakeConnection(uuid, self.storage_address)
        storage_uuid = self.identifyToMasterNode(port=self.storage_port+1)
        offset = 1
        cell_list = [(offset, uuid, CellStates.FEEDING),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        service.notifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)

        # send for another node, must not be take into account
        conn = self.getFakeConnection(uuid, self.storage_address)
        offset = 1
        cell_list = [(offset, storage_uuid, CellStates.UP_TO_DATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        service.notifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)

        # send for itself, must be taken into account
        # and the feeding node must be removed
        conn = self.getFakeConnection(uuid, self.storage_address)
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        # mark the second storage node as feeding and say we are up to date
        # second node must go to discarded state and first one to up to date state
        self.app.pt.setCell(offset, self.app.nm.getByUUID(storage_uuid),
                CellStates.FEEDING)
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE),]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            if cell == storage_uuid:
                self.assertEquals(state, CellStates.FEEDING)
            else:
                self.assertEquals(state, CellStates.OUT_OF_DATE)        
        lptid = self.app.pt.getID()
        service.notifyPartitionChanges(conn, packet, self.app.pt.getID(), cell_list)
        self.failUnless(lptid < self.app.pt.getID())
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            if cell == uuid:
                self.assertEquals(state, CellStates.UP_TO_DATE)
            else:
                self.assertEquals(state, CellStates.DISCARDED)
        

    def test_15_peerBroken(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        # add a second storage node and then declare it as broken
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.RUNNING)
        service.peerBroken(conn)
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.BROKEN) 
        self.failUnless(lptid < self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING) 
        self.assertRaises(OperationFailure, service.peerBroken, conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.BROKEN) 
        self.failUnless(lptid < self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = AskBeginTransaction()
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getByUUID(client_uuid).getState(),
                NodeStates.RUNNING)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.peerBroken(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)
        

    def test_16_timeoutExpired(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.RUNNING)
        service.timeoutExpired(conn)
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.TEMPORARILY_DOWN) 
        self.assertEquals(lptid, self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING) 
        self.assertRaises(OperationFailure, service.timeoutExpired, conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.TEMPORARILY_DOWN) 
        self.assertEquals(lptid, self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = AskBeginTransaction()
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getByUUID(client_uuid).getState(),
                NodeStates.RUNNING)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.timeoutExpired(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)


    def test_17_connectionClosed(self):
        service = self.service
        uuid = self.identifyToMasterNode()
        # do nothing if no uuid
        conn = self.getFakeConnection(None, self.storage_address)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING)
        # add a second storage node and then declare it as temp down
        self.identifyToMasterNode(port = self.storage_port+2)
        storage_uuid = self.identifyToMasterNode(port = self.storage_port+1)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        conn = self.getFakeConnection(storage_uuid, ('127.0.0.1', self.storage_port+1))
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.RUNNING)
        service.connectionClosed(conn)
        self.assertEquals(self.app.nm.getByUUID(storage_uuid).getState(),
                NodeStates.TEMPORARILY_DOWN) 
        self.assertEquals(lptid, self.app.pt.getID())        
        # give an uuid, must raise as no other storage node available
        conn = self.getFakeConnection(uuid, self.storage_address)
        lptid = self.app.pt.getID()
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.RUNNING) 
        self.assertRaises(OperationFailure, service.connectionClosed, conn)
        self.assertEquals(self.app.nm.getByUUID(uuid).getState(), NodeStates.TEMPORARILY_DOWN) 
        self.assertEquals(lptid, self.app.pt.getID())
        # give a client uuid which have unfinished transactions
        client_uuid = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port = self.client_port)
        conn = self.getFakeConnection(client_uuid, self.client_address)
        lptid = self.app.pt.getID()
        packet = AskBeginTransaction()
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        service.askBeginTransaction(conn, packet)
        self.assertEquals(self.app.nm.getByUUID(client_uuid).getState(),
                NodeStates.RUNNING)
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 3)
        service.connectionClosed(conn)
        # node must be have been remove, and no more transaction must remains
        self.assertEquals(self.app.nm.getByUUID(client_uuid), None) 
        self.assertEquals(lptid, self.app.pt.getID())
        self.assertEquals(len(self.app.finishing_transaction_dict.keys()), 0)




if __name__ == '__main__':
    unittest.main()

