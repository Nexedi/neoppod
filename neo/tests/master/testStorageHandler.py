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
from neo.master.handlers.client import ClientServiceHandler
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
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
        self.service = StorageServiceHandler(self.app)
        self.client_handler = ClientServiceHandler(self.app)
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
        nm = self.app.nm
        uuid = self.getNewUUID()
        node = nm.createFromNodeType(node_type, address=(ip, port),
                uuid=uuid)
        conn = self.getFakeConnection(node.getUUID(),node.getAddress())
        return (node, conn)


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

    def test_10_notifyInformationLocked(self):
        service = self.service
        node, conn = self.identifyToMasterNode(port=10020)
        packet = Packets.NotifyInformationLocked()
        packet.setId(0)
        # give an older tid than the PMN known, must abort
        oid_list = []
        self.app.ltid = pack('!LL', 0, 1)
        new_tid = pack('!LL', 0, 10)
        self.checkUnexpectedPacketRaised(service.notifyInformationLocked, conn, packet, new_tid)
        # job done through dispatch -> peerBroken
        self.app.nm.remove(node)
        self.app.pt.dropNode(node)

        # do the right job
        node, conn = self.identifyToMasterNode(node_type=NodeTypes.CLIENT, port=self.client_port)
        self.client_handler.askBeginTransaction(conn, packet, None)
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
        node, conn = self.identifyToMasterNode()
        packet = Packets.AskLastIDs()
        packet.setId(0)
        # give a uuid
        conn = self.getFakeConnection(node.getUUID(), self.storage_address)
        ptid = self.app.pt.getID()
        oid = self.app.loid = '\1' * 8
        tid = self.app.ltid = '\1' * 8
        service.askLastIDs(conn, packet)
        packet = self.checkAnswerLastIDs(conn, answered_packet=packet)
        loid, ltid, lptid = packet.decode()
        self.assertEqual(loid, oid)
        self.assertEqual(ltid, tid)
        self.assertEqual(lptid, ptid)


    def test_13_askUnfinishedTransactions(self):
        service = self.service
        node, conn = self.identifyToMasterNode()
        packet = Packets.AskUnfinishedTransactions()
        packet.setId(0)
        # give a uuid
        service.askUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        packet.setId(0)
        tid_list, = packet.decode()
        self.assertEqual(tid_list, [])
        # create some transaction
        node, conn = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        client_uuid = node.getUUID()
        self.client_handler.askBeginTransaction(conn, packet, None)
        self.client_handler.askBeginTransaction(conn, packet, None)
        self.client_handler.askBeginTransaction(conn, packet, None)
        conn = self.getFakeConnection(node.getUUID(), self.storage_address)
        service.askUnfinishedTransactions(conn, packet)
        packet = self.checkAnswerUnfinishedTransactions(conn, answered_packet=packet)
        (tid_list, ) = packet.decode()
        self.assertEqual(len(tid_list), 3)


    def test_14_notifyPartitionChanges(self):
        service = self.service
        node, conn = self.identifyToMasterNode()
        uuid = node.getUUID()
        packet = Packets.NotifyPartitionChanges()
        # do not answer if not a storage node
        client, client_conn = self.identifyToMasterNode(node_type=NodeTypes.CLIENT,
                                                port=self.client_port)
        client_uuid = client.getUUID()
        self.checkUnexpectedPacketRaised(service.notifyPartitionChanges,
                conn, packet, None, None)

        # send a bad state, must not be take into account
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

    def _testWithMethod(self, method, state):
        service = self.service
        # define two nodes
        node1, conn1 = self.identifyToMasterNode()
        node2, conn2 = self.identifyToMasterNode()
        node1.setRunning()
        node2.setRunning()
        self.assertEquals(node1.getState(), NodeStates.RUNNING)
        self.assertEquals(node2.getState(), NodeStates.RUNNING)
        # filled the pt
        self.app.pt.make(self.app.nm.getStorageList())
        self.assertTrue(self.app.pt.filled())
        self.assertTrue(self.app.pt.operational())
        # drop one node
        lptid = self.app.pt.getID()
        method(conn1)
        self.assertEquals(node1.getState(), state)
        self.failUnless(lptid < self.app.pt.getID())
        # drop the second, no storage node left
        lptid = self.app.pt.getID()
        self.assertEquals(node2.getState(), NodeStates.RUNNING)
        self.assertRaises(OperationFailure, method, conn2)
        self.assertEquals(node2.getState(), state)
        self.assertEquals(lptid, self.app.pt.getID())

    def test_15_peerBroken(self):
        self._testWithMethod(self.service.peerBroken, NodeStates.BROKEN)

    def test_16_timeoutExpired(self):
        self._testWithMethod(self.service.timeoutExpired,
                NodeStates.TEMPORARILY_DOWN)

    def test_17_connectionClosed(self):
        self._testWithMethod(self.service.connectionClosed,
                NodeStates.TEMPORARILY_DOWN)


if __name__ == '__main__':
    unittest.main()

