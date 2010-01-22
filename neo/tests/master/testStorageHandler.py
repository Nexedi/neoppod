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
from mock import Mock
from struct import pack
from neo.tests import NeoTestBase
from neo.protocol import Packets
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

    def _allocatePort(self):
        self.port = getattr(self, 'port', 1000) + 1
        return self.port

    def _getClient(self):
        return self.identifyToMasterNode(node_type=NodeTypes.CLIENT, 
                ip='127.0.0.1', port=self._allocatePort())
        
    def _getStorage(self):
        return self.identifyToMasterNode(node_type=NodeTypes.STORAGE,
                ip='127.0.0.1', port=self._allocatePort())

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
        service.notifyInformationLocked(storage_conn_1, packet, tid)
        self.checkLockInformation(storage_conn_1)
        self.checkLockInformation(storage_conn_2)
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
        tid = '\1' * 8
        self.app.tm.setLastTID(tid)
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

