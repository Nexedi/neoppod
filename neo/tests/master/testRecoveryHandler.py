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
from struct import pack, unpack
from neo.tests import NeoTestBase
from neo.protocol import Packet, Packets
from neo.protocol import NodeTypes, NodeStates, CellStates
from neo.master.handlers.recovery import RecoveryHandler
from neo.master.app import Application

class MasterRecoveryTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(config)
        self.app.pt.clear()
        self.app.finishing_transaction_dict = {}
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
        self.recovery = RecoveryHandler(self.app)
        self.app.unconnected_master_node_set = set()
        self.app.negotiating_master_node_set = set()
        for node in self.app.nm.getMasterList():
            self.app.unconnected_master_node_set.add(node.getAddress())
            node.setState(NodeStates.RUNNING)

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

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        uuid = self.getNewUUID()
        return uuid

    # Tests
    def test_01_connectionClosed(self):
        uuid = self.identifyToMasterNode(node_type=NodeTypes.MASTER, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.RUNNING)
        self.recovery.connectionClosed(conn)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.TEMPORARILY_DOWN)

    def test_02_timeoutExpired(self):
        uuid = self.identifyToMasterNode(node_type=NodeTypes.MASTER, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.RUNNING)
        self.recovery.timeoutExpired(conn)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.TEMPORARILY_DOWN)


    def test_03_peerBroken(self):
        uuid = self.identifyToMasterNode(node_type=NodeTypes.MASTER, port=self.master_port)
        conn = self.getFakeConnection(uuid, self.master_address)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.RUNNING)
        self.recovery.peerBroken(conn)
        self.assertEqual(self.app.nm.getByAddress(conn.getAddress()).getState(),
                NodeStates.BROKEN)

    def test_08_notifyNodeInformation(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(NodeTypes.MASTER, port=self.master_port)
        packet = Packets.NotifyNodeInformation()
        # tell about a client node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.CLIENT, '127.0.0.1', self.client_port,
            self.getNewUUID(), NodeStates.DOWN),]
        self.assertEqual(len(self.app.nm.getList()), 0)
        recovery.notifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getList()), 0)

        # tell the master node about itself, if running must do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.1', self.master_port-1,
            self.app.uuid, NodeStates.RUNNING),]
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        recovery.notifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port-1))

        # tell the master node about itself, if down must raise
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.1', self.master_port-1,
            self.app.uuid, NodeStates.DOWN),]
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port-1))
        self.assertEqual(node, None)
        self.assertRaises(RuntimeError, recovery.notifyNodeInformation, conn, packet, node_list)

        # tell about an unknown storage node, do nothing
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.STORAGE, '127.0.0.1', self.master_port - 1,
            self.getNewUUID(), NodeStates.DOWN),]
        self.assertEqual(len(self.app.nm.getStorageList()), 0)
        recovery.notifyNodeInformation(conn, packet, node_list)
        self.assertEqual(len(self.app.nm.getStorageList()), 0)

        # tell about a known node but different address
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.2', self.master_port, uuid,
            NodeStates.DOWN),]
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), NodeStates.RUNNING)
        recovery.notifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), NodeStates.RUNNING)

        # tell about a known node
        conn = self.getFakeConnection(uuid, self.master_address)
        node_list = [(NodeTypes.MASTER, '127.0.0.1', self.master_port, uuid,
            NodeStates.DOWN),]
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), NodeStates.RUNNING)
        recovery.notifyNodeInformation(conn, packet, node_list)
        node = self.app.nm.getByAddress(("127.0.0.1", self.master_port))
        self.assertEqual(node.getState(), NodeStates.DOWN)


    def test_09_answerLastIDs(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode()
        packet = Packets.AnswerLastIDs()
        loid = self.app.loid
        ltid = self.app.tm.getLastTID()
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
        self.failUnless(new_tid > self.app.tm.getLastTID())
        self.assertEquals(self.app.target_uuid, None)
        recovery.answerLastIDs(conn, packet, new_oid, new_tid, new_ptid)
        self.assertEquals(new_oid, self.app.loid)
        self.assertEquals(new_tid, self.app.tm.getLastTID())
        self.assertEquals(new_ptid, self.app.pt.getID())
        self.assertEquals(self.app.target_uuid,uuid)


    def test_10_answerPartitionTable(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(NodeTypes.MASTER, port=self.master_port)
        packet = Packets.AnswerPartitionTable()
        # not from target node, ignore
        uuid = self.identifyToMasterNode(NodeTypes.STORAGE, port=self.storage_port)
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertNotEquals(self.app.target_uuid, uuid)
        offset = 1
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        recovery.answerPartitionTable(conn, packet, None, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        # from target node, taken into account
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertNotEquals(self.app.target_uuid, uuid)
        self.app.target_uuid = uuid
        self.assertEquals(self.app.target_uuid, uuid)
        offset = 1
        cell_list = [(offset, ((uuid, CellStates.UP_TO_DATE,),),)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        recovery.answerPartitionTable(conn, packet, None, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.UP_TO_DATE)
        # give a bad offset, must send error
        conn = self.getFakeConnection(uuid, self.storage_port)
        self.assertEquals(self.app.target_uuid, uuid)
        offset = 1000000
        self.assertFalse(self.app.pt.hasOffset(offset))
        cell_list = [(offset, ((uuid, NodeStates.DOWN,),),)]
        self.checkUnexpectedPacketRaised(recovery.answerPartitionTable, conn, packet, None, cell_list)


if __name__ == '__main__':
    unittest.main()

