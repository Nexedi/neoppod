#
# Copyright (C) 2009-2010  Nexedi SA
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
from neo.protocol import NodeTypes, NodeStates, CellStates
from neo.master.recovery import RecoveryManager
from neo.master.app import Application

class MasterRecoveryTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(config)
        self.app.pt.clear()
        self.recovery = RecoveryManager(self.app)
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
        address = (ip, port)
        uuid = self.getNewUUID()
        self.app.nm.createFromNodeType(node_type, address=address, uuid=uuid,
            state=NodeStates.RUNNING)
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


    def test_09_answerLastIDs(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode()
        loid = self.app.loid = '\1' * 8
        self.app.tm.setLastTID('\1' * 8)
        ltid = self.app.tm.getLastTID()
        self.app.pt.setID('\1' * 8)
        lptid = self.app.pt.getID()
        # send information which are later to what PMN knows, this must update target node
        conn = self.getFakeConnection(uuid, self.storage_port)
        new_ptid = unpack('!Q', lptid)[0]
        new_ptid = pack('!Q', new_ptid + 1)
        oid = unpack('!Q', loid)[0]
        new_oid = pack('!Q', oid + 1)
        upper, lower = unpack('!LL', ltid)
        new_tid = pack('!LL', upper, lower + 10)
        self.assertTrue(new_ptid > self.app.pt.getID())
        self.assertTrue(new_oid > self.app.loid)
        self.assertTrue(new_tid > self.app.tm.getLastTID())
        recovery.answerLastIDs(conn, new_oid, new_tid, new_ptid)
        self.assertEquals(new_oid, self.app.loid)
        self.assertEquals(new_tid, self.app.tm.getLastTID())
        self.assertEquals(new_ptid, recovery.target_ptid)


    def test_10_answerPartitionTable(self):
        recovery = self.recovery
        uuid = self.identifyToMasterNode(NodeTypes.MASTER, port=self.master_port)
        # not from target node, ignore
        uuid = self.identifyToMasterNode(NodeTypes.STORAGE, port=self.storage_port)
        conn = self.getFakeConnection(uuid, self.storage_port)
        offset = 1
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        recovery.target_ptid = 2
        recovery.answerPartitionTable(conn, 1, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        # from target node, taken into account
        conn = self.getFakeConnection(uuid, self.storage_port)
        offset = 1
        cell_list = [(offset, ((uuid, CellStates.UP_TO_DATE,),),)]
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.OUT_OF_DATE)
        recovery.answerPartitionTable(conn, None, cell_list)
        cells = self.app.pt.getRow(offset)
        for cell, state in cells:
            self.assertEquals(state, CellStates.UP_TO_DATE)
        # give a bad offset, must send error
        self.recovery.target_uuid = uuid
        conn = self.getFakeConnection(uuid, self.storage_port)
        offset = 1000000
        self.assertFalse(self.app.pt.hasOffset(offset))
        cell_list = [(offset, ((uuid, NodeStates.DOWN,),),)]
        self.checkProtocolErrorRaised(recovery.answerPartitionTable, conn,
            2, cell_list)


if __name__ == '__main__':
    unittest.main()

