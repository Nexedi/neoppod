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
from mock import Mock
from neo.tests import NeoUnitTestBase
from neo.lib.protocol import NodeStates, CellStates
from neo.master.pt import PartitionTable
from neo.lib.node import StorageNode

class MasterPartitionTableTests(NeoUnitTestBase):

    def test_02_PartitionTable_creation(self):
        num_partitions = 5
        num_replicas = 3
        pt = PartitionTable(num_partitions, num_replicas)
        self.assertEqual(pt.np, num_partitions)
        self.assertEqual(pt.nr, num_replicas)
        self.assertEqual(pt.num_filled_rows, 0)
        partition_list = pt.partition_list
        self.assertEqual(len(partition_list), num_partitions)
        for x in xrange(num_partitions):
            part = partition_list[x]
            self.assertTrue(isinstance(part, list))
            self.assertEqual(len(part), 0)
        self.assertEqual(len(pt.count_dict), 0)
        # no nodes or cells for now
        self.assertEqual(len(pt.getNodeList()), 0)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.getCellList(x)), 0)
            self.assertEqual(len(pt.getCellList(x, True)), 0)
            self.assertEqual(len(pt.getRow(x)), 0)
        self.assertFalse(pt.operational())
        self.assertFalse(pt.filled())
        self.assertRaises(RuntimeError, pt.make, [])
        self.assertFalse(pt.operational())
        self.assertFalse(pt.filled())

    def test_11_findLeastUsedNode(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1, NodeStates.RUNNING)
        pt.setCell(0, sn1, CellStates.UP_TO_DATE)
        pt.setCell(1, sn1, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.UP_TO_DATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(Mock(), server2, uuid2, NodeStates.RUNNING)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        pt.setCell(1, sn2, CellStates.UP_TO_DATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(Mock(), server3, uuid3, NodeStates.RUNNING)
        pt.setCell(0, sn3, CellStates.UP_TO_DATE)
        # test
        node = pt.findLeastUsedNode()
        self.assertEqual(node, sn3)
        node = pt.findLeastUsedNode((sn3, ))
        self.assertEqual(node, sn2)
        node = pt.findLeastUsedNode((sn3, sn2))
        self.assertEqual(node, sn1)

    def test_13_outdate(self):
        # create nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(Mock(), server2, uuid2)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = StorageNode(Mock(), server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = StorageNode(Mock(), server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 19005)
        sn5 = StorageNode(Mock(), server5, uuid5)
        # create partition table
        num_partitions = 5
        num_replicas = 3
        pt = PartitionTable(num_partitions, num_replicas)
        pt.setCell(0, sn1, CellStates.OUT_OF_DATE)
        sn1.setState(NodeStates.RUNNING)
        pt.setCell(1, sn2, CellStates.UP_TO_DATE)
        sn2.setState(NodeStates.TEMPORARILY_DOWN)
        pt.setCell(2, sn3, CellStates.UP_TO_DATE)
        sn3.setState(NodeStates.DOWN)
        pt.setCell(3, sn4, CellStates.UP_TO_DATE)
        sn4.setState(NodeStates.BROKEN)
        pt.setCell(4, sn5, CellStates.UP_TO_DATE)
        sn5.setState(NodeStates.RUNNING)
        # outdate nodes
        cells_outdated = pt.outdate()
        self.assertEqual(len(cells_outdated), 3)
        for offset, uuid, state in cells_outdated:
            self.assertTrue(offset in (1, 2, 3))
            self.assertTrue(uuid in (uuid2, uuid3, uuid4))
            self.assertEqual(state, CellStates.OUT_OF_DATE)
        # check each cell
        # part 1, already outdated
        cells = pt.getCellList(0)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        # part 2, must be outdated
        cells = pt.getCellList(1)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        # part 3, must be outdated
        cells = pt.getCellList(2)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        # part 4, already outdated
        cells = pt.getCellList(3)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        # part 5, remains running
        cells = pt.getCellList(4)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)

    def test_14_addNode(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1)
        # add it to an empty pt
        cell_list = pt.addNode(sn1)
        self.assertEqual(len(cell_list), 5)
        # it must be added to all partitions
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 1)
            self.assertEqual(pt.getCellList(x)[0].getState(), CellStates.OUT_OF_DATE)
            self.assertEqual(pt.getCellList(x)[0].getNode(), sn1)
        self.assertEqual(pt.count_dict[sn1], 5)
        # add same node again, must remain the same
        cell_list = pt.addNode(sn1)
        self.assertEqual(len(cell_list), 0)
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 1)
            self.assertEqual(pt.getCellList(x)[0].getState(), CellStates.OUT_OF_DATE)
            self.assertEqual(pt.getCellList(x)[0].getNode(), sn1)
        self.assertEqual(pt.count_dict[sn1], 5)
        # add a second node to fill the partition table
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(Mock(), server2, uuid2)
        # add it
        cell_list = pt.addNode(sn2)
        self.assertEqual(len(cell_list), 5)
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)
            self.assertEqual(pt.getCellList(x)[0].getState(), CellStates.OUT_OF_DATE)
            self.assertTrue(pt.getCellList(x)[0].getNode() in (sn1, sn2))
        # test the most used node is remove from some partition
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(Mock(), server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(Mock(), server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 1900)
        sn5 = StorageNode(Mock(), server5, uuid5)
        # partition looks like:
        # 0 : sn1, sn2
        # 1 : sn1, sn3
        # 2 : sn1, sn4
        # 3 : sn1, sn5
        num_partitions = 4
        num_replicas = 1
        pt = PartitionTable(num_partitions, num_replicas)
        # node most used is out of date, just dropped
        pt.setCell(0, sn1, CellStates.OUT_OF_DATE)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        pt.setCell(1, sn1, CellStates.OUT_OF_DATE)
        pt.setCell(1, sn3, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.OUT_OF_DATE)
        pt.setCell(2, sn4, CellStates.UP_TO_DATE)
        pt.setCell(3, sn1, CellStates.OUT_OF_DATE)
        pt.setCell(3, sn5, CellStates.UP_TO_DATE)
        uuid6 = self.getNewUUID()
        server6 = ("127.0.0.6", 19006)
        sn6 = StorageNode(Mock(), server6, uuid6)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0, 1):
                if uuid == uuid1:
                    self.assertEqual(state, CellStates.DISCARDED)
                elif uuid == uuid6:
                    self.assertEqual(state, CellStates.OUT_OF_DATE)
                else:
                    self.assertTrue(uuid in (uuid1, uuid6))
            else:
                self.assertTrue(offset in (0, 1))
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)
        # there is a feeding cell, just dropped
        pt.clear()
        pt.setCell(0, sn1, CellStates.UP_TO_DATE)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        pt.setCell(0, sn3, CellStates.FEEDING)
        pt.setCell(1, sn1, CellStates.UP_TO_DATE)
        pt.setCell(1, sn2, CellStates.FEEDING)
        pt.setCell(1, sn3, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.UP_TO_DATE)
        pt.setCell(2, sn4, CellStates.FEEDING)
        pt.setCell(2, sn5, CellStates.UP_TO_DATE)
        pt.setCell(3, sn1, CellStates.UP_TO_DATE)
        pt.setCell(3, sn4, CellStates.UP_TO_DATE)
        pt.setCell(3, sn5, CellStates.FEEDING)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0, 1):
                if uuid == uuid1:
                    self.assertEqual(state, CellStates.DISCARDED)
                elif uuid == uuid6:
                    self.assertEqual(state, CellStates.OUT_OF_DATE)
                else:
                    self.assertTrue(uuid in (uuid1, uuid6))
            else:
                self.assertTrue(offset in (0, 1))
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 3)
        # there is no feeding cell, marked as feeding
        pt.clear()
        pt.setCell(0, sn1, CellStates.UP_TO_DATE)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        pt.setCell(1, sn1, CellStates.UP_TO_DATE)
        pt.setCell(1, sn3, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.UP_TO_DATE)
        pt.setCell(2, sn4, CellStates.UP_TO_DATE)
        pt.setCell(3, sn1, CellStates.UP_TO_DATE)
        pt.setCell(3, sn5, CellStates.UP_TO_DATE)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0, 1):
                if uuid == uuid1:
                    self.assertEqual(state, CellStates.FEEDING)
                elif uuid == uuid6:
                    self.assertEqual(state, CellStates.OUT_OF_DATE)
                else:
                    self.assertTrue(uuid in (uuid1, uuid6))
            else:
                self.assertTrue(offset in (0, 1))
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 3)

    def test_15_dropNode(self):
        num_partitions = 4
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1, NodeStates.RUNNING)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(Mock(), server2, uuid2, NodeStates.RUNNING)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(Mock(), server3, uuid3, NodeStates.RUNNING)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(Mock(), server4, uuid4, NodeStates.RUNNING)
        # partition looks like:
        # 0 : sn1, sn2
        # 1 : sn1, sn3
        # 2 : sn1, sn3
        # 3 : sn1, sn4
        # node is not feeding, so retrive least use node to replace it
        # so sn2 must be repaced by sn4 in partition 0
        pt.setCell(0, sn1, CellStates.UP_TO_DATE)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        pt.setCell(1, sn1, CellStates.UP_TO_DATE)
        pt.setCell(1, sn3, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.UP_TO_DATE)
        pt.setCell(2, sn3, CellStates.UP_TO_DATE)
        pt.setCell(3, sn1, CellStates.UP_TO_DATE)
        pt.setCell(3, sn4, CellStates.UP_TO_DATE)
        cell_list = pt.dropNode(sn2)
        self.assertEqual(len(cell_list), 2)
        for offset, uuid, state  in cell_list:
            self.assertEqual(offset, 0)
            if uuid == uuid2:
                self.assertEqual(state, CellStates.DISCARDED)
            elif uuid == uuid4:
                self.assertEqual(state, CellStates.OUT_OF_DATE)
            else:
                self.assertTrue(uuid in (uuid2, uuid4))

        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)
        # same test but with feeding state, no other will be added
        pt.clear()
        pt.setCell(0, sn1, CellStates.UP_TO_DATE)
        pt.setCell(0, sn2, CellStates.FEEDING)
        pt.setCell(1, sn1, CellStates.UP_TO_DATE)
        pt.setCell(1, sn3, CellStates.UP_TO_DATE)
        pt.setCell(2, sn1, CellStates.UP_TO_DATE)
        pt.setCell(2, sn3, CellStates.UP_TO_DATE)
        pt.setCell(3, sn1, CellStates.UP_TO_DATE)
        pt.setCell(3, sn4, CellStates.UP_TO_DATE)
        cell_list = pt.dropNode(sn2)
        self.assertEqual(len(cell_list), 1)
        for offset, uuid, state  in cell_list:
            self.assertEqual(offset, 0)
            self.assertEqual(state, CellStates.DISCARDED)
            self.assertEqual(uuid, uuid2)
        for x in xrange(num_replicas):
            if x == 0:
                self.assertEqual(len(pt.getCellList(x)), 1)
            else:
                self.assertEqual(len(pt.getCellList(x)), 2)

    def test_16_make(self):
        num_partitions = 5
        num_replicas = 1
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1, NodeStates.RUNNING)
        # add not running node
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(Mock(), server2, uuid2)
        sn2.setState(NodeStates.TEMPORARILY_DOWN)
        # add node without uuid
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(Mock(), server3, None, NodeStates.RUNNING)
        # add clear node
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(Mock(), server4, uuid4, NodeStates.RUNNING)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 1900)
        sn5 = StorageNode(Mock(), server5, uuid5, NodeStates.RUNNING)
        # make the table
        pt.make([sn1, sn2, sn3, sn4, sn5])
        # check it's ok, only running nodes and node with uuid
        # must be present
        for x in xrange(num_partitions):
            cells = pt.getCellList(x)
            self.assertEqual(len(cells), 2)
            nodes = [x.getNode() for x in cells]
            for node in nodes:
                self.assertTrue(node in (sn1, sn4, sn5))
                self.assertTrue(node not in (sn2, sn3))
        self.assertTrue(pt.filled())
        self.assertTrue(pt.operational())
        # create a pt with less nodes
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        pt.make([sn1])
        # check it's ok
        for x in xrange(num_partitions):
            cells = pt.getCellList(x)
            self.assertEqual(len(cells), 1)
            nodes = [x.getNode() for x in cells]
            for node in nodes:
                self.assertEqual(node, sn1)
        self.assertTrue(pt.filled())
        self.assertTrue(pt.operational())

    def test_17_tweak(self):
        # remove broken node
        # remove if too many feeding nodes
        # remove feeding if all cells are up to date
        # if too many cells, remove most used cell
        # if not enought cell, add least used node

        # create nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1, NodeStates.RUNNING)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(Mock(), server2, uuid2, NodeStates.RUNNING)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = StorageNode(Mock(), server3, uuid3, NodeStates.RUNNING)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = StorageNode(Mock(), server4, uuid4, NodeStates.RUNNING)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 19005)
        sn5 = StorageNode(Mock(), server5, uuid5, NodeStates.RUNNING)
        # create partition table
        # 0 : sn1(discarded), sn2(up), -> sn2 must remain
        # 1 : sn1(feeding), sn2(feeding), sn3(up) -> one feeding and sn3 must remain
        # 2 : sn1(feeding), sn2(up), sn3(up) -> sn2 and sn3 must remain, feeding must go away
        # 3 : sn1(up), sn2(up), sn3(up), sn4(up) -> only 3 cell must remain
        # 4 : sn1(up), sn5(up) -> one more cell must be added
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # part 0
        pt.setCell(0, sn1, CellStates.DISCARDED)
        pt.setCell(0, sn2, CellStates.UP_TO_DATE)
        # part 1
        pt.setCell(1, sn1, CellStates.FEEDING)
        pt.setCell(1, sn2, CellStates.FEEDING)
        pt.setCell(1, sn3, CellStates.OUT_OF_DATE)
        # part 2
        pt.setCell(2, sn1, CellStates.FEEDING)
        pt.setCell(2, sn2, CellStates.UP_TO_DATE)
        pt.setCell(2, sn3, CellStates.UP_TO_DATE)
        # part 3
        pt.setCell(3, sn1, CellStates.UP_TO_DATE)
        pt.setCell(3, sn2, CellStates.UP_TO_DATE)
        pt.setCell(3, sn3, CellStates.UP_TO_DATE)
        pt.setCell(3, sn4, CellStates.UP_TO_DATE)
        # part 4
        pt.setCell(4, sn1, CellStates.UP_TO_DATE)
        pt.setCell(4, sn5, CellStates.UP_TO_DATE)
        # now tweak the table
        pt.tweak()
        # check part 1
        cells =  pt.getCellList(0)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            self.assertNotEqual(cell.getState(), CellStates.DISCARDED)
            if cell.getNode() == sn2:
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
            else:
                self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        self.assertTrue(sn2 in [x.getNode() for x in cells])

        # check part 2
        cells =  pt.getCellList(1)
        self.assertEqual(len(cells), 4)
        for cell in cells:
            if cell.getNode() == sn1:
                self.assertEqual(cell.getState(), CellStates.FEEDING)
            else:
                self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        self.assertTrue(sn3 in [x.getNode() for x in cells])
        self.assertTrue(sn1 in [x.getNode() for x in cells])

        # check part 3
        cells =  pt.getCellList(2)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            if cell.getNode() in (sn2, sn3):
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
            else:
                self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        self.assertTrue(sn3 in [x.getNode() for x in cells])
        self.assertTrue(sn2 in [x.getNode() for x in cells])

        # check part 4
        cells =  pt.getCellList(3)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)

        # check part 5
        cells =  pt.getCellList(4)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            if cell.getNode() in (sn1, sn5):
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
            else:
                self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        self.assertTrue(sn1 in [x.getNode() for x in cells])
        self.assertTrue(sn5 in [x.getNode() for x in cells])


if __name__ == '__main__':
    unittest.main()

