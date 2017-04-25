#
# Copyright (C) 2009-2017  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import unittest
from neo.lib.protocol import NodeStates, CellStates
from neo.lib.pt import Cell, PartitionTable, PartitionTableException
from . import NeoUnitTestBase

class PartitionTableTests(NeoUnitTestBase):

    def test_01_Cell(self):
        uuid = self.getStorageUUID()
        server = ("127.0.0.1", 19001)
        sn = self.createStorage(server, uuid)
        cell = Cell(sn)
        self.assertEqual(cell.node, sn)
        self.assertEqual(cell.state, CellStates.UP_TO_DATE)
        cell = Cell(sn, CellStates.OUT_OF_DATE)
        self.assertEqual(cell.node, sn)
        self.assertEqual(cell.state, CellStates.OUT_OF_DATE)
        # check getter
        self.assertEqual(cell.getNode(), sn)
        self.assertEqual(cell.getState(), CellStates.OUT_OF_DATE)
        self.assertEqual(cell.getNodeState(), NodeStates.DOWN)
        self.assertEqual(cell.getUUID(), uuid)
        self.assertEqual(cell.getAddress(), server)
        # check state setter
        cell.setState(CellStates.FEEDING)
        self.assertEqual(cell.getState(), CellStates.FEEDING)


    def test_03_setCell(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        # add a cell to an empty row
        self.assertFalse(pt.count_dict.has_key(sn1))
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 1)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # try to add to a nonexistent partition
        self.assertRaises(IndexError, pt._setCell, 10, sn1, CellStates.UP_TO_DATE)
        # if we add in discards state, must be removed
        pt._setCell(0, sn1, CellStates.DISCARDED)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        # add a feeding node into empty row
        pt._setCell(0, sn1, CellStates.FEEDING)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), CellStates.FEEDING)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # re-add it as feeding, nothing change
        pt._setCell(0, sn1, CellStates.FEEDING)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), CellStates.FEEDING)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # now add it as up to date
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 1)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)

        # now add down state, must not be taken into account
        pt._setCell(0, sn1, CellStates.DISCARDED)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        sn1.setState(NodeStates.UNKNOWN)
        self.assertRaises(PartitionTableException, pt._setCell,
            0, sn1, CellStates.UP_TO_DATE)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)


    def test_04_removeCell(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        # add a cell to an empty row
        self.assertFalse(pt.count_dict.has_key(sn1))
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 1)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # remove it
        pt.removeCell(0, sn1)
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        # add a feeding cell
        pt._setCell(0, sn1, CellStates.FEEDING)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # remove it
        pt.removeCell(0, sn1)
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)

    def test_05_getCellList(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unusable
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = self.createStorage(server2, uuid2)
        pt._setCell(0, sn2, CellStates.OUT_OF_DATE)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = self.createStorage(server3, uuid3)
        pt._setCell(0, sn3, CellStates.FEEDING)
        uuid4 = self.getStorageUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = self.createStorage(server4, uuid4)
        pt._setCell(0, sn4, CellStates.DISCARDED) # won't be added
        # now checks result
        self.assertEqual(len(pt.partition_list[0]), 3)
        for x in xrange(num_partitions):
            if x == 0:
                # all nodes
                all_cell = pt.getCellList(0)
                all_nodes = [x.getNode() for x in all_cell]
                self.assertEqual(len(all_cell), 3)
                self.assertTrue(sn1 in all_nodes)
                self.assertTrue(sn2 in all_nodes)
                self.assertTrue(sn3 in all_nodes)
                self.assertTrue(sn4 not in all_nodes)
                # readable nodes
                all_cell = pt.getCellList(0, readable=True)
                all_nodes = [x.getNode() for x in all_cell]
                self.assertEqual(len(all_cell), 2)
                self.assertTrue(sn1 in all_nodes)
                self.assertTrue(sn2 not in all_nodes)
                self.assertTrue(sn3 in all_nodes)
                self.assertTrue(sn4 not in all_nodes)
            else:
                self.assertEqual(len(pt.getCellList(1, False)), 0)
                self.assertEqual(len(pt.getCellList(1, True)), 0)

    def test_06_clear(self):
        # add some nodes
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unusable
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = self.createStorage(server2, uuid2)
        pt._setCell(1, sn2, CellStates.OUT_OF_DATE)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = self.createStorage(server3, uuid3)
        pt._setCell(2, sn3, CellStates.FEEDING)
        # now checks result
        self.assertEqual(len(pt.partition_list[0]), 1)
        self.assertEqual(len(pt.partition_list[1]), 1)
        self.assertEqual(len(pt.partition_list[2]), 1)
        pt.clear()
        partition_list = pt.partition_list
        self.assertEqual(len(partition_list), num_partitions)
        for x in xrange(num_partitions):
            part = partition_list[x]
            self.assertTrue(isinstance(part, list))
            self.assertEqual(len(part), 0)
        self.assertEqual(len(pt.count_dict), 0)

    def test_07_getNodeSet(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unusable
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = self.createStorage(server2, uuid2)
        pt._setCell(0, sn2, CellStates.OUT_OF_DATE)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = self.createStorage(server3, uuid3)
        pt._setCell(0, sn3, CellStates.FEEDING)
        uuid4 = self.getStorageUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = self.createStorage(server4, uuid4)
        pt._setCell(0, sn4, CellStates.DISCARDED) # won't be added
        # must get only two node as feeding and discarded not taken
        # into account
        self.assertEqual(pt.getNodeSet(True), {sn1, sn3})
        self.assertEqual(len(pt.getNodeSet()), 3)

    def test_08_filled(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        self.assertEqual(pt.np, num_partitions)
        self.assertEqual(pt.num_filled_rows, 0)
        self.assertFalse(pt.filled())
        # adding a node in all partition
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        for x in xrange(num_partitions):
            pt._setCell(x, sn1, CellStates.UP_TO_DATE)
        self.assertEqual(pt.num_filled_rows, num_partitions)
        self.assertTrue(pt.filled())

    def test_09_hasOffset(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unusable
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        # now test
        self.assertTrue(pt.hasOffset(0))
        self.assertFalse(pt.hasOffset(1))
        # unknown partition
        self.assertFalse(pt.hasOffset(50))

    def test_10_operational(self):
        def createStorage():
            uuid = self.getStorageUUID()
            return self.createStorage(("127.0.0.1", uuid), uuid)
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        sn1 = createStorage()
        for x in xrange(num_partitions):
            pt._setCell(x, sn1, CellStates.UP_TO_DATE)
        self.assertTrue(pt.filled())
        # it's up to date and running, so operational
        sn1.setState(NodeStates.RUNNING)
        self.assertTrue(pt.operational())
        # same with feeding state
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        sn1 = createStorage()
        for x in xrange(num_partitions):
            pt._setCell(x, sn1, CellStates.FEEDING)
        self.assertTrue(pt.filled())
        # it's feeding and running, so operational
        sn1.setState(NodeStates.RUNNING)
        self.assertTrue(pt.operational())

        # same with feeding state but non running node
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        sn1 = createStorage()
        sn1.setState(NodeStates.DOWN)
        for x in xrange(num_partitions):
            pt._setCell(x, sn1, CellStates.FEEDING)
        self.assertTrue(pt.filled())
        # it's up to date and not running, so not operational
        self.assertFalse(pt.operational())

        # same with out of date state and running
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        sn1 = createStorage()
        for x in xrange(num_partitions):
            pt._setCell(x, sn1, CellStates.OUT_OF_DATE)
        self.assertTrue(pt.filled())
        # it's not up to date and running, so not operational
        self.assertFalse(pt.operational())

    def test_12_getRow(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        pt._setCell(0, sn1, CellStates.UP_TO_DATE)
        pt._setCell(1, sn1, CellStates.UP_TO_DATE)
        pt._setCell(2, sn1, CellStates.UP_TO_DATE)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = self.createStorage(server2, uuid2)
        pt._setCell(0, sn2, CellStates.UP_TO_DATE)
        pt._setCell(1, sn2, CellStates.UP_TO_DATE)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = self.createStorage(server3, uuid3)
        pt._setCell(0, sn3, CellStates.UP_TO_DATE)
        # test
        row_0 = pt.getRow(0)
        self.assertEqual(len(row_0), 3)
        for uuid, state in row_0:
            self.assertTrue(uuid in (sn1.getUUID(), sn2.getUUID(), sn3.getUUID()))
            self.assertEqual(state, CellStates.UP_TO_DATE)
        row_1 = pt.getRow(1)
        self.assertEqual(len(row_1), 2)
        for uuid, state in row_1:
            self.assertTrue(uuid in (sn1.getUUID(), sn2.getUUID()))
            self.assertEqual(state, CellStates.UP_TO_DATE)
        row_2 = pt.getRow(2)
        self.assertEqual(len(row_2), 1)
        for uuid, state in row_2:
            self.assertEqual(uuid, sn1.getUUID())
            self.assertEqual(state, CellStates.UP_TO_DATE)
        row_3 = pt.getRow(3)
        self.assertEqual(len(row_3), 0)
        row_4 = pt.getRow(4)
        self.assertEqual(len(row_4), 0)
        # unknown row
        self.assertRaises(IndexError,  pt.getRow, 5)

if __name__ == '__main__':
    unittest.main()

