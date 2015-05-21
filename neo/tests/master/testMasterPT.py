#
# Copyright (C) 2009-2015  Nexedi SA
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
from collections import defaultdict
from mock import Mock
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeStates, CellStates
from neo.lib.pt import PartitionTableException
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
        self.assertFalse(pt.getNodeSet())
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.getCellList(x)), 0)
            self.assertEqual(len(pt.getCellList(x, True)), 0)
            self.assertEqual(len(pt.getRow(x)), 0)
        self.assertFalse(pt.operational())
        self.assertFalse(pt.filled())
        self.assertRaises(RuntimeError, pt.make, [])
        self.assertFalse(pt.operational())
        self.assertFalse(pt.filled())

    def test_13_outdate(self):
        # create nodes
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(Mock(), server2, uuid2)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = StorageNode(Mock(), server3, uuid3)
        uuid4 = self.getStorageUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = StorageNode(Mock(), server4, uuid4)
        uuid5 = self.getStorageUUID()
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

    def test_15_dropNodeList(self):
        sn = [StorageNode(Mock(), None, i + 1, NodeStates.RUNNING)
              for i in xrange(3)]
        pt = PartitionTable(3, 0)
        pt.setCell(0, sn[0], CellStates.OUT_OF_DATE)
        pt.setCell(1, sn[1], CellStates.FEEDING)
        pt.setCell(1, sn[2], CellStates.OUT_OF_DATE)
        pt.setCell(2, sn[0], CellStates.OUT_OF_DATE)
        pt.setCell(2, sn[1], CellStates.FEEDING)
        pt.setCell(2, sn[2], CellStates.UP_TO_DATE)

        self.assertEqual(sorted(pt.dropNodeList(sn[:1], True)), [
            (0, 1, CellStates.DISCARDED),
            (2, 1, CellStates.DISCARDED),
            (2, 2, CellStates.UP_TO_DATE)])

        self.assertEqual(sorted(pt.dropNodeList(sn[2:], True)), [
            (1, 2, CellStates.UP_TO_DATE),
            (1, 3, CellStates.DISCARDED),
            (2, 2, CellStates.UP_TO_DATE),
            (2, 3, CellStates.DISCARDED)])

        self.assertRaises(PartitionTableException, pt.dropNodeList, sn[1:2])
        pt.setCell(1, sn[2], CellStates.UP_TO_DATE)
        self.assertEqual(sorted(pt.dropNodeList(sn[1:2])), [
            (1, 2, CellStates.DISCARDED),
            (2, 2, CellStates.DISCARDED)])

        self.assertEqual(self.tweak(pt), [(2, 3, CellStates.FEEDING)])

    def test_16_make(self):
        num_partitions = 5
        num_replicas = 1
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(Mock(), server1, uuid1, NodeStates.RUNNING)
        # add not running node
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(Mock(), server2, uuid2)
        sn2.setState(NodeStates.TEMPORARILY_DOWN)
        # add node without uuid
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(Mock(), server3, None, NodeStates.RUNNING)
        # add clear node
        uuid4 = self.getStorageUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(Mock(), server4, uuid4, NodeStates.RUNNING)
        uuid5 = self.getStorageUUID()
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

    def _pt_states(self, pt):
        node_dict = defaultdict(list)
        for offset, row in enumerate(pt.partition_list):
            for cell in row:
                state_list = node_dict[cell.getNode()]
                if state_list:
                    self.assertTrue(state_list[-1][0] < offset)
                state_list.append((offset, str(cell.getState())[0]))
        return map(dict, sorted(node_dict.itervalues()))

    def checkPT(self, pt, exclude_empty=False):
        new_pt = PartitionTable(pt.np, pt.nr)
        new_pt.make(node for node, count in pt.count_dict.iteritems()
                         if count or not exclude_empty)
        self.assertEqual(self._pt_states(pt), self._pt_states(new_pt))

    def update(self, pt, change_list=None):
        if change_list is None:
            for offset, row in enumerate(pt.partition_list):
                for cell in list(row):
                    if cell.isOutOfDate():
                        pt.setUpToDate(cell.getNode(), offset)
        else:
            node_dict = {x.getUUID(): x for x in pt.count_dict}
            for offset, uuid, state in change_list:
                if state is CellStates.OUT_OF_DATE:
                    pt.setUpToDate(node_dict[uuid], offset)

    def tweak(self, pt, drop_list=()):
        change_list = pt.tweak(drop_list)
        self.assertFalse(pt.tweak(drop_list))
        return change_list

    def test_17_tweak(self):
        sn = [StorageNode(Mock(), None, i + 1, NodeStates.RUNNING)
              for i in xrange(5)]
        pt = PartitionTable(5, 2)
        # part 0
        pt.setCell(0, sn[0], CellStates.DISCARDED)
        pt.setCell(0, sn[1], CellStates.UP_TO_DATE)
        # part 1
        pt.setCell(1, sn[0], CellStates.FEEDING)
        pt.setCell(1, sn[1], CellStates.FEEDING)
        pt.setCell(1, sn[2], CellStates.OUT_OF_DATE)
        # part 2
        pt.setCell(2, sn[0], CellStates.FEEDING)
        pt.setCell(2, sn[1], CellStates.UP_TO_DATE)
        pt.setCell(2, sn[2], CellStates.UP_TO_DATE)
        # part 3
        pt.setCell(3, sn[0], CellStates.UP_TO_DATE)
        pt.setCell(3, sn[1], CellStates.UP_TO_DATE)
        pt.setCell(3, sn[2], CellStates.UP_TO_DATE)
        pt.setCell(3, sn[3], CellStates.UP_TO_DATE)
        # part 4
        pt.setCell(4, sn[0], CellStates.UP_TO_DATE)
        pt.setCell(4, sn[4], CellStates.UP_TO_DATE)

        count_dict = defaultdict(int)
        change_list = self.tweak(pt)
        for offset, uuid, state in change_list:
            count_dict[state] += 1
        self.assertEqual(count_dict, {CellStates.DISCARDED: 3,
                                      CellStates.OUT_OF_DATE: 5,
                                      CellStates.UP_TO_DATE: 3})
        self.update(pt, change_list)
        self.checkPT(pt)

        self.assertRaises(PartitionTableException, pt.dropNodeList, sn[1:4])
        self.assertEqual(6, len(pt.dropNodeList(sn[1:3], True)))
        self.assertEqual(3, len(pt.dropNodeList([sn[1]])))
        pt.addNodeList([sn[1]])
        change_list = self.tweak(pt)
        self.assertEqual(3, len(change_list))
        self.update(pt, change_list)
        self.checkPT(pt)

        for np, i in (12, 0), (12, 1), (13, 2):
            pt = PartitionTable(np, i)
            i += 1
            pt.make(sn[:i])
            for n in sn[i:i+3]:
                self.assertEqual([n], pt.addNodeList([n]))
                self.update(pt, self.tweak(pt))
                self.checkPT(pt)
            pt.clear()
            pt.make(sn[:i])
            for n in sn[i:i+3]:
                self.assertEqual([n], pt.addNodeList([n]))
                self.tweak(pt)
            self.update(pt)
            self.checkPT(pt)

        pt = PartitionTable(7, 0)
        pt.make(sn[:1])
        pt.addNodeList(sn[1:3])
        self.update(pt, self.tweak(pt, sn[:1]))
        self.checkPT(pt, True)


if __name__ == '__main__':
    unittest.main()

