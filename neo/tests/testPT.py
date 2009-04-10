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

import unittest, os
from mock import Mock
from neo.protocol import UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, \
        DISCARDED_STATE, RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        BROKEN_STATE, INVALID_UUID
from neo.pt import Cell, PartitionTable
from neo.node import StorageNode


class testPartitionTable(unittest.TestCase):

    def setUp(self):
        pass
    def tearDown(self):
        pass

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def test_01_Cell(self):
        uuid = self.getNewUUID()
        server = ("127.0.0.1", 19001)
        sn = StorageNode(server, uuid)
        cell = Cell(sn)
        self.assertEquals(cell.node, sn)
        self.assertEquals(cell.state, UP_TO_DATE_STATE)
        cell = Cell(sn, OUT_OF_DATE_STATE)
        self.assertEquals(cell.node, sn)
        self.assertEquals(cell.state, OUT_OF_DATE_STATE)
        # check getter
        self.assertEquals(cell.getNode(), sn)
        self.assertEquals(cell.getState(), OUT_OF_DATE_STATE)
        self.assertEquals(cell.getNodeState(), RUNNING_STATE)
        self.assertEquals(cell.getUUID(), uuid)
        self.assertEquals(cell.getServer(), server)
        # check state setter
        cell.setState(FEEDING_STATE)
        self.assertEquals(cell.getState(), FEEDING_STATE)


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
            self.failUnless(isinstance(part, list))
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

    def test_03_setCell(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        # add a cell to an empty row
        self.assertFalse(pt.count_dict.has_key(sn1))
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 1)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # try to add to an unexistant partition
        self.assertRaises(IndexError, pt.setCell, 10, sn1, UP_TO_DATE_STATE)
        # if we add in discardes state, must be removed
        pt.setCell(0, sn1, DISCARDED_STATE)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        # add a feeding node into empty row
        pt.setCell(0, sn1, FEEDING_STATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), FEEDING_STATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # re-add it as feeding, nothing change
        pt.setCell(0, sn1, FEEDING_STATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 0)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), FEEDING_STATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)
        # now add it as up to date
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        self.assertTrue(pt.count_dict.has_key(sn1))
        self.assertEqual(pt.count_dict[sn1], 1)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.partition_list[x]), 1)
                cell = pt.partition_list[x][0]
                self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
            else:
                self.assertEqual(len(pt.partition_list[x]), 0)

        # now add broken and down state, must not be taken into account
        pt.setCell(0, sn1, DISCARDED_STATE)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        sn1.setState(BROKEN_STATE)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        sn1.setState(DOWN_STATE)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        self.assertEqual(pt.count_dict[sn1], 0)
        
                
    def test_04_removeCell(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            self.assertEqual(len(pt.partition_list[x]), 0)
        # add a cell to an empty row
        self.assertFalse(pt.count_dict.has_key(sn1))
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
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
        pt.setCell(0, sn1, FEEDING_STATE)
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
        # add two kind of node, usable and unsable
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        pt.setCell(0, sn2, OUT_OF_DATE_STATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        pt.setCell(0, sn3, FEEDING_STATE)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(server4, uuid4)
        pt.setCell(0, sn4, DISCARDED_STATE) # won't be added
        # now checks result
        self.assertEqual(len(pt.partition_list[0]), 3)
        for x in xrange(num_partitions):
            if x == 0:
                self.assertEqual(len(pt.getCellList(0, False)), 3)
                all_cell = pt.getCellList(0, False)
                all_nodes = [x.getNode() for x in all_cell]
                self.failUnless(sn1 in all_nodes)
                self.failUnless(sn2 in all_nodes)
                self.failUnless(sn3 in all_nodes)
                self.failUnless(sn4 not in all_nodes)
                self.assertEqual(len(pt.getCellList(0, True)), 2)
                all_cell = pt.getCellList(0, True)
                all_nodes = [x.getNode() for x in all_cell]
                self.failUnless(sn1 in all_nodes)
                self.failUnless(sn2 not in all_nodes)
                self.failUnless(sn3 in all_nodes)
                self.failUnless(sn4 not in all_nodes)
            else:
                self.assertEqual(len(pt.getCellList(1, False)), 0)
                self.assertEqual(len(pt.getCellList(1, True)), 0)

    def test_06_clear(self):
        # add some nodes
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unsable
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        pt.setCell(1, sn2, OUT_OF_DATE_STATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        pt.setCell(2, sn3, FEEDING_STATE)
        # now checks result
        self.assertEqual(len(pt.partition_list[0]), 1)
        self.assertEqual(len(pt.partition_list[1]), 1)
        self.assertEqual(len(pt.partition_list[2]), 1)
        pt.clear()
        partition_list = pt.partition_list
        self.assertEqual(len(partition_list), num_partitions)
        for x in xrange(num_partitions):
            part = partition_list[x]
            self.failUnless(isinstance(part, list))
            self.assertEqual(len(part), 0)
        self.assertEqual(len(pt.count_dict), 0)

    def test_07_getNodeList(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unsable
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        pt.setCell(0, sn2, OUT_OF_DATE_STATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        pt.setCell(0, sn3, FEEDING_STATE)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(server4, uuid4)
        pt.setCell(0, sn4, DISCARDED_STATE) # won't be added
        # must get only two node as feeding and discarded not taken
        # into account
        self.assertEqual(len(pt.getNodeList()), 2)
        nodes = pt.getNodeList()
        self.failUnless(sn1 in nodes)
        self.failUnless(sn2 in nodes)        
        self.failUnless(sn3 not in nodes)
        self.failUnless(sn4 not in nodes)

    def test_08_filled(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        self.assertEqual(pt.np, num_partitions)
        self.assertEqual(pt.num_filled_rows, 0)
        self.assertFalse(pt.filled())
        # adding a node in all partition
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            pt.setCell(x, sn1, UP_TO_DATE_STATE)            
        self.assertEqual(pt.num_filled_rows, num_partitions)        
        self.assertTrue(pt.filled())

    def test_09_hasOffset(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add two kind of node, usable and unsable
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        # now test
        self.assertTrue(pt.hasOffset(0))
        self.assertFalse(pt.hasOffset(1))
        # unknonw partition
        self.assertFalse(pt.hasOffset(50))

    def test_10_operational(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            pt.setCell(x, sn1, UP_TO_DATE_STATE)
        self.assertTrue(pt.filled())
        # it's up to date and running, so operational
        self.assertTrue(pt.operational())
        # same with feeding state
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            pt.setCell(x, sn1, FEEDING_STATE)
        self.assertTrue(pt.filled())
        # it's up to date and running, so operational
        self.assertTrue(pt.operational())

        # same with feeding state but non running node
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        sn1.setState(TEMPORARILY_DOWN_STATE)
        for x in xrange(num_partitions):
            pt.setCell(x, sn1, FEEDING_STATE)
        self.assertTrue(pt.filled())
        # it's up to date and not running, so not operational
        self.assertFalse(pt.operational())

        # same with out of date state and running
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        # adding a node in all partition
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        for x in xrange(num_partitions):
            pt.setCell(x, sn1, OUT_OF_DATE_STATE)
        self.assertTrue(pt.filled())
        # it's not up to date and running, so not operational
        self.assertFalse(pt.operational())

    def test_11_findLeastUsedNode(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(1, sn2, UP_TO_DATE_STATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        pt.setCell(0, sn3, UP_TO_DATE_STATE)
        # test
        node = pt.findLeastUsedNode()
        self.assertEqual(node, sn3)
        node = pt.findLeastUsedNode((sn3,))
        self.assertEqual(node, sn2)
        node = pt.findLeastUsedNode((sn3,sn2))
        self.assertEqual(node, sn1)

    def test_12_getRow(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(1, sn2, UP_TO_DATE_STATE)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        pt.setCell(0, sn3, UP_TO_DATE_STATE)
        # test
        row_0 = pt.getRow(0)
        self.assertEqual(len(row_0), 3)
        for uuid, state in row_0:
            self.failUnless(uuid in (sn1.getUUID(), sn2.getUUID(), sn3.getUUID()))
            self.assertEqual(state, UP_TO_DATE_STATE)
        row_1 = pt.getRow(1)
        self.assertEqual(len(row_1), 2)
        for uuid, state in row_1:
            self.failUnless(uuid in (sn1.getUUID(), sn2.getUUID()))
            self.assertEqual(state, UP_TO_DATE_STATE)
        row_2 = pt.getRow(2)
        self.assertEqual(len(row_2), 1)
        for uuid, state in row_2:
            self.assertEqual(uuid, sn1.getUUID())
            self.assertEqual(state, UP_TO_DATE_STATE)
        row_3 = pt.getRow(3)
        self.assertEqual(len(row_3), 0)
        row_4 = pt.getRow(4)
        self.assertEqual(len(row_4), 0)
        # unknwon row
        self.assertRaises(IndexError,  pt.getRow, 5)

    def test_13_outdate(self):
        # create nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(server2, uuid2)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = StorageNode(server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = StorageNode(server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 19005)
        sn5 = StorageNode(server5, uuid5)
        # create partition table
        num_partitions = 5
        num_replicas = 3
        pt = PartitionTable(num_partitions, num_replicas)
        pt.setCell(0, sn1, OUT_OF_DATE_STATE)
        sn1.setState(RUNNING_STATE)
        pt.setCell(1, sn2, UP_TO_DATE_STATE)
        sn2.setState(TEMPORARILY_DOWN_STATE)
        pt.setCell(2, sn3, UP_TO_DATE_STATE)
        sn3.setState(DOWN_STATE)
        pt.setCell(3, sn4, UP_TO_DATE_STATE)
        sn4.setState(BROKEN_STATE)
        pt.setCell(4, sn5, UP_TO_DATE_STATE)
        sn5.setState(RUNNING_STATE)
        # outdate nodes
        cells_outdated = pt.outdate()
        self.assertEqual(len(cells_outdated), 3)
        for offset, uuid, state in cells_outdated:
            self.failUnless(offset in (1,2,3))
            self.failUnless(uuid in (uuid2,uuid3,uuid4))
            self.assertEqual(state, OUT_OF_DATE_STATE)
        # check each cell
        # part 1, already outdated
        cells = pt.getCellList(0)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        # part 2, must be outdated
        cells = pt.getCellList(1)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        # part 3, must be outdated
        cells = pt.getCellList(2)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        # part 4, already outdated
        cells = pt.getCellList(3)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        # part 5, remains running
        cells = pt.getCellList(4)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
        
    def test_14_addNode(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        # add it to an empty pt
        cell_list = pt.addNode(sn1)
        self.assertEqual(len(cell_list), 5)
        # it must be added to all partitions
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 1)
            self.assertEqual(pt.getCellList(x)[0].getState(), OUT_OF_DATE_STATE)
            self.assertEqual(pt.getCellList(x)[0].getNode(), sn1)
        self.assertEqual(pt.count_dict[sn1], 5)
        # add same node again, must remain the same
        cell_list = pt.addNode(sn1)
        self.assertEqual(len(cell_list), 0)
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 1)
            self.assertEqual(pt.getCellList(x)[0].getState(), OUT_OF_DATE_STATE)
            self.assertEqual(pt.getCellList(x)[0].getNode(), sn1)
        self.assertEqual(pt.count_dict[sn1], 5)
        # add a second node to fill the partition table
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(server2, uuid2)
        # add it
        cell_list = pt.addNode(sn2)
        self.assertEqual(len(cell_list), 5)
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)
            self.assertEqual(pt.getCellList(x)[0].getState(), OUT_OF_DATE_STATE)
            self.failUnless(pt.getCellList(x)[0].getNode() in (sn1, sn2))
        # test the most used node is remove from some partition
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 1900)
        sn5 = StorageNode(server5, uuid5)
        # partition looks like:
        # 0 : sn1, sn2
        # 1 : sn1, sn3
        # 2 : sn1, sn4
        # 3 : sn1, sn5
        num_partitions = 4
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # node most used is out of date, just dropped
        pt.setCell(0, sn1, OUT_OF_DATE_STATE)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(1, sn1, OUT_OF_DATE_STATE)
        pt.setCell(1, sn3, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, OUT_OF_DATE_STATE)
        pt.setCell(2, sn4, UP_TO_DATE_STATE)
        pt.setCell(3, sn1, OUT_OF_DATE_STATE)
        pt.setCell(3, sn5, UP_TO_DATE_STATE)
        uuid6 = self.getNewUUID()
        server6 = ("127.0.0.6", 19006)
        sn6 = StorageNode(server6, uuid6)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0,1):
                if uuid == uuid1:
                    self.assertEqual(state, DISCARDED_STATE)
                elif uuid == uuid6:
                    self.assertEqual(state, OUT_OF_DATE_STATE)
                else:
                    self.failUnless(uuid in (uuid1, uuid6))
            else:
                self.failUnless(offset in (0, 1))            
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)        
        # there is a feeding cell, just dropped
        pt.clear()
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(0, sn3, FEEDING_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn2, FEEDING_STATE)
        pt.setCell(1, sn3, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn4, FEEDING_STATE)
        pt.setCell(2, sn5, UP_TO_DATE_STATE)
        pt.setCell(3, sn1, UP_TO_DATE_STATE)
        pt.setCell(3, sn4, UP_TO_DATE_STATE)
        pt.setCell(3, sn5, FEEDING_STATE)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0,1):
                if uuid == uuid1:
                    self.assertEqual(state, DISCARDED_STATE)
                elif uuid == uuid6:
                    self.assertEqual(state, OUT_OF_DATE_STATE)
                else:
                    self.failUnless(uuid in (uuid1, uuid6))
            else:
                self.failUnless(offset in (0, 1))            
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 3)        
        # there is no feeding cell, marked as feeding
        pt.clear()
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn3, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn4, UP_TO_DATE_STATE)
        pt.setCell(3, sn1, UP_TO_DATE_STATE)
        pt.setCell(3, sn5, UP_TO_DATE_STATE)
        cell_list = pt.addNode(sn6)
        # sn1 is removed twice and sn6 is added twice
        self.assertEqual(len(cell_list), 4)
        for offset, uuid, state  in cell_list:
            if offset in (0,1):
                if uuid == uuid1:
                    self.assertEqual(state, FEEDING_STATE)
                elif uuid == uuid6:
                    self.assertEqual(state, OUT_OF_DATE_STATE)
                else:
                    self.failUnless(uuid in (uuid1, uuid6))
            else:
                self.failUnless(offset in (0, 1))            
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 3)
        
    def test_15_dropNode(self):
        num_partitions = 4
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(server2, uuid2)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(server4, uuid4)
        # partition looks like:
        # 0 : sn1, sn2
        # 1 : sn1, sn3
        # 2 : sn1, sn3
        # 3 : sn1, sn4
        # node is not feeding, so retrive least use node to replace it
        # so sn2 must be repaced by sn4 in partition 0
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn3, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn3, UP_TO_DATE_STATE)
        pt.setCell(3, sn1, UP_TO_DATE_STATE)
        pt.setCell(3, sn4, UP_TO_DATE_STATE)
        cell_list = pt.dropNode(sn2)
        self.assertEqual(len(cell_list), 2)
        for offset, uuid, state  in cell_list:
            self.assertEqual(offset, 0)
            if uuid == uuid2:
                self.assertEqual(state, DISCARDED_STATE)
            elif uuid == uuid4:
                self.assertEqual(state, OUT_OF_DATE_STATE)
            else:
                self.failUnless(uuid in (uuid2, uuid4))
                
        for x in xrange(num_replicas):
            self.assertEqual(len(pt.getCellList(x)), 2)
        # same test but with feeding state, no other will be added
        pt.clear()
        pt.setCell(0, sn1, UP_TO_DATE_STATE)
        pt.setCell(0, sn2, FEEDING_STATE)
        pt.setCell(1, sn1, UP_TO_DATE_STATE)
        pt.setCell(1, sn3, UP_TO_DATE_STATE)
        pt.setCell(2, sn1, UP_TO_DATE_STATE)
        pt.setCell(2, sn3, UP_TO_DATE_STATE)
        pt.setCell(3, sn1, UP_TO_DATE_STATE)
        pt.setCell(3, sn4, UP_TO_DATE_STATE)
        cell_list = pt.dropNode(sn2)
        self.assertEqual(len(cell_list), 1)
        for offset, uuid, state  in cell_list:
            self.assertEqual(offset, 0)
            self.assertEqual(state, DISCARDED_STATE)
            self.assertEqual(uuid ,uuid2)
        for x in xrange(num_replicas):
            if x == 0:
                self.assertEqual(len(pt.getCellList(x)), 1)
            else:
                self.assertEqual(len(pt.getCellList(x)), 2)
        
    def test_16_make(self):
        num_partitions = 5
        num_replicas = 2
        pt = PartitionTable(num_partitions, num_replicas)
        # add nodes
        uuid1 = self.getNewUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = StorageNode(server1, uuid1)
        # add not running node
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19001)
        sn2 = StorageNode(server2, uuid2)
        sn2.setState(TEMPORARILY_DOWN_STATE)
        # add node without uuid
        server3 = ("127.0.0.3", 19001)
        sn3 = StorageNode(server3, None)
        # add clear node
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19001)
        sn4 = StorageNode(server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 1900)
        sn5 = StorageNode(server5, uuid5)
        # make the table
        pt.make([sn1, sn2, sn3, sn4, sn5,])
        # check it's ok, only running nodes and node with uuid
        # must be present
        for x in xrange(num_partitions):
            cells = pt.getCellList(x)
            self.assertEqual(len(cells), 2)
            nodes = [x.getNode() for x in cells]
            for node in nodes:
                self.failUnless(node in (sn1, sn4, sn5))
                self.failUnless(node not in (sn2, sn3))
        self.assertTrue(pt.filled())
        self.assertTrue(pt.operational())
        # create a pt with less nodes
        pt.clear()
        self.assertFalse(pt.filled())
        self.assertFalse(pt.operational())
        pt.make([sn1,])
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
        sn1 = StorageNode(server1, uuid1)
        uuid2 = self.getNewUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = StorageNode(server2, uuid2)
        uuid3 = self.getNewUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = StorageNode(server3, uuid3)
        uuid4 = self.getNewUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = StorageNode(server4, uuid4)
        uuid5 = self.getNewUUID()
        server5 = ("127.0.0.5", 19005)
        sn5 = StorageNode(server5, uuid5)
        # create partition table
        # 0 : sn1(discarded), sn2(up), -> sn2 must remain
        # 1 : sn1(feeding), sn2(feeding), sn3(up) -> one feeding and sn3 must remain
        # 2 : sn1(feeding), sn2(up), sn3(up) -> sn2 and sn3 must remain, feeding must go away
        # 3 : sn1(up), sn2(up), sn3(up), sn4(up) -> only 3 cell must remain
        # 4 : sn1(up), sn5(up) -> one more cell must be added
        num_partitions = 5
        num_replicas = 3
        pt = PartitionTable(num_partitions, num_replicas)
        # part 0
        pt.setCell(0, sn1, DISCARDED_STATE)
        pt.setCell(0, sn2, UP_TO_DATE_STATE)
        # part 1
        pt.setCell(1, sn1, FEEDING_STATE)
        pt.setCell(1, sn2, FEEDING_STATE)
        pt.setCell(1, sn3, OUT_OF_DATE_STATE)
        # part 2
        pt.setCell(2, sn1, FEEDING_STATE)
        pt.setCell(2, sn2, UP_TO_DATE_STATE)
        pt.setCell(2, sn3, UP_TO_DATE_STATE)
        # part 3
        pt.setCell(3, sn1, UP_TO_DATE_STATE)
        pt.setCell(3, sn2, UP_TO_DATE_STATE)
        pt.setCell(3, sn3, UP_TO_DATE_STATE)
        pt.setCell(3, sn4, UP_TO_DATE_STATE)
        # part 4
        pt.setCell(4, sn1, UP_TO_DATE_STATE)
        pt.setCell(4, sn5, UP_TO_DATE_STATE)
        # now tweak the table
        pt.tweak()
        # check part 1
        cells =  pt.getCellList(0)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            self.assertNotEqual(cell.getState(), BROKEN_STATE)
            if cell.getNode() == sn2:
                self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
            else:
                self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        self.failUnless(sn2 in [x.getNode() for x in cells])
        
        # check part 2
        cells =  pt.getCellList(1)
        self.assertEqual(len(cells), 4)
        for cell in cells:
            if cell.getNode() == sn1:
                self.assertEqual(cell.getState(), FEEDING_STATE)
            else:
                self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        self.failUnless(sn3 in [x.getNode() for x in cells])
        self.failUnless(sn1 in [x.getNode() for x in cells])
        
        # check part 3
        cells =  pt.getCellList(2)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            if cell.getNode() in (sn2, sn3):
                self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
            else:
                self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        self.failUnless(sn3 in [x.getNode() for x in cells])
        self.failUnless(sn2 in [x.getNode() for x in cells])

        # check part 4
        cells =  pt.getCellList(3)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            self.assertEqual(cell.getState(), UP_TO_DATE_STATE)

        # check part 5
        cells =  pt.getCellList(4)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            if cell.getNode() in (sn1, sn5):
                self.assertEqual(cell.getState(), UP_TO_DATE_STATE)
            else:
                self.assertEqual(cell.getState(), OUT_OF_DATE_STATE)
        self.failUnless(sn1 in [x.getNode() for x in cells])
        self.failUnless(sn5 in [x.getNode() for x in cells])


if __name__ == '__main__':
    unittest.main()

