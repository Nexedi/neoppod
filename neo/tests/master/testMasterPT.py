#
# Copyright (C) 2009-2019  Nexedi SA
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

import random, time, unittest
from collections import Counter, defaultdict
from .. import NeoUnitTestBase
from neo.lib import logging
from neo.lib.protocol import NodeStates, CellStates
from neo.lib.pt import PartitionTableException
from neo.master.pt import PartitionTable

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
        self.assertRaises(AssertionError, pt.make, [])
        self.assertFalse(pt.operational())
        self.assertFalse(pt.filled())

    def test_13_outdate(self):
        # create nodes
        uuid1 = self.getStorageUUID()
        server1 = ("127.0.0.1", 19001)
        sn1 = self.createStorage(server1, uuid1)
        uuid2 = self.getStorageUUID()
        server2 = ("127.0.0.2", 19002)
        sn2 = self.createStorage(server2, uuid2)
        uuid3 = self.getStorageUUID()
        server3 = ("127.0.0.3", 19003)
        sn3 = self.createStorage(server3, uuid3)
        uuid4 = self.getStorageUUID()
        server4 = ("127.0.0.4", 19004)
        sn4 = self.createStorage(server4, uuid4)
        # create partition table
        num_partitions = 4
        num_replicas = 3
        pt = PartitionTable(num_partitions, num_replicas)
        pt._setCell(0, sn1, CellStates.OUT_OF_DATE)
        sn1.setState(NodeStates.RUNNING)
        pt._setCell(1, sn2, CellStates.UP_TO_DATE)
        sn2.setState(NodeStates.DOWN)
        pt._setCell(2, sn3, CellStates.UP_TO_DATE)
        sn3.setState(NodeStates.UNKNOWN)
        pt._setCell(3, sn4, CellStates.UP_TO_DATE)
        sn4.setState(NodeStates.RUNNING)
        # outdate nodes
        cells_outdated = pt.outdate()
        self.assertEqual(len(cells_outdated), 2)
        for offset, uuid, state in cells_outdated:
            self.assertIn(offset, (1, 2))
            self.assertIn(uuid, (uuid2, uuid3))
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
        # part 4, remains running
        cells = pt.getCellList(3)
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)

    def test_15_dropNodeList(self):
        sn = [self.createStorage(None, i + 1, NodeStates.RUNNING)
              for i in xrange(3)]
        pt = PartitionTable(3, 0)
        pt._setCell(0, sn[0], CellStates.OUT_OF_DATE)
        pt._setCell(1, sn[1], CellStates.FEEDING)
        pt._setCell(1, sn[2], CellStates.OUT_OF_DATE)
        pt._setCell(2, sn[0], CellStates.OUT_OF_DATE)
        pt._setCell(2, sn[1], CellStates.FEEDING)
        pt._setCell(2, sn[2], CellStates.UP_TO_DATE)

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
        pt._setCell(1, sn[2], CellStates.UP_TO_DATE)
        self.assertEqual(sorted(pt.dropNodeList(sn[1:2])), [
            (1, 2, CellStates.DISCARDED),
            (2, 2, CellStates.DISCARDED)])

        pt._setCell(0, sn[0], CellStates.UP_TO_DATE)
        self.assertEqual(self.tweak(pt), [(2, 3, CellStates.FEEDING)])

    def test_16_make(self):
        node_list = [self.createStorage(
                ("127.0.0.1", 19000 + i), self.getStorageUUID(),
                NodeStates.RUNNING)
            for i in xrange(4)]
        for np, nr, expected in (
                (3, 0, 'U..|.U.|..U'),
                (5, 1, 'UU..|..UU|UU..|..UU|UU..'),
                (9, 2, 'UUU.|UU.U|U.UU|.UUU|UUU.|UU.U|U.UU|.UUU|UUU.'),
                ):
            pt = PartitionTable(np, nr)
            pt.make(node_list)
            self.assertPartitionTable(pt, expected)
            self.assertTrue(pt.filled())
            self.assertTrue(pt.operational())
            # create a pt with less nodes
            pt.clear()
            self.assertFalse(pt.filled())
            self.assertFalse(pt.operational())
            pt.make(node_list[:1])
            self.assertPartitionTable(pt, '|'.join('U' * np))
            self.assertTrue(pt.filled())
            self.assertTrue(pt.operational())

    def update(self, pt, change_list=None):
        offset_list = xrange(pt.np)
        for node in pt.count_dict:
            pt.updatable(node.getUUID(), offset_list)
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
        pt.log()

    def tweak(self, pt, drop_list=()):
        change_list = pt.tweak(drop_list)
        pt.log()
        self.assertFalse(pt.tweak(drop_list))
        return change_list

    def test_17_tweak(self):
        sn = [self.createStorage(None, i + 1, NodeStates.RUNNING)
              for i in xrange(5)]
        pt = PartitionTable(5, 2)
        pt.setID(1)
        # part 0
        pt._setCell(0, sn[0], CellStates.DISCARDED)
        pt._setCell(0, sn[1], CellStates.UP_TO_DATE)
        # part 1
        pt._setCell(1, sn[0], CellStates.FEEDING)
        pt._setCell(1, sn[1], CellStates.FEEDING)
        pt._setCell(1, sn[2], CellStates.OUT_OF_DATE)
        # part 2
        pt._setCell(2, sn[0], CellStates.FEEDING)
        pt._setCell(2, sn[1], CellStates.UP_TO_DATE)
        pt._setCell(2, sn[2], CellStates.UP_TO_DATE)
        # part 3
        pt._setCell(3, sn[0], CellStates.UP_TO_DATE)
        pt._setCell(3, sn[1], CellStates.UP_TO_DATE)
        pt._setCell(3, sn[2], CellStates.UP_TO_DATE)
        pt._setCell(3, sn[3], CellStates.UP_TO_DATE)
        # part 4
        pt._setCell(4, sn[0], CellStates.UP_TO_DATE)
        pt._setCell(4, sn[4], CellStates.UP_TO_DATE)

        count_dict = defaultdict(int)
        self.assertPartitionTable(pt, (
            '.U...',
            'FFO..',
            'FUU..',
            'UUUU.',
            'U...U'))
        change_list = self.tweak(pt)
        self.assertPartitionTable(pt, (
            '.UO.O',
            'UU.O.',
            'UFU.O',
            '.UUU.',
            'U..OU'))
        for offset, uuid, state in change_list:
            count_dict[state] += 1
        self.assertEqual(count_dict, {CellStates.DISCARDED: 2,
                                      CellStates.FEEDING: 1,
                                      CellStates.OUT_OF_DATE: 5,
                                      CellStates.UP_TO_DATE: 3})
        self.update(pt)
        self.assertPartitionTable(pt, (
            '.UU.U',
            'UU.U.',
            'U.U.U',
            '.UUU.',
            'U..UU'))
        self.assertRaises(PartitionTableException, pt.dropNodeList, sn[1:4])
        self.assertEqual(6, len(pt.dropNodeList(sn[1:3], True)))
        self.assertEqual(3, len(pt.dropNodeList([sn[1]])))
        pt.addNodeList([sn[1]])
        self.assertPartitionTable(pt, (
            '..U.U',
            'U..U.',
            'U.U.U',
            '..UU.',
            'U..UU'))
        change_list = self.tweak(pt)
        self.assertPartitionTable(pt, (
            '.OU.U',
            'UO.U.',
            'U.U.U',
            '.OUU.',
            'U..UU'))
        self.assertEqual(3, len(change_list))
        self.update(pt, change_list)

        for np, i, expected in (
                (12, 0, ('U...|.U..|..U.|...U|'
                         'U...|.U..|..U.|...U|'
                         'U...|.U..|..U.|...U',)),
                (12, 1, ('UU...|..UU.|U...U|.UU..|...UU|'
                         'UU...|..UU.|U...U|.UU..|...UU|'
                         'UU...|..UU.',)),
                (13, 2, ('U.UU.|.U.UU|UUU..|..UUU|UU..U|'
                         'U.UU.|.U.UU|UUU..|..UUU|UU..U|'
                         'U.UU.|.U.UU|UUU..',
                         'UUU..|U..UU|.UUU.|UU..U|..UUU|'
                         'UUU..|U..UU|.UUU.|UU..U|..UUU|'
                         'UUU..|U..UU|.UUU.')),
                ):
            pt = PartitionTable(np, i)
            i += 1
            pt.make(sn[:i])
            pt.log()
            for n in sn[i:i+3]:
                self.assertEqual([n], pt.addNodeList([n]))
                self.update(pt, self.tweak(pt))
            self.assertPartitionTable(pt, expected[0])
            pt.clear()
            pt.make(sn[:i])
            for n in sn[i:i+3]:
                self.assertEqual([n], pt.addNodeList([n]))
                self.tweak(pt)
            self.update(pt)
            self.assertPartitionTable(pt, expected[-1])

        pt = PartitionTable(7, 0)
        pt.make(sn[:1])
        pt.addNodeList(sn[1:3])
        self.assertPartitionTable(pt, 'U..|U..|U..|U..|U..|U..|U..')
        self.update(pt, self.tweak(pt, sn[:1]))
        # See note in PartitionTable.tweak() about drop_list.
        #self.assertPartitionTable(pt,'.U.|..U|.U.|..U|.U.|..U|.U.')
        self.assertPartitionTable(pt, 'UU.|U.U|UU.|U.U|UU.|U.U|UU.')

    def test_18_tweakBigPT(self):
        seed = repr(time.time())
        logging.info("using seed %r", seed)
        sn_count = 11
        sn = [self.createStorage(None, i + 1, NodeStates.RUNNING)
              for i in xrange(sn_count)]
        for topo in 0, 1:
            r = random.Random(seed)
            if topo:
                for i, s in enumerate(sn, sn_count):
                    s.devpath = str(i % 5),
            pt = PartitionTable(1000, 2)
            pt.setID(1)
            for offset in xrange(pt.np):
                state = CellStates.UP_TO_DATE
                k = r.randrange(1, sn_count)
                for s in r.sample(sn, k):
                    pt._setCell(offset, s, state)
                    if k * r.random() < 1:
                        state = CellStates.OUT_OF_DATE
            pt.log()
            self.tweak(pt)
            self.update(pt)

    def test_19_topology(self):
        sn_count = 16
        sn = [self.createStorage(None, i + 1, NodeStates.RUNNING)
              for i in xrange(sn_count)]
        pt = PartitionTable(48, 2)
        pt.make(sn)
        pt.log()
        for i, s in enumerate(sn, sn_count):
            s.devpath = tuple(bin(i)[3:-1])
        self.assertEqual(Counter(x[2] for x in self.tweak(pt)), {
            CellStates.OUT_OF_DATE: 96,
            CellStates.FEEDING: 96,
        })
        self.update(pt)
        x = lambda n, *x: ('|'.join(x[:1]*n), '|'.join(x[1:]*n))
        for even, np, i, topo, expected in (
            ## Optimal topology.
            # All nodes have same number of cells.
            (1, 2, 2, ("00", "01", "02", "10", "11", "12"), ('UU...U|..UUU.',
                                                             'UU.U..|..U.UU')),
            (1, 7, 1, "0001122", (
                'U.....U|.U.U...|..U.U..|U....U.|.U....U|..UU...|....UU.',
                'U..U...|.U...U.|..U.U..|U.....U|.U.U...|..U..U.|....U.U')),
            (1, 4, 1, "00011122", ('U......U|.U.U....|..U.U...|.....UU.',
                                   'U..U....|.U..U...|..U...U.|.....U.U')),
            (1, 9, 1, "000111222", ('U.......U|.U.U.....|..U.U....|'
                                    '.....UU..|U......U.|.U......U|'
                                    '..UU.....|....U.U..|.....U.U.',
                                    'U..U.....|.U....U..|..U.U....|'
                                    '.....U.U.|U.......U|.U.U.....|'
                                    '..U...U..|....U..U.|.....U..U')),
            # Some nodes have a extra cell.
            (0, 8, 1, "0001122", ('U.....U|.U.U...|..U.U..|U....U.|'
                                  '.U....U|..UU...|....UU.|U.....U',
                                  'U..U...|.U...U.|..U.U..|U.....U|'
                                  '.U.U...|..U..U.|....U.U|U..U...')),
            ## Topology ignored.
            (1, 6, 1, ("00", "01", "1"), 'UU.|U.U|.UU|UU.|U.U|.UU'),
            (1, 5, 2, "01233", 'UUU..|U..UU|.UUU.|UU..U|..UUU'),
        ):
            assert len(topo) <= sn_count
            sn2 = sn[:len(topo)]
            for s in sn2:
                s.devpath = ()
            k = (1,7)[even]
            pt = PartitionTable(np*k, i)
            pt.make(sn2)
            for devpath, s in zip(topo, sn2):
                s.devpath = tuple(devpath)
            if type(expected) is tuple:
                self.assertTrue(self.tweak(pt))
                self.update(pt)
                self.assertPartitionTable(pt, '|'.join(expected[:1]*k))
                pt.clear()
                pt.make(sn2)
                self.assertPartitionTable(pt, '|'.join(expected[1:]*k))
                self.assertFalse(pt.tweak())
            else:
                expected = '|'.join((expected,)*k)
                self.assertFalse(pt.tweak())
                self.assertPartitionTable(pt, expected)
                pt.clear()
                pt.make(sn2)
                self.assertPartitionTable(pt, expected)

if __name__ == '__main__':
    unittest.main()

