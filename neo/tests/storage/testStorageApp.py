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

import unittest
from ..mock import Mock
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.lib.protocol import CellStates
from neo.lib.pt import PartitionTable

class StorageAppTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageAppTests, self)._tearDown(success)

    def test_01_loadPartitionTable(self):
        self.app.dm = Mock({
            'getPartitionTable': [],
        })
        self.assertEqual(self.app.pt, None)
        num_partitions = 3
        num_replicas = 2
        self.app.pt = PartitionTable(num_partitions, num_replicas)
        self.assertFalse(self.app.pt.getNodeSet())
        self.assertFalse(self.app.pt.filled())
        for x in xrange(num_partitions):
            self.assertFalse(self.app.pt.hasOffset(x))

        # load an empty table
        self.app.loadPartitionTable()
        self.assertFalse(self.app.pt.getNodeSet())
        self.assertFalse(self.app.pt.filled())
        for x in xrange(num_partitions):
            self.assertFalse(self.app.pt.hasOffset(x))

        # add some node, will be remove when loading table
        master_uuid = self.getMasterUUID()
        master = self.app.nm.createMaster(uuid=master_uuid)
        storage_uuid = self.getStorageUUID()
        storage = self.app.nm.createStorage(uuid=storage_uuid)
        client_uuid = self.getClientUUID()

        self.app.pt._setCell(0, master, CellStates.UP_TO_DATE)
        self.app.pt._setCell(0, storage, CellStates.UP_TO_DATE)
        self.assertEqual(len(self.app.pt.getNodeSet()), 2)
        self.assertFalse(self.app.pt.filled())
        for x in xrange(num_partitions):
            if x == 0:
                self.assertTrue(self.app.pt.hasOffset(x))
            else:
                self.assertFalse(self.app.pt.hasOffset(x))
        # load an empty table, everything removed
        self.app.loadPartitionTable()
        self.assertFalse(self.app.pt.getNodeSet())
        self.assertFalse(self.app.pt.filled())
        for x in xrange(num_partitions):
            self.assertFalse(self.app.pt.hasOffset(x))

        # add some node
        self.app.pt._setCell(0, master, CellStates.UP_TO_DATE)
        self.app.pt._setCell(0, storage, CellStates.UP_TO_DATE)
        self.assertEqual(len(self.app.pt.getNodeSet()), 2)
        self.assertFalse(self.app.pt.filled())
        for x in xrange(num_partitions):
            if x == 0:
                self.assertTrue(self.app.pt.hasOffset(x))
            else:
                self.assertFalse(self.app.pt.hasOffset(x))
        # fill partition table
        self.app.dm = Mock({
            'getPartitionTable': [
                (0, client_uuid, CellStates.UP_TO_DATE),
                (1, client_uuid, CellStates.UP_TO_DATE),
                (1, storage_uuid, CellStates.UP_TO_DATE),
                (2, storage_uuid, CellStates.UP_TO_DATE),
                (2, master_uuid, CellStates.UP_TO_DATE),
            ],
            'getPTID': 1,
        })
        self.app.pt.clear()
        self.app.loadPartitionTable()
        self.assertTrue(self.app.pt.filled())
        for x in xrange(num_partitions):
            self.assertTrue(self.app.pt.hasOffset(x))
        # check each row
        cell_list = self.app.pt.getCellList(0)
        self.assertEqual(len(cell_list), 1)
        self.assertEqual(cell_list[0].getUUID(), client_uuid)
        cell_list = self.app.pt.getCellList(1)
        self.assertEqual(len(cell_list), 2)
        self.assertTrue(cell_list[0].getUUID() in (client_uuid, storage_uuid))
        self.assertTrue(cell_list[1].getUUID() in (client_uuid, storage_uuid))
        cell_list = self.app.pt.getCellList(2)
        self.assertEqual(len(cell_list), 2)
        self.assertTrue(cell_list[0].getUUID() in (master_uuid, storage_uuid))
        self.assertTrue(cell_list[1].getUUID() in (master_uuid, storage_uuid))

if __name__ == '__main__':
    unittest.main()

