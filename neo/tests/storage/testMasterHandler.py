#
# Copyright (C) 2009-2016  Nexedi SA
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
from mock import Mock
from collections import deque
from .. import NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.master import MasterOperationHandler
from neo.lib.pt import PartitionTable
from neo.lib.protocol import CellStates

class StorageMasterHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.transaction_dict = {}
        self.app.store_lock_dict = {}
        self.app.load_lock_dict = {}
        self.app.event_queue = deque()
        # handler
        self.operation = MasterOperationHandler(self.app)
        # set pmn
        self.master_uuid = self.getMasterUUID()
        pmn = self.app.nm.getMasterList()[0]
        pmn.setUUID(self.master_uuid)
        self.app.primary_master_node = pmn
        self.master_port = 10010

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageMasterHandlerTests, self)._tearDown(success)

    def getMasterConnection(self):
        address = ("127.0.0.1", self.master_port)
        return self.getFakeConnection(uuid=self.master_uuid, address=address)

    def test_14_notifyPartitionChanges1(self):
        # old partition change -> do nothing
        app = self.app
        conn = self.getMasterConnection()
        app.replicator = Mock({})
        self.app.pt = Mock({'getID': 1})
        count = len(self.app.nm.getList())
        self.operation.notifyPartitionChanges(conn, 0, ())
        self.assertEqual(self.app.pt.getID(), 1)
        self.assertEqual(len(self.app.nm.getList()), count)
        calls = self.app.replicator.mockGetNamedCalls('removePartition')
        self.assertEqual(len(calls), 0)
        calls = self.app.replicator.mockGetNamedCalls('addPartition')
        self.assertEqual(len(calls), 0)

    def test_14_notifyPartitionChanges2(self):
        # cases :
        uuid1, uuid2, uuid3 = [self.getStorageUUID() for i in range(3)]
        cells = (
            (0, uuid1, CellStates.UP_TO_DATE),
            (1, uuid2, CellStates.DISCARDED),
            (2, uuid3, CellStates.OUT_OF_DATE),
        )
        # context
        conn = self.getMasterConnection()
        app = self.app
        # register nodes
        app.nm.createStorage(uuid=uuid1)
        app.nm.createStorage(uuid=uuid2)
        app.nm.createStorage(uuid=uuid3)
        ptid1, ptid2 = (1, 2)
        self.assertNotEqual(ptid1, ptid2)
        app.pt = PartitionTable(3, 1)
        app.dm = Mock({ })
        app.replicator = Mock({})
        self.operation.notifyPartitionChanges(conn, ptid2, cells)
        # ptid set
        self.assertEqual(app.pt.getID(), ptid2)
        # dm call
        calls = self.app.dm.mockGetNamedCalls('changePartitionTable')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(ptid2, cells)

if __name__ == "__main__":
    unittest.main()
