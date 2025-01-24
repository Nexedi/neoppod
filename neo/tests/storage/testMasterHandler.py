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

from neo import *
from .. import Mock, MockObject, NeoUnitTestBase
from neo.storage.app import Application
from neo.storage.handlers.master import MasterOperationHandler
from neo.lib.exception import ProtocolError
from neo.lib.protocol import CellStates
from neo.lib.pt import PartitionTable

class StorageMasterHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
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

    def test_notifyPartitionChanges(self):
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
        app.pt = PartitionTable(3, 1)
        app.pt._id = 1
        ptid = 2
        app.dm.close()
        app.dm = Mock()
        app.replicator = Mock()
        args = ptid, 1, cells
        self.operation.notifyPartitionChanges(conn, *args)
        self.assertEqual(app.pt.getID(), ptid)
        m = app.dm.changePartitionTable
        m.assert_called_once_with(app, *args)
        m.reset_mock()
        self.assertRaises(ProtocolError, self.operation.notifyPartitionChanges,
                          conn, *args)
        self.assertEqual(app.pt.getID(), ptid)
        m.assert_not_called()
