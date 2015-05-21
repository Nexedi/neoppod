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
from .. import NeoUnitTestBase
from neo.lib.pt import PartitionTable
from neo.storage.app import Application
from neo.storage.handlers.initialization import InitializationHandler
from neo.lib.protocol import CellStates
from neo.lib.exception import PrimaryFailure

class StorageInitializationHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.verification = InitializationHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.client_port = 11011
        self.num_partitions = 1009
        self.num_replicas = 2
        self.app.operational = False
        self.app.load_lock_dict = {}
        self.app.pt = PartitionTable(self.num_partitions, self.num_replicas)

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageInitializationHandlerTests, self)._tearDown(success)

    def getClientConnection(self):
        address = ("127.0.0.1", self.client_port)
        return self.getFakeConnection(uuid=self.getClientUUID(),
                                      address=address)

    def test_03_connectionClosed(self):
        conn = self.getClientConnection()
        self.app.listening_conn = object() # mark as running
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_09_answerPartitionTable(self):
        # send a table
        conn = self.getClientConnection()
        self.app.pt = PartitionTable(3, 2)
        node_1 = self.getStorageUUID()
        node_2 = self.getStorageUUID()
        node_3 = self.getStorageUUID()
        self.app.uuid = node_1
        # SN already know all nodes
        self.app.nm.createStorage(uuid=node_1)
        self.app.nm.createStorage(uuid=node_2)
        self.app.nm.createStorage(uuid=node_3)
        self.assertFalse(list(self.app.dm.getPartitionTable()))
        row_list = [(0, ((node_1, CellStates.UP_TO_DATE), (node_2, CellStates.UP_TO_DATE))),
                    (1, ((node_3, CellStates.UP_TO_DATE), (node_1, CellStates.UP_TO_DATE))),
                    (2, ((node_2, CellStates.UP_TO_DATE), (node_3, CellStates.UP_TO_DATE)))]
        self.assertFalse(self.app.pt.filled())
        # send a complete new table and ack
        self.verification.answerPartitionTable(conn, 2, row_list)
        self.assertTrue(self.app.pt.filled())
        self.assertEqual(self.app.pt.getID(), 2)
        self.assertTrue(list(self.app.dm.getPartitionTable()))


if __name__ == "__main__":
    unittest.main()

