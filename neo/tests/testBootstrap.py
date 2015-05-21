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
from . import NeoUnitTestBase
from neo.storage.app import Application
from neo.lib.bootstrap import BootstrapManager
from neo.lib.protocol import NodeTypes

class BootstrapManagerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration()
        self.app = Application(config)
        self.bootstrap = BootstrapManager(self.app, 'main', NodeTypes.STORAGE)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.num_partitions = 1009
        self.num_replicas = 2

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(BootstrapManagerTests, self)._tearDown(success)

    # Tests
    def testConnectionCompleted(self):
        address = ("127.0.0.1", self.master_port)
        conn = self.getFakeConnection(address=address)
        self.bootstrap.current = self.app.nm.createMaster(address=address)
        self.bootstrap.connectionCompleted(conn)
        self.checkRequestIdentification(conn)

    def testHandleNotReady(self):
        # the primary is not ready
        address = ("127.0.0.1", self.master_port)
        conn = self.getFakeConnection(address=address)
        self.bootstrap.current = self.app.nm.createMaster(address=address)
        self.bootstrap.notReady(conn, '')
        self.checkClosed(conn)
        self.checkNoPacketSent(conn)


if __name__ == "__main__":
    unittest.main()

