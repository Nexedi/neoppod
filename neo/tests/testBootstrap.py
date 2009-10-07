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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import unittest
from mock import Mock
from neo.tests import NeoTestBase
from neo.storage.app import Application
from neo.bootstrap import BootstrapManager
from neo.protocol import NodeTypes

class BootstrapManagerTests(NeoTestBase):

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration()
        self.app = Application(**config)
        for address in self.app.master_node_list:
            self.app.nm.createMaster(address=address)
        self.bootstrap = BootstrapManager(self.app, 'main', NodeTypes.STORAGE)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.num_partitions = 1009
        self.num_replicas = 2
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid

    # Tests
    def testConnectionCompleted(self):
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.master_port)})
        self.bootstrap.connectionCompleted(conn)
        self.checkAskPrimary(conn)

    def testHandleNotReady(self):
        # the primary is not ready 
        conn = Mock({})
        packet = Mock({})
        self.bootstrap.notReady(conn, packet, '')
        self.checkClosed(conn)
        self.checkNoPacketSent(conn)

    
if __name__ == "__main__":
    unittest.main()

