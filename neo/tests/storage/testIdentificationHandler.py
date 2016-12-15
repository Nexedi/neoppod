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
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, BrokenNodeDisallowedError
from neo.lib.pt import PartitionTable
from neo.storage.app import Application
from neo.storage.handlers.identification import IdentificationHandler

class StorageIdentificationHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(config)
        self.app.name = 'NEO'
        self.app.ready = True
        self.app.pt = PartitionTable(4, 1)
        self.identification = IdentificationHandler(self.app)

    def _tearDown(self, success):
        self.app.close()
        del self.app
        super(StorageIdentificationHandlerTests, self)._tearDown(success)

    def test_requestIdentification3(self):
        """ broken nodes must be rejected """
        uuid = self.getClientUUID()
        conn = self.getFakeConnection(uuid=uuid)
        node = self.app.nm.createClient(uuid=uuid)
        node.setBroken()
        self.assertRaises(BrokenNodeDisallowedError,
                self.identification.requestIdentification,
                conn,
                NodeTypes.CLIENT,
                uuid,
                None,
                self.app.name,
                None,
        )

if __name__ == "__main__":
    unittest.main()
