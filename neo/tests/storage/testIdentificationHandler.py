#
# Copyright (C) 2009-2010  Nexedi SA
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
from neo.tests import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, NotReadyError, \
        BrokenNodeDisallowedError
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

    def tearDown(self):
        self.app.close()
        del self.app
        super(StorageIdentificationHandlerTests, self).tearDown()

    def test_requestIdentification1(self):
        """ nodes are rejected during election or if unknown storage """
        self.app.ready = False
        self.assertRaises(
                NotReadyError,
                self.identification.requestIdentification,
                self.getFakeConnection(),
                NodeTypes.CLIENT,
                self.getNewUUID(),
                None,
                self.app.name,
        )
        self.app.ready = True
        self.assertRaises(
                NotReadyError,
                self.identification.requestIdentification,
                self.getFakeConnection(),
                NodeTypes.STORAGE,
                self.getNewUUID(),
                None,
                self.app.name,
        )

    def test_requestIdentification3(self):
        """ broken nodes must be rejected """
        uuid = self.getNewUUID()
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
        )

    def test_requestIdentification2(self):
        """ accepted client must be connected and running """
        uuid = self.getNewUUID()
        conn = self.getFakeConnection(uuid=uuid)
        node = self.app.nm.createClient(uuid=uuid)
        self.identification.requestIdentification(conn, NodeTypes.CLIENT, uuid,
                None, self.app.name)
        self.assertTrue(node.isRunning())
        self.assertTrue(node.isConnected())
        self.assertEqual(node.getUUID(), uuid)
        self.assertTrue(node.getConnection() is conn)
        self.checkUUIDSet(conn, uuid)
        args = self.checkAcceptIdentification(conn, decode=True)
        node_type, address, _np, _nr, _uuid = args
        self.assertEqual(node_type, NodeTypes.STORAGE)
        self.assertEqual(address, None)
        self.assertEqual(_uuid, uuid)


if __name__ == "__main__":
    unittest.main()
