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
from neo.tests import NeoTestBase
from neo.master.app import Application
from neo.util import p64, u64

class MasterAppTests(NeoTestBase):

    def setUp(self):
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(config)
        self.app.pt.clear()

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def test_05_getNewOIDList(self):
        # must raise as we don"t have one
        self.assertEqual(self.app.loid, None)
        self.app.loid = None
        self.assertRaises(RuntimeError, self.app.getNewOIDList, 1)
        # ask list
        self.app.loid = p64(1)
        oid_list = self.app.getNewOIDList(15)
        self.assertEqual(len(oid_list), 15)
        i = 2
        # begin from 0, so generated oid from 1 to 15
        for oid in oid_list:
            self.assertEqual(u64(oid), i)
            i+=1

    def test_06_broadcastNodeInformation(self):
        # defined some nodes to which data will be send
        master_uuid = self.getNewUUID()
        master = self.app.nm.createMaster(uuid=master_uuid)
        storage_uuid = self.getNewUUID()
        storage = self.app.nm.createStorage(uuid=storage_uuid)
        client_uuid = self.getNewUUID()
        client = self.app.nm.createClient(uuid=client_uuid)
        # create conn and patch em
        master_conn = Mock()
        storage_conn = Mock()
        client_conn = Mock()
        master.setConnection(master_conn)
        storage.setConnection(storage_conn)
        client.setConnection(client_conn)
        master.setRunning()
        client.setRunning()
        storage.setRunning()
        self.app.nm.add(storage)
        self.app.nm.add(client)

        # no address defined, not send to client node
        c_node = self.app.nm.createClient(uuid = self.getNewUUID())
        self.app.broadcastNodesInformation([c_node])
        # check conn
        self.checkNoPacketSent(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

        # address defined and client type
        s_node = self.app.nm.createClient(
            uuid = self.getNewUUID(),
            address=("127.1.0.1", 3361)
        )
        self.app.broadcastNodesInformation([c_node])
        # check conn
        self.checkNoPacketSent(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

        # address defined and storage type
        s_node = self.app.nm.createStorage(
            uuid=self.getNewUUID(),
            address=("127.0.0.1", 1351)
        )

        self.app.broadcastNodesInformation([s_node])
        # check conn
        self.checkNotifyNodeInformation(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

        # node not running, don't send informations
        client.setPending()

        self.app.broadcastNodesInformation([s_node])
        # check conn
        self.checkNotifyNodeInformation(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)


if __name__ == '__main__':
    unittest.main()

