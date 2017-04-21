#
# Copyright (C) 2009-2017  Nexedi SA
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
from neo.lib.protocol import Packets
from neo.master.app import Application

class MasterAppTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration()
        self.app = Application(config)
        self.app.pt.clear()

    def _tearDown(self, success):
        self.app.close()
        NeoUnitTestBase._tearDown(self, success)

    def checkNotifyNodeInformation(self, conn):
        return self.checkNotifyPacket(conn, Packets.NotifyNodeInformation)

    def test_06_broadcastNodeInformation(self):
        # defined some nodes to which data will be send
        master_uuid = self.getMasterUUID()
        master = self.app.nm.createMaster(uuid=master_uuid)
        storage_uuid = self.getStorageUUID()
        storage = self.app.nm.createStorage(uuid=storage_uuid)
        client_uuid = self.getClientUUID()
        client = self.app.nm.createClient(uuid=client_uuid)
        # create conn and patch em
        master_conn = self.getFakeConnection()
        storage_conn = self.getFakeConnection(is_server=True)
        client_conn = self.getFakeConnection(is_server=True)
        master.setConnection(master_conn)
        storage.setConnection(storage_conn)
        client.setConnection(client_conn)
        self.app.nm.add(storage)
        self.app.nm.add(client)

        # no address defined, not send to client node
        c_node = self.app.nm.createClient(uuid=self.getClientUUID())
        self.app.broadcastNodesInformation([c_node])
        # check conn
        self.checkNoPacketSent(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

        # address defined and client type
        s_node = self.app.nm.createClient(
            uuid=self.getClientUUID(),
            address=("127.1.0.1", 3361)
        )
        self.app.broadcastNodesInformation([c_node])
        # check conn
        self.checkNoPacketSent(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

        # address defined and storage type
        s_node = self.app.nm.createStorage(
            uuid=self.getStorageUUID(),
            address=("127.0.0.1", 1351)
        )

        self.app.broadcastNodesInformation([s_node])
        # check conn
        self.checkNotifyNodeInformation(client_conn)
        self.checkNoPacketSent(master_conn)
        self.checkNotifyNodeInformation(storage_conn)

if __name__ == '__main__':
    unittest.main()

