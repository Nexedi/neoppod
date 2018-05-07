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
from ..mock import Mock
from .. import NeoUnitTestBase
from neo.lib.util import p64
from neo.lib.protocol import NodeTypes, NodeStates, Packets
from neo.master.app import Application
from neo.master.handlers.client import ClientServiceHandler

class MasterClientHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.pt.setID(1)
        self.app.em = Mock()
        self.app.loid = '\0' * 8
        self.app.tm.setLastTID('\0' * 8)
        self.service = ClientServiceHandler(self.app)
        # define some variable to simulate client and storage node
        self.client_port = 11022
        self.storage_port = 10021
        self.client_address = ('127.0.0.1', self.client_port)
        self.storage_address = ('127.0.0.1', self.storage_port)
        self.storage_uuid = self.getStorageUUID()
        # register the storage
        self.app.nm.createStorage(
            uuid=self.storage_uuid,
            address=self.storage_address,
        )

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN """
        # register the master itself
        uuid = self.getNewUUID(node_type)
        self.app.nm.createFromNodeType(
            node_type,
            address=(ip, port),
            uuid=uuid,
            state=NodeStates.RUNNING,
        )
        return uuid

    def test_askPack(self):
        self.assertEqual(self.app.packing, None)
        self.app.nm.createClient()
        tid = self.getNextTID()
        peer_id = 42
        conn = self.getFakeConnection(peer_id=peer_id)
        storage_uuid = self.storage_uuid
        storage_conn = self.getFakeConnection(storage_uuid,
            self.storage_address, is_server=True)
        self.app.nm.getByUUID(storage_uuid).setConnection(storage_conn)
        self.service.askPack(conn, tid)
        self.checkNoPacketSent(conn)
        ptid = self.checkAskPacket(storage_conn, Packets.AskPack)._args[0]
        self.assertEqual(ptid, tid)
        self.assertTrue(self.app.packing[0] is conn)
        self.assertEqual(self.app.packing[1], peer_id)
        self.assertEqual(self.app.packing[2], {storage_uuid})
        # Asking again to pack will cause an immediate error
        storage_uuid = self.identifyToMasterNode(port=10022)
        storage_conn = self.getFakeConnection(storage_uuid,
            self.storage_address, is_server=True)
        self.app.nm.getByUUID(storage_uuid).setConnection(storage_conn)
        self.service.askPack(conn, tid)
        self.checkNoPacketSent(storage_conn)
        status = self.checkAnswerPacket(conn, Packets.AnswerPack)._args[0]
        self.assertFalse(status)

if __name__ == '__main__':
    unittest.main()

