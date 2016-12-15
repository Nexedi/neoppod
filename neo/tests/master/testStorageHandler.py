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
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes, Packets
from neo.master.handlers.storage import StorageServiceHandler
from neo.master.app import Application

class MasterStorageHandlerTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # create an application object
        config = self.getMasterConfiguration(master_number=1, replicas=1)
        self.app = Application(config)
        self.app.em.close()
        self.app.pt.clear()
        self.app.em = Mock()
        self.service = StorageServiceHandler(self.app)

    def _allocatePort(self):
        self.port = getattr(self, 'port', 1000) + 1
        return self.port

    def _getStorage(self):
        return self.identifyToMasterNode(node_type=NodeTypes.STORAGE,
                ip='127.0.0.1', port=self._allocatePort())

    def identifyToMasterNode(self, node_type=NodeTypes.STORAGE, ip="127.0.0.1",
                             port=10021):
        """Do first step of identification to MN
        """
        nm = self.app.nm
        uuid = self.getNewUUID(node_type)
        node = nm.createFromNodeType(node_type, address=(ip, port),
                uuid=uuid)
        conn = self.getFakeConnection(node.getUUID(), node.getAddress(), True)
        node.setConnection(conn)
        return (node, conn)

    def test_answerPack(self):
        # Note: incoming status has no meaning here, so it's left to False.
        node1, conn1 = self._getStorage()
        node2, conn2 = self._getStorage()
        self.app.packing = None
        # Does nothing
        self.service.answerPack(None, False)

        client_conn = Mock({
            'getPeerId': 512,
        })
        client_peer_id = 42
        self.app.packing = (client_conn, client_peer_id,
                            {conn1.getUUID(), conn2.getUUID()})
        self.service.answerPack(conn1, False)
        self.checkNoPacketSent(client_conn)
        self.assertEqual(self.app.packing[2], {conn2.getUUID()})
        self.service.answerPack(conn2, False)
        status = self.checkAnswerPacket(client_conn, Packets.AnswerPack,
            decode=True)[0]
        # TODO: verify packet peer id
        self.assertTrue(status)
        self.assertEqual(self.app.packing, None)

if __name__ == '__main__':
    unittest.main()

