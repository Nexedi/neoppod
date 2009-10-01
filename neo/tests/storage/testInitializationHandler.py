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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest
from mock import Mock
from neo.tests import NeoTestBase
from neo.pt import PartitionTable
from neo.storage.app import Application
from neo.storage.handlers.initialization import InitializationHandler
from neo.protocol import Packet, PacketTypes, CellStates
from neo.exception import PrimaryFailure

class StorageInitializationHandlerTests(NeoTestBase):

    def setUp(self):
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getStorageConfiguration(master_number=1)
        self.app = Application(**config)
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

    def tearDown(self):
        NeoTestBase.tearDown(self)

    # Common methods
    def getLastUUID(self):
        return self.uuid
        
    def test_02_timeoutExpired(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServer" : False})
        self.assertRaises(PrimaryFailure, self.verification.timeoutExpired, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_03_connectionClosed(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServer" : False})
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_04_peerBroken(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServer" : False})
        self.assertRaises(PrimaryFailure, self.verification.peerBroken, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_09_handleSendPartitionTable(self):
        packet = Packet(msg_type=PacketTypes.SEND_PARTITION_TABLE)
        uuid = self.getNewUUID()
        # send a table
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServer" : False})

        self.app.pt = PartitionTable(3, 2)
        node_1 = self.getNewUUID()
        node_2 = self.getNewUUID()
        node_3 = self.getNewUUID()
        # SN already know all nodes 
        self.app.nm.createStorage(uuid=node_1)
        self.app.nm.createStorage(uuid=node_2)
        self.app.nm.createStorage(uuid=node_3)
        self.assertEqual(self.app.dm.getPartitionTable(), [])
        row_list = [(0, ((node_1, CellStates.UP_TO_DATE), (node_2, CellStates.UP_TO_DATE))),
                    (1, ((node_3, CellStates.UP_TO_DATE), (node_1, CellStates.UP_TO_DATE))),
                    (2, ((node_2, CellStates.UP_TO_DATE), (node_3, CellStates.UP_TO_DATE)))]
        self.assertFalse(self.app.pt.filled())
        # send part of the table, won't be filled
        self.verification.handleSendPartitionTable(conn, packet, 1, row_list[:1])
        self.assertFalse(self.app.pt.filled())
        self.assertEqual(self.app.pt.getID(), 1)
        self.assertEqual(self.app.dm.getPartitionTable(), [])
        # send remaining of the table (ack with AnswerPartitionTable)
        self.verification.handleSendPartitionTable(conn, packet, 1, row_list[1:])
        self.verification.handleAnswerPartitionTable(conn, packet, 1, [])
        self.assertTrue(self.app.pt.filled())
        self.assertEqual(self.app.pt.getID(), 1)
        self.assertNotEqual(self.app.dm.getPartitionTable(), [])
        # send a complete new table and ack
        self.verification.handleSendPartitionTable(conn, packet, 2, row_list)
        self.verification.handleAnswerPartitionTable(conn, packet, 2, [])
        self.assertTrue(self.app.pt.filled())
        self.assertEqual(self.app.pt.getID(), 2)
        self.assertNotEqual(self.app.dm.getPartitionTable(), [])

    
if __name__ == "__main__":
    unittest.main()

