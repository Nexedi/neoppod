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

import os
import unittest
import logging
from mock import Mock
from neo.tests.base import NeoTestBase
from neo import protocol
from neo.node import MasterNode
from neo.pt import PartitionTable
from neo.storage.app import Application, StorageNode
from neo.storage.handlers import InitializationHandler
from neo.protocol import STORAGE_NODE_TYPE, MASTER_NODE_TYPE, CLIENT_NODE_TYPE
from neo.protocol import BROKEN_STATE, RUNNING_STATE, Packet, INVALID_UUID, \
     UP_TO_DATE_STATE, INVALID_TID, PROTOCOL_ERROR_CODE
from neo.protocol import ACCEPT_NODE_IDENTIFICATION, REQUEST_NODE_IDENTIFICATION, \
     NOTIFY_PARTITION_CHANGES, STOP_OPERATION, ASK_LAST_IDS, ASK_PARTITION_TABLE, \
     ANSWER_OBJECT_PRESENT, ASK_OBJECT_PRESENT, OID_NOT_FOUND_CODE, LOCK_INFORMATION, \
     UNLOCK_INFORMATION, TID_NOT_FOUND_CODE, ASK_TRANSACTION_INFORMATION, \
     COMMIT_TRANSACTION, ASK_UNFINISHED_TRANSACTIONS, SEND_PARTITION_TABLE
from neo.protocol import ERROR, BROKEN_NODE_DISALLOWED_CODE, ASK_PRIMARY_MASTER
from neo.protocol import ANSWER_PRIMARY_MASTER
from neo.exception import PrimaryFailure, OperationFailure
from neo.storage.mysqldb import MySQLDatabaseManager, p64, u64

class StorageInitializationHandlerTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.ERROR)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getConfigFile(master_number=1)
        self.app = Application(config, "storage1")
        self.verification = InitializationHandler(self.app)
        # define some variable to simulate client and storage node
        self.master_port = 10010
        self.storage_port = 10020
        self.client_port = 11011
        self.num_partitions = 1009
        self.num_replicas = 2
        self.app.num_partitions = 1009
        self.app.num_replicas = 2
        self.app.operational = False
        self.app.load_lock_dict = {}
        self.app.pt = PartitionTable(self.app.num_partitions, self.app.num_replicas)

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
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.timeoutExpired, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_03_connectionClosed(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.connectionClosed, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_04_peerBroken(self):
        # client connection
        uuid = self.getNewUUID()
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})
        self.assertRaises(PrimaryFailure, self.verification.peerBroken, conn,)
        # nothing happens
        self.checkNoPacketSent(conn)

    def test_09_handleSendPartitionTable(self):
        packet = Packet(msg_type=SEND_PARTITION_TABLE)
        uuid = self.getNewUUID()
        # send a table
        conn = Mock({"getUUID" : uuid,
                     "getAddress" : ("127.0.0.1", self.client_port),
                     "isServerConnection" : False})

        self.app.pt = PartitionTable(3, 2)
        node_1 = self.getNewUUID()
        node_2 = self.getNewUUID()
        node_3 = self.getNewUUID()
        # SN already known one of the node
        self.app.nm.add(StorageNode(uuid=node_1))
        self.app.ptid = 1
        self.app.num_partitions = 3
        self.app.num_replicas =2 
        self.assertEqual(self.app.dm.getPartitionTable(), ())
        row_list = [(0, ((node_1, UP_TO_DATE_STATE), (node_2, UP_TO_DATE_STATE))),
                    (1, ((node_3, UP_TO_DATE_STATE), (node_1, UP_TO_DATE_STATE))),
                    (2, ((node_2, UP_TO_DATE_STATE), (node_3, UP_TO_DATE_STATE)))]
        self.assertFalse(self.app.pt.filled())
        # send part of the table, won't be filled
        self.verification.handleSendPartitionTable(conn, packet, "1", row_list[:1])
        self.assertFalse(self.app.pt.filled())
        self.assertEqual(self.app.ptid, "1")
        self.assertEqual(self.app.dm.getPartitionTable(), ())
        # send remaining of the table
        self.verification.handleSendPartitionTable(conn, packet, "1", row_list[1:])
        self.assertTrue(self.app.pt.filled())
        self.assertEqual(self.app.ptid, "1")
        self.assertNotEqual(self.app.dm.getPartitionTable(), ())
        # send a complete new table
        self.verification.handleSendPartitionTable(conn, packet, "2", row_list)
        self.assertTrue(self.app.pt.filled())
        self.assertEqual(self.app.ptid, "2")
        self.assertNotEqual(self.app.dm.getPartitionTable(), ())

    
if __name__ == "__main__":
    unittest.main()

