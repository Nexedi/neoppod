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

import unittest, logging, os
from mock import Mock
from neo.tests.base import NeoTestBase
from neo.storage.app import Application
from neo.protocol import INVALID_PTID, INVALID_OID, INVALID_TID, \
     INVALID_UUID, Packet, NOTIFY_NODE_INFORMATION, UP_TO_DATE_STATE
from neo.node import MasterNode, ClientNode, StorageNode
from neo.storage.mysqldb import p64, u64, MySQLDatabaseManager
from collections import deque
from neo.pt import PartitionTable

class StorageAppTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        self.prepareDatabase(number=1)
        # create an application object
        config = self.getConfigFile(master_number=1)
        self.app = Application(config, "storage1")
        self.app.event_queue = deque()
        
    def tearDown(self):
        NeoTestBase.tearDown(self)

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def test_01_loadPartitionTable(self):
      self.assertEqual(len(self.app.dm.getPartitionTable()), 0)
      self.assertEqual(self.app.pt, None)
      num_partitions = 3
      num_replicas = 2
      self.app.pt = PartitionTable(num_partitions, num_replicas)
      self.assertEqual(self.app.pt.getNodeList(), [])
      self.assertFalse(self.app.pt.filled())
      for x in xrange(num_partitions):
        self.assertFalse(self.app.pt.hasOffset(x))

      # load an empty table
      self.app.loadPartitionTable()
      self.assertEqual(self.app.pt.getNodeList(), [])
      self.assertFalse(self.app.pt.filled())
      for x in xrange(num_partitions):
        self.assertFalse(self.app.pt.hasOffset(x))

      # add some node, will be remove when loading table
      master_uuid = self.getNewUUID()      
      master = MasterNode(uuid=master_uuid)
      storage_uuid = self.getNewUUID()      
      storage = StorageNode(uuid=storage_uuid)
      client_uuid = self.getNewUUID()      
      client = ClientNode(uuid=client_uuid)

      self.app.pt.setCell(0, master, UP_TO_DATE_STATE)
      self.app.pt.setCell(0, storage, UP_TO_DATE_STATE)
      self.assertEqual(len(self.app.pt.getNodeList()), 2)
      self.assertFalse(self.app.pt.filled())
      for x in xrange(num_partitions):
        if x == 0:
          self.assertTrue(self.app.pt.hasOffset(x))
        else:
          self.assertFalse(self.app.pt.hasOffset(x))
      # load an empty table, everything removed
      self.assertEqual(len(self.app.dm.getPartitionTable()), 0)
      self.app.loadPartitionTable()
      self.assertEqual(self.app.pt.getNodeList(), [])
      self.assertFalse(self.app.pt.filled())
      for x in xrange(num_partitions):
        self.assertFalse(self.app.pt.hasOffset(x))

      # add some node
      self.app.pt.setCell(0, master, UP_TO_DATE_STATE)
      self.app.pt.setCell(0, storage, UP_TO_DATE_STATE)
      self.assertEqual(len(self.app.pt.getNodeList()), 2)
      self.assertFalse(self.app.pt.filled())
      for x in xrange(num_partitions):
        if x == 0:
          self.assertTrue(self.app.pt.hasOffset(x))
        else:
          self.assertFalse(self.app.pt.hasOffset(x))
      # fill partition table
      self.app.dm.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" % 
                        (0, client_uuid, UP_TO_DATE_STATE))
      self.app.dm.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" % 
                        (1, client_uuid, UP_TO_DATE_STATE))
      self.app.dm.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" % 
                        (1, storage_uuid, UP_TO_DATE_STATE))
      self.app.dm.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" % 
                        (2, storage_uuid, UP_TO_DATE_STATE))
      self.app.dm.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" % 
                        (2, master_uuid, UP_TO_DATE_STATE))
      self.assertEqual(len(self.app.dm.getPartitionTable()), 5)
      self.app.loadPartitionTable()
      self.assertTrue(self.app.pt.filled())
      for x in xrange(num_partitions):        
        self.assertTrue(self.app.pt.hasOffset(x))
      # check each row
      cell_list = self.app.pt.getCellList(0)
      self.assertEqual(len(cell_list), 1)
      self.assertEqual(cell_list[0].getUUID(), client_uuid)
      cell_list = self.app.pt.getCellList(1)
      self.assertEqual(len(cell_list), 2)
      self.failUnless(cell_list[0].getUUID() in (client_uuid, storage_uuid))
      self.failUnless(cell_list[1].getUUID() in (client_uuid, storage_uuid))
      cell_list = self.app.pt.getCellList(2)
      self.assertEqual(len(cell_list), 2)
      self.failUnless(cell_list[0].getUUID() in (master_uuid, storage_uuid))
      self.failUnless(cell_list[1].getUUID() in (master_uuid, storage_uuid))
      
    def test_02_queueEvent(self):
      self.assertEqual(len(self.app.event_queue), 0)
      event = Mock({"getId": 1325136})
      self.app.queueEvent(event, "test", key="value")
      self.assertEqual(len(self.app.event_queue), 1)
      event, args, kw = self.app.event_queue[0]
      self.assertEqual(event.getId(), 1325136)
      self.assertEqual(len(args), 1)
      self.assertEqual(args[0], "test")
      self.assertEqual(kw, {"key" : "value"})
      
    def test_03_executeQueuedEvents(self):
      self.assertEqual(len(self.app.event_queue), 0)
      event = Mock({"getId": 1325136})
      self.app.queueEvent(event, "test", key="value")
      self.app.executeQueuedEvents()
      self.assertEquals(len(event.mockGetNamedCalls("__call__")), 1)
      call = event.mockGetNamedCalls("__call__")[0]
      params = call.getParam(0)
      self.assertEqual(params, "test")
      params = call.kwparams
      self.assertEqual(params, {'key': 'value'})
    
    def test_04_getPartition(self):
      self.app.num_partitions = 3
      p = self.app.getPartition(p64(1))
      self.assertEqual(p, 1)
      p = self.app.getPartition(p64(2))
      self.assertEqual(p, 2)
      p = self.app.getPartition(p64(3))
      self.assertEqual(p, 0)

if __name__ == '__main__':
    unittest.main()

