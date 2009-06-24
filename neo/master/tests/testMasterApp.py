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
from neo.master.app import Application
from neo.protocol import INVALID_PTID, INVALID_OID, INVALID_TID, \
     INVALID_UUID, Packet, NOTIFY_NODE_INFORMATION
from neo.node import MasterNode, ClientNode, StorageNode
from neo.storage.mysqldb import p64, u64

class MasterAppTests(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config = self.getConfigFile()
        self.app = Application(config, "master1")
        self.app.pt.clear()

    def tearDown(self):
        NeoTestBase.tearDown(self)

    def test_02_getNextOID(self):
      # must raise as we don"t have one
      self.assertEqual(self.app.loid, INVALID_OID)
      self.app.loid = None
      self.assertRaises(RuntimeError, self.app.getNextOID)
      # set one
      self.app.loid = p64(23)
      noid = self.app.getNextOID()
      self.assertEqual(self.app.loid, noid)
      self.failUnless(u64(self.app.loid) > 23)
      self.assertEqual(u64(self.app.loid), 24)
    
    def test_03_getNextTID(self):
      self.assertEqual(self.app.ltid, INVALID_TID)
      ntid = self.app.getNextTID()
      self.assertEqual(self.app.ltid, ntid)
      # generate new one
      tid = self.app.getNextTID()
      self.assertEqual(self.app.ltid, tid)
      self.failUnless(tid > ntid)
    
    def test_04_getPartition(self):
      self.app.pt.num_partitions = 3
      p = self.app.getPartition(p64(1))
      self.assertEqual(p, 1)
      p = self.app.getPartition(p64(2))
      self.assertEqual(p, 2)
      p = self.app.getPartition(p64(1009)) # 1009 defined in config
      self.assertEqual(p, 0)

    def test_05_getNewOIDList(self):
      oid_list = self.app.getNewOIDList(15)
      self.assertEqual(len(oid_list), 15)
      i = 1
      # begin from 0, so generated oid from 1 to 15
      for oid in oid_list:
        self.assertEqual(u64(oid), i)
        i+=1

    def test_06_broadcastNodeInformation(self):
      # defined some nodes to which data will be send
      master_uuid = self.getNewUUID()      
      master = MasterNode(uuid=master_uuid)
      storage_uuid = self.getNewUUID()      
      storage = StorageNode(uuid=storage_uuid)
      client_uuid = self.getNewUUID()      
      client = ClientNode(uuid=client_uuid)
      self.app.nm.add(master)
      self.app.nm.add(storage)
      self.app.nm.add(client)
      # create conn and patch em
      master_conn = Mock({"getUUID" : master_uuid})
      storage_conn = Mock({"getUUID" : storage_uuid})
      client_conn = Mock({"getUUID" : client_uuid})
      self.app.em = Mock({"getConnectionList" : (master_conn, storage_conn, client_conn)})

      # no address defined, not send to client node
      c_node = ClientNode(uuid = self.getNewUUID())
      self.app.broadcastNodeInformation(c_node)
      # check conn
      self.checkNoPacketSent(client_conn)
      self.checkNotifyNodeInformation(master_conn)
      self.checkNotifyNodeInformation(storage_conn)

      # address defined and client type
      master_conn = Mock({"getUUID" : master_uuid})
      storage_conn = Mock({"getUUID" : storage_uuid})
      client_conn = Mock({"getUUID" : client_uuid})
      self.app.em = Mock({"getConnectionList" : (master_conn, storage_conn, client_conn)})
      s_node = ClientNode(uuid = self.getNewUUID(), server=("127.1.0.1", 3361))
      self.app.broadcastNodeInformation(c_node)
      # check conn
      self.checkNoPacketSent(client_conn)
      self.checkNotifyNodeInformation(master_conn)
      self.checkNotifyNodeInformation(storage_conn)
      
      # address defined and storage type
      master_conn = Mock({"getUUID" : master_uuid})
      storage_conn = Mock({"getUUID" : storage_uuid})
      client_conn = Mock({"getUUID" : client_uuid})
      self.app.em = Mock({"getConnectionList" : (master_conn, storage_conn, client_conn)})
      s_node = StorageNode(uuid = self.getNewUUID(), server=("127.0.0.1", 1351))
      self.app.broadcastNodeInformation(s_node)
      # check conn
      self.checkNotifyNodeInformation(client_conn)
      self.checkNotifyNodeInformation(master_conn)
      self.checkNotifyNodeInformation(storage_conn)


if __name__ == '__main__':
    unittest.main()

