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
from tempfile import mkstemp
from mock import Mock
from neo.master.app import Application
from neo.protocol import INVALID_PTID, INVALID_OID, INVALID_TID, \
     INVALID_UUID, Packet, NOTIFY_NODE_INFORMATION
from neo.node import MasterNode, ClientNode, StorageNode
from neo.storage.mysqldb import p64, u64

class MasterAppTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)
        # create an application object
        config_file_text = """# Default parameters.
[DEFAULT]
# The list of master nodes.
master_nodes: 127.0.0.1:10010
# The number of replicas.
replicas: 2
# The number of partitions.
partitions: 1009
# The name of this cluster.
name: main
# The user name for the database.
user: neo
# The password for the database.
password: neo

connector: SocketConnector

# The first master.
[mastertest]
server: 127.0.0.1:10010

# The first storage.
[storage1]
database: neotest1
server: 127.0.0.1:10020

# The second storage.
[storage2]
database: neotest2
server: 127.0.0.1:10021

# The third storage.
[storage3]
database: neotest3
server: 127.0.0.1:10022

# The fourth storage.
[storage4]
database: neotest4
server: 127.0.0.1:10023
"""
        tmp_id, self.tmp_path = mkstemp()
        tmp_file = os.fdopen(tmp_id, "w+b")
        tmp_file.write(config_file_text)
        tmp_file.close()
        self.app = Application(self.tmp_path, "mastertest")        
        self.app.pt.clear()

    def tearDown(self):
        # Delete tmp file
        os.remove(self.tmp_path)

    def getNewUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

    def test_01_getNextPartitionTableID(self):
      # must raise as we don"t have one
      self.assertEqual(self.app.lptid, INVALID_PTID)
      self.app.lptid = INVALID_PTID
      self.assertRaises(RuntimeError, self.app.getNextPartitionTableID)
      # set one
      self.app.lptid = p64(23)
      nptid = self.app.getNextPartitionTableID()
      self.assertEqual(self.app.lptid, nptid)
      self.failUnless(u64(self.app.lptid) > 23)
      self.assertEqual(u64(self.app.lptid), 24)

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
      self.app.num_partitions = 3
      p = self.app.getPartition(p64(1))
      self.assertEqual(p, 1)
      p = self.app.getPartition(p64(2))
      self.assertEqual(p, 2)
      p = self.app.getPartition(p64(3))
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
      self.assertEquals(len(client_conn.mockGetNamedCalls("addPacket")), 0)
      self.assertEquals(len(master_conn.mockGetNamedCalls("notify")), 1)
      call = master_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)
      self.assertEquals(len(storage_conn.mockGetNamedCalls("notify")), 1)
      call = storage_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)

      # address defined and client type
      master_conn = Mock({"getUUID" : master_uuid})
      storage_conn = Mock({"getUUID" : storage_uuid})
      client_conn = Mock({"getUUID" : client_uuid})
      self.app.em = Mock({"getConnectionList" : (master_conn, storage_conn, client_conn)})
      s_node = ClientNode(uuid = self.getNewUUID(), server=("127.1.0.1", 3361))
      self.app.broadcastNodeInformation(c_node)
      # check conn
      self.assertEquals(len(client_conn.mockGetNamedCalls("addPacket")), 0)
      self.assertEquals(len(master_conn.mockGetNamedCalls("notify")), 1)
      call = master_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)
      self.assertEquals(len(storage_conn.mockGetNamedCalls("notify")), 1)
      call = storage_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)
      
      # address defined and storage type
      master_conn = Mock({"getUUID" : master_uuid})
      storage_conn = Mock({"getUUID" : storage_uuid})
      client_conn = Mock({"getUUID" : client_uuid})
      self.app.em = Mock({"getConnectionList" : (master_conn, storage_conn, client_conn)})
      s_node = StorageNode(uuid = self.getNewUUID(), server=("127.0.0.1", 1351))
      self.app.broadcastNodeInformation(s_node)
      # check conn
      self.assertEquals(len(client_conn.mockGetNamedCalls("notify")), 1)
      call = client_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)
      self.assertEquals(len(master_conn.mockGetNamedCalls("notify")), 1)
      call = master_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)
      self.assertEquals(len(storage_conn.mockGetNamedCalls("notify")), 1)
      call = storage_conn.mockGetNamedCalls("notify")[0]
      packet = call.getParam(0)
      self.assertTrue(isinstance(packet, Packet))
      self.assertEqual(packet.getType(), NOTIFY_NODE_INFORMATION)


if __name__ == '__main__':
    unittest.main()

