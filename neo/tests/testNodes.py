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

import unittest, os
from mock import Mock
from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        BROKEN_STATE, UNKNOWN_STATE, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, \
        CLIENT_NODE_TYPE, INVALID_UUID
from neo.node import Node, MasterNode, StorageNode, ClientNode, NodeManager
from neo.tests import NeoTestBase
from time import time

class NodesTests(NeoTestBase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_01_node(self):
        # initialisation
        server = ("127.0.0.1", 10000)
        uuid = self.getNewUUID()
        node = Node(server, uuid)
        manager = Mock()
        node.setManager(manager)
        self.assertEqual(node.state, UNKNOWN_STATE)
        self.assertEqual(node.server, server)
        self.assertEqual(node.uuid, uuid)
        self.assertEqual(node.manager, manager)
        self.assertNotEqual(node.last_state_change, None)
        # test getter
        self.assertEqual(node.getState(), UNKNOWN_STATE)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(node.getUUID(), uuid)
        self.assertRaises(NotImplementedError, node.getType)
        self.assertNotEqual(node.getLastStateChange(), None)
        last_change = node.getLastStateChange()
        self.failUnless(isinstance(last_change, float))
        now = time()
        self.failUnless(last_change < now)
        # change the state
        node.setState(DOWN_STATE)
        self.assertEqual(node.getState(), DOWN_STATE)
        # tmie of change must be updated
        self.failUnless(node.getLastStateChange() > now)
        # set new uuid
        new_uuid = self.getNewUUID()
        self.assertNotEqual(new_uuid, uuid)
        node.setUUID(new_uuid)
        self.assertEqual(node.getUUID(), new_uuid)
        # set new server
        new_server = ("127.0.0.1", 10001)
        self.assertNotEqual(new_server, server)
        node.setServer(new_server)
        self.assertEqual(node.getServer(), new_server)
        # add manager
        manager = Mock()
        node.setManager(manager)
        self.assertNotEqual(node.manager, None)
        # set server and uuid and check method are well called on manager
        self.assertNotEqual(node.getUUID(), uuid)
        node.setUUID(uuid)
        self.assertEqual(node.getUUID(), uuid)
        self.assertNotEqual(node.getServer(), server)
        node.setServer(server)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(len(manager.mockGetNamedCalls("unregisterServer")), 1)
        call = manager.mockGetNamedCalls("unregisterServer")[0]
        self.assertEqual(call.getParam(0), node)
        self.assertEqual(len(manager.mockGetNamedCalls("registerServer")), 1)
        call = manager.mockGetNamedCalls("registerServer")[0]
        self.assertEqual(call.getParam(0), node)
        self.assertEqual(len(manager.mockGetNamedCalls("unregisterUUID")), 1)
        call = manager.mockGetNamedCalls("unregisterUUID")[0]
        self.assertEqual(call.getParam(0), node)
        self.assertEqual(len(manager.mockGetNamedCalls("registerUUID")), 1)
        call = manager.mockGetNamedCalls("registerUUID")[0]
        self.assertEqual(call.getParam(0), node)

    def test_02_master_node(self):
        server = ("127.0.0.1", 10000)
        uuid = self.getNewUUID()
        node = MasterNode(server, uuid)
        self.assertEqual(node.manager, None)
        self.assertNotEqual(node.last_state_change, None)
        # test getter
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(MASTER_NODE_TYPE, node.getType())
        
    def test_02_storage_node(self):
        server = ("127.0.0.1", 10000)
        uuid = self.getNewUUID()
        node = StorageNode(server, uuid)
        self.assertEqual(node.manager, None)
        self.assertNotEqual(node.last_state_change, None)
        # test getter
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(STORAGE_NODE_TYPE, node.getType())

    def test_04_client_node(self):
        server = ("127.0.0.1", 10000)
        uuid = self.getNewUUID()
        node = ClientNode(server, uuid)
        self.assertEqual(node.manager, None)
        self.assertNotEqual(node.last_state_change, None)
        # test getter
        self.assertEqual(node.getState(), RUNNING_STATE)
        self.assertEqual(node.getServer(), server)
        self.assertEqual(node.getUUID(), uuid)
        self.assertEqual(CLIENT_NODE_TYPE, node.getType())


    def test_05_node_manager(self):
        nm = NodeManager()
        self.assertEqual(len(nm.node_list), 0) 
        self.assertEqual(len(nm.server_dict), 0)
        self.assertEqual(len(nm.uuid_dict), 0)
        self.assertEqual(len(nm.getNodeList()), 0)
        self.assertEqual(len(nm.getMasterNodeList()), 0)        
        self.assertEqual(len(nm.getStorageNodeList()), 0)
        self.assertEqual(len(nm.getClientNodeList()), 0)
        # Create some node and add them to node manager
        # add a storage node
        sn_server = ("127.0.0.1", 10000)
        sn_uuid = self.getNewUUID()
        sn = StorageNode(sn_server, sn_uuid)
        nm.add(sn)
        self.assertEqual(sn.manager, nm)
        self.assertTrue(nm.server_dict.has_key(sn_server))
        self.assertEqual(nm.server_dict[sn_server], sn)
        self.assertTrue(nm.uuid_dict.has_key(sn_uuid))
        self.assertEqual(nm.uuid_dict[sn_uuid], sn)
        self.assertEqual(len(nm.getNodeList()), 1)
        self.assertEqual(len(nm.getMasterNodeList()), 0)
        self.assertEqual(len(nm.getClientNodeList()), 0)
        self.assertEqual(len(nm.getStorageNodeList()), 1)
        sn_list = nm.getStorageNodeList()
        self.assertEqual(sn_list[0], sn)
        # add a Master node
        mn_server = ("127.0.0.1", 10001)
        mn_uuid = self.getNewUUID()
        mn = MasterNode(mn_server, mn_uuid)
        nm.add(mn)
        self.assertEqual(mn.manager, nm)
        self.assertTrue(nm.server_dict.has_key(mn_server))
        self.assertEqual(nm.server_dict[mn_server], mn)
        self.assertTrue(nm.uuid_dict.has_key(mn_uuid))
        self.assertEqual(nm.uuid_dict[mn_uuid], mn)
        self.assertEqual(len(nm.getNodeList()), 2)
        self.assertEqual(len(nm.getMasterNodeList()), 1)        
        self.assertEqual(len(nm.getClientNodeList()), 0)
        self.assertEqual(len(nm.getStorageNodeList()), 1)
        mn_list = nm.getMasterNodeList()
        self.assertEqual(mn_list[0], mn)
        sn_list = nm.getStorageNodeList()
        self.assertEqual(sn_list[0], sn)
        # add a Client node
        cn_server = ("127.0.0.1", 10002)
        cn_uuid = self.getNewUUID()
        cn = ClientNode(cn_server, cn_uuid)
        nm.add(cn)
        self.assertEqual(cn.manager, nm)
        self.assertTrue(nm.server_dict.has_key(cn_server))
        self.assertEqual(nm.server_dict[cn_server], cn)
        self.assertTrue(nm.uuid_dict.has_key(cn_uuid))
        self.assertEqual(nm.uuid_dict[cn_uuid], cn)
        self.assertEqual(len(nm.getNodeList()), 3)
        self.assertEqual(len(nm.getMasterNodeList()), 1)        
        self.assertEqual(len(nm.getClientNodeList()), 1)
        self.assertEqual(len(nm.getStorageNodeList()), 1)
        cn_list = nm.getClientNodeList()
        self.assertEqual(cn_list[0], cn)
        mn_list = nm.getMasterNodeList()
        self.assertEqual(mn_list[0], mn)
        sn_list = nm.getStorageNodeList()
        self.assertEqual(sn_list[0], sn)
        # check we can get the nodes
        self.assertEqual(len(nm.server_dict), 3)
        self.assertEqual(len(nm.uuid_dict), 3)
        node_list = nm.getNodeList()
        self.failUnless(cn in node_list)
        self.failUnless(mn in node_list)
        self.failUnless(sn in node_list)
        node = nm.getNodeByServer(cn_server)
        self.assertEqual(node, cn)
        node = nm.getNodeByUUID(cn_uuid)
        self.assertEqual(node, cn)
        node = nm.getNodeByServer(sn_server)
        self.assertEqual(node, sn)
        node = nm.getNodeByUUID(sn_uuid)
        self.assertEqual(node, sn)
        node = nm.getNodeByServer(mn_server)
        self.assertEqual(node, mn)
        node = nm.getNodeByUUID(mn_uuid)
        self.assertEqual(node, mn)
        # remove the nodes
        # remove the storage node
        nm.remove(sn)
        self.assertFalse(nm.server_dict.has_key(sn_server))
        self.assertFalse(nm.uuid_dict.has_key(sn_uuid))
        self.assertEqual(len(nm.getNodeList()), 2)
        self.assertEqual(len(nm.getMasterNodeList()), 1)
        self.assertEqual(len(nm.getClientNodeList()), 1)
        self.assertEqual(len(nm.getStorageNodeList()), 0)
        node = nm.getNodeByServer(sn_server)
        self.assertEqual(node, None)
        node = nm.getNodeByUUID(sn_uuid)
        self.assertEqual(node, None)
        # remove the client node
        nm.remove(cn)
        self.assertFalse(nm.server_dict.has_key(cn_server))
        self.assertFalse(nm.uuid_dict.has_key(cn_uuid))
        self.assertEqual(len(nm.getNodeList()), 1)
        self.assertEqual(len(nm.getMasterNodeList()), 1)
        self.assertEqual(len(nm.getClientNodeList()), 0)
        self.assertEqual(len(nm.getStorageNodeList()), 0)
        node = nm.getNodeByServer(cn_server)
        self.assertEqual(node, None)
        node = nm.getNodeByUUID(cn_uuid)
        self.assertEqual(node, None)
        # remove the master node
        nm.remove(mn)
        self.assertFalse(nm.server_dict.has_key(mn_server))
        self.assertFalse(nm.uuid_dict.has_key(mn_uuid))
        self.assertEqual(len(nm.getNodeList()), 0)
        self.assertEqual(len(nm.getMasterNodeList()), 0)
        self.assertEqual(len(nm.getClientNodeList()), 0)
        self.assertEqual(len(nm.getStorageNodeList()), 0)
        node = nm.getNodeByServer(mn_server)
        self.assertEqual(node, None)
        node = nm.getNodeByUUID(mn_uuid)
        self.assertEqual(node, None)
        # test method to register uuid/server
        self.assertEqual(len(nm.server_dict), 0)
        nm.registerServer(cn)
        self.assertEqual(len(nm.server_dict), 1)
        self.assertTrue(nm.server_dict.has_key(cn_server))        
        self.assertEqual(len(nm.uuid_dict), 0)
        nm.registerUUID(cn)
        self.assertEqual(len(nm.uuid_dict), 1)
        self.assertTrue(nm.uuid_dict.has_key(cn_uuid))
        # test for unregister methods
        self.assertEqual(len(nm.server_dict), 1)
        nm.unregisterServer(cn)
        self.assertEqual(len(nm.server_dict), 0)
        self.assertFalse(nm.server_dict.has_key(cn_server))        
        self.assertEqual(len(nm.uuid_dict), 1)
        nm.unregisterUUID(cn)
        self.assertEqual(len(nm.uuid_dict), 0)
        self.assertFalse(nm.uuid_dict.has_key(cn_uuid))
        
if __name__ == '__main__':
    unittest.main()

