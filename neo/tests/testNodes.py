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
from neo import protocol
from neo.protocol import NodeTypes, NodeStates
from neo.node import Node, MasterNode, StorageNode, ClientNode, AdminNode, \
        NodeManager
from neo.tests import NeoTestBase
from time import time

class NodesTests(NeoTestBase):

    def setUp(self):
        self.manager = Mock()

    def _updatedByAddress(self, node, index=0):
        calls = self.manager.mockGetNamedCalls('_updateAddress')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)

    def _updatedByUUID(self, node, index=0):
        calls = self.manager.mockGetNamedCalls('_updateUUID')
        self.assertEqual(len(calls), index + 1)
        self.assertEqual(calls[index].getParam(0), node)

    def testInit(self):
        """ Check the node initialization """
        address = ('127.0.0.1', 10000)
        uuid = self.getNewUUID()
        node = Node(self.manager, address=address, uuid=uuid)
        self.assertEqual(node.getState(), NodeStates.UNKNOWN)
        self.assertEqual(node.getAddress(), address)
        self.assertEqual(node.getUUID(), uuid)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())

    def testState(self):
        """ Check if the last changed time is updated when state is changed """
        node = Node(self.manager)
        self.assertEqual(node.getState(), NodeStates.UNKNOWN)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())
        previous_time = node.getLastStateChange()
        node.setState(NodeStates.RUNNING)
        self.assertEqual(node.getState(), NodeStates.RUNNING)
        self.assertTrue(previous_time < node.getLastStateChange())
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())

    def testAddress(self):
        """ Check if the node is indexed by address """
        node = Node(self.manager)
        self.assertEqual(node.getAddress(), None)
        address = ('127.0.0.1', 10000)
        node.setAddress(address)
        self._updatedByAddress(node)

    def testUUID(self):
        """ As for Address but UUID """
        node = Node(self.manager)
        self.assertEqual(node.getAddress(), None)
        uuid = self.getNewUUID()
        node.setUUID(uuid)
        self._updatedByUUID(node)

    def testTypes(self):
        """ Check that the abstract node has no type """
        node = Node(self.manager)
        self.assertRaises(NotImplementedError, node.getType)
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testMaster(self):
        """ Check Master sub class """
        node = MasterNode(self.manager)
        self.assertEqual(node.getType(), protocol.NodeTypes.MASTER)
        self.assertTrue(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testStorage(self):
        """ Check Storage sub class """
        node = StorageNode(self.manager)
        self.assertEqual(node.getType(), protocol.NodeTypes.STORAGE)
        self.assertTrue(node.isStorage())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isClient())
        self.assertFalse(node.isAdmin())

    def testClient(self):
        """ Check Client sub class """
        node = ClientNode(self.manager)
        self.assertEqual(node.getType(), protocol.NodeTypes.CLIENT)
        self.assertTrue(node.isClient())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isAdmin())

    def testAdmin(self):
        """ Check Admin sub class """
        node = AdminNode(self.manager)
        self.assertEqual(node.getType(), protocol.NodeTypes.ADMIN)
        self.assertTrue(node.isAdmin())
        self.assertFalse(node.isMaster())
        self.assertFalse(node.isStorage())
        self.assertFalse(node.isClient())


class NodeManagerTests(NeoTestBase):

    def setUp(self):
        self.manager = nm = NodeManager.__class__()
        self.storage = StorageNode(nm, ('127.0.0.1', 1000), self.getNewUUID())
        self.master = MasterNode(nm, ('127.0.0.1', 2000), self.getNewUUID())
        self.client = ClientNode(nm, None, self.getNewUUID())
        self.admin = AdminNode(nm, ('127.0.0.1', 4000), self.getNewUUID())

    def checkNodes(self, node_list):
        manager = self.manager
        self.assertEqual(sorted(manager.getList()), sorted(node_list))

    def checkMasters(self, master_list):
        manager = self.manager
        self.assertEqual(manager.getMasterList(), master_list)

    def checkStorages(self, storage_list):
        manager = self.manager
        self.assertEqual(manager.getStorageList(), storage_list)

    def checkClients(self, client_list):
        manager = self.manager
        self.assertEqual(manager.getClientList(), client_list)

    def checkByServer(self, node):
        node_found = self.manager.getByAddress(node.getAddress())
        self.assertEqual(node_found, node)

    def checkByUUID(self, node):
        node_found = self.manager.getByUUID(node.getUUID())
        self.assertEqual(node_found, node)

    def testInit(self):
        """ Check the manager is empty when started """
        manager = self.manager
        self.checkNodes([])
        self.checkMasters([])
        self.checkStorages([])
        self.checkClients([])
        address = ('127.0.0.1', 10000)
        self.assertEqual(manager.getByAddress(address), None)
        self.assertEqual(manager.getByAddress(None), None)
        uuid = self.getNewUUID()
        self.assertEqual(manager.getByUUID(uuid), None)
        self.assertEqual(manager.getByUUID(None), None)

    def testAdd(self):
        """ Check if new nodes are registered in the manager """
        manager = self.manager
        self.checkNodes([])
        # storage
        manager.add(self.storage)
        self.checkNodes([self.storage])
        self.checkStorages([self.storage])
        self.checkMasters([])
        self.checkClients([])
        self.checkByServer(self.storage)
        self.checkByUUID(self.storage)
        # master
        manager.add(self.master)
        self.checkNodes([self.storage, self.master])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([])
        self.checkByServer(self.master)
        self.checkByUUID(self.master)
        # client
        manager.add(self.client)
        self.checkNodes([self.storage, self.master, self.client])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        # client node has no address
        self.assertEqual(manager.getByAddress(self.client.getAddress()), None)
        self.checkByUUID(self.client)
        # admin
        manager.add(self.admin)
        self.checkNodes([self.storage, self.master, self.client, self.admin])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        self.checkByServer(self.admin)
        self.checkByUUID(self.admin)

    def testInit(self):
        """ Check that the manager clear all its content """
        manager = self.manager
        self.checkNodes([])
        self.checkStorages([])
        self.checkMasters([])
        self.checkClients([])
        manager.add(self.master)
        self.checkMasters([self.master])
        manager.init()
        self.checkNodes([])
        self.checkMasters([])
        manager.add(self.storage)
        self.checkStorages([self.storage])
        manager.init()
        self.checkNodes([])
        self.checkStorages([])
        manager.add(self.client)
        self.checkClients([self.client])
        manager.init()
        self.checkNodes([])
        self.checkClients([])

    def testUpdate(self):
        """ Check manager content update """
        # set up four nodes
        manager = self.manager
        manager.add(self.master)
        manager.add(self.storage)
        manager.add(self.client)
        manager.add(self.admin)
        self.checkNodes([self.master, self.storage, self.client, self.admin])
        self.checkMasters([self.master])
        self.checkStorages([self.storage])
        self.checkClients([self.client])
        # build changes
        old_address = self.master.getAddress()
        new_address = ('127.0.0.1', 2001)
        new_uuid = self.getNewUUID()
        node_list = (
            (NodeTypes.CLIENT, None, self.client.getUUID(), NodeStates.DOWN),
            (NodeTypes.MASTER, new_address, self.master.getUUID(), NodeStates.RUNNING),
            (NodeTypes.STORAGE, self.storage.getAddress(), new_uuid,
                NodeStates.RUNNING),
            (NodeTypes.ADMIN, self.admin.getAddress(), self.admin.getUUID(),
                NodeStates.UNKNOWN),
        )
        # update manager content
        manager.update(node_list)
        # - the client gets down
        self.checkClients([])
        # - master change it's address
        self.checkMasters([self.master])
        self.assertEqual(manager.getByAddress(old_address), None)
        self.master.setAddress(new_address)
        self.checkByServer(self.master)
        # a new storage replaced the old one
        self.assertNotEqual(manager.getStorageList(), [self.storage])
        self.assertTrue(len(manager.getStorageList()), 1)
        new_storage = manager.getStorageList()[0]
        self.assertEqual(new_storage.getState(), NodeStates.RUNNING)
        self.assertNotEqual(new_storage, self.storage)
        # admin is still here but in UNKNOWN state
        self.checkNodes([self.master, self.admin, new_storage])
        self.assertEqual(self.admin.getState(), NodeStates.UNKNOWN)


if __name__ == '__main__':
    unittest.main()

