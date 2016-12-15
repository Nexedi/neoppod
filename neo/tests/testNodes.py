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

import shutil
import unittest
from mock import Mock
from neo.lib.protocol import NodeTypes, NodeStates
from neo.lib.node import Node, MasterDB
from . import NeoUnitTestBase, getTempDirectory
from time import time
from os import chmod, mkdir, rmdir
from os.path import join, exists

class NodesTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self.nm = Mock()

    def testInit(self):
        """ Check the node initialization """
        address = ('127.0.0.1', 10000)
        uuid = self.getNewUUID(None)
        node = Node(self.nm, address=address, uuid=uuid)
        self.assertEqual(node.getState(), NodeStates.UNKNOWN)
        self.assertEqual(node.getAddress(), address)
        self.assertEqual(node.getUUID(), uuid)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())

    def testState(self):
        """ Check if the last changed time is updated when state is changed """
        node = Node(self.nm)
        self.assertEqual(node.getState(), NodeStates.UNKNOWN)
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())
        previous_time = node.getLastStateChange()
        node.setState(NodeStates.RUNNING)
        self.assertEqual(node.getState(), NodeStates.RUNNING)
        self.assertTrue(previous_time < node.getLastStateChange())
        self.assertTrue(time() - 1 < node.getLastStateChange() < time())

class NodeManagerTests(NeoUnitTestBase):

    def _addStorage(self):
        self.storage = self.nm.createStorage(
            address=('127.0.0.1', 1000), uuid=self.getStorageUUID())

    def _addMaster(self):
        self.master = self.nm.createMaster(
            address=('127.0.0.1', 2000), uuid=self.getMasterUUID())

    def _addClient(self):
        self.client = self.nm.createClient(uuid=self.getClientUUID())

    def _addAdmin(self):
        self.admin = self.nm.createAdmin(
            address=('127.0.0.1', 4000), uuid=self.getAdminUUID())

    def checkNodes(self, node_list):
        self.assertEqual(sorted(self.nm.getList()), sorted(node_list))

    def checkMasters(self, master_list):
        self.assertEqual(self.nm.getMasterList(), master_list)

    def checkStorages(self, storage_list):
        self.assertEqual(self.nm.getStorageList(), storage_list)

    def checkClients(self, client_list):
        self.assertEqual(self.nm.getClientList(), client_list)

    def checkByServer(self, node):
        self.assertEqual(node, self.nm.getByAddress(node.getAddress()))

    def checkByUUID(self, node):
        self.assertEqual(node, self.nm.getByUUID(node.getUUID()))

    def testInit(self):
        """ Check the manager is empty when started """
        manager = self.nm
        self.checkNodes([])
        self.checkMasters([])
        self.checkStorages([])
        self.checkClients([])
        address = ('127.0.0.1', 10000)
        self.assertEqual(manager.getByAddress(address), None)
        self.assertEqual(manager.getByAddress(None), None)
        uuid = self.getNewUUID(None)
        self.assertEqual(manager.getByUUID(uuid), None)
        self.assertEqual(manager.getByUUID(None), None)

    def testAdd(self):
        """ Check if new nodes are registered in the manager """
        manager = self.nm
        self.checkNodes([])
        # storage
        self._addStorage()
        self.checkNodes([self.storage])
        self.checkStorages([self.storage])
        self.checkMasters([])
        self.checkClients([])
        self.checkByServer(self.storage)
        self.checkByUUID(self.storage)
        # master
        self._addMaster()
        self.checkNodes([self.storage, self.master])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([])
        self.checkByServer(self.master)
        self.checkByUUID(self.master)
        # client
        self._addClient()
        self.checkNodes([self.storage, self.master, self.client])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        # client node has no address
        self.assertEqual(manager.getByAddress(self.client.getAddress()), None)
        self.checkByUUID(self.client)
        # admin
        self._addAdmin()
        self.checkNodes([self.storage, self.master, self.client, self.admin])
        self.checkStorages([self.storage])
        self.checkMasters([self.master])
        self.checkClients([self.client])
        self.checkByServer(self.admin)
        self.checkByUUID(self.admin)

    def testUpdate(self):
        """ Check manager content update """
        # set up four nodes
        manager = self.nm
        self._addMaster()
        self._addStorage()
        self._addClient()
        self._addAdmin()
        self.checkNodes([self.master, self.storage, self.client, self.admin])
        self.checkMasters([self.master])
        self.checkStorages([self.storage])
        self.checkClients([self.client])
        # build changes
        old_address = self.master.getAddress()
        new_address = ('127.0.0.1', 2001)
        old_uuid = self.storage.getUUID()
        new_uuid = self.getStorageUUID()
        node_list = (
            (NodeTypes.CLIENT, None, self.client.getUUID(), NodeStates.DOWN, None),
            (NodeTypes.MASTER, new_address, self.master.getUUID(), NodeStates.RUNNING, None),
            (NodeTypes.STORAGE, self.storage.getAddress(), new_uuid,
                NodeStates.RUNNING, None),
            (NodeTypes.ADMIN, self.admin.getAddress(), self.admin.getUUID(),
                NodeStates.UNKNOWN, None),
        )
        # update manager content
        manager.update(Mock(), node_list)
        # - the client gets down
        self.checkClients([])
        # - master change it's address
        self.checkMasters([self.master])
        self.assertEqual(manager.getByAddress(old_address), None)
        self.master.setAddress(new_address)
        self.checkByServer(self.master)
        # - storage change it's UUID
        storage_list = manager.getStorageList()
        self.assertTrue(len(storage_list), 1)
        new_storage = storage_list[0]
        self.assertNotEqual(new_storage.getUUID(), old_uuid)
        self.assertEqual(new_storage.getState(), NodeStates.RUNNING)
        # admin is still here but in UNKNOWN state
        self.checkNodes([self.master, self.admin, new_storage])
        self.assertEqual(self.admin.getState(), NodeStates.UNKNOWN)

class MasterDBTests(NeoUnitTestBase):

    def testInitialAccessRights(self):
        """
        Verify MasterDB raises immediately on instantiation if it cannot
        create a non-existing database. This does not guarantee any later
        open will succeed, but makes the simple error case obvious.
        """
        temp_dir = getTempDirectory()
        directory = join(temp_dir, 'read_only')
        db_file = join(directory, 'not_created')
        mkdir(directory, 0400)
        try:
            self.assertRaises(IOError, MasterDB, db_file)
        finally:
            rmdir(directory)

    def testLaterAccessRights(self):
        """
        Verify MasterDB does not raise when modifying database.
        """
        temp_dir = getTempDirectory()
        directory = join(temp_dir, 'read_write')
        db_file = join(directory, 'db')
        mkdir(directory)
        try:
            db = MasterDB(db_file)
            self.assertTrue(exists(db_file), db_file)
            chmod(db_file, 0400)
            address = ('example.com', 1024)
            # Must not raise
            db.add(address)
            # Value is stored
            self.assertTrue(address in db, [x for x in db])
            # But not visible to a new db instance (write access restored so
            # it can be created)
            chmod(db_file, 0600)
            db2 = MasterDB(db_file)
            self.assertFalse(address in db2, [x for x in db2])
        finally:
            shutil.rmtree(directory)

    def testPersistence(self):
        temp_dir = getTempDirectory()
        directory = join(temp_dir, 'read_write')
        db_file = join(directory, 'db')
        mkdir(directory)
        try:
            db = MasterDB(db_file)
            self.assertTrue(exists(db_file), db_file)
            address = ('example.com', 1024)
            db.add(address)
            address2 = ('example.org', 1024)
            db.add(address2)
            # Values are visible to a new db instance
            db2 = MasterDB(db_file)
            self.assertTrue(address in db2, [x for x in db2])
            self.assertTrue(address2 in db2, [x for x in db2])
            db.discard(address)
            # Create yet another instance (file is not supposed to be shared)
            db3 = MasterDB(db_file)
            self.assertFalse(address in db3, [x for x in db3])
            self.assertTrue(address2 in db3, [x for x in db3])
        finally:
            shutil.rmtree(directory)

if __name__ == '__main__':
    unittest.main()

