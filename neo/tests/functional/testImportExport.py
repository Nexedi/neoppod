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
import tempfile
import transaction

import ZODB
from ZODB.FileStorage import FileStorage
from Persistence import Persistent

from neo.tests.functional import NEOCluster
from neo.client.Storage import Storage as NEOStorage


TREE_SIZE = 6

class Tree(Persistent):
    """ A simple binary tree """

    def __init__(self, depth):
        self.depth = depth
        if depth <= 0:
            return
        depth -= 1
        self.right = Tree(depth)
        self.left = Tree(depth)


class ImportExportTests(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='neo_import_export_')
        print "using the temp directory %s" % self.temp_dir

    def tearDown(self):
        pass

    def checkTree(self, tree, depth=TREE_SIZE):
        self.assertTrue(isinstance(tree, Tree))
        self.assertEqual(depth, tree.depth)
        depth -= 1
        if depth <= 0:
            return
        self.checkTree(tree.right, depth)
        self.checkTree(tree.left, depth)

    def getDataFS(self, reset=False):
        name = os.path.join(self.temp_dir, 'data.fs')
        if reset and os.path.exists(name):
            os.remove(name)
        storage = FileStorage(file_name=name)
        db = ZODB.DB(storage=storage)
        return (db, storage)
    
    def populate(self, db, tree_size=TREE_SIZE):
        conn = db.open()
        root = conn.root()
        root['trees'] = Tree(tree_size)
        transaction.commit()
        conn.close()

    def testImport(self):

        # source database
        dfs_db, dfs_storage  = self.getDataFS()
        self.populate(dfs_db)

        # create a neo cluster
        databases = ['test_neo1', 'test_neo2']
        neo = NEOCluster(databases, port_base=20000, master_node_count=2)
        neo.setupDB()
        neo.start()

        # create a neo storage
        neo_args = {'connector': 'SocketConnector', 'name': neo.cluster_name}
        neo_storage = NEOStorage(master_nodes=neo.master_nodes, **neo_args)

        # copy data fs to neo
        neo_storage.copyTransactionsFrom(dfs_storage, verbose=0)

        # check neo content
        neo_db = ZODB.DB(storage=neo_storage)
        conn = neo_db.open()
        root = conn.root()

        self.checkTree(root['trees'])

    def testExport(self):

        # create a neo cluster
        databases = ['test_neo1', 'test_neo2']
        neo = NEOCluster(databases, port_base=20000, master_node_count=2)
        neo.setupDB()
        neo.start()

        # create a neo storage
        neo_args = {'connector': 'SocketConnector', 'name': neo.cluster_name}
        neo_storage = NEOStorage(master_nodes=neo.master_nodes, **neo_args)
        neo_db = ZODB.DB(storage=neo_storage)
        self.populate(neo_db)

        # copy neo to data fs
        dfs_db, dfs_storage  = self.getDataFS(reset=True)
        dfs_storage.copyTransactionsFrom(neo_storage, verbose=0)

        # check data fs content
        conn = dfs_db.open()
        root = conn.root()

        self.checkTree(root['trees'])
        

if __name__ == "__main__":
    unittest.main()
