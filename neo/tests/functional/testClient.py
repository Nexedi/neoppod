#
# Copyright (C) 2009-2019  Nexedi SA
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

from __future__ import print_function
import os
import transaction
import ZODB
import socket

from struct import pack
from neo.lib.util import makeChecksum, u64
from ZODB.FileStorage import FileStorage
from ZODB.tests.StorageTestBase import zodb_pickle
from persistent import Persistent
from . import NEOCluster, NEOFunctionalTest, NEOProcess

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

class PObject(Persistent):
    pass

class ClientTests(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        self.neo = NEOCluster(
            ['test_neo1', 'test_neo2', 'test_neo3', 'test_neo4'],
            partitions=3,
            replicas=2,
            master_count=1,
            temp_dir=self.getTempDirectory()
        )

    def _tearDown(self, success):
        self.neo.stop()
        del self.neo
        NEOFunctionalTest._tearDown(self, success)

    def __setup(self):
        self.neo.start()
        self.neo.expectClusterRunning()
        self.db = ZODB.DB(self.neo.getZODBStorage())

    def makeTransaction(self):
        # create a transaction a get the root object
        txn = transaction.TransactionManager()
        conn = self.db.open(transaction_manager=txn)
        return (txn, conn)

    def testIsolationAtZopeLevel(self):
        """ Check transaction isolation within zope connection """
        self.__setup()
        t, c = self.makeTransaction()
        root = c.root()
        root['item'] = 0
        root['other'] = 'bla'
        t.commit()
        t1, c1 = self.makeTransaction()
        t2, c2 = self.makeTransaction()
        # Makes c2 take a snapshot of database state
        c2.root()['other']
        c1.root()['item'] = 1
        t1.commit()
        # load object from zope cache
        self.assertEqual(c1.root()['item'], 1)
        self.assertEqual(c2.root()['item'], 0)

    def testIsolationWithoutZopeCache(self):
        """ Check isolation with zope cache cleared """
        self.__setup()
        t, c = self.makeTransaction()
        root = c.root()
        root['item'] = 0
        root['other'] = 'bla'
        t.commit()
        t1, c1 = self.makeTransaction()
        t2, c2 = self.makeTransaction()
        # Makes c2 take a snapshot of database state
        c2.root()['other']
        c1.root()['item'] = 1
        t1.commit()
        # clear zope cache to force re-ask NEO
        c1.cacheMinimize()
        c2.cacheMinimize()
        self.assertEqual(c1.root()['item'], 1)
        self.assertEqual(c2.root()['item'], 0)

    def __checkTree(self, tree, depth=TREE_SIZE):
        self.assertTrue(isinstance(tree, Tree))
        self.assertEqual(depth, tree.depth)
        depth -= 1
        if depth <= 0:
            return
        self.__checkTree(tree.right, depth)
        self.__checkTree(tree.left, depth)

    def __getDataFS(self):
        name = os.path.join(self.getTempDirectory(), 'data.fs')
        if os.path.exists(name):
            os.remove(name)
        return FileStorage(file_name=name)

    def __populate(self, db, tree_size=TREE_SIZE, with_undo=True):
        if isinstance(db.storage, FileStorage):
            from base64 import b64encode as undo_tid
        else:
            undo_tid = lambda x: x
        def undo(tid=None):
            db.undo(undo_tid(tid or db.lastTransaction()))
            transaction.commit()
        conn = db.open()
        root = conn.root()
        root['trees'] = Tree(tree_size)
        ob = root['trees'].right
        left = ob.left
        del ob.left
        transaction.commit()
        ob._p_changed = 1
        transaction.commit()
        t2 = db.lastTransaction()
        ob.left = left
        transaction.commit()
        if with_undo:
            undo()
            t4 = db.lastTransaction()
            undo(t2)
            undo()
            undo(t4)
            undo()
            undo()
        conn.close()

    def testImport(self):

        # source database
        dfs_storage  = self.__getDataFS()
        dfs_db = ZODB.DB(dfs_storage)
        self.__populate(dfs_db)

        # create a neo storage
        self.neo.start()
        neo_storage = self.neo.getZODBStorage()

        # copy data fs to neo
        neo_storage.copyTransactionsFrom(dfs_storage, verbose=0)
        dfs_db.close()

        # check neo content
        (neo_db, neo_conn) = self.neo.getZODBConnection()
        self.__checkTree(neo_conn.root()['trees'])

    def testMigrationTool(self):
        dfs_storage  = self.__getDataFS()
        dfs_db = ZODB.DB(dfs_storage)
        self.__populate(dfs_db, with_undo=False)
        dump = self.__dump(dfs_storage)
        fs_path = dfs_storage.__name__
        dfs_db.close()

        neo = self.neo
        neo.start()

        kw = {'cluster': neo.cluster_name, 'quiet': None}
        master_nodes = neo.master_nodes.replace('/', ' ')
        if neo.SSL:
            kw['ca'], kw['cert'], kw['key'] = neo.SSL

        p = NEOProcess('neomigrate', fs_path, master_nodes, **kw)
        p.start()
        p.wait()

        os.remove(fs_path)
        p = NEOProcess('neomigrate', master_nodes, fs_path, **kw)
        p.start()
        p.wait()

        self.assertEqual(dump, self.__dump(FileStorage(fs_path)))

    def __dump(self, storage, sorted=sorted):
        return {u64(t.tid): sorted((u64(o.oid), o.data_txn and u64(o.data_txn),
                              None if o.data is None else makeChecksum(o.data))
                            for o in t)
                for t in storage.iterator()}

    def testExport(self):

        # create a neo storage
        self.neo.start()
        (neo_db, neo_conn) = self.neo.getZODBConnection()
        self.__populate(neo_db)
        dump = self.__dump(neo_db.storage, list)

        # copy neo to data fs
        dfs_storage  = self.__getDataFS()
        neo_storage = self.neo.getZODBStorage()
        dfs_storage.copyTransactionsFrom(neo_storage)

        # check data fs content
        dfs_db = ZODB.DB(dfs_storage)
        root = dfs_db.open().root()

        self.__checkTree(root['trees'])
        dfs_db.close()
        self.neo.stop()

        self.neo = NEOCluster(db_list=['test_neo1'], partitions=3,
            importer={"zodb": [("root", {
                "storage": "<filestorage>\npath %s\n</filestorage>"
                            % dfs_storage.getName()})]},
            temp_dir=self.getTempDirectory())
        self.neo.start()
        neo_db, neo_conn = self.neo.getZODBConnection()
        self.__checkTree(neo_conn.root()['trees'])
        # BUG: The following check is sometimes done whereas the import is not
        #      finished, resulting in a failure because getReplicationTIDList
        #      is not implemented by the Importer backend.
        self.assertEqual(dump, self.__dump(neo_db.storage, list))

    def testIPv6Client(self):
        """ Test the connectivity of an IPv6 connection for neo client """

        def test():
            """
            Implement the IPv6Client test
            """
            self.neo = NEOCluster(['test_neo1'], replicas=0,
                temp_dir = self.getTempDirectory(),
                address_type = socket.AF_INET6
                )
            self.neo.start()
            db1, conn1 = self.neo.getZODBConnection()
            db2, conn2 = self.neo.getZODBConnection()
        self.runWithTimeout(40, test)

    def testGreaterOIDSaved(self):
        """
            Store an object with an OID greater than the last generated by the
            master. This OID must be intercepted at commit, used for next OID
            generations and persistently saved on storage nodes.
        """
        self.neo.start()
        db1, conn1 = self.neo.getZODBConnection()
        st1 = conn1._storage
        t1 = transaction.Transaction()
        rev = '\0' * 8
        data = zodb_pickle(PObject())
        my_oid = pack('!Q', 100000)
        # store an object with this OID
        st1.tpc_begin(t1)
        st1.store(my_oid, rev, data, '', t1)
        st1.tpc_vote(t1)
        st1.tpc_finish(t1)
        # request an oid, should be greater than mine
        oid = st1.new_oid()
        self.assertTrue(oid > my_oid)
