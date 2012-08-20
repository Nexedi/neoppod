#
# Copyright (C) 2009-2012  Nexedi SA
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

import os
import unittest
import transaction
import ZODB
import socket

from struct import pack
from neo.neoctl.neoctl import NeoCTL
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ConflictError
from ZODB.tests.StorageTestBase import zodb_pickle
from persistent import Persistent
from . import NEOCluster, NEOFunctionalTest

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


# simple persitent object with conflict resolution
class PCounter(Persistent):

    _value = 0

    def value(self):
        return self._value

    def inc(self):
        self._value += 1


class PCounterWithResolution(PCounter):

    def _p_resolveConflict(self, old, saved, new):
        new['_value'] = saved['_value'] + new['_value']
        return new

class PObject(Persistent):
    pass

class ClientTests(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        self.neo = NEOCluster(
            ['test_neo1', 'test_neo2', 'test_neo3', 'test_neo4'],
            replicas=2,
            master_count=1,
            temp_dir=self.getTempDirectory()
        )

    def _tearDown(self, success):
        if self.neo is not None:
            self.neo.stop()
        NEOFunctionalTest._tearDown(self, success)

    def __setup(self):
        # start cluster
        self.neo.setupDB()
        self.neo.start()
        self.neo.expectClusterRunning()
        self.db = ZODB.DB(self.neo.getZODBStorage())

    def makeTransaction(self):
        # create a transaction a get the root object
        txn = transaction.TransactionManager()
        conn = self.db.open(transaction_manager=txn)
        return (txn, conn)

    def testConflictResolutionTriggered1(self):
        """ Check that ConflictError is raised on write conflict """
        # create the initial objects
        self.__setup()
        t, c = self.makeTransaction()
        c.root()['without_resolution'] = PCounter()
        t.commit()

        # first with no conflict resolution
        t1, c1 = self.makeTransaction()
        t2, c2 = self.makeTransaction()
        o1 = c1.root()['without_resolution']
        o2 = c2.root()['without_resolution']
        self.assertEqual(o1.value(), 0)
        self.assertEqual(o2.value(), 0)
        o1.inc()
        o2.inc()
        o2.inc()
        t1.commit()
        self.assertEqual(o1.value(), 1)
        self.assertEqual(o2.value(), 2)
        self.assertRaises(ConflictError, t2.commit)

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
        # load objet from zope cache
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

    def __getDataFS(self, reset=False):
        name = os.path.join(self.getTempDirectory(), 'data.fs')
        if reset and os.path.exists(name):
            os.remove(name)
        storage = FileStorage(file_name=name)
        db = ZODB.DB(storage=storage)
        return (db, storage)

    def __populate(self, db, tree_size=TREE_SIZE, filestorage_bug=True):
        conn = db.open()
        root = conn.root()
        root['trees'] = Tree(tree_size)
        if filestorage_bug:
            ob = root['trees'].right
            left = ob.left
            del ob.left
            transaction.commit()
            ob._p_changed = 1
            transaction.commit()
            ob.left = left
        transaction.commit()
        conn.close()

    def testImport(self):

        # source database
        dfs_db, dfs_storage  = self.__getDataFS()
        self.__populate(dfs_db)

        # create a neo storage
        self.neo.start()
        neo_storage = self.neo.getZODBStorage()

        # copy data fs to neo
        neo_storage.copyTransactionsFrom(dfs_storage, verbose=0)

        # check neo content
        (neo_db, neo_conn) = self.neo.getZODBConnection()
        self.__checkTree(neo_conn.root()['trees'])

    def testExport(self, filestorage_bug=False):

        # create a neo storage
        self.neo.start()
        (neo_db, neo_conn) = self.neo.getZODBConnection()
        self.__populate(neo_db, filestorage_bug=filestorage_bug)

        # copy neo to data fs
        dfs_db, dfs_storage  = self.__getDataFS(reset=True)
        neo_storage = self.neo.getZODBStorage()
        dfs_storage.copyTransactionsFrom(neo_storage)

        # check data fs content
        conn = dfs_db.open()
        root = conn.root()

        self.__checkTree(root['trees'])

    def testExportFileStorageBug(self):
        # currently fails due to a bug in ZODB.FileStorage
        self.testExport(True)

    def testLockTimeout(self):
        """ Hold a lock on an object to block a second transaction """
        def test():
            self.neo = NEOCluster(['test_neo1'], replicas=0,
                temp_dir=self.getTempDirectory())
            neoctl = self.neo.getNEOCTL()
            self.neo.start()
            # BUG: The following 2 lines creates 2 app, i.e. 2 TCP connections
            #      to the storage, so there may be a race condition at network
            #      level and 'st2.store' may be effective before 'st1.store'.
            db1, conn1 = self.neo.getZODBConnection()
            db2, conn2 = self.neo.getZODBConnection()
            st1, st2 = conn1._storage, conn2._storage
            t1, t2 = transaction.Transaction(), transaction.Transaction()
            t1.user = t2.user = 'user'
            t1.description = t2.description = 'desc'
            oid = st1.new_oid()
            rev = '\0' * 8
            data = zodb_pickle(PObject())
            st2.tpc_begin(t2)
            st1.tpc_begin(t1)
            st1.store(oid, rev, data, '', t1)
            # this store will be delayed
            st2.store(oid, rev, data, '', t2)
            # the vote will timeout as t1 never release the lock
            self.assertRaises(ConflictError, st2.tpc_vote, t2)
        self.runWithTimeout(40, test)

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
            neoctl = NeoCTL(('::1', 0))
            self.neo.start()
            db1, conn1 = self.neo.getZODBConnection()
            db2, conn2 = self.neo.getZODBConnection()
        self.runWithTimeout(40, test)

    def testDelayedLocksCancelled(self):
        """
            Hold a lock on an object, try to get another lock on the same
            object to delay it. Then cancel the second transaction and check
            that the lock is not hold when the first transaction ends
        """
        def test():
            self.neo = NEOCluster(['test_neo1'], replicas=0,
                temp_dir=self.getTempDirectory())
            neoctl = self.neo.getNEOCTL()
            self.neo.start()
            db1, conn1 = self.neo.getZODBConnection()
            db2, conn2 = self.neo.getZODBConnection()
            st1, st2 = conn1._storage, conn2._storage
            t1, t2 = transaction.Transaction(), transaction.Transaction()
            t1.user = t2.user = 'user'
            t1.description = t2.description = 'desc'
            oid = st1.new_oid()
            rev = '\0' * 8
            data = zodb_pickle(PObject())
            st1.tpc_begin(t1)
            st2.tpc_begin(t2)
            # t1 own the lock
            st1.store(oid, rev, data, '', t1)
            # t2 store is delayed
            st2.store(oid, rev, data, '', t2)
            # cancel t2, should cancel the store too
            st2.tpc_abort(t2)
            # finish t1, should release the lock
            st1.tpc_vote(t1)
            st1.tpc_finish(t1)
            db3, conn3 = self.neo.getZODBConnection()
            st3 = conn3._storage
            t3 = transaction.Transaction()
            t3.user = 'user'
            t3.description = 'desc'
            st3.tpc_begin(t3)
            # retreive the last revision
            data, serial = st3.load(oid, '')
            # try to store again, should not be delayed
            st3.store(oid, serial, data, '', t3)
            # the vote should not timeout
            st3.tpc_vote(t3)
            st3.tpc_finish(t3)
        self.runWithTimeout(10, test)

    def testGreaterOIDSaved(self):
        """
            Store an object with an OID greater than the last generated by the
            master. This OID must be intercepted at commit, used for next OID
            generations and persistently saved on storage nodes.
        """
        self.neo = NEOCluster(['test_neo1'], replicas=0,
            temp_dir=self.getTempDirectory())
        neoctl = self.neo.getNEOCTL()
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

def test_suite():
    return unittest.makeSuite(ClientTests)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")

