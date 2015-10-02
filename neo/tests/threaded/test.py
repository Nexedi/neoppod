#
# Copyright (C) 2011-2015  Nexedi SA
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
import sys
import threading
import transaction
import unittest
from thread import get_ident
from zlib import compress
from persistent import Persistent
from ZODB import POSException
from neo.storage.transactions import TransactionManager, \
    DelayedError, ConflictError
from neo.lib.connection import ConnectionClosed, MTClientConnection
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, Packets, \
    ZERO_TID
from .. import expectedFailure, _UnexpectedSuccess, Patch
from . import NEOCluster, NEOThreadedTest
from neo.lib.util import add64, makeChecksum
from neo.client.exception import NEOStorageError
from neo.client.pool import CELL_CONNECTED, CELL_GOOD

class PCounter(Persistent):
    value = 0

class PCounterWithResolution(PCounter):
    def _p_resolveConflict(self, old, saved, new):
        new['value'] += saved['value'] - old.get('value', 0)
        return new

class Test(NEOThreadedTest):

    def testBasicStore(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.getZODBStorage()
            data_info = {}
            compressible = 'x' * 20
            compressed = compress(compressible)
            oid_list = []
            if cluster.storage.getAdapter() == 'SQLite':
                big = None
                data = 'foo', '', 'foo', compressed, compressible
            else:
                big = os.urandom(65536) * 600
                assert len(big) < len(compress(big))
                data = ('foo', big, '', 'foo', big[:2**24-1], big,
                        compressed, compressible, big[:2**24])
                self.assertFalse(cluster.storage.sqlCount('bigdata'))
            self.assertFalse(cluster.storage.sqlCount('data'))
            for data in data:
                if data is compressible:
                    key = makeChecksum(compressed), 1
                else:
                    key = makeChecksum(data), 0
                oid = storage.new_oid()
                txn = transaction.Transaction()
                storage.tpc_begin(txn)
                r1 = storage.store(oid, None, data, '', txn)
                r2 = storage.tpc_vote(txn)
                data_info[key] = 1
                self.assertEqual(data_info, cluster.storage.getDataLockInfo())
                serial = storage.tpc_finish(txn)
                data_info[key] = 0
                self.tic()
                self.assertEqual(data_info, cluster.storage.getDataLockInfo())
                self.assertEqual((data, serial), storage.load(oid, ''))
                storage._cache.clear()
                self.assertEqual((data, serial), storage.load(oid, ''))
                self.assertEqual((data, serial), storage.load(oid, ''))
                oid_list.append((oid, data, serial))
            if big:
                self.assertTrue(cluster.storage.sqlCount('bigdata'))
            self.assertTrue(cluster.storage.sqlCount('data'))
            for i, (oid, data, serial) in enumerate(oid_list, 1):
                storage._cache.clear()
                cluster.storage.dm.deleteObject(oid)
                self.assertRaises(POSException.POSKeyError,
                    storage.load, oid, '')
                for oid, data, serial in oid_list[i:]:
                    self.assertEqual((data, serial), storage.load(oid, ''))
            if big:
                self.assertFalse(cluster.storage.sqlCount('bigdata'))
            self.assertFalse(cluster.storage.sqlCount('data'))
        finally:
            cluster.stop()

    def testDeleteObject(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.getZODBStorage()
            for clear_cache in 0, 1:
                for tst in 'a.', 'bcd.':
                    oid = storage.new_oid()
                    serial = None
                    for data in tst:
                        txn = transaction.Transaction()
                        storage.tpc_begin(txn)
                        if data == '.':
                            storage.deleteObject(oid, serial, txn)
                        else:
                            storage.store(oid, serial, data, '', txn)
                        storage.tpc_vote(txn)
                        serial = storage.tpc_finish(txn)
                        if clear_cache:
                            storage._cache.clear()
                    self.assertRaises(POSException.POSKeyError,
                        storage.load, oid, '')
        finally:
            cluster.stop()

    def testCreationUndoneHistory(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.getZODBStorage()
            oid = storage.new_oid()
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            storage.store(oid, None, 'foo', '', txn)
            storage.tpc_vote(txn)
            tid1 = storage.tpc_finish(txn)
            storage.tpc_begin(txn)
            storage.undo(tid1, txn)
            tid2 = storage.tpc_finish(txn)
            storage.tpc_begin(txn)
            storage.undo(tid2, txn)
            tid3 = storage.tpc_finish(txn)
            expected = [(tid1, 3), (tid2, 0), (tid3, 3)]
            for x in storage.history(oid, 10):
                self.assertEqual((x['tid'], x['size']), expected.pop())
            self.assertFalse(expected)
        finally:
            cluster.stop()

    def testStorageDataLock(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.getZODBStorage()
            data_info = {}

            data = 'foo'
            key = makeChecksum(data), 0
            oid = storage.new_oid()
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            r1 = storage.store(oid, None, data, '', txn)
            r2 = storage.tpc_vote(txn)
            tid = storage.tpc_finish(txn)
            data_info[key] = 0
            storage.sync()

            txn = [transaction.Transaction() for x in xrange(3)]
            for t in txn:
                storage.tpc_begin(t)
                storage.store(tid and oid or storage.new_oid(),
                              tid, data, '', t)
                tid = None
            for t in txn:
                storage.tpc_vote(t)
            data_info[key] = 3
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[1])
            storage.sync()
            data_info[key] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            tid1 = storage.tpc_finish(txn[2])
            self.tic()
            data_info[key] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[0])
            storage.sync()
            data_info[key] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())
        finally:
            cluster.stop()

    def testDelayedUnlockInformation(self):
        except_list = []
        def delayUnlockInformation(conn, packet):
            return isinstance(packet, Packets.NotifyUnlockInformation)
        def onStoreObject(orig, tm, ttid, serial, oid, *args):
            if oid == resume_oid and delayUnlockInformation in m2s:
                m2s.remove(delayUnlockInformation)
            try:
                return orig(tm, ttid, serial, oid, *args)
            except Exception, e:
                except_list.append(e.__class__)
                raise
        cluster = NEOCluster(storage_count=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = ob = PCounter()
            with cluster.master.filterConnection(cluster.storage) as m2s:
                resume_oid = None
                m2s.add(delayUnlockInformation,
                    Patch(TransactionManager, storeObject=onStoreObject))
                t.commit()
                resume_oid = ob._p_oid
                ob._p_changed = 1
                t.commit()
                self.assertFalse(delayUnlockInformation in m2s)
        finally:
            cluster.stop()
        self.assertEqual(except_list, [DelayedError])

    def _testDeadlockAvoidance(self, scenario):
        except_list = []
        delay = threading.Event(), threading.Event()
        ident = get_ident()
        def onStoreObject(orig, tm, ttid, serial, oid, *args):
            if oid == counter_oid:
                scenario[1] -= 1
                if not scenario[1]:
                    delay[0].set()
            try:
                return orig(tm, ttid, serial, oid, *args)
            except Exception, e:
                except_list.append(e.__class__)
                raise
        def onAsk(orig, conn, packet, *args, **kw):
            c2 = get_ident() == ident
            switch = isinstance(packet, Packets.AskBeginTransaction)
            if switch:
                if c2:
                    delay[1].wait()
            elif isinstance(packet, (Packets.AskStoreObject,
                                     Packets.AskFinishTransaction)):
                delay[c2].wait()
                scenario[0] -= 1
                switch = not scenario[0]
            try:
                return orig(conn, packet, *args, **kw)
            finally:
                if switch:
                    delay[c2].clear()
                    delay[1-c2].set()

        cluster = NEOCluster(storage_count=2, replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = ob = PCounterWithResolution()
            t.commit()
            counter_oid = ob._p_oid
            del ob, t, c

            t1, c1 = cluster.getTransaction()
            t2, c2 = cluster.getTransaction()
            o1 = c1.root()[0]
            o2 = c2.root()[0]
            o1.value += 1
            o2.value += 2

            with Patch(TransactionManager, storeObject=onStoreObject), \
                 Patch(MTClientConnection, ask=onAsk):
                t = self.newThread(t1.commit)
                t2.commit()
                t.join()
            t1.begin()
            t2.begin()
            self.assertEqual(o1.value, 3)
            self.assertEqual(o2.value, 3)
        finally:
            cluster.stop()
        return except_list

    def testDelayedStore(self):
        # 0: C1 -> S1, S2
        # 1: C2 -> S1, S2 (delayed)
        # 2: C1 commits
        # 3: C2 resolves conflict
        self.assertEqual(self._testDeadlockAvoidance([2, 4]),
            [DelayedError, DelayedError, ConflictError, ConflictError])

    @expectedFailure(POSException.ConflictError)
    def testDeadlockAvoidance(self):
        # This test fail because deadlock avoidance is not fully implemented.
        # 0: C1 -> S1
        # 1: C2 -> S1, S2 (delayed)
        # 2: C1 -> S2 (deadlock)
        # 3: C2 commits
        # 4: C1 resolves conflict
        self.assertEqual(self._testDeadlockAvoidance([1, 3]),
            [DelayedError, ConflictError, "???" ])

    def testConflictResolutionTriggered2(self):
        """ Check that conflict resolution works """
        cluster = NEOCluster()
        try:
            cluster.start()
            # create the initial object
            t, c = cluster.getTransaction()
            c.root()['with_resolution'] = ob = PCounterWithResolution()
            t.commit()
            self.assertEqual(ob._p_changed, 0)
            oid = ob._p_oid
            tid1 = ob._p_serial
            self.assertNotEqual(tid1, ZERO_TID)
            del ob, t, c

            # then check resolution
            t1, c1 = cluster.getTransaction()
            t2, c2 = cluster.getTransaction()
            o1 = c1.root()['with_resolution']
            o2 = c2.root()['with_resolution']
            self.assertEqual(o1.value, 0)
            self.assertEqual(o2.value, 0)
            o1.value += 1
            o2.value += 2
            t1.commit()
            self.assertEqual(o1._p_changed, 0)
            tid2 = o1._p_serial
            self.assertTrue(tid1 < tid2)
            self.assertEqual(o1.value, 1)
            self.assertEqual(o2.value, 2)
            t2.commit()
            self.assertEqual(o2._p_changed, None)
            t1.begin()
            t2.begin()
            self.assertEqual(o2.value, 3)
            self.assertEqual(o1.value, 3)
            tid3 = o1._p_serial
            self.assertTrue(tid2 < tid3)
            self.assertEqual(tid3, o2._p_serial)

            # check history
            history = c1.db().history
            self.assertEqual([x['tid'] for x in history(oid, size=1)], [tid3])
            self.assertEqual([x['tid'] for x in history(oid, size=10)],
                             [tid3, tid2, tid1])
        finally:
            cluster.stop()

    def test_notifyNodeInformation(self):
        # translated from MasterNotificationsHandlerTests
        # (neo.tests.client.testMasterHandler)
        cluster = NEOCluster(replicas=1)
        try:
            cluster.start()
            cluster.db # open DB
            s0, s1 = cluster.client.nm.getStorageList()
            conn = s0.getConnection()
            self.assertFalse(conn.isClosed())
            getCellSortKey = cluster.client.cp.getCellSortKey
            self.assertEqual(getCellSortKey(s0), CELL_CONNECTED)
            cluster.neoctl.dropNode(s0.getUUID())
            self.assertEqual([s1], cluster.client.nm.getStorageList())
            self.assertTrue(conn.isClosed())
            self.assertEqual(getCellSortKey(s0), CELL_GOOD)
            # XXX: the test originally checked that 'unregister' method
            #      was called (even if it's useless in this case),
            #      but we would need an API to do that easily.
            self.assertFalse(cluster.client.dispatcher.registered(conn))
        finally:
            cluster.stop()

    def testRestartWithMissingStorage(self):
        # translated from neo.tests.functional.testStorage.StorageTest
        cluster = NEOCluster(replicas=1, partitions=10)
        s1, s2 = cluster.storage_list
        try:
            cluster.start()
            self.assertEqual([], cluster.getOudatedCells())
        finally:
            cluster.stop()
        # restart it with one storage only
        cluster.reset()
        try:
            cluster.start(storage_list=(s1,))
            self.assertEqual(NodeStates.UNKNOWN, cluster.getNodeState(s2))
        finally:
            cluster.stop()

    def testVerificationCommitUnfinishedTransactions(self):
        """ Verification step should commit unfinished transactions """
        # translated from neo.tests.functional.testCluster.ClusterTests
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = 'ok'
            t.commit()
            self.tic()
            data_info = cluster.storage.getDataLockInfo()
            self.assertEqual(data_info.values(), [0, 0])
            # (obj|trans) become t(obj|trans)
            cluster.storage.switchTables()
        finally:
            cluster.stop()
        cluster.reset()
        self.assertEqual(dict.fromkeys(data_info, 1),
                         cluster.storage.getDataLockInfo())
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            # transaction should be verified and commited
            self.assertEqual(c.root()[0], 'ok')
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())
        finally:
            cluster.stop()

    def testStorageReconnectDuringStore(self):
        cluster = NEOCluster(replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = 'ok'
            while cluster.client.cp.connection_dict:
                cluster.client.cp._dropConnections()
            t.commit() # store request
        finally:
            cluster.stop()

    def testStorageReconnectDuringTransactionLog(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            while cluster.client.cp.connection_dict:
                cluster.client.cp._dropConnections()
            tid, (t1,) = cluster.client.transactionLog(
                ZERO_TID, c.db().lastTransaction(), 10)
        finally:
            cluster.stop()

    def testStorageReconnectDuringUndoLog(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            while cluster.client.cp.connection_dict:
                cluster.client.cp._dropConnections()
            t1, = cluster.client.undoLog(0, 10)
        finally:
            cluster.stop()

    def testDropNodeThenRestartCluster(self):
        """ Start a cluster with more than one storage, down one, shutdown the
        cluster then restart it. The partition table recovered must not include
        the dropped node """
        def checkNodeState(state):
            self.assertEqual(cluster.getNodeState(s1), state)
            self.assertEqual(cluster.getNodeState(s2), NodeStates.RUNNING)

        # start with two storage / one replica
        cluster = NEOCluster(storage_count=2, replicas=1)
        s1, s2 = cluster.storage_list
        try:
            cluster.start()
            checkNodeState(NodeStates.RUNNING)
            self.assertEqual([], cluster.getOudatedCells())
            # drop one
            cluster.neoctl.dropNode(s1.uuid)
            checkNodeState(None)
            self.tic() # Let node state update reach remaining storage
            checkNodeState(None)
            self.assertEqual([], cluster.getOudatedCells())
            # restart with s2 only
        finally:
            cluster.stop()
        cluster.reset()
        try:
            cluster.start(storage_list=[s2])
            checkNodeState(None)
            # then restart it, it must be in pending state
            s1.start()
            self.tic()
            checkNodeState(NodeStates.PENDING)
        finally:
            cluster.stop()

    def test2Clusters(self):
        cluster1 = NEOCluster()
        cluster2 = NEOCluster()
        try:
            cluster1.start()
            cluster2.start()
            t1, c1 = cluster1.getTransaction()
            t2, c2 = cluster2.getTransaction()
            c1.root()['1'] = c2.root()['2'] = ''
            t1.commit()
            t2.commit()
        finally:
            cluster1.stop()
            cluster2.stop()

    def testAbortStorage(self):
        cluster = NEOCluster(partitions=2, storage_count=2)
        storage = cluster.storage_list[0]
        try:
            cluster.start()
            # prevent storage to reconnect, in order to easily test
            # that cluster becomes non-operational
            storage.connectToPrimary = sys.exit
            # send an unexpected to master so it aborts connection to storage
            storage.master_conn.answer(Packets.Pong())
            self.tic()
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.VERIFYING)
        finally:
            cluster.stop()

    def testShutdown(self):
        cluster = NEOCluster(master_count=3, partitions=10,
                             replicas=1, storage_count=3)
        try:
            cluster.start()
            # fill DB a little
            t, c = cluster.getTransaction()
            c.root()[''] = ''
            t.commit()
            # tell admin to shutdown the cluster
            cluster.neoctl.setClusterState(ClusterStates.STOPPING)
            # all nodes except clients should exit
            cluster.join(cluster.master_list
                       + cluster.storage_list
                       + cluster.admin_list)
        finally:
            cluster.stop()
        cluster.reset() # reopen DB to check partition tables
        dm = cluster.storage_list[0].dm
        self.assertEqual(1, dm.getPTID())
        pt = list(dm.getPartitionTable())
        self.assertEqual(20, len(pt))
        for _, _, state in pt:
            self.assertEqual(state, CellStates.UP_TO_DATE)
        for s in cluster.storage_list[1:]:
            self.assertEqual(s.dm.getPTID(), 1)
            self.assertEqual(list(s.dm.getPartitionTable()), pt)

    def testInternalInvalidation(self):
        l1 = threading.Lock(); l1.acquire()
        l2 = threading.Lock(); l2.acquire()
        def _handlePacket(orig, conn, packet, kw={}, handler=None):
            if type(packet) is Packets.AnswerTransactionFinished:
                l1.release()
                l2.acquire()
            orig(conn, packet, kw, handler)
        cluster = NEOCluster()
        try:
            cluster.start()
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x1 = PCounter()
            t1.commit()
            t1.begin()
            x1.value = 1
            t2, c2 = cluster.getTransaction()
            x2 = c2.root()['x']
            p = Patch(cluster.client, _handlePacket=_handlePacket)
            try:
                p.apply()
                t = self.newThread(t1.commit)
                l1.acquire()
                t2.begin()
            finally:
                del p
                l2.release()
            t.join()
            self.assertEqual(x2.value, 1)
        finally:
            cluster.stop()

    def testExternalInvalidation(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            cache = cluster.client._cache
            # Initialize objects
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x1 = PCounter()
            c1.root()['y'] = y = PCounter()
            y.value = 1
            t1.commit()
            # Get pickle of y
            t1.begin()
            x = c1._storage.load(x1._p_oid)[0]
            y = c1._storage.load(y._p_oid)[0]
            # Start the testing transaction
            # (at this time, we still have x=0 and y=1)
            t2, c2 = cluster.getTransaction()
            # Copy y to x using a different Master-Client connection
            client = cluster.newClient()
            txn = transaction.Transaction()
            client.tpc_begin(txn)
            client.store(x1._p_oid, x1._p_serial, y, '', txn)
            # Delay invalidation for x
            with cluster.master.filterConnection(cluster.client) as m2c:
                m2c.add(lambda conn, packet:
                    isinstance(packet, Packets.InvalidateObjects))
                tid = client.tpc_finish(txn, None)
                # Change to x is committed. Testing connection must ask the
                # storage node to return original value of x, even if we
                # haven't processed yet any invalidation for x.
                x2 = c2.root()['x']
                cache.clear() # bypass cache
                self.assertEqual(x2.value, 0)
            x2._p_deactivate()
            t1.begin() # process invalidation and sync connection storage
            self.assertEqual(x2.value, 0)
            # New testing transaction. Now we can see the last value of x.
            t2.begin()
            self.assertEqual(x2.value, 1)

            # Now test cache invalidation during a load from a storage
            l1 = threading.Lock(); l1.acquire()
            l2 = threading.Lock(); l2.acquire()
            def _loadFromStorage(orig, *args):
                try:
                    return orig(*args)
                finally:
                    l1.release()
                    l2.acquire()
            x2._p_deactivate()
            # Remove last version of x from cache
            cache._remove(cache._oid_dict[x2._p_oid].pop())
            p = Patch(cluster.client, _loadFromStorage=_loadFromStorage)
            try:
                p.apply()
                t = self.newThread(x2._p_activate)
                l1.acquire()
                # At this point, x could not be found the cache and the result
                # from the storage (which is <value=1, next_tid=None>) is about
                # to be processed.
                # Now modify x to receive an invalidation for it.
                txn = transaction.Transaction()
                client.tpc_begin(txn)
                client.store(x2._p_oid, tid, x, '', txn) # value=0
                tid = client.tpc_finish(txn, None)
                t1.begin() # make sure invalidation is processed
            finally:
                del p
                # Resume processing of answer from storage. An entry should be
                # added in cache for x=1 with a fixed next_tid (i.e. not None)
                l2.release()
            t.join()
            self.assertEqual(x2.value, 1)
            self.assertEqual(x1.value, 0)

            # l1 is acquired and l2 is released
            # Change x again from 0 to 1, while the checking connection c1
            # is suspended at the beginning of the transaction t1,
            # between Storage.sync() and flush of invalidations.
            def _flush_invalidations(orig):
                l1.release()
                l2.acquire()
                orig()
            x1._p_deactivate()
            t1.abort()
            p = Patch(c1, _flush_invalidations=_flush_invalidations)
            try:
                p.apply()
                t = self.newThread(t1.begin)
                l1.acquire()
                txn = transaction.Transaction()
                client.tpc_begin(txn)
                client.store(x2._p_oid, tid, y, '', txn)
                tid = client.tpc_finish(txn, None)
                client.close()
            finally:
                del p
                l2.release()
            t.join()
            # A transaction really begins when it acquires the lock to flush
            # invalidations. The previous lastTransaction() only does a ping
            # to make sure we have a recent enough view of the DB.
            self.assertEqual(x1.value, 1)

        finally:
            cluster.stop()

    def testClientReconnection(self):
        conn = [None]
        def getConnForNode(orig, node):
            self.assertTrue(node.isRunning())
            return conn.pop()
        cluster = NEOCluster()
        try:
            cluster.start()
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x1 = PCounter()
            c1.root()['y'] = y = PCounter()
            y.value = 1
            t1.commit()
            x = c1._storage.load(x1._p_oid)[0]
            y = c1._storage.load(y._p_oid)[0]

            # close connections to master & storage
            c, = cluster.master.nm.getClientList()
            c.getConnection().close()
            c, = cluster.storage.nm.getClientList()
            c.getConnection().close()
            self.tic()

            # modify x with another client
            client = cluster.newClient()
            txn = transaction.Transaction()
            client.tpc_begin(txn)
            client.store(x1._p_oid, x1._p_serial, y, '', txn)
            tid = client.tpc_finish(txn, None)
            client.close()
            self.tic()

            # Check reconnection to storage.
            with Patch(cluster.client.cp, getConnForNode=getConnForNode):
                self.assertFalse(cluster.client.history(x1._p_oid))
            self.assertFalse(conn)
            self.assertTrue(cluster.client.history(x1._p_oid))

            # Check successful reconnection to master.
            t1.begin()
            self.assertEqual(x1._p_changed ,None)
            self.assertEqual(x1.value, 1)
        finally:
            cluster.stop()

    def testInvalidTTID(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            client = cluster.client
            txn = transaction.Transaction()
            client.tpc_begin(txn)
            txn_context = client._txn_container.get(txn)
            txn_context['ttid'] = add64(txn_context['ttid'], 1)
            self.assertRaises(POSException.StorageError,
                              client.tpc_finish, txn, None)
        finally:
            cluster.stop()

    def testStorageFailureDuringTpcFinish(self):
        def answerTransactionFinished(conn, packet):
            if isinstance(packet, Packets.AnswerTransactionFinished):
                c, = cluster.storage.getConnectionList(cluster.master)
                c.abort()
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()['x'] = PCounter()
            with cluster.master.filterConnection(cluster.client) as m2c:
                m2c.add(answerTransactionFinished)
                # XXX: This is an expected failure. A ttid column was added to
                #      'trans' table to permit recovery, by checking that the
                #      transaction was really committed.
                try:
                    t.commit()
                    raise _UnexpectedSuccess
                except ConnectionClosed:
                    pass
            t.begin()
            expectedFailure(self.assertIn)('x', c.root())
        finally:
            cluster.stop()

    def testEmptyTransaction(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            txn = transaction.Transaction()
            storage = cluster.getZODBStorage()
            storage.tpc_begin(txn)
            storage.tpc_vote(txn)
            serial = storage.tpc_finish(txn)
            t, = storage.iterator()
            self.assertEqual(t.tid, serial)
            self.assertFalse(t.oid_list)
        finally:
            cluster.stop()

    def testRecycledClientUUID(self):
        def delayNotifyInformation(conn, packet):
            return isinstance(packet, Packets.NotifyNodeInformation)
        def notReady(orig, *args):
            m2s.discard(delayNotifyInformation)
            return orig(*args)
        cluster = NEOCluster()
        try:
            cluster.start()
            cluster.getTransaction()
            with cluster.master.filterConnection(cluster.storage) as m2s:
                m2s.add(delayNotifyInformation)
                cluster.client.master_conn.close()
                client = cluster.newClient()
                p = Patch(client.storage_bootstrap_handler, notReady=notReady)
                try:
                    p.apply()
                    x = client.load(ZERO_TID)
                finally:
                    del p
                    client.close()
                self.assertNotIn(delayNotifyInformation, m2s)
        finally:
            cluster.stop()

    def testAutostart(self):
        def startCluster():
            getClusterState = cluster.neoctl.getClusterState
            self.assertEqual(ClusterStates.RECOVERING, getClusterState())
            cluster.storage_list[2].start()
        cluster = NEOCluster(storage_count=3, autostart=3)
        try:
            cluster.startCluster = startCluster
            cluster.start(cluster.storage_list[:2])
        finally:
            cluster.stop()
            del cluster.startCluster

if __name__ == "__main__":
    unittest.main()
