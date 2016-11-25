#
# Copyright (C) 2011-2016  Nexedi SA
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
from persistent import Persistent, GHOST
from transaction.interfaces import TransientError
from ZODB import DB, POSException
from ZODB.DB import TransactionalUndo
from neo.storage.transactions import TransactionManager, \
    DelayedError, ConflictError
from neo.lib.connection import ServerConnection, MTClientConnection
from neo.lib.exception import DatabaseFailure, StoppedOperation
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, Packets, \
    ZERO_OID, ZERO_TID
from .. import expectedFailure, Patch
from . import LockLock, NEOCluster, NEOThreadedTest
from neo.lib.util import add64, makeChecksum, p64, u64
from neo.client.exception import NEOPrimaryMasterLost, NEOStorageError
from neo.client.pool import CELL_CONNECTED, CELL_GOOD
from neo.master.handlers.client import ClientServiceHandler
from neo.storage.handlers.client import ClientOperationHandler
from neo.storage.handlers.initialization import InitializationHandler

class PCounter(Persistent):
    value = 0

class PCounterWithResolution(PCounter):
    def _p_resolveConflict(self, old, saved, new):
        new['value'] = (
          saved.get('value', 0)
          + new.get('value', 0)
          - old.get('value', 0))
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

    def testUndoConflict(self, conflict_during_store=False):
        def waitResponses(orig, *args):
            orig(*args)
            p.revert()
            ob.value += 3
            t.commit()
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = ob = PCounterWithResolution()
            t.commit()
            ob.value += 1
            t.commit()
            undo = TransactionalUndo(cluster.db, (ob._p_serial,))
            txn = transaction.Transaction()
            undo.tpc_begin(txn)
            if conflict_during_store:
                with Patch(cluster.client, waitResponses=waitResponses) as p:
                    undo.commit(txn)
            else:
                ob.value += 3
                t.commit()
                undo.commit(txn)
            undo.tpc_vote(txn)
            undo.tpc_finish(txn)
            t.begin()
            self.assertEqual(ob.value, 3)
        finally:
            cluster.stop()

    @expectedFailure(POSException.ConflictError)
    def testUndoConflictDuringStore(self):
        self.testUndoConflict(True)

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
            tid0 = ob._p_serial
            self.assertNotEqual(tid0, ZERO_TID)
            del ob, t, c

            # then check resolution
            t1, c1 = cluster.getTransaction()
            t2, c2 = cluster.getTransaction()
            t3, c3 = cluster.getTransaction()
            o1 = c1.root()['with_resolution']
            o2 = c2.root()['with_resolution']
            o3 = c3.root()['with_resolution']
            self.assertEqual(o1.value, 0)
            self.assertEqual(o2.value, 0)
            self.assertEqual(o3.value, 0)
            o1.value += 3
            o2.value += 5
            o3.value += 7

            t1.commit()
            self.assertEqual(o1._p_changed, 0)
            self.assertEqual(o1.value, 3)
            tid1 = o1._p_serial

            resolved = []
            last = (t2.get(), t3.get()).index
            def _handleConflicts(orig, txn_context, *args):
                resolved.append(last(txn_context['txn']))
                return orig(txn_context, *args)
            def tpc_vote(orig, transaction, *args):
                (l3 if last(transaction) else l2)()
                return orig(transaction, *args)
            with Patch(cluster.client, _handleConflicts=_handleConflicts):
                with LockLock() as l3, Patch(cluster.client, tpc_vote=tpc_vote):
                    with LockLock() as l2:
                        tt = []
                        for t, l in (t2, l2), (t3, l3):
                            tt.append(self.newThread(t.commit))
                            l()
                    tt.pop(0).join()
                    self.assertEqual(o2._p_changed, None)
                    self.assertEqual(o2.value, 8)
                    tid2 = o2._p_serial
                tt.pop(0).join()
                self.assertEqual(o3._p_changed, None)
                self.assertEqual(o3.value, 15)
                tid3 = o3._p_serial

            self.assertEqual(resolved, [0, 1, 1])
            self.assertTrue(tid0 < tid1 < tid2 < tid3)
            t1.begin()
            t2.begin()
            t3.begin()
            self.assertIs(o1._p_changed, None)
            self.assertIs(o2._p_changed, None)
            self.assertEqual(o3._p_changed, 0)
            self.assertEqual(o1.value, 15)
            self.assertEqual(o2.value, 15)

            # check history
            self.assertEqual([x['tid'] for x in c1.db().history(oid, size=10)],
                             [tid3, tid2, tid1, tid0])
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
            self.assertEqual([], cluster.getOutdatedCells())
        finally:
            cluster.stop()
        # restart it with one storage only
        cluster.reset()
        try:
            cluster.start(storage_list=(s1,))
            self.assertEqual(NodeStates.UNKNOWN, cluster.getNodeState(s2))
        finally:
            cluster.stop()

    def testRestartStoragesWithReplicas(self):
        """
        Check that the master must discard its partition table when the
        cluster is not operational anymore. Which means that it must go back
        to RECOVERING state and remain there as long as the partition table
        can't be operational.
        This also checks that if the master remains the primary one after going
        back to recovery, it automatically starts the cluster if possible
        (i.e. without manual intervention).
        """
        outdated = []
        def doOperation(orig):
            outdated.append(cluster.getOutdatedCells())
            orig()
        def stop():
            with cluster.master.filterConnection(s0) as m2s0:
                m2s0.add(lambda conn, packet:
                    isinstance(packet, Packets.NotifyPartitionChanges))
                s1.stop()
                cluster.join((s1,))
                self.assertEqual(getClusterState(), ClusterStates.RUNNING)
                self.assertEqual(cluster.getOutdatedCells(),
                                 [(0, s1.uuid), (1, s1.uuid)])
                s0.stop()
                cluster.join((s0,))
            self.assertNotEqual(getClusterState(), ClusterStates.RUNNING)
            s0.resetNode()
            s1.resetNode()
        cluster = NEOCluster(storage_count=2, partitions=2, replicas=1)
        try:
            cluster.start()
            s0, s1 = cluster.storage_list
            getClusterState = cluster.neoctl.getClusterState
            if 1:
                # Scenario 1: When all storage nodes are restarting,
                # we want a chance to not restart with outdated cells.
                stop()
                with Patch(s1, doOperation=doOperation):
                    s0.start()
                    s1.start()
                    self.tic()
                self.assertEqual(getClusterState(), ClusterStates.RUNNING)
                self.assertEqual(outdated, [[]])
            if 1:
                # Scenario 2: When only the first storage node to be stopped
                # is started, the cluster must be able to restart.
                stop()
                s1.start()
                self.tic()
                # The master doesn't wait for s0 to come back.
                self.assertEqual(getClusterState(), ClusterStates.RUNNING)
                self.assertEqual(cluster.getOutdatedCells(),
                                [(0, s0.uuid), (1, s0.uuid)])
        finally:
            cluster.stop()

    def testVerificationCommitUnfinishedTransactions(self):
        """ Verification step should commit locked transactions """
        def delayUnlockInformation(conn, packet):
            return isinstance(packet, Packets.NotifyUnlockInformation)
        def onLockTransaction(storage, die=False):
            def lock(orig, *args, **kw):
                if die:
                    sys.exit()
                orig(*args, **kw)
                storage.master_conn.close()
            return Patch(storage.tm, lock=lock)
        cluster = NEOCluster(partitions=2, storage_count=2)
        try:
            cluster.start()
            s0, s1 = cluster.sortStorageList()
            t, c = cluster.getTransaction()
            r = c.root()
            r[0] = PCounter()
            tids = [r._p_serial]
            with onLockTransaction(s0), onLockTransaction(s1):
                t.commit()
            self.assertEqual(r._p_state, GHOST)
            self.tic()
            t.begin()
            x = r[0]
            self.assertEqual(x.value, 0)
            cluster.master.tm._last_oid = x._p_oid
            tids.append(r._p_serial)
            r[1] = PCounter()
            c.readCurrent(x)
            with cluster.moduloTID(1):
                with onLockTransaction(s0), onLockTransaction(s1):
                    t.commit()
                self.tic()
                t.begin()
                # The following line checks that s1 moved the transaction
                # metadata to final place during the verification phase.
                # If it didn't, a NEOStorageError would be raised.
                self.assertEqual(3, len(c.db().history(r._p_oid, 4)))
                y = r[1]
                self.assertEqual(y.value, 0)
                self.assertEqual([u64(o._p_oid) for o in (r, x, y)], range(3))
                r[2] = 'ok'
                with cluster.master.filterConnection(s0) as m2s:
                    m2s.add(delayUnlockInformation)
                    t.commit()
                    x.value = 1
                    # s0 will accept to store y (because it's not locked) but will
                    # never lock the transaction (packets from master delayed),
                    # so the last transaction will be dropped.
                    y.value = 2
                    di0 = s0.getDataLockInfo()
                    with onLockTransaction(s1, die=True):
                        self.commitWithStorageFailure(cluster.client, t)
        finally:
            cluster.stop()
        cluster.reset()
        (k, v), = set(s0.getDataLockInfo().iteritems()
                      ).difference(di0.iteritems())
        self.assertEqual(v, 1)
        k, = (k for k, v in di0.iteritems() if v == 1)
        di0[k] = 0 # r[2] = 'ok'
        self.assertEqual(di0.values(), [0, 0, 0, 0, 0])
        di1 = s1.getDataLockInfo()
        k, = (k for k, v in di1.iteritems() if v == 1)
        del di1[k] # x.value = 1
        self.assertEqual(di1.values(), [0])
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            r = c.root()
            self.assertEqual(r[0].value, 0)
            self.assertEqual(r[1].value, 0)
            self.assertEqual(r[2], 'ok')
            self.assertEqual(di0, s0.getDataLockInfo())
            self.assertEqual(di1, s1.getDataLockInfo())
        finally:
            cluster.stop()

    def testVerificationWithNodesWithoutReadableCells(self):
        def onLockTransaction(storage, die_after):
            def lock(orig, *args, **kw):
                if die_after:
                    orig(*args, **kw)
                sys.exit()
            return Patch(storage.tm, lock=lock)
        cluster = NEOCluster(replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = None
            s0, s1 = cluster.storage_list
            with onLockTransaction(s0, False), onLockTransaction(s1, True):
                self.commitWithStorageFailure(cluster.client, t)
            s0.resetNode()
            s0.start()
            t.begin()
            c.root()[1] = None
            t.commit()
            cluster.master.stop()
            x = cluster.master, s1
            cluster.join(x)
            for x in x:
                x.resetNode()
                x.start()
            # Verification must drop the first transaction because it's only
            # locked on a node without any readable cell, and other nodes may
            # have cleared ttrans/tobj (which is the case here).
            self.tic()
            t.begin()
            s0.stop() # force client to ask s1
            self.assertEqual(sorted(c.root()), [1])
            self.tic()
            t0, t1 = c.db().storage.iterator()
        finally:
            cluster.stop()

    def testDropUnfinishedData(self):
        def lock(orig, *args, **kw):
            orig(*args, **kw)
            storage.master_conn.close()
        r = []
        def dropUnfinishedData(orig):
            r.append(len(orig.__self__.getUnfinishedTIDDict()))
            orig()
            r.append(len(orig.__self__.getUnfinishedTIDDict()))
        cluster = NEOCluster(partitions=2, storage_count=2, replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()._p_changed = 1
            storage = cluster.storage_list[0]
            with Patch(storage.tm, lock=lock), \
                 Patch(storage.dm, dropUnfinishedData=dropUnfinishedData):
                t.commit()
                self.tic()
            self.assertEqual(r, [1, 0])
        finally:
            cluster.stop()

    def testStorageUpgrade1(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.storage
            t, c = cluster.getTransaction()
            storage.dm.setConfiguration("version", None)
            c.root()._p_changed = 1
            t.commit()
            storage.stop()
            cluster.join((storage,))
            storage.em.onTimeout() # deferred commit
            storage.resetNode()
            storage.start()
            t.begin()
            storage.dm.setConfiguration("version", None)
            c.root()._p_changed = 1
            with Patch(storage.tm, lock=lambda *_: sys.exit()):
                self.commitWithStorageFailure(cluster.client, t)
            self.assertRaises(DatabaseFailure, storage.resetNode)
        finally:
            cluster.stop()

    def testStorageReconnectDuringStore(self):
        cluster = NEOCluster(replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = 'ok'
            cluster.client.cp.closeAll()
            t.commit() # store request
        finally:
            cluster.stop()

    def testStorageReconnectDuringTransactionLog(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            cluster.client.cp.closeAll()
            tid, (t1,) = cluster.client.transactionLog(
                ZERO_TID, c.db().lastTransaction(), 10)
        finally:
            cluster.stop()

    def testStorageReconnectDuringUndoLog(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            cluster.client.cp.closeAll()
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
            self.assertEqual([], cluster.getOutdatedCells())
            # drop one
            cluster.neoctl.dropNode(s1.uuid)
            checkNodeState(None)
            self.tic() # Let node state update reach remaining storage
            checkNodeState(None)
            self.assertEqual([], cluster.getOutdatedCells())
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
            with Patch(storage, connectToPrimary=sys.exit):
                # send an unexpected to master so it aborts connection to storage
                storage.master_conn.answer(Packets.Pong())
                self.tic()
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RECOVERING)
            storage.resetNode()
            storage.start()
            self.tic()
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)
        finally:
            cluster.stop()

    def testShutdown(self):
        # BUG: Due to bugs in election, master nodes sometimes crash, or they
        #      declare themselves primary too quickly. The consequence is
        #      often an endless tic loop.
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
        def _handlePacket(orig, conn, packet, kw={}, handler=None):
            if type(packet) is Packets.AnswerTransactionFinished:
                ll()
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
            with LockLock() as ll, Patch(cluster.client,
                    _handlePacket=_handlePacket):
                t = self.newThread(t1.commit)
                ll()
                t2.begin()
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
            ll = LockLock()
            def break_after(orig, *args):
                try:
                    return orig(*args)
                finally:
                    ll()
            x2._p_deactivate()
            # Remove last version of x from cache
            cache._remove(cache._oid_dict[x2._p_oid].pop())
            with ll, Patch(cluster.client, _loadFromStorage=break_after):
                t = self.newThread(x2._p_activate)
                ll()
                # At this point, x could not be found the cache and the result
                # from the storage (which is <value=1, next_tid=None>) is about
                # to be processed.
                # Now modify x to receive an invalidation for it.
                txn = transaction.Transaction()
                client.tpc_begin(txn)
                client.store(x2._p_oid, tid, x, '', txn) # value=0
                tid = client.tpc_finish(txn, None)
                t1.begin() # make sure invalidation is processed
                # Resume processing of answer from storage. An entry should be
                # added in cache for x=1 with a fixed next_tid (i.e. not None)
            t.join()
            self.assertEqual(x2.value, 1)
            self.assertEqual(x1.value, 0)

            def invalidations(conn):
                try:
                    return conn._storage._invalidations
                except AttributeError: # BBB: ZODB < 5
                    return conn._invalidated

            # Change x again from 0 to 1, while the checking connection c1
            # is suspended at the beginning of the transaction t1,
            # between Storage.sync() and flush of invalidations.
            x1._p_deactivate()
            t1.abort()
            with ll, Patch(c1._storage, sync=break_after):
                t = self.newThread(t1.begin)
                ll()
                txn = transaction.Transaction()
                client.tpc_begin(txn)
                client.store(x2._p_oid, tid, y, '', txn)
                tid = client.tpc_finish(txn, None)
                client.close()
                self.assertEqual(invalidations(c1), {x1._p_oid})
            t.join()
            # A transaction really begins when it gets the last tid from the
            # storage, just before flushing invalidations (on ZODB < 5, it's
            # when it acquires the lock to flush invalidations). The previous
            # call to sync() only does a ping to make sure we have a recent
            # enough view of the DB.
            self.assertFalse(invalidations(c1))
            self.assertEqual(x1.value, 1)

        finally:
            cluster.stop()

    def testReadVerifyingStorage(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x = PCounter()
            t1.commit()
            # We need a second client for external invalidations.
            t2 = transaction.TransactionManager()
            db = DB(storage=cluster.getZODBStorage(client=cluster.newClient()))
            try:
                c2 = db.open(t2)
                t2.begin()
                r = c2.root()
                r['y'] = None
                r['x']._p_activate()
                c2.readCurrent(r['x'])
                # Force the new tid to be even, like the modified oid and
                # unlike the oid on which we used readCurrent. Thus we check
                # that the node containing only the partition 1 is also
                # involved in tpc_finish.
                with cluster.moduloTID(0):
                    t2.commit()
                for storage in cluster.storage_list:
                    self.assertFalse(storage.tm._transaction_dict)
            finally:
                db.close()
            # Check we didn't get an invalidation, which would cause an
            # assertion failure in the cache. Connection does the same check in
            # _setstate_noncurrent so this could be also done by starting a
            # transaction before the last one, and clearing the cache before
            # reloading x.
            c1._storage.load(x._p_oid)
            t0, t1, t2 = c1.db().storage.iterator()
            self.assertEqual(map(u64, t0.oid_list), [0])
            self.assertEqual(map(u64, t1.oid_list), [0, 1])
            # Check oid 1 is part of transaction metadata.
            self.assertEqual(t2.oid_list, t1.oid_list)
        finally:
            cluster.stop()

    def testClientReconnection(self):
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
            try:
                txn = transaction.Transaction()
                client.tpc_begin(txn)
                client.store(x1._p_oid, x1._p_serial, y, '', txn)
                tid = client.tpc_finish(txn, None)
            finally:
                client.close()
            self.tic()

            # Check reconnection to the master and storage.
            self.assertTrue(cluster.client.history(x1._p_oid))
            self.assertIsNot(None, cluster.client.master_conn)
            t1.begin()
            self.assertEqual(x1._p_changed, None)
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
                raise StoppedOperation
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()['x'] = PCounter()
            with cluster.master.filterConnection(cluster.client) as m2c:
                m2c.add(answerTransactionFinished)
                # After a storage failure during tpc_finish, the client
                # reconnects and checks that the transaction was really
                # committed.
                t.commit()
            # Also check that the master reset the last oid to a correct value.
            t.begin()
            self.assertEqual(1, u64(c.root()['x']._p_oid))
            self.assertFalse(cluster.client.new_oid_list)
            self.assertEqual(2, u64(cluster.client.new_oid()))
        finally:
            cluster.stop()

    def testClientFailureDuringTpcFinish(self):
        """
        Third scenario:

          C                   M                   S     | TID known by
           ---- Finish ----->                           |
           ---- Disconnect --   ----- Lock ------>      |
                                ----- C down ---->      |
           ---- Connect ---->                           | M
                                ----- C up ------>      |
                                <---- Locked -----      |
        ------------------------------------------------+--------------
                                -- unlock ...           |
           ---- FinalTID --->                           | S (TM)
           ---- Connect + FinalTID -------------->      |
                                   ... unlock --->      |
        ------------------------------------------------+--------------
                                                        | S (DM)
        """
        def delayAnswerLockInformation(conn, packet):
            if isinstance(packet, Packets.AnswerInformationLocked):
                cluster.client.master_conn.close()
                return True
        def askFinalTID(orig, *args):
            s2m.remove(delayAnswerLockInformation)
            orig(*args)
        def _getFinalTID(orig, ttid):
            s2m.remove(delayAnswerLockInformation)
            self.tic()
            return orig(ttid)
        def _connectToPrimaryNode(orig):
            conn = orig()
            self.tic()
            s2m.remove(delayAnswerLockInformation)
            return conn
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            r = c.root()
            r['x'] = PCounter()
            tid0 = r._p_serial
            with cluster.storage.filterConnection(cluster.master) as s2m:
                s2m.add(delayAnswerLockInformation,
                    Patch(ClientServiceHandler, askFinalTID=askFinalTID))
                t.commit() # the final TID is returned by the master
            t.begin()
            r['x'].value += 1
            tid1 = r._p_serial
            self.assertTrue(tid0 < tid1)
            with cluster.storage.filterConnection(cluster.master) as s2m:
                s2m.add(delayAnswerLockInformation,
                    Patch(cluster.client, _getFinalTID=_getFinalTID))
                t.commit() # the final TID is returned by the storage backend
            t.begin()
            r['x'].value += 1
            tid2 = r['x']._p_serial
            self.assertTrue(tid1 < tid2)
            # The whole test would be simpler if we always delayed the
            # AskLockInformation packet. However, it would also delay
            # NotifyNodeInformation and the client would fail to connect
            # to the storage node.
            with cluster.storage.filterConnection(cluster.master) as s2m, \
                 cluster.master.filterConnection(cluster.storage) as m2s:
                s2m.add(delayAnswerLockInformation, Patch(cluster.client,
                    _connectToPrimaryNode=_connectToPrimaryNode))
                m2s.add(lambda conn, packet:
                    isinstance(packet, Packets.NotifyUnlockInformation))
                t.commit() # the final TID is returned by the storage (tm)
            t.begin()
            self.assertEqual(r['x'].value, 2)
            self.assertTrue(tid2 < r['x']._p_serial)
        finally:
            cluster.stop()

    def testMasterFailureBeforeVote(self):
        def waitStoreResponses(orig, *args):
            result = orig(*args)
            m2c, = cluster.master.getConnectionList(orig.__self__)
            m2c.close()
            self.tic()
            return result
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()['x'] = PCounter() # 1 store() to each storage
            with Patch(cluster.client, waitStoreResponses=waitStoreResponses):
                self.assertRaises(POSException.StorageError, t.commit)
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)
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

    def testAbortVotedTransaction(self):
        r = []
        def tpc_finish(*args, **kw):
            for storage in cluster.storage_list:
                r.append(len(storage.dm.getUnfinishedTIDDict()))
            raise NEOStorageError
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()['x'] = PCounter()
            with Patch(cluster.client, tpc_finish=tpc_finish):
                self.assertRaises(NEOStorageError, t.commit)
                self.tic()
            self.assertEqual(r, [1, 1])
            for storage in cluster.storage_list:
                self.assertFalse(storage.dm.getUnfinishedTIDDict())
            t.begin()
            self.assertNotIn('x', c.root())
        finally:
            cluster.stop()

    def testStorageLostDuringRecovery(self):
        # Initialize a cluster.
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
        finally:
            cluster.stop()
        cluster.reset()
        # Restart with a connection failure for the first AskPartitionTable.
        # The master must not be stuck in RECOVERING state
        # or re-make the partition table.
        def make(*args):
            sys.exit()
        def askPartitionTable(orig, self, conn):
            p.revert()
            conn.close()
        try:
            with Patch(cluster.master.pt, make=make), \
                 Patch(InitializationHandler,
                       askPartitionTable=askPartitionTable) as p:
                cluster.start()
                self.assertFalse(p.applied)
        finally:
            cluster.stop()

    def testTruncate(self):
        calls = [0, 0]
        def dieFirst(i):
            def f(orig, *args, **kw):
                calls[i] += 1
                if calls[i] == 1:
                    sys.exit()
                return orig(*args, **kw)
            return f
        cluster = NEOCluster(replicas=1)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            r = c.root()
            tids = []
            for x in xrange(4):
                r[x] = None
                t.commit()
                tids.append(r._p_serial)
            truncate_tid = tids[2]
            r['x'] = PCounter()
            s0, s1 = cluster.storage_list
            with Patch(s0.tm, unlock=dieFirst(0)), \
                 Patch(s1.dm, truncate=dieFirst(1)):
                t.commit()
                cluster.neoctl.truncate(truncate_tid)
                self.tic()
                getClusterState = cluster.neoctl.getClusterState
                # Unless forced, the cluster waits all nodes to be up,
                # so that all nodes are truncated.
                self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
                self.assertEqual(calls, [1, 0])
                s0.resetNode()
                s0.start()
                # s0 died with unfinished data, and before processing the
                # Truncate packet from the master.
                self.assertFalse(s0.dm.getTruncateTID())
                self.assertEqual(s1.dm.getTruncateTID(), truncate_tid)
                self.tic()
                self.assertEqual(calls, [1, 1])
                self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
            s1.resetNode()
            with Patch(s1.dm, truncate=dieFirst(1)):
                s1.start()
                self.assertEqual(s0.dm.getLastIDs()[0], truncate_tid)
                self.assertEqual(s1.dm.getLastIDs()[0], r._p_serial)
                self.tic()
                self.assertEqual(calls, [1, 2])
                self.assertEqual(getClusterState(), ClusterStates.RUNNING)
            t.begin()
            self.assertEqual(r, dict.fromkeys(xrange(3)))
            self.assertEqual(r._p_serial, truncate_tid)
            self.assertEqual(1, u64(c._storage.new_oid()))
            for s in cluster.storage_list:
                self.assertEqual(s.dm.getLastIDs()[0], truncate_tid)
        finally:
            cluster.stop()

    def testConnectionTimeout(self):
        conn = self.getLoopbackConnection()
        conn.KEEP_ALIVE
        with Patch(conn, KEEP_ALIVE=0):
            while conn.connecting:
                conn.em.poll(1)
            def onTimeout(orig):
                conn.idle()
                orig()
            with Patch(conn, onTimeout=onTimeout):
                conn.em.poll(1)
        self.assertFalse(conn.isClosed())

    def testClientDisconnectedFromMaster(self):
        def disconnect(conn, packet):
            if isinstance(packet, Packets.AskObject):
                m2c.close()
                #return True
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            m2c, = cluster.master.getConnectionList(cluster.client)
            cluster.client._cache.clear()
            c.cacheMinimize()
            # Make the master disconnects the client when the latter is about
            # to send a AskObject packet to the storage node.
            with cluster.client.filterConnection(cluster.storage) as c2s:
                c2s.add(disconnect)
                # Storages are currently notified of clients that get
                # disconnected from the master and disconnect them in turn.
                # Should it change, the clients would have to disconnect on
                # their own.
                self.assertRaises(TransientError, getattr, c, "root")
            uuid = cluster.client.uuid
            # Let's use a second client to steal the node id of the first one.
            client = cluster.newClient()
            try:
                client.sync()
                self.assertEqual(uuid, client.uuid)
                # The client reconnects successfully to the master and storage,
                # with a different node id. This time, we get a different error
                # if it's only disconnected from the storage.
                with Patch(ClientOperationHandler,
                        askObject=lambda orig, self, conn, *args: conn.close()):
                    self.assertRaises(NEOStorageError, getattr, c, "root")
                self.assertNotEqual(uuid, cluster.client.uuid)
                # Second reconnection, for a successful load.
                c.root
            finally:
                client.close()
        finally:
            cluster.stop()

    def testIdTimestamp(self):
        """
        Given a master M, a storage S, and 2 clients Ca and Cb.

        While Ca(id=1) is being identified by S:
        1. connection between Ca and M breaks
        2. M -> S: C1 down
        3. Cb connect to M: id=1
        4. M -> S: C1 up
        5. S processes RequestIdentification from Ca with id=1

        At 5, S must reject Ca, otherwise Cb can't connect to S. This is where
        id timestamps come into play: with C1 up since t2, S rejects Ca due to
        a request with t1  < t2.

        To avoid issues with clocks that are out of sync, the client gets its
        connection timestamp by being notified about itself from the master.
        """
        s2c = []
        def __init__(orig, self, *args, **kw):
            orig(self, *args, **kw)
            self.readable = bool
            s2c.append(self)
            ll()
        def connectToStorage(client):
            next(client.cp.iterateForObject(0))
        cluster = NEOCluster()
        try:
            cluster.start()
            Ca = cluster.client
            Ca.pt      # only connect to the master
            # In a separate thread, connect to the storage but suspend the
            # processing of the RequestIdentification packet, until the
            # storage is notified about the existence of the other client.
            with LockLock() as ll, Patch(ServerConnection, __init__=__init__):
                t = self.newThread(connectToStorage, Ca)
                ll()
            s2c, = s2c
            m2c, = cluster.master.getConnectionList(cluster.client)
            m2c.close()
            Cb = cluster.newClient()
            try:
                Cb.pt  # only connect to the master
                del s2c.readable
                self.assertRaises(NEOPrimaryMasterLost, t.join)
                self.assertTrue(s2c.isClosed())
                connectToStorage(Cb)
            finally:
                Cb.close()
        finally:
            cluster.stop()


if __name__ == "__main__":
    unittest.main()
