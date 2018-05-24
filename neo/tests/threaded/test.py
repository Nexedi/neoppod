#
# Copyright (C) 2011-2017  Nexedi SA
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
import time
import transaction
import unittest
from collections import defaultdict
from contextlib import contextmanager
from thread import get_ident
from persistent import Persistent, GHOST
from transaction.interfaces import TransientError
from ZODB import DB, POSException
from ZODB.DB import TransactionalUndo
from neo.storage.transactions import TransactionManager, ConflictError
from neo.lib.connection import ConnectionClosed, \
    ServerConnection, MTClientConnection
from neo.lib.exception import StoppedOperation
from neo.lib.handler import DelayEvent, EventHandler
from neo.lib import logging
from neo.lib.protocol import (CellStates, ClusterStates, NodeStates, NodeTypes,
    Packets, Packet, uuid_str, ZERO_OID, ZERO_TID, MAX_TID)
from .. import expectedFailure, unpickle_state, Patch, TransactionalResource
from . import ClientApplication, ConnectionFilter, LockLock, NEOCluster, \
    NEOThreadedTest, RandomConflictDict, ThreadId, with_cluster
from neo.lib.util import add64, makeChecksum, p64, u64
from neo.client.exception import NEOPrimaryMasterLost, NEOStorageError
from neo.client.transactions import Transaction
from neo.master.handlers.client import ClientServiceHandler
from neo.storage.database import DatabaseFailure
from neo.storage.handlers.client import ClientOperationHandler
from neo.storage.handlers.identification import IdentificationHandler
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

    def testBasicStore(self, dedup=False):
        with NEOCluster(dedup=dedup) as cluster:
            cluster.start()
            storage = cluster.getZODBStorage()
            storage.sync()
            storage.app.max_reconnection_to_master = 0
            compress = storage.app.compress._compress
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

    @with_cluster()
    def testDeleteObject(self, cluster):
        if 1:
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

    def _testUndoConflict(self, cluster, *inc):
        def waitResponses(orig, *args):
            orig(*args)
            p.revert()
            t.commit()
        t, c = cluster.getTransaction()
        c.root()[0] = ob = PCounterWithResolution()
        t.commit()
        tids = []
        for x in inc:
            ob.value += x
            t.commit()
            tids.append(ob._p_serial)
        undo = TransactionalUndo(cluster.db, tids)
        txn = transaction.Transaction()
        undo.tpc_begin(txn)
        ob.value += 5
        with Patch(cluster.client, waitResponses=waitResponses) as p:
            undo.commit(txn)
        undo.tpc_vote(txn)
        undo.tpc_finish(txn)
        t.begin()
        self.assertEqual(ob.value, 5)
        return ob

    @with_cluster()
    def testUndoConflictSmallCache(self, cluster):
        big = 'x' * cluster.cache_size
        def resolve(orig, *args):
            state = orig(*args)
            state['x'] = big
            return state
        with Patch(PCounterWithResolution, _p_resolveConflict=resolve):
            self.assertEqual(self._testUndoConflict(cluster, 1, 3).x, big)

    @expectedFailure(POSException.ConflictError)
    @with_cluster()
    def testUndoConflictDuringStore(self, cluster):
        self._testUndoConflict(cluster, 1)

    def testStorageDataLock(self, dedup=False):
        with NEOCluster(dedup=dedup) as cluster:
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

            txn = [transaction.Transaction() for x in xrange(4)]
            for t in txn:
                storage.tpc_begin(t)
                storage.store(oid if tid else storage.new_oid(),
                              tid, data, '', t)
                tid = None
            data_info[key] = 4 if dedup else 1
            self.tic()
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn.pop())
            for t in txn:
                storage.tpc_vote(t)
            self.tic()
            data_info[key] -= dedup
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[1])
            self.tic()
            data_info[key] -= dedup
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            tid1 = storage.tpc_finish(txn[2])
            self.tic()
            data_info[key] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[0])
            self.tic()
            data_info[key] -= dedup
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

    def testStorageDataLockWithDeduplication(self, dedup=False):
        self.testStorageDataLock(True)

    @with_cluster()
    def testStorageDataLock2(self, cluster):
        storage = cluster.getZODBStorage()
        def t(data):
            oid = storage.new_oid()
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            storage.store(oid, None, data, '', txn)
            return txn
        t1 = t('foo')
        storage.tpc_finish(t('bar'))
        s = cluster.storage
        s.stop()
        cluster.join((s,))
        s.resetNode()
        storage.app.max_reconnection_to_master = 0
        self.assertRaises(NEOPrimaryMasterLost, storage.tpc_vote, t1)
        expectedFailure(self.assertFalse)(s.dm.getOrphanList())

    @with_cluster(storage_count=1)
    def testDelayedUnlockInformation(self, cluster):
        except_list = []
        def onStoreObject(orig, tm, ttid, serial, oid, *args):
            if oid == resume_oid and delayUnlockInformation in m2s:
                m2s.remove(delayUnlockInformation)
            try:
                return orig(tm, ttid, serial, oid, *args)
            except Exception, e:
                except_list.append(e.__class__)
                raise
        if 1:
            t, c = cluster.getTransaction()
            c.root()[0] = ob = PCounter()
            with cluster.master.filterConnection(cluster.storage) as m2s:
                resume_oid = None
                delayUnlockInformation = m2s.delayNotifyUnlockInformation(
                    Patch(TransactionManager, storeObject=onStoreObject))
                t.commit()
                resume_oid = ob._p_oid
                ob._p_changed = 1
                t.commit()
                self.assertNotIn(delayUnlockInformation, m2s)
        self.assertEqual(except_list, [DelayEvent])

    @with_cluster(storage_count=2, replicas=1)
    def _testDeadlockAvoidance(self, cluster, scenario):
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

        if 1:
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
        return except_list

    def testDelayedStore(self):
        # 0: C1 -> S1, S2
        # 1: C2 -> S1, S2 (delayed)
        # 2: C1 commits
        # 3: C2 resolves conflict
        self.assertEqual(self._testDeadlockAvoidance([2, 4]),
            [DelayEvent, DelayEvent, ConflictError, ConflictError])

    def testDeadlockAvoidance(self):
        # This test fail because deadlock avoidance is not fully implemented.
        # 0: C1 -> S1
        # 1: C2 -> S1, S2 (delayed)
        # 2: C1 -> S2 (deadlock)
        # 3: C2 commits
        # 4: C1 resolves conflict
        self.assertEqual(self._testDeadlockAvoidance([1, 3]),
            [DelayEvent, DelayEvent, DelayEvent, ConflictError])

    @with_cluster()
    def testConflictResolutionTriggered2(self, cluster):
        """ Check that conflict resolution works """
        if 1:
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
            last = lambda txn: txn._extension['last'] # BBB
            def _handleConflicts(orig, txn_context):
                resolved.append(last(txn_context.txn))
                orig(txn_context)
            def tpc_vote(orig, transaction):
                (l3 if last(transaction) else l2)()
                return orig(transaction)
            with Patch(cluster.client, _handleConflicts=_handleConflicts):
                with LockLock() as l3, Patch(cluster.client, tpc_vote=tpc_vote):
                    with LockLock() as l2:
                        tt = []
                        for i, t, l in (0, t2, l2), (1, t3, l3):
                            t.get().setExtendedInfo('last', i)
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

    @with_cluster()
    def testDelayedLoad(self, cluster):
        """
        Check that a storage node delays reads from the database,
        when the requested data may still be in a temporary place.
        """
        l = threading.Lock()
        l.acquire()
        idle = []
        def askObject(orig, *args):
            try:
                orig(*args)
            finally:
                idle.append(cluster.storage.em.isIdle())
                l.release()
        if 1:
            t, c = cluster.getTransaction()
            r = c.root()
            r[''] = ''
            with Patch(ClientOperationHandler, askObject=askObject):
                with cluster.master.filterConnection(cluster.storage) as m2s:
                    m2s.delayNotifyUnlockInformation()
                    t.commit()
                    c.cacheMinimize()
                    cluster.client._cache.clear()
                    load = self.newThread(r._p_activate)
                    l.acquire()
                l.acquire()
                # The request from the client is processed again
                # (upon reception on unlock notification from the master),
                # once exactly, and now with success.
            load.join()
            self.assertEqual(idle, [1, 0])
            self.assertIn('', r)

    @with_cluster(replicas=1)
    def test_notifyNodeInformation(self, cluster):
        # translated from MasterNotificationsHandlerTests
        # (neo.tests.client.testMasterHandler)
        good = [1, 0].pop
        if 1:
            cluster.db # open DB
            s0, s1 = cluster.client.nm.getStorageList()
            conn = s0.getConnection()
            self.assertFalse(conn.isClosed())
            getCellSortKey = cluster.client.cp.getCellSortKey
            self.assertEqual(getCellSortKey(s0, good), 0)
            cluster.neoctl.dropNode(s0.getUUID())
            self.assertEqual([s1], cluster.client.nm.getStorageList())
            self.assertTrue(conn.isClosed())
            self.assertEqual(getCellSortKey(s0, good), 1)
            # XXX: the test originally checked that 'unregister' method
            #      was called (even if it's useless in this case),
            #      but we would need an API to do that easily.
            self.assertFalse(cluster.client.dispatcher.registered(conn))

    @with_cluster(replicas=2)
    def test_notifyPartitionChanges(self, cluster):
        cluster.db
        s0, s1, s2 = cluster.storage_list
        s1.stop()
        cluster.join((s1,))
        s1.resetNode()
        # This checks that s1 processes any PT update
        # (by MasterOperationHandler) even if it receives one before the
        # AnswerUnfinishedTransactions packet.
        with ConnectionFilter() as f:
            f.delayAskUnfinishedTransactions()
            s1.start()
            self.tic()
            s2.stop()
            cluster.join((s2,))
        self.tic()
        self.assertPartitionTable(cluster, 'UUO', s1)

    @with_cluster()
    def testStartOperation(self, cluster):
        t, c = cluster.getTransaction()
        c.root()._p_changed = 1
        cluster.storage.stop()
        cluster.join(cluster.storage_list)
        cluster.storage.resetNode()
        delayed = []
        def delayConnection(conn, packet):
            return conn in delayed
        def startOperation(orig, self, conn, backup):
            assert not delayed, delayed
            delayed.append(conn)
            orig(self, conn, backup)
        def askBeginTransaction(orig, *args):
            f.discard(delayConnection)
            orig(*args)
        with ConnectionFilter() as f, \
             Patch(InitializationHandler, startOperation=startOperation), \
             Patch(cluster.master.client_service_handler,
                   askBeginTransaction=askBeginTransaction) as p:
            f.add(delayConnection)
            cluster.storage.start()
            self.tic()
            t.commit()
            self.assertNotIn(delayConnection, f)
            self.assertTrue(delayed)

    @with_cluster(replicas=1, partitions=10)
    def testRestartWithMissingStorage(self, cluster):
        # translated from neo.tests.functional.testStorage.StorageTest
        s1, s2 = cluster.storage_list
        if 1:
            self.assertEqual([], cluster.getOutdatedCells())
        cluster.stop()
        # restart it with one storage only
        if 1:
            cluster.start(storage_list=(s1,))
            self.assertEqual(NodeStates.DOWN,
                             cluster.getNodeState(s2))

    @with_cluster(storage_count=2, partitions=2, replicas=1)
    def testRestartStoragesWithReplicas(self, cluster):
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
                m2s0.delayNotifyPartitionChanges()
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
        if 1:
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

    @with_cluster(partitions=2, storage_count=2)
    def testVerificationCommitUnfinishedTransactions(self, cluster):
        """ Verification step should commit locked transactions """
        def onLockTransaction(storage, die=False):
            def lock(orig, *args, **kw):
                if die:
                    sys.exit()
                orig(*args, **kw)
                storage.master_conn.close()
            return Patch(storage.tm, lock=lock)
        if 1:
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
                    m2s.delayNotifyUnlockInformation()
                    t.commit()
                    x.value = 1
                    # s0 will accept to store y (because it's not locked) but will
                    # never lock the transaction (packets from master delayed),
                    # so the last transaction will be dropped.
                    y.value = 2
                    di0 = s0.getDataLockInfo()
                    with onLockTransaction(s1, die=True):
                        self.commitWithStorageFailure(cluster.client, t)
        cluster.stop()
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
        if 1:
            cluster.start()
            t, c = cluster.getTransaction()
            r = c.root()
            self.assertEqual(r[0].value, 0)
            self.assertEqual(r[1].value, 0)
            self.assertEqual(r[2], 'ok')
            self.assertEqual(di0, s0.getDataLockInfo())
            self.assertEqual(di1, s1.getDataLockInfo())

    @with_cluster(replicas=1)
    def testVerificationWithNodesWithoutReadableCells(self, cluster):
        def onLockTransaction(storage, die_after):
            def lock(orig, *args, **kw):
                if die_after:
                    orig(*args, **kw)
                sys.exit()
            return Patch(storage.tm, lock=lock)
        if 1:
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

    @with_cluster(partitions=2, storage_count=2, replicas=1)
    def testDropUnfinishedData(self, cluster):
        def lock(orig, *args, **kw):
            orig(*args, **kw)
            storage.master_conn.close()
        r = []
        def dropUnfinishedData(orig):
            r.append(len(orig.__self__.getUnfinishedTIDDict()))
            orig()
            r.append(len(orig.__self__.getUnfinishedTIDDict()))
        if 1:
            t, c = cluster.getTransaction()
            c.root()._p_changed = 1
            storage = cluster.storage_list[0]
            with Patch(storage.tm, lock=lock), \
                 Patch(storage.dm, dropUnfinishedData=dropUnfinishedData):
                t.commit()
                self.tic()
            self.assertEqual(r, [1, 0])

    @with_cluster()
    def testStorageUpgrade1(self, cluster):
        if 1:
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
            cluster.join((storage,))
            self.assertRaises(DatabaseFailure, storage.resetNode)

    @with_cluster(replicas=1)
    def testStorageReconnectDuringStore(self, cluster):
        if 1:
            t, c = cluster.getTransaction()
            c.root()[0] = 'ok'
            cluster.client.cp.closeAll()
            t.commit() # store request

    @with_cluster(storage_count=2, partitions=2)
    def testStorageReconnectDuringTransactionLog(self, cluster):
        if 1:
            t, c = cluster.getTransaction()
            cluster.client.cp.closeAll()
            tid, (t1,) = cluster.client.transactionLog(
                ZERO_TID, c.db().lastTransaction(), 10)

    @with_cluster(storage_count=2, partitions=2)
    def testStorageReconnectDuringUndoLog(self, cluster):
        if 1:
            t, c = cluster.getTransaction()
            cluster.client.cp.closeAll()
            t1, = cluster.client.undoLog(0, 10)

    @with_cluster(storage_count=2, replicas=1)
    def testDropNodeThenRestartCluster(self, cluster):
        """ Start a cluster with more than one storage, down one, shutdown the
        cluster then restart it. The partition table recovered must not include
        the dropped node """
        def checkNodeState(state):
            self.assertEqual(cluster.getNodeState(s1), state)
            self.assertEqual(cluster.getNodeState(s2), NodeStates.RUNNING)

        # start with two storage / one replica
        s1, s2 = cluster.storage_list
        if 1:
            checkNodeState(NodeStates.RUNNING)
            self.assertEqual([], cluster.getOutdatedCells())
            # drop one
            cluster.neoctl.dropNode(s1.uuid)
            checkNodeState(None)
            self.tic() # Let node state update reach remaining storage
            checkNodeState(None)
            self.assertEqual([], cluster.getOutdatedCells())
            # restart with s2 only
        cluster.stop()
        if 1:
            cluster.start(storage_list=[s2])
            checkNodeState(None)
            # then restart it, it must be in pending state
            s1.start()
            self.tic()
            checkNodeState(NodeStates.PENDING)

    @with_cluster()
    @with_cluster()
    def test2Clusters(self, cluster1, cluster2):
        if 1:
            t1, c1 = cluster1.getTransaction()
            t2, c2 = cluster2.getTransaction()
            c1.root()['1'] = c2.root()['2'] = ''
            t1.commit()
            t2.commit()

    @with_cluster(partitions=2, storage_count=2)
    def testAbortStorage(self, cluster):
        storage = cluster.storage_list[0]
        if 1:
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

    @with_cluster(master_count=3, partitions=10, replicas=1, storage_count=3)
    def testShutdown(self, cluster):
        def before_finish(_):
            # tell admin to shutdown the cluster
            cluster.neoctl.setClusterState(ClusterStates.STOPPING)
            self.tic()
            l = threading.Lock(); l.acquire()
            with ConnectionFilter() as f:
                # Make we sure that we send t2/BeginTransaction
                # before t1/AskFinishTransaction
                @f.delayAskBeginTransaction
                def delay(_):
                    l.release()
                    return False
                t2.start()
                l.acquire()
        t1, c1 = cluster.getTransaction()
        ob = c1.root()['1'] = PCounter()
        t1.commit()
        ob.value += 1
        TransactionalResource(t1, 0, tpc_finish=before_finish)
        t2, c2 = cluster.getTransaction()
        c2.root()['2'] = None
        t2 = self.newPausedThread(t2.commit)
        with Patch(cluster.client, _connectToPrimaryNode=lambda *_:
                self.fail("unexpected reconnection to master")):
            t1.commit()
        self.assertRaises(ConnectionClosed, t2.join)
        # all nodes except clients should exit
        cluster.join(cluster.master_list
                   + cluster.storage_list
                   + cluster.admin_list)
        cluster.stop() # stop and reopen DB to check partition tables
        cluster.start()
        pt = cluster.admin.pt
        self.assertEqual(1, pt.getID())
        for row in pt.partition_list:
            for cell in row:
                self.assertEqual(cell.getState(), CellStates.UP_TO_DATE)
        t, c = cluster.getTransaction()
        self.assertEqual(c.root()['1'].value, 1)
        self.assertNotIn('2', c.root())

    @with_cluster()
    def testInternalInvalidation(self, cluster):
        def _handlePacket(orig, conn, packet, kw={}, handler=None):
            if type(packet) is Packets.AnswerTransactionFinished:
                ll()
            orig(conn, packet, kw, handler)
        if 1:
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

    @with_cluster()
    def testExternalInvalidation(self, cluster):
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
        with cluster.newClient() as client:
            cache = cluster.client._cache
            txn = transaction.Transaction()
            client.tpc_begin(None, txn)
            client.store(x1._p_oid, x1._p_serial, y, '', txn)
            # Delay invalidation for x
            with cluster.master.filterConnection(cluster.client) as m2c:
                m2c.delayInvalidateObjects()
                tid = client.tpc_finish(txn)
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
                client.tpc_begin(None, txn)
                client.store(x2._p_oid, tid, x, '', txn) # value=0
                tid = client.tpc_finish(txn)
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
                client.tpc_begin(None, txn)
                client.store(x2._p_oid, tid, y, '', txn)
                tid = client.tpc_finish(txn)
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

    @with_cluster(storage_count=2, partitions=2)
    def testReadVerifyingStorage(self, cluster):
        if 1:
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x = PCounter()
            t1.commit()
            # We need a second client for external invalidations.
            with cluster.newClient(1) as db:
                t2, c2 = cluster.getTransaction(db)
                r = c2.root()
                r['y'] = None
                self.readCurrent(r['x'])
                # Force the new tid to be even, like the modified oid and
                # unlike the oid on which we used readCurrent. Thus we check
                # that the node containing only the partition 1 is also
                # involved in tpc_finish.
                with cluster.moduloTID(0):
                    t2.commit()
                for storage in cluster.storage_list:
                    self.assertFalse(storage.tm._transaction_dict)
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

    @with_cluster()
    def testClientReconnection(self, cluster):
        if 1:
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
            with cluster.newClient() as client:
                txn = transaction.Transaction()
                client.tpc_begin(None, txn)
                client.store(x1._p_oid, x1._p_serial, y, '', txn)
                tid = client.tpc_finish(txn)
            self.tic()

            # Check reconnection to the master and storage.
            self.assertTrue(cluster.client.history(x1._p_oid))
            self.assertIsNot(None, cluster.client.master_conn)
            t1.begin()
            self.assertEqual(x1._p_changed, None)
            self.assertEqual(x1.value, 1)

    @with_cluster()
    def testInvalidTTID(self, cluster):
        if 1:
            client = cluster.client
            txn = transaction.Transaction()
            client.tpc_begin(None, txn)
            txn_context = client._txn_container.get(txn)
            txn_context.ttid = add64(txn_context.ttid, 1)
            self.assertRaises(POSException.StorageError,
                              client.tpc_finish, txn)

    @with_cluster()
    def testStorageFailureDuringTpcFinish(self, cluster):
        def answerTransactionFinished(conn, packet):
            if isinstance(packet, Packets.AnswerTransactionFinished):
                raise StoppedOperation
        if 1:
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

    @with_cluster()
    def testClientFailureDuringTpcFinish(self, cluster):
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
        if 1:
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
                m2s.delayNotifyUnlockInformation()
                t.commit() # the final TID is returned by the storage (tm)
            t.begin()
            self.assertEqual(r['x'].value, 2)
            self.assertTrue(tid2 < r['x']._p_serial)

    @with_cluster(storage_count=2, partitions=2)
    def testMasterFailureBeforeVote(self, cluster):
        def waitStoreResponses(orig, *args):
            result = orig(*args)
            m2c, = cluster.master.getConnectionList(orig.__self__)
            m2c.close()
            self.tic()
            return result
        if 1:
            t, c = cluster.getTransaction()
            c.root()['x'] = PCounter() # 1 store() to each storage
            with Patch(cluster.client, waitStoreResponses=waitStoreResponses):
                self.assertRaises(POSException.StorageError, t.commit)
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)

    @with_cluster()
    def testEmptyTransaction(self, cluster):
        if 1:
            txn = transaction.Transaction()
            storage = cluster.getZODBStorage()
            storage.tpc_begin(txn)
            storage.tpc_vote(txn)
            serial = storage.tpc_finish(txn)
            t, = storage.iterator()
            self.assertEqual(t.tid, serial)
            self.assertFalse(t.oid_list)

    @with_cluster()
    def testRecycledClientUUID(self, cluster):
        l = threading.Semaphore(0)
        idle = []
        def requestIdentification(orig, *args):
            try:
                orig(*args)
            finally:
                idle.append(cluster.storage.em.isIdle())
                l.release()
        cluster.db
        with cluster.master.filterConnection(cluster.storage) as m2s:
            delayNotifyInformation = m2s.delayNotifyNodeInformation()
            cluster.client.master_conn.close()
            with cluster.newClient() as client:
                with Patch(IdentificationHandler,
                           requestIdentification=requestIdentification):
                    load = self.newThread(client.load, ZERO_TID)
                    l.acquire()
                    m2s.remove(delayNotifyInformation) # 2 packets pending
                    # Identification of the second client is retried
                    # after each processed notification:
                    l.acquire() # first client down
                    l.acquire() # new client up
                load.join()
                self.assertEqual(idle, [1, 1, 0])

    @with_cluster(start_cluster=0, storage_count=3, autostart=3)
    def testAutostart(self, cluster):
        cluster.start(cluster.storage_list[:2], recovering=True)
        cluster.storage_list[2].start()
        self.tic()
        cluster.checkStarted(ClusterStates.RUNNING)

    @with_cluster(storage_count=2, partitions=2)
    def testAbortVotedTransaction(self, cluster):
        r = []
        def tpc_finish(*args, **kw):
            for storage in cluster.storage_list:
                r.append(len(storage.dm.getUnfinishedTIDDict()))
            raise NEOStorageError
        if 1:
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

    @with_cluster(storage_count=2, partitions=2)
    def testStorageLostDuringRecovery(self, cluster):
        # Initialize a cluster.
        cluster.stop()
        # Restart with a connection failure for the first AskPartitionTable.
        # The master must not be stuck in RECOVERING state
        # or re-make the partition table.
        def make(*args):
            sys.exit()
        def askPartitionTable(orig, self, conn):
            p.revert()
            del conn._queue[:] # XXX
            conn.close()
        if 1:
            with Patch(cluster.master.pt, make=make), \
                 Patch(InitializationHandler,
                       askPartitionTable=askPartitionTable) as p:
                cluster.start()
                self.assertFalse(p.applied)

    @with_cluster(replicas=1)
    def testTruncate(self, cluster):
        calls = [0, 0]
        def dieFirst(i):
            def f(orig, *args, **kw):
                calls[i] += 1
                if calls[i] == 1:
                    sys.exit()
                return orig(*args, **kw)
            return f
        if 1:
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

    def testConnectionAbort(self):
        with self.getLoopbackConnection() as client:
            poll = client.em.poll
            while client.connecting:
                poll(1)
            server, = (c for c in client.em.connection_dict.itervalues()
                         if c.isServer())
            client.send(Packets.NotifyReady())
            def writable(orig):
                p.revert()
                r = orig()
                client.send(Packets.Ping())
                return r
            def process(orig):
                self.assertFalse(server.aborted)
                r = orig()
                self.assertTrue(server.aborted)
                server.em.removeWriter(server)
                return r
            with Patch(client, writable=writable) as p, \
                 Patch(server, process=process):
                poll(0)
                poll(0)
                server.em.addWriter(server)
                self.assertIsNot(server.connector, None)
                poll(0)
                self.assertIs(server.connector, None)
                poll(0)
                self.assertIs(client.connector, None)

    @with_cluster()
    def testClientDisconnectedFromMaster(self, cluster):
        def disconnect(conn, packet):
            if isinstance(packet, Packets.AskObject):
                m2c.close()
                #return True
        if 1:
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
            with cluster.newClient() as client:
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

    @with_cluster()
    def testIdTimestamp(self, cluster):
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
            client._askStorageForRead(0, None, lambda *_: None)
        if 1:
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
            with cluster.newClient() as Cb:
                Cb.pt  # only connect to the master
                del s2c.readable
                self.assertRaises(NEOPrimaryMasterLost, t.join)
                self.assertTrue(s2c.isClosed())
                connectToStorage(Cb)

    @with_cluster(storage_count=2, partitions=2)
    def testPruneOrphan(self, cluster):
        if 1:
            cluster.importZODB()(3)
            bad = []
            ok = []
            def data_args(value):
                return makeChecksum(value), ZERO_OID, value, 0
            node_list = []
            for i, s in enumerate(cluster.storage_list):
                node_list.append(s.uuid)
                if i:
                    s.dm.holdData(*data_args('boo'))
                ok.append(s.getDataLockInfo())
                for i in xrange(3 - i):
                    s.dm.storeData(*data_args('!' * i))
                bad.append(s.getDataLockInfo())
                s.dm.commit()
            def check(dry_run, expected):
                cluster.neoctl.repair(node_list, dry_run)
                for e, s in zip(expected, cluster.storage_list):
                    while 1:
                        self.tic()
                        if s.dm._repairing is None:
                            break
                        time.sleep(.1)
                    self.assertEqual(e, s.getDataLockInfo())
            check(1, bad)
            check(0, ok)
            check(1, ok)

    @with_cluster(replicas=1)
    def testLateConflictOnReplica(self, cluster):
        """
        Already resolved conflict: check the case of a storage node that
        reports a conflict after that this conflict was fully resolved with
        another node.
        """
        def answerStoreObject(orig, conn, conflict, oid):
            if not conflict:
                p.revert()
                ll()
            orig(conn, conflict, oid)
        if 1:
            s0, s1 = cluster.storage_list
            t1, c1 = cluster.getTransaction()
            c1.root()['x'] = x = PCounterWithResolution()
            t1.commit()
            x.value += 1
            t2, c2 = cluster.getTransaction()
            c2.root()['x'].value += 2
            t2.commit()
            with LockLock() as ll, s1.filterConnection(cluster.client) as f, \
                    Patch(cluster.client.storage_handler,
                          answerStoreObject=answerStoreObject) as p:
                f.delayAnswerStoreObject()
                t = self.newThread(t1.commit)
                ll()
            t.join()

    @with_cluster()
    def testSameNewOidAndConflictOnBigValue(self, cluster):
        storage = cluster.getZODBStorage()
        oid = storage.new_oid()

        txn = transaction.Transaction()
        storage.tpc_begin(txn)
        storage.store(oid, None, 'foo', '', txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)

        txn = transaction.Transaction()
        storage.tpc_begin(txn)
        self.assertRaises(POSException.ConflictError, storage.store,
                          oid, None, '*' * cluster.cache_size, '', txn)

    @with_cluster(replicas=1)
    def testConflictWithOutOfDateCell(self, cluster):
        """
        C1         S1         S0         C2
        begin      down                  begin
                              U <------- commit
                   up (remaining out-of-date due to suspended replication)
        store ---> O (stored lockless)
             `--------------> conflict
        resolve -> stored lockless
               `------------> locked
        committed
        """
        s0, s1 = cluster.storage_list
        t1, c1 = cluster.getTransaction()
        c1.root()['x'] = x = PCounterWithResolution()
        t1.commit()
        s1.stop()
        cluster.join((s1,))
        x.value += 1
        t2, c2 = cluster.getTransaction()
        c2.root()['x'].value += 2
        t2.commit()
        with ConnectionFilter() as f:
            f.delayAskFetchTransactions()
            s1.resetNode()
            s1.start()
            self.tic()
            t1.commit()

    @with_cluster(replicas=1)
    def testReplicaDisconnectionDuringCommit(self, cluster):
        """
        S0         C         S1
          <------- c1+=1 -->
          <------- c2+=2 --> C-S1 closed
          <------- c3+=3
        U                    U
                   finish    O
                             U
        down
                   loads <--
        """
        count = [0]
        def ask(orig, self, packet, **kw):
            if (isinstance(packet, Packets.AskStoreObject)
                and self.getUUID() == s1.uuid):
                count[0] += 1
                if count[0] == 2:
                    self.close()
            return orig(self, packet, **kw)
        s0, s1 = cluster.storage_list
        t, c = cluster.getTransaction()
        r = c.root()
        for x in xrange(3):
            r[x] = PCounter()
        t.commit()
        for x in xrange(3):
            r[x].value += x
        with ConnectionFilter() as f, Patch(MTClientConnection, ask=ask):
            f.delayAskFetchTransactions()
            t.commit()
            self.assertEqual(count[0], 2)
            self.assertPartitionTable(cluster, 'UO')
        self.tic()
        s0.stop()
        cluster.join((s0,))
        cluster.client._cache.clear()
        value_list = []
        for x in xrange(3):
            r[x]._p_deactivate()
            value_list.append(r[x].value)
        self.assertEqual(value_list, range(3))

    @with_cluster(replicas=1, partitions=3, storage_count=3)
    def testMasterArbitratingVote(self, cluster):
        """
        p\S 1  2  3
        0   U  U  .
        1   .  U  U
        2   U  .  U

        With the above setup, check when a client C1 fails to connect to S2
        and another C2 fails to connect to S1.

        For the first 2 scenarios:
        - C1 first votes (the master accepts)
        - C2 vote is delayed until C1 decides to finish or abort
        """
        def delayAbort(conn, packet):
            return isinstance(packet, Packets.AbortTransaction)
        def c1_vote(txn):
            def vote(orig, *args):
                try:
                    return orig(*args)
                finally:
                    ll()
            with LockLock() as ll, Patch(cluster.master.tm, vote=vote):
                commit2.start()
                ll()
            if c1_aborts:
                raise Exception
        pt = [{x.getUUID() for x in x}
            for x in cluster.master.pt.partition_list]
        cluster.storage_list.sort(key=lambda x:
            (x.uuid not in pt[0], x.uuid in pt[1]))
        pt = 'UU.|.UU|U.U'
        self.assertPartitionTable(cluster, pt)
        s1, s2, s3 = cluster.storage_list
        t1, c1 = cluster.getTransaction()
        with cluster.newClient(1) as db:
            t2, c2 = cluster.getTransaction(db)
            with self.noConnection(c1, s2), self.noConnection(c2, s1):
                cluster.client.cp.connection_dict[s2.uuid].close()
                self.tic()
                for c1_aborts in 0, 1:
                    # 0: C1 finishes, C2 vote fails
                    # 1: C1 aborts, C2 finishes
                    #
                    # Although we try to modify the same oid, there's no
                    # conflict because each storage node sees a single
                    # and different transaction: vote to storages is done
                    # in parallel, and the master must be involved as an
                    # arbitrator, which ultimately rejects 1 of the 2
                    # transactions, preferably before the second phase of
                    # the commit.
                    t1.begin(); c1.root()._p_changed = 1
                    t2.begin(); c2.root()._p_changed = 1
                    commit2 = self.newPausedThread(t2.commit)
                    TransactionalResource(t1, 1, tpc_vote=c1_vote)
                    with ConnectionFilter() as f:
                        if not c1_aborts:
                            f.add(delayAbort)
                        f.delayAskFetchTransactions(lambda _:
                            f.discard(delayAbort))
                        try:
                            t1.commit()
                            self.assertFalse(c1_aborts)
                        except Exception:
                            self.assertTrue(c1_aborts)
                        try:
                            commit2.join()
                            self.assertTrue(c1_aborts)
                        except NEOStorageError:
                            self.assertFalse(c1_aborts)
                        self.tic()
                        self.assertPartitionTable(cluster,
                            'OU.|.UU|O.U' if c1_aborts else 'UO.|.OU|U.U')
                    self.tic()
                    self.assertPartitionTable(cluster, pt)
                # S3 fails while C1 starts to finish
                with ConnectionFilter() as f:
                    f.add(lambda conn, packet: conn.getUUID() == s3.uuid and
                        isinstance(packet, Packets.AcceptIdentification))
                    t1.begin(); c1.root()._p_changed = 1
                    TransactionalResource(t1, 0, tpc_finish=lambda *_:
                        cluster.master.nm.getByUUID(s3.uuid)
                        .getConnection().close())
                    self.assertRaises(NEOStorageError, t1.commit)
                    self.assertPartitionTable(cluster, 'UU.|.UO|U.O')
                self.tic()
                self.assertPartitionTable(cluster, pt)

    @with_cluster()
    def testAbortTransaction(self, cluster):
        t, c = cluster.getTransaction()
        r = c.root()
        r._p_changed = 1
        def abort(_):
            raise Exception
        TransactionalResource(t, 0, tpc_vote=abort)
        with cluster.client.filterConnection(cluster.storage) as cs:
            cs.delayAskStoreObject()
            self.assertRaises(Exception, t.commit)
        t.begin()
        r._p_changed = 1
        t.commit()

    @with_cluster(replicas=1)
    def testPartialConflict(self, cluster):
        """
        This scenario proves that the client must keep the data of a modified
        oid until it is successfully stored to all storages. Indeed, if a
        concurrent transaction fails to commit to all storage nodes, we must
        handle inconsistent results from replicas.

        C1         S1         S2         C2
                                         no connection between S1 and C2
        store ---> locked        <------ commit
             `--------------> conflict
        """
        def begin1(*_):
            t2.commit()
            f.add(delayAnswerStoreObject, Patch(Transaction, written=written))
        def delayAnswerStoreObject(conn, packet):
            return (isinstance(packet, Packets.AnswerStoreObject)
                and getattr(conn.getHandler(), 'app', None) is s)
        def written(orig, *args):
            orig(*args)
            f.remove(delayAnswerStoreObject)
        def sync(orig):
            mc1.remove(delayMaster)
            orig()
        s1 = cluster.storage_list[0]
        t1, c1 = cluster.getTransaction()
        c1.root()['x'] = x = PCounterWithResolution()
        t1.commit()
        with cluster.newClient(1) as db:
            t2, c2 = cluster.getTransaction(db)
            with self.noConnection(c2, s1):
                for s in cluster.storage_list:
                    logging.info("late answer from %s", uuid_str(s.uuid))
                    x.value += 1
                    c2.root()['x'].value += 2
                    TransactionalResource(t1, 1, tpc_begin=begin1)
                    s1m, = s1.getConnectionList(cluster.master)
                    try:
                        s1.em.removeReader(s1m)
                        with ConnectionFilter() as f, \
                             cluster.master.filterConnection(
                                cluster.client) as mc1:
                            f.delayAskFetchTransactions()
                            delayMaster = mc1.delayNotifyNodeInformation(
                                Patch(cluster.client, sync=sync))
                            t1.commit()
                            self.assertPartitionTable(cluster, 'OU')
                    finally:
                        s1.em.addReader(s1m)
                    self.tic()
                    self.assertPartitionTable(cluster, 'UU')
        self.assertEqual(x.value, 6)

    @contextmanager
    def thread_switcher(self, threads, order, expected):
        self.assertGreaterEqual(len(order), len(expected))
        thread_id = ThreadId()
        l = [threading.Lock() for l in xrange(len(threads)+1)]
        l[0].acquire()
        end = defaultdict(list)
        order = iter(order)
        expected = iter(expected)
        def sched(orig, *args, **kw):
            i = thread_id()
            logging.info('%s: %s%r', i, orig.__name__, args)
            try:
                x = u64(kw['oid'])
            except KeyError:
                for x in args:
                    if isinstance(x, Packet):
                        x = type(x).__name__
                        break
                else:
                    x = orig.__name__
            try:
                j = next(order)
            except StopIteration:
                end[i].append(x)
                j = None
                try:
                    while 1:
                        l.pop().release()
                except IndexError:
                    pass
            else:
                try:
                    self.assertEqual(next(expected), x)
                except StopIteration:
                    end[i].append(x)
            try:
                if callable(j):
                    with contextmanager(j)(*args, **kw) as j:
                        return orig(*args, **kw)
                else:
                    return orig(*args, **kw)
            finally:
                if i != j is not None:
                    try:
                        l[j].release()
                    except threading.ThreadError:
                        l[j].acquire()
                        threads[j-1].start()
                    if x != 'StoreTransaction':
                        try:
                            l[i].acquire()
                        except IndexError:
                            pass
        def _handlePacket(orig, *args):
            if isinstance(args[2], Packets.AnswerRebaseTransaction):
                return sched(orig, *args)
            return orig(*args)
        with RandomConflictDict, \
             Patch(Transaction, write=sched), \
             Patch(ClientApplication, _handlePacket=_handlePacket), \
             Patch(ClientApplication, tpc_abort=sched), \
             Patch(ClientApplication, tpc_begin=sched), \
             Patch(ClientApplication, _askStorageForWrite=sched):
            yield end
        self.assertFalse(list(expected))
        self.assertFalse(list(order))

    @with_cluster()
    def _testComplexDeadlockAvoidanceWithOneStorage(self, cluster, changes,
            order, expected_packets, expected_values,
            except2=POSException.ReadConflictError):
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        oids = []
        for x in 'abcd':
            r[x] = PCounterWithResolution()
            t1.commit()
            oids.append(u64(r[x]._p_oid))
        # The test relies on the implementation-defined behavior that ZODB
        # processes oids by order of registration. It's also simpler with
        # oids from a=1 to d=4.
        self.assertEqual(oids, range(1, 5))
        t2, c2 = cluster.getTransaction()
        t3, c3 = cluster.getTransaction()
        changes(r, c2.root(), c3.root())
        threads = map(self.newPausedThread, (t2.commit, t3.commit))
        with self.thread_switcher(threads, order, expected_packets) as end:
            t1.commit()
            if except2 is None:
                threads[0].join()
            else:
                self.assertRaises(except2, threads[0].join)
            threads[1].join()
        t3.begin()
        r = c3.root()
        self.assertEqual(expected_values, [r[x].value for x in 'abcd'])
        return dict(end)

    def testCascadedDeadlockAvoidanceWithOneStorage1(self):
        """
        locking tids: t1 < t2 < t3
        1. A2 (t2 stores A)
        2. B1, c2 (t2 checks C)
        3. A3 (delayed), B3 (delayed), D3 (delayed)
        4. C1 -> deadlock: B3
        5. d2 -> deadlock: A3
        locking tids: t3 < t1 < t2
        6. t3 commits
        7. t2 rebase: conflicts on A and D
        8. t1 rebase: new deadlock on C
        9. t2 aborts (D non current)
        all locks released for t1, which rebases and resolves conflicts
        """
        def changes(r1, r2, r3):
            r1['b'].value += 1
            r1['c'].value += 2
            r2['a'].value += 3
            self.readCurrent(r2['c'])
            self.readCurrent(r2['d'])
            r3['a'].value += 4
            r3['b'].value += 5
            r3['d'].value += 6
        x = self._testComplexDeadlockAvoidanceWithOneStorage(changes,
            (1, 1, 0, 1, 2, 2, 2, 2, 0, 1, 2, 1, 0, 0, 1, 0, 0, 1),
            ('tpc_begin', 'tpc_begin', 1, 2, 3, 'tpc_begin', 1, 2, 4, 3, 4,
             'StoreTransaction', 'RebaseTransaction', 'RebaseTransaction',
             'AnswerRebaseTransaction', 'AnswerRebaseTransaction',
             'RebaseTransaction', 'AnswerRebaseTransaction'),
            [4, 6, 2, 6])
        try:
            x[1].remove(1)
        except ValueError:
            pass
        self.assertEqual(x, {0: [2, 'StoreTransaction'], 1: ['tpc_abort']})

    def testCascadedDeadlockAvoidanceWithOneStorage2(self):
        def changes(r1, r2, r3):
            r1['a'].value += 1
            r1['b'].value += 2
            r1['c'].value += 3
            r2['a'].value += 4
            r3['b'].value += 5
            r3['c'].value += 6
            self.readCurrent(r2['c'])
            self.readCurrent(r2['d'])
            self.readCurrent(r3['d'])
            def unlock(orig, *args):
                f.remove(rebase)
                return orig(*args)
            rebase = f.delayAskRebaseTransaction(
                Patch(TransactionManager, unlock=unlock))
        with ConnectionFilter() as f:
            x = self._testComplexDeadlockAvoidanceWithOneStorage(changes,
                (0, 1, 1, 0, 1, 2, 2, 2, 2, 0, 1, 2, 1,
                 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 1),
                ('tpc_begin', 1, 'tpc_begin', 1, 2, 3, 'tpc_begin',
                 2, 3, 4, 3, 4, 'StoreTransaction', 'RebaseTransaction',
                 'RebaseTransaction', 'AnswerRebaseTransaction'),
                [1, 7, 9, 0])
        x[0].sort(key=str)
        try:
            x[1].remove(1)
        except ValueError:
            pass
        self.assertEqual(x, {
            0: [2, 3, 'AnswerRebaseTransaction',
                'RebaseTransaction', 'StoreTransaction'],
            1: ['AnswerRebaseTransaction','RebaseTransaction',
                'AnswerRebaseTransaction', 'tpc_abort'],
        })

    def testCascadedDeadlockAvoidanceOnCheckCurrent(self):
        """Transaction checking an oid more than once

        1. t1 < t2
        2. t1 deadlocks, t2 gets all locks
        3. t2 < t1 < t3
        4. t2 finishes: conflict on A, t1 locks C, t3 locks B
        5. t1 rebases B -> second deadlock
        6. t1 resolves A
        7. t3 resolves A -> deadlock, and t1 locks B
        8. t1 rebases B whereas it was already locked
        """
        def changes(*r):
            for r in r:
                r['a'].value += 1
                self.readCurrent(r['b'])
                self.readCurrent(r['c'])
        t = []
        def vote_t2(*args, **kw):
            yield 0
            t.append(threading.currentThread())
        def tic_t1(*args, **kw):
            # Make sure t2 finishes before rebasing B,
            # so that B is locked by a newer transaction (t3).
            t[0].join()
            yield 0
        end = self._testComplexDeadlockAvoidanceWithOneStorage(changes,
            (0, 1, 1, 0, 1, 1, 0, 0, 2, 2, 2, 2, 1, vote_t2, tic_t1),
            ('tpc_begin', 1) * 2, [3, 0, 0, 0], None)
        self.assertLessEqual(2, end[0].count('RebaseTransaction'))

    def testFailedConflictOnBigValueDuringDeadlockAvoidance(self):
        def changes(r1, r2, r3):
            r1['b'].value = 1
            r1['d'].value = 2
            r2['a'].value = '*' * r2._p_jar.db().storage._cache._max_size
            r2['b'].value = 3
            r2['c'].value = 4
            r3['a'].value = 5
            self.readCurrent(r3['c'])
            self.readCurrent(r3['d'])
        with ConnectionFilter() as f:
            x = self._testComplexDeadlockAvoidanceWithOneStorage(changes,
                (1, 1, 1, 2, 2, 2, 1, 2, 2, 0, 0, 1, 1, 1, 0),
                ('tpc_begin', 'tpc_begin', 1, 2, 'tpc_begin', 1, 3, 3, 4,
                'StoreTransaction', 2, 4, 'RebaseTransaction',
                'AnswerRebaseTransaction', 'tpc_abort'),
                [5, 1, 0, 2], POSException.ConflictError)
        self.assertEqual(x, {0: ['StoreTransaction']})

    @with_cluster(replicas=1, partitions=4)
    def testNotifyReplicated(self, cluster):
        """
        Check replication while several concurrent transactions leads to
        conflict resolutions and deadlock avoidances, and in particular the
        handling of write-locks when the storage node is about to notify the
        master that partitions are replicated.
        Transactions are committed in the following order:
        - t2
        - t4, conflict on 'd'
        - t1, deadlock on 'a'
        - t3, deadlock on 'b', and 2 conflicts on 'a'
        Special care is also taken for the change done by t3 on 'a', to check
        that the client resolves conflicts with correct oldSerial:
        1. The initial store (a=8) is first delayed by t2.
        2. It is then kept aside by the deadlock.
        3. On s1, deadlock avoidance happens after t1 stores a=7 and the store
           is delayed again. However, it's the contrary on s0, and a conflict
           is reported to the client.
        4. Second store (a=12) based on t2.
        5. t1 finishes and s1 reports the conflict for first store (with t1).
           At that point, the base serial of this store is meaningless:
           the client only has data for last store (based on t2), and it's its
           base serial that must be used. t3 write 15 (and not 19 !).
        6. Conflicts for the second store are with t2 and they're ignored
           because they're already resolved.
        Note that this test method lacks code to enforce some events to happen
        in the expected order. Sometimes, the above scenario is not reproduced
        entirely, but it's so rare that there's no point in making the code
        further complicated.
        """
        s0, s1 = cluster.storage_list
        s1.stop()
        cluster.join((s1,))
        s1.resetNode()
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'abcd':
            r[x] = PCounterWithResolution()
            t1.commit()
        t3, c3 = cluster.getTransaction()
        r['c'].value += 1
        t1.commit()
        r['b'].value += 2
        r['a'].value += 3
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['a'].value += 4
        r['c'].value += 5
        r['d'].value += 6
        r = c3.root()
        r['c'].value += 7
        r['a'].value += 8
        r['b'].value += 9
        t4, c4 = cluster.getTransaction()
        r = c4.root()
        r['d'].value += 10
        threads = map(self.newPausedThread, (t2.commit, t3.commit, t4.commit))
        def t3_c(*args, **kw):
            yield 1
            # We want to resolve the conflict before storing A.
            self.tic()
        def t3_resolve(*args, **kw):
            self.assertPartitionTable(cluster, 'UO|UO|UO|UO')
            f.remove(delay)
            self.tic()
            self.assertPartitionTable(cluster, 'UO|UO|UU|UO')
            yield
        def t1_rebase(*args, **kw):
            self.tic()
            self.assertPartitionTable(cluster, 'UO|UU|UU|UO')
            yield
        def t3_b(*args, **kw):
            yield 1
            self.tic()
            self.assertPartitionTable(cluster, 'UO|UU|UU|UU')
        def t4_d(*args, **kw):
            self.tic()
            self.assertPartitionTable(cluster, 'UU|UU|UU|UU')
            yield 2
        # Delay the conflict for the second store of 'a' by t3.
        delay_conflict = {s0.uuid: [1], s1.uuid: [1,0]}
        def delayConflict(conn, packet):
            app = conn.getHandler().app
            if (isinstance(packet, Packets.AnswerStoreObject)
                and packet.decode()[0]):
                conn, = cluster.client.getConnectionList(app)
                kw = conn._handlers._pending[0][0][packet._id][1]
                return 1 == u64(kw['oid']) and delay_conflict[app.uuid].pop()
        def writeA(orig, txn_context, oid, serial, data):
            if u64(oid) == 1:
                value = unpickle_state(data)['value']
                if value > 12:
                    f.remove(delayConflict)
                elif value == 12:
                    f.add(delayConflict)
            return orig(txn_context, oid, serial, data)
        ###
        with ConnectionFilter() as f, \
             Patch(cluster.client, _store=writeA), \
             self.thread_switcher(threads,
                (1, 2, 3, 0, 1, 0, 2, t3_c, 1, 3, 2, t3_resolve, 0, 0, 0,
                 t1_rebase, 2, t3_b, 3, t4_d, 0, 2, 2),
                ('tpc_begin', 'tpc_begin', 'tpc_begin', 'tpc_begin', 2, 1, 1,
                 3, 3, 4, 4, 3, 1, 'RebaseTransaction', 'RebaseTransaction',
                 'AnswerRebaseTransaction', 'AnswerRebaseTransaction', 2
                 )) as end:
            delay = f.delayAskFetchTransactions()
            s1.start()
            self.tic()
            t1.commit()
            for t in threads:
                t.join()
        t4.begin()
        self.assertEqual([15, 11, 13, 16], [r[x].value for x in 'abcd'])
        self.assertEqual([2, 2], map(end.pop(2).count,
            ['RebaseTransaction', 'AnswerRebaseTransaction']))
        self.assertEqual(end, {
            0: [1, 'StoreTransaction'],
            1: ['StoreTransaction'],
            3: [4, 'StoreTransaction'],
        })

    @with_cluster(replicas=1)
    def testNotifyReplicated2(self, cluster):
        s0, s1 = cluster.storage_list
        s1.stop()
        cluster.join((s1,))
        s1.resetNode()
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'ab':
            r[x] = PCounterWithResolution()
            t1.commit()
        r['a'].value += 1
        r['b'].value += 2
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['a'].value += 3
        r['b'].value += 4
        thread = self.newPausedThread(t2.commit)
        def t2_b(*args, **kw):
            self.assertPartitionTable(cluster, 'UO')
            f.remove(delay)
            self.tic()
            self.assertPartitionTable(cluster, 'UO')
            yield 0
        def t2_vote(*args, **kw):
            self.tic()
            self.assertPartitionTable(cluster, 'UU')
            yield 0
        with ConnectionFilter() as f, \
             self.thread_switcher((thread,),
                 (1, 0, 1, 1, t2_b, 0, 0, 1, t2_vote, 0, 0),
                 ('tpc_begin', 'tpc_begin', 1, 1, 2, 2,
                  'RebaseTransaction', 'RebaseTransaction', 'StoreTransaction',
                  'AnswerRebaseTransaction', 'AnswerRebaseTransaction',
                  )) as end:
            delay = f.delayAskFetchTransactions()
            s1.start()
            self.tic()
            t1.commit()
            thread.join()
        t2.begin()
        self.assertEqual([4, 6], [r[x].value for x in 'ab'])

    @with_cluster(storage_count=2, partitions=2)
    def testDeadlockAvoidanceBeforeInvolvingAnotherNode(self, cluster):
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'abc':
            r[x] = PCounterWithResolution()
            t1.commit()
        r['a'].value += 1
        r['c'].value += 2
        r['b'].value += 3
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['c'].value += 4
        r['a'].value += 5
        r['b'].value += 6
        t = self.newPausedThread(t2.commit)
        def t1_b(*args, **kw):
            yield 1
            self.tic()
        with self.thread_switcher((t,), (1, 0, 1, 0, t1_b, 0, 0, 0, 1),
            ('tpc_begin', 'tpc_begin', 1, 3, 3, 1, 'RebaseTransaction',
             2, 'AnswerRebaseTransaction')) as end:
            t1.commit()
            t.join()
        t2.begin()
        self.assertEqual([6, 9, 6], [r[x].value for x in 'abc'])
        self.assertEqual([2, 2], map(end.pop(1).count,
            ['RebaseTransaction', 'AnswerRebaseTransaction']))
        # Rarely, there's an extra deadlock for t1:
        # 0: ['AnswerRebaseTransaction', 'RebaseTransaction',
        #     'RebaseTransaction', 'AnswerRebaseTransaction',
        #     'AnswerRebaseTransaction', 2, 3, 1,
        #     'StoreTransaction', 'VoteTransaction']
        self.assertEqual(end.pop(0)[0], 'AnswerRebaseTransaction')
        self.assertFalse(end)

    @with_cluster()
    def testDelayedStoreOrdering(self, cluster):
        """
        By processing delayed stores (EventQueue) in the order of their locking
        tid, we minimize the number deadlocks. Here, we trigger a first
        deadlock, so that the delayed check for t1 is reordered after that of
        t3.
        """
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'abcd':
            r[x] = PCounter()
            t1.commit()
        r['a'].value += 1
        self.readCurrent(r['d'])
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['b'].value += 1
        self.readCurrent(r['d'])
        t3, c3 = cluster.getTransaction()
        r = c3.root()
        r['c'].value += 1
        self.readCurrent(r['d'])
        threads = map(self.newPausedThread, (t2.commit, t3.commit))
        with self.thread_switcher(threads, (1, 2, 0, 1, 2, 1, 0, 2, 0, 1, 2),
            ('tpc_begin', 'tpc_begin', 'tpc_begin', 1, 2, 3, 4, 4, 4,
             'RebaseTransaction', 'StoreTransaction')) as end:
            t1.commit()
            for t in threads:
                t.join()
        self.assertEqual(end, {
            0: ['AnswerRebaseTransaction', 'StoreTransaction'],
            2: ['StoreTransaction']})

    @with_cluster(replicas=1)
    def testConflictAfterDeadlockWithSlowReplica1(self, cluster,
                                                  slow_rebase=False):
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'ab':
            r[x] = PCounterWithResolution()
            t1.commit()
        r['a'].value += 1
        r['b'].value += 2
        s1 = cluster.storage_list[1]
        with cluster.newClient(1) as db, \
             (s1.filterConnection(cluster.client) if slow_rebase else
              cluster.client.filterConnection(s1)) as f, \
             cluster.client.extraCellSortKey(lambda cell:
                cell.getUUID() == s1.uuid):
            t2, c2 = cluster.getTransaction(db)
            r = c2.root()
            r['a'].value += 3
            self.readCurrent(r['b'])
            t = self.newPausedThread(t2.commit)
            def tic_t1(*args, **kw):
                yield 0
                self.tic()
            def tic_t2(*args, **kw):
                yield 1
                self.tic()
            def load(orig, *args, **kw):
                f.remove(delayStore)
                return orig(*args, **kw)
            order = [tic_t2, 0, tic_t2, 1, tic_t1, 0, 0, 0, 1, tic_t1, 0]
            def t1_resolve(*args, **kw):
                yield
                f.remove(delay)
            if slow_rebase:
                order.append(t1_resolve)
                delay = f.delayAnswerRebaseObject()
            else:
                order[-1] = t1_resolve
                delay = f.delayAskStoreObject()
            with self.thread_switcher((t,), order,
                ('tpc_begin', 'tpc_begin', 1, 1, 2, 2, 'RebaseTransaction',
                'RebaseTransaction', 'AnswerRebaseTransaction',
                'StoreTransaction')) as end:
                t1.commit()
                t.join()
            self.assertNotIn(delay, f)
            t2.begin()
            end[0].sort(key=str)
            self.assertEqual(end, {0: [1, 'AnswerRebaseTransaction',
                                       'StoreTransaction']})
            self.assertEqual([4, 2], [r[x].value for x in 'ab'])

    def testConflictAfterDeadlockWithSlowReplica2(self):
        self.testConflictAfterDeadlockWithSlowReplica1(True)

    @with_cluster(start_cluster=0, master_count=3)
    def testElection(self, cluster):
        m0, m1, m2 = cluster.master_list
        cluster.start(master_list=(m0,), recovering=True)
        getClusterState = cluster.neoctl.getClusterState
        m0.em.removeReader(m0.listening_conn)
        m1.start()
        self.tic()
        m2.start()
        self.tic()
        self.assertTrue(m0.primary)
        self.assertTrue(m1.primary)
        self.assertFalse(m2.primary)
        m0.em.addReader(m0.listening_conn)
        with ConnectionFilter() as f:
            f.delayAcceptIdentification()
            self.tic()
        self.tic()
        self.assertTrue(m0.primary)
        self.assertFalse(m1.primary)
        self.assertFalse(m2.primary)
        self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
        cluster.startCluster()
        def stop(node):
            node.stop()
            cluster.join((node,))
            node.resetNode()
        stop(m1)
        self.tic()
        self.assertEqual(getClusterState(), ClusterStates.RUNNING)
        self.assertTrue(m0.primary)
        self.assertFalse(m2.primary)
        stop(m0)
        self.tic()
        self.assertEqual(getClusterState(), ClusterStates.RUNNING)
        self.assertTrue(m2.primary)
        # Check for proper update of node ids on first NotifyNodeInformation.
        stop(m2)
        m0.start()
        def update(orig, app, timestamp, node_list):
            orig(app, timestamp, sorted(node_list, reverse=1))
        with Patch(cluster.storage.nm, update=update):
            with ConnectionFilter() as f:
                f.add(lambda conn, packet:
                    isinstance(packet, Packets.RequestIdentification)
                    and packet.decode()[0] == NodeTypes.STORAGE)
                self.tic()
                m2.start()
                self.tic()
            self.tic()
        self.assertEqual(getClusterState(), ClusterStates.RUNNING)
        self.assertTrue(m0.primary)
        self.assertFalse(m2.primary)

    @with_cluster(start_cluster=0, master_count=2)
    def testIdentifyUnknownMaster(self, cluster):
        m0, m1 = cluster.master_list
        cluster.master_nodes = ()
        m0.resetNode()
        cluster.start(master_list=(m0,))
        m1.start()
        self.tic()
        self.assertEqual(cluster.neoctl.getClusterState(),
                         ClusterStates.RUNNING)
        self.assertTrue(m0.primary)
        self.assertTrue(m0.is_alive())
        self.assertFalse(m1.primary)
        self.assertTrue(m1.is_alive())

    @with_cluster(start_cluster=0, master_count=2,
                  partitions=2, storage_count=2, autostart=2)
    def testSplitBrainAtCreation(self, cluster):
        """
        Check cluster creation when storage nodes are identified before all
        masters see each other and elect a primary.
        XXX: Do storage nodes need a node id before the cluster is created ?
             Another solution is that they treat their ids as temporary as long
             as the partition table is empty.
        """
        for m, s in zip((min, max), cluster.storage_list):
            s.nm.remove(m(s.nm.getMasterList(),
                key=lambda node: node.getAddress()))
        with ConnectionFilter() as f:
            f.add(lambda conn, packet:
                isinstance(packet, Packets.RequestIdentification)
                and packet.decode()[0] == NodeTypes.MASTER)
            cluster.start(recovering=True)
            neoctl = cluster.neoctl
            getClusterState = neoctl.getClusterState
            getStorageList = lambda: neoctl.getNodeList(NodeTypes.STORAGE)
            self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
            self.assertEqual(1, len(getStorageList()))
        with Patch(EventHandler, protocolError=lambda *_: sys.exit()):
            self.tic()
        expectedFailure(self.assertEqual)(neoctl.getClusterState(),
                                          ClusterStates.RUNNING)
        self.assertEqual({1: NodeStates.RUNNING, 2: NodeStates.RUNNING},
            {x[2]: x[3] for x in neoctl.getNodeList(NodeTypes.STORAGE)})

    @with_cluster(partitions=2, storage_count=2)
    def testStorageBackendLastIDs(self, cluster):
        """
        Check that getLastIDs/getLastTID ignore data from unassigned partitions.

        XXX: this kind of test should not be reexecuted with SSL
        """
        cluster.sortStorageList()
        t, c = cluster.getTransaction()
        c.root()[''] = PCounter()
        t.commit()
        big_id_list = ('\x7c' * 8, '\x7e' * 8), ('\x7b' * 8, '\x7d' * 8)
        for i in 0, 1:
            dm = cluster.storage_list[i].dm
            expected = dm.getLastTID(u64(MAX_TID)), dm.getLastIDs()
            oid, tid = big_id_list[i]
            for j, expected in (
                    (1 - i, (dm.getLastTID(u64(MAX_TID)), dm.getLastIDs())),
                    (i, (u64(tid), (tid, {}, {}, oid)))):
                oid, tid = big_id_list[j]
                # Somehow we abuse 'storeTransaction' because we ask it to
                # write data for unassigned partitions. This is not checked
                # so for the moment, the test works.
                dm.storeTransaction(tid, ((oid, None, None),),
                                    ((oid,), '', '', '', 0, tid), False)
                self.assertEqual(expected,
                    (dm.getLastTID(u64(MAX_TID)), dm.getLastIDs()))

    def testStorageUpgrade(self):
        path = os.path.join(os.path.dirname(__file__),
                            self._testMethodName + '-%s',
                            's%s.sql')
        dump_dict = {}
        def switch(s):
            dm = s.dm
            dm.commit()
            dump_dict[s.uuid] = dm.dump()
            dm.erase()
            with open(path % (s.getAdapter(), s.uuid)) as f:
                dm.restore(f.read())
        with NEOCluster(storage_count=3, partitions=3, replicas=1,
                        name=self._testMethodName) as cluster:
            s1, s2, s3 = cluster.storage_list
            cluster.start(storage_list=(s1,))
            for s in s2, s3:
                s.start()
                self.tic()
                cluster.neoctl.enableStorageList([s.uuid])
                cluster.neoctl.tweakPartitionTable()
            self.tic()
            nid_list = [s.uuid for s in cluster.storage_list]
            switch(s3)
            s3.stop()
            storage = cluster.getZODBStorage()
            txn = transaction.Transaction()
            storage.tpc_begin(txn, p64(85**9)) # partition 1
            storage.store(p64(0), None, 'foo', '', txn)
            storage.tpc_vote(txn)
            storage.tpc_finish(txn)
            self.tic()
            switch(s1)
            switch(s2)
            cluster.stop()
            for i, s in zip(nid_list, cluster.storage_list):
                self.assertMultiLineEqual(s.dm.dump(), dump_dict[i])


if __name__ == "__main__":
    unittest.main()
