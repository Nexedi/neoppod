# -*- coding: utf-8 -*-
#
# Copyright (C) 2011-2019  Nexedi SA
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
import random
import sys
import threading
import time
import transaction
from collections import defaultdict
from contextlib import contextmanager
from six.moves._thread import get_ident
from persistent import Persistent, GHOST
from transaction.interfaces import TransientError
from ZODB import DB, POSException
from ZODB.DB import TransactionalUndo
from neo import *
from neo.storage.transactions import TransactionManager, ConflictError
from neo.lib.connection import ConnectionClosed, \
    ClientConnection, ServerConnection, MTClientConnection
from neo.lib.exception import StoppedOperation
from neo.lib.handler import DelayEvent, EventHandler
from neo.lib import logging
from neo.lib.protocol import (CellStates, ClusterStates, NodeStates, NodeTypes,
    Packets, Packet, uuid_str, ZERO_OID, ZERO_TID, MAX_TID)
from .. import Patch, TransactionalResource, getTransactionMetaData
from . import ClientApplication, ConnectionFilter, LockLock, NEOCluster, \
    NEOThreadedTest, RandomConflictDict, Serialized, ThreadId, with_cluster
from neo.lib.util import add64, makeChecksum, p64, u64
from neo.client import exception
from neo.client.exception import NEOPrimaryMasterLost, NEOStorageError
from neo.client.handlers.storage import _DeadlockPacket
from neo.client.transactions import Transaction
from neo.master.handlers.client import ClientServiceHandler
from neo.master.pt import PartitionTable
from neo.neoctl.neoctl import NotReadyException
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
            compressible = b'x' * 20
            compressed = compress(compressible)
            oid_list = []
            if cluster.storage.getAdapter() == 'SQLite':
                big = None
                data = b'foo', b'', b'foo', compressed, compressible
            else:
                big = os.urandom(65536) * 600
                assert len(big) < len(compress(big))
                data = (b'foo', big, b'', b'foo', big[:2**24-1], big,
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
                dm = cluster.storage.dm
                with dm.lock:
                    cluster.storage.dm.deleteObject(oid)
                self.assertRaises(POSException.POSKeyError,
                    storage.load, oid, '')
                for oid, data, serial in oid_list[i:]:
                    self.assertEqual((data, serial), storage.load(oid, ''))
            if big:
                self.assertFalse(cluster.storage.sqlCount('bigdata'))
            self.assertFalse(cluster.storage.sqlCount('data'))

    @with_cluster(storage_count=3, replicas=1, partitions=5)
    def testIterOIDs(self, cluster):
        storage = cluster.getZODBStorage()
        client = cluster.client
        oids = []
        for i in range(5):
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            for i in range(7):
                oid = client.new_oid()
                oids.append(u64(oid))
                storage.store(oid, None, b'', '', txn)
            client.new_oid()
            storage.tpc_vote(txn)
            storage.tpc_finish(txn)
        tid = client.last_tid
        self.assertEqual(oids, list(map(u64, client.oids(tid))))
        def askOIDsFrom(orig, self, conn, partition, length, min_oid, tid):
            return orig(self, conn, partition, 3, min_oid, tid)
        with Patch(ClientOperationHandler, askOIDsFrom=askOIDsFrom):
            self.assertEqual(oids[3:-3],
                list(map(u64, client.oids(tid, p64(oids[3]), p64(oids[-4])))))
            random.shuffle(oids)
            while oids:
                txn = transaction.Transaction()
                storage.tpc_begin(txn)
                for i in oids[-6:]:
                    oid = p64(i)
                    storage.deleteObject(oid, storage.load(oid)[1], txn)
                storage.tpc_vote(txn)
                i = storage.tpc_finish(txn)
                self.assertEqual(sorted(oids), list(map(u64, client.oids(tid))))
                del oids[-6:]
                tid = i
                self.assertEqual(sorted(oids), list(map(u64, client.oids(tid))))

    def _testUndoConflict(self, cluster, *inc):
        def waitResponses(orig, *args):
            orig(*args)
            p.revert()
            t.commit()
        t, c = cluster.getTransaction()
        c.root()[0] = ob = PCounterWithResolution()
        t.commit()
        tids = []
        c.readCurrent(c.root())
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
        dm = cluster.storage.dm
        with dm.lock:
            self.assertFalse(dm.getOrphanList())
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

    @with_cluster()
    def testUndoConflictDuringStore(self, cluster):
        self._testUndoConflict(cluster, 1)

    @with_cluster()
    def testUndoConflictCreationUndo(self, cluster):
        def waitResponses(orig, *args):
            orig(*args)
            p.revert()
            t.commit()
        t, c = cluster.getTransaction()
        c.root()[0] = ob = PCounterWithResolution()
        t.commit()
        undo = TransactionalUndo(cluster.db, [ob._p_serial])
        txn = transaction.Transaction()
        undo.tpc_begin(txn)
        ob.value += 1
        with Patch(cluster.client, waitResponses=waitResponses) as p:
            self.assertRaises(POSException.ConflictError, undo.commit, txn)
        t.begin()
        self.assertEqual(ob.value, 1)

    def testStorageDataLock(self, dedup=False):
        with NEOCluster(dedup=dedup) as cluster:
            cluster.start()
            storage = cluster.getZODBStorage()
            data_info = {}

            data = b'foo'
            key = makeChecksum(data), 0
            oid = storage.new_oid()
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            r1 = storage.store(oid, None, data, '', txn)
            r2 = storage.tpc_vote(txn)
            tid = storage.tpc_finish(txn)

            txn = [transaction.Transaction() for x in range(4)]
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
        t1 = t(b'foo')
        storage.tpc_finish(t(b'bar'))
        s = cluster.storage
        s.stop()
        cluster.join((s,))
        s.resetNode()
        storage.app.max_reconnection_to_master = 0
        self.assertRaises(NEOPrimaryMasterLost, storage.tpc_vote, t1)
        with self.expectedFailure(): \
        self.assertFalse(s.dm.getOrphanList())

    @with_cluster(storage_count=1)
    def testDelayedUnlockInformation(self, cluster):
        except_list = []
        def onStoreObject(orig, tm, ttid, serial, oid, *args):
            if oid == resume_oid and delayUnlockInformation in m2s:
                m2s.remove(delayUnlockInformation)
            try:
                return orig(tm, ttid, serial, oid, *args)
            except Exception as e:
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
    def _testDeadlockAvoidance(self, cluster, scenario, delayed_conflict=None):
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
            except Exception as e:
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
            if c2 and delayed:
                f.remove(delayed.pop())
                self.assertFalse(delayed)
            try:
                return orig(conn, packet, *args, **kw)
            finally:
                if switch:
                    delay[c2].clear()
                    delay[1-c2].set()

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

        delayed = []
        if delayed_conflict:
            def finish1(*_):
                d = f.byPacket(delayed_conflict, lambda _: delayed.append(d))
            TransactionalResource(t1, 0, tpc_finish=finish1)

        with ConnectionFilter() as f, \
             Patch(TransactionManager, storeObject=onStoreObject), \
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

    def testDeadlockAvoidance(self, **kw):
        # 0: C1 -> S1
        # 1: C2 -> S1, S2 (delayed)
        # 2: C1 -> S2 (deadlock)
        # 3: C2 -> S2 (delayed)
        # 4: C1 commits
        # 5: C2 resolves conflict
        self.assertEqual(self._testDeadlockAvoidance([1, 3], **kw),
            [DelayEvent, DelayEvent, ConflictError])

    # The following 2 tests cover extra paths when processing
    # AnswerRelockObject.

    def testDeadlockAvoidanceConflictVsLateRelock(self):
        self.testDeadlockAvoidance(delayed_conflict=Packets.AnswerRelockObject)

    def testDeadlockAvoidanceRelockVsLateConflict(self):
        self.testDeadlockAvoidance(delayed_conflict=Packets.AnswerStoreObject)

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
    def testSlowConflictResolution(self, cluster):
        """
        Check that a slow conflict resolution does not always result in a new
        conflict because a concurrent client keeps modifying the same object
        quickly.
        An idea to fix it is to take the lock before the second attempt to
        resolve.
        """
        t1, c1 = cluster.getTransaction()
        c1.root()[''] = ob = PCounterWithResolution()
        t1.commit()
        l1 = threading.Lock(); l1.acquire()
        l2 = threading.Lock(); l2.acquire()
        conflicts = []
        def _p_resolveConflict(orig, *args):
            conflicts.append(get_ident())
            l1.release(); l2.acquire()
            return orig(*args)
        with cluster.newClient(1) as db, Patch(PCounterWithResolution,
                   _p_resolveConflict=_p_resolveConflict):
            t2, c2 = cluster.getTransaction(db)
            c2.root()[''].value += 1
            for i in range(10):
                ob.value += 1
                t1.commit()
                if i:
                    l2.release()
                else:
                    t = self.newThread(t2.commit)
                l1.acquire()
            l2.release()
            t.join()
        with self.expectedFailure(): \
        self.assertIn(get_ident(), conflicts)

    @with_cluster()
    def testReadLockVsAskObject(self, cluster):
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

    @with_cluster(partitions=2, storage_count=2)
    def testReadLockVsAskTIDsFrom(self, cluster):
        """
        Similar to testReadLockVsAskObject, here to check IStorage.iterator
        """
        l = threading.Lock()
        l.acquire()
        idle = []
        def askTIDsFrom(orig, *args):
            try:
                orig(*args)
            finally:
                idle.append(s1.em.isIdle())
                l.release()
        s0, s1 = cluster.sortStorageList()
        t, c = cluster.getTransaction()
        t0 = cluster.last_tid
        r = c.root()
        r[''] = ''
        with Patch(ClientOperationHandler(s1), askTIDsFrom=askTIDsFrom):
            with cluster.master.filterConnection(s1) as m2s1:
                m2s1.delayNotifyUnlockInformation()
                with cluster.moduloTID(1):
                    t.commit()
                t1 = cluster.last_tid
                r._p_changed = 1
                with cluster.moduloTID(0):
                    t.commit()
                t2 = cluster.last_tid
                tid_list = []
                cluster.client._cache.clear()
                @self.newThread
                def iterTrans():
                    for txn in c.db().storage.iterator():
                        tid_list.append(txn.tid)
                l.acquire()
            l.acquire()
        iterTrans.join()
        self.assertEqual(idle, [1, 0])
        self.assertEqual(tid_list, [t0, t1, t2])

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
            getCellSortKey = cluster.client.getCellSortKey
            self.assertEqual(getCellSortKey(s0, good), 0)
            cluster.neoctl.killNode(s0.getUUID())
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
                self.assertEqual([u64(o._p_oid) for o in (r, x, y)], list(range(3)))
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
        (k, v), = set(six.iteritems(s0.getDataLockInfo())
                      ).difference(six.iteritems(di0))
        self.assertEqual(v, 1)
        k, = (k for k, v in six.iteritems(di0) if v == 1)
        di0[k] = 0 # r[2] = 'ok'
        self.assertEqual(list(di0.values()), [0] * 5)
        di1 = s1.getDataLockInfo()
        k, = (k for k, v in six.iteritems(di1) if v == 1)
        del di1[k] # x.value = 1
        self.assertEqual(list(di1.values()), [0])
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
        storage = cluster.storage
        # Disable migration steps that aren't idempotent.
        def noop(*_): pass
        with Patch(storage.dm.__class__, _migrate3=noop), \
             Patch(storage.dm.__class__, _migrate4=noop):
            t, c = cluster.getTransaction()
            with storage.dm.lock:
                storage.dm.setConfiguration("version", None)
            c.root()._p_changed = 1
            t.commit()
            storage.stop()
            cluster.join((storage,))
            storage.em.onTimeout() # deferred commit
            storage.resetNode()
            storage.start()
            t.begin()
            with storage.dm.lock:
                storage.dm.setConfiguration("version", None)
            c.root()._p_changed = 1
            with Patch(storage.tm, lock=lambda *_: sys.exit()):
                self.commitWithStorageFailure(cluster.client, t)
            cluster.join((storage,))
            self.assertRaises(DatabaseFailure, storage.resetNode)

    @with_cluster(replicas=1)
    def testStorageReconnectDuringStore(self, cluster):
        t, c = cluster.getTransaction()
        c.root()[0] = 'ok'
        cluster.client.closeAllStorageConnections()
        t.commit() # store request

    @with_cluster(storage_count=2, partitions=2)
    def testStorageReconnectDuringTransactionLog(self, cluster):
        t, c = cluster.getTransaction()
        cluster.client.closeAllStorageConnections()
        tid, (t1,) = cluster.client.transactionLog(
            ZERO_TID, c.db().lastTransaction(), 10)

    @with_cluster(storage_count=2, partitions=2)
    def testStorageReconnectDuringUndoLog(self, cluster):
        t, c = cluster.getTransaction()
        cluster.client.closeAllStorageConnections()
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
            cluster.neoctl.killNode(s1.uuid)
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
    def testLoadVsFinish(self, cluster):
        t1, c1 = cluster.getTransaction()
        c1.root()['x'] = x1 = PCounter()
        t1.commit()
        t1.begin()
        x1.value = 1
        t2, c2 = cluster.getTransaction()
        x2 = c2.root()['x']
        cluster.client._cache.clear()
        def _loadFromStorage(orig, *args):
            r = orig(*args)
            ll()
            return r
        with LockLock() as ll, Patch(cluster.client,
                _loadFromStorage=_loadFromStorage):
            t = self.newThread(x2._p_activate)
            ll()
            t1.commit()
        t.join()

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
    def testInternalInvalidation2(self, cluster):
        # same as testExternalInvalidation3 but with internal invalidations
        t, c = cluster.getTransaction()
        x = c.root()[''] = PCounter()
        t.commit()
        l1 = threading.Lock(); l1.acquire()
        l2 = threading.Lock(); l2.acquire()
        def sync(orig):
            orig()
            l2.release()
            l1.acquire()
        def raceBeforeInvalidateZODB(orig, transaction, f):
            def callback(tid):
                l1.release()
                begin1.join()
                f(tid)
            return orig(transaction, callback)
        def raceAfterInvalidateZODB(orig, transaction, f):
            def callback(tid):
                f(tid)
                l1.release()
                begin1.join()
            return orig(transaction, callback)
        class CacheLock(object):
            def __init__(self):
                self._lock = client._cache_lock
            def __enter__(self):
                self._lock.acquire()
            def __exit__(self, t, v, tb):
                self._lock.release()
                p.revert()
                load1.start()
                l1.acquire()
        def _loadFromStorage(orig, *args):
            l1.release()
            return orig(*args)
        client = cluster.client
        t2, c2 = cluster.getTransaction()
        x2 = c2.root()['']
        x2.value = 1
        with Patch(client, tpc_finish=raceBeforeInvalidateZODB):
            with Patch(client, sync=sync):
                begin1 = self.newThread(t.begin)
                l2.acquire()
            t2.commit()
        self.assertEqual(x.value, 0)
        x._p_deactivate()
        # On ZODB≥5, the following check would fail
        # if tpc_finish updated app.last_tid earlier.
        self.assertEqual(x.value, 0)
        t.begin()
        self.assertEqual(x.value, 1)
        x2.value = big = 'x' * cluster.cache_size # force load from storage
        with Patch(client, _cache_lock=CacheLock()) as p, \
             Patch(client, _loadFromStorage=_loadFromStorage), \
             Patch(client, tpc_finish=raceAfterInvalidateZODB):
            with Patch(client, sync=sync):
                begin1 = self.newThread(t.begin)
                l2.acquire()
            load1 = self.newPausedThread(lambda: x.value)
            t2.commit()
        # On ZODB<5, the following check would fail
        # if tpc_finish updated app.last_tid later.
        self.assertEqual(load1.join(), big)

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
            self.assertEqual((x2._p_serial, x1._p_serial),
                cluster.client._cache.load(x1._p_oid, x1._p_serial)[1:])

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

    @with_cluster(serialized=False)
    def testExternalInvalidation2(self, cluster):
        t, c = cluster.getTransaction()
        r = c.root()
        x = r[''] = PCounter()
        t.commit()
        tid1 = x._p_serial
        nonlocal_ = [0, 0, 0]
        l1 = threading.Lock(); l1.acquire()
        l2 = threading.Lock(); l2.acquire()
        def invalidateObjects(orig, *args):
            if not nonlocal_[0]:
                l1.acquire()
            orig(*args)
            nonlocal_[0] += 1
            if nonlocal_[0] == 2:
                l2.release()
        class CacheLock(object):
            def __init__(self, client):
                self._lock = client._cache_lock
            def __enter__(self):
                self._lock.acquire()
            def __exit__(self, t, v, tb):
                count = nonlocal_[1]
                nonlocal_[1] = count + 1
                self._lock.release()
                if count == 0:
                    load_same.start()
                    l2.acquire()
                elif count == 1:
                    load_other.start()
        def _loadFromStorage(orig, *args):
            count = nonlocal_[2]
            nonlocal_[2] = count + 1
            if not count:
                l1.release()
            return orig(*args)
        with cluster.newClient() as client, \
              Patch(client.notifications_handler,
                    invalidateObjects=invalidateObjects):
            client.sync()
            with cluster.master.filterConnection(client) as mc2:
                mc2.delayInvalidateObjects()
                # A first client node (C1) modifies an oid whereas
                # invalidations to the other node (C2) are delayed.
                x._p_changed = 1
                t.commit()
                tid2 = x._p_serial
                # C2 loads the most recent revision of this oid (last_tid=tid1).
                self.assertEqual((tid1, tid2), client.load(x._p_oid)[1:])
            # C2 poll thread is frozen just before processing invalidation
            # packet for tid2. C1 modifies something else -> tid3
            r._p_changed = 1
            t.commit()
            self.assertEqual(tid1, client.last_tid)
            load_same = self.newPausedThread(client.load, x._p_oid)
            load_other = self.newPausedThread(client.load, r._p_oid)
            with Patch(client, _cache_lock=CacheLock(client)), \
                 Patch(client, _loadFromStorage=_loadFromStorage):
                # 1. Just after having found nothing in cache, the worker
                #    thread asks the poll thread to get notified about
                #    invalidations for the loading oid.
                # <context switch> (l1)
                # 2. Both invalidations are processed. -> last_tid=tid3
                # <context switch> (l2)
                # 3. The worker thread loads before tid3+1.
                #    The poll thread notified [tid2], which must be ignored.
                # In parallel, 2 other loads are done (both cache misses):
                # - one for the same oid, which waits for first load to
                #   complete and in particular fill cache, in order to
                #   avoid asking the same data to the storage node
                # - another for a different oid, which doesn't wait, as shown
                #   by the fact that it returns an old record (i.e. before any
                #   invalidation packet is processed)
                loaded = client.load(x._p_oid)
            self.assertEqual((tid2, None), loaded[1:])
            self.assertEqual(loaded, load_same.join())
            self.assertEqual((tid1, r._p_serial), load_other.join()[1:])
            # To summary:
            # - 3 concurrent loads starting with cache misses
            # - 2 loads from storage
            # - 1 load ending with a cache hit
        self.assertEqual(nonlocal_, [2, 8, 2])

    @with_cluster(serialized=False)
    def testExternalInvalidation3(self, cluster):
        # same as testInternalInvalidation2 but with external invalidations
        t, c = cluster.getTransaction()
        x = c.root()[''] = PCounter()
        t.commit()
        def sync(orig):
            orig()
            ll_sync()
        def raceBeforeInvalidateZODB(orig, *args):
            ll_inv()
            orig(*args)
        def raceAfterInvalidateZODB(orig, *args):
            orig(*args)
            ll_inv()
        l1 = threading.Lock(); l1.acquire()
        l2 = threading.Lock(); l2.acquire()
        class CacheLock(object):
            def __init__(self):
                self._lock = client._cache_lock
            def __enter__(self):
                self._lock.acquire()
            def __exit__(self, t, v, tb):
                self._lock.release()
                l1.release()
                l2.acquire()
        def _loadFromStorage(orig, *args):
            l2.release()
            return orig(*args)
        client = cluster.client
        with cluster.newClient(1) as db:
            t2, c2 = cluster.getTransaction(db)
            x2 = c2.root()['']
            x2.value = 1
            with Patch(client._db, invalidate=raceBeforeInvalidateZODB), \
                    LockLock() as ll_inv:
                with Patch(client, sync=sync), LockLock() as ll_sync:
                    begin1 = self.newThread(t.begin)
                    ll_sync()
                    t2.commit()
                    ll_inv()
                begin1.join()
            self.assertEqual(x.value, 0)
            x._p_deactivate()
            # On ZODB≥5, the following check would fail if
            # invalidateObjects updated app.last_tid earlier.
            self.assertEqual(x.value, 0)
            t.begin()
            self.assertEqual(x.value, 1)
            x2.value = 2
            with Patch(client, _cache_lock=CacheLock()), \
                 Patch(client._db, invalidate=raceAfterInvalidateZODB), \
                 LockLock() as ll_inv:
                with Patch(client, sync=sync), LockLock() as ll_sync:
                    begin1 = self.newThread(t.begin)
                    ll_sync()
                    t2.commit()
                    ll_inv()
                begin1.join()
            with Patch(client, _loadFromStorage=_loadFromStorage):
                # On ZODB<5, the following check would fail if
                # invalidateObjects updated app.last_tid later.
                self.assertEqual(x.value, 2)

    @with_cluster(storage_count=2, partitions=2)
    def testReadVerifyingStorage(self, cluster):
        s1, s2 = cluster.sortStorageList()
        t1, c1 = cluster.getTransaction()
        c1.root()['x'] = x = PCounter()
        t1.commit()
        # We need a second client for external invalidations.
        with cluster.newClient(1) as db:
            t2, c2 = cluster.getTransaction(db)
            r = c2.root()
            r._p_changed = 1
            self.readCurrent(r['x'])
            # Force the new tid to be even, like the modified oid and unlike
            # the oid on which we used readCurrent. Thus we check that the node
            # containing only the partition 1 is also involved in tpc_finish.
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
            # In particular, check oid 1 is listed in the last transaction.
            self.assertEqual([[0], [0, 1], [0, 1]],
                [list(map(u64, t.oid_list)) for t in c1.db().storage.iterator()])

            # Another test, this time to check we also vote to storage nodes
            # that are only involved in checking current serial.
            t1.begin()
            s2c2, = s2.getConnectionList(db.storage.app)
            def t1_vote(txn):
                # Check that the storage hasn't answered to the store,
                # which means that a lock is still taken for r['x'] by t2.
                self.tic()
                txn = getTransactionMetaData(txn, c1)
                txn_context = cluster.client._txn_container.get(txn)
                empty = txn_context.queue.empty()
                ll()
                self.assertTrue(empty)
            def t2_vote(_):
                s2c2.close()
                commit1.start()
                ll()
            TransactionalResource(t1, 0, tpc_vote=t1_vote)
            x.value += 1
            TransactionalResource(t2, 1, tpc_vote=t2_vote)
            r._p_changed = 1
            self.readCurrent(r['x'])
            with cluster.moduloTID(0), LockLock() as ll:
                commit1 = self.newPausedThread(t1.commit)
                t2.commit()
            commit1.join()

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
            self.assertRaises(NEOStorageError, client.tpc_finish, txn)

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
            self.assertFalse(cluster.client.new_oids)
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
                self.assertRaises(NEOStorageError, t.commit)
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
                with storage.dm.lock:
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
                with storage.dm.lock:
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
            with Patch(PartitionTable, make=make), \
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
            for x in range(4):
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
                with s0.ignoreDmLock() as dm:
                    self.assertFalse(dm.getTruncateTID())
                with s1.ignoreDmLock() as dm:
                    self.assertEqual(dm.getTruncateTID(), truncate_tid)
                self.tic()
                self.assertEqual(calls, [1, 1])
                self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
            s1.resetNode()
            with Patch(s1.dm, truncate=dieFirst(1)):
                s1.start()
                with s0.ignoreDmLock() as dm:
                    self.assertFalse(dm.getLastIDs()[0])
                with s1.ignoreDmLock() as dm:
                    self.assertEqual(dm.getLastIDs()[0], r._p_serial)
                self.tic()
                self.assertEqual(calls, [1, 2])
                self.assertEqual(getClusterState(), ClusterStates.RUNNING)
            t.begin()
            self.assertEqual(r, dict.fromkeys(range(3)))
            self.assertEqual(r._p_serial, truncate_tid)
            self.assertEqual(1, u64(c._storage.new_oid()))
            for s in cluster.storage_list:
                with s.dm.lock:
                    self.assertEqual(s.dm.getLastIDs()[0], truncate_tid)
        # Warn user about noop truncation.
        self.assertRaises(SystemExit, cluster.neoctl.truncate, truncate_tid)

    def testConnectionAbort(self):
        with self.getLoopbackConnection() as client:
            poll = client.em.poll
            while client.connecting:
                poll(1)
            server, = (c for c in six.itervalues(client.em.connection_dict)
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
            cluster.emptyCache(c)
            if not hasattr(sys, 'getrefcount'): # PyPy
                # See persistent commit ff64867cca3179b1a6379c93b6ef90db565da36c
                import gc; gc.collect()
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
        cluster.importZODB()(3)
        bad = []
        ok = []
        def data_args(value):
            return makeChecksum(value), ZERO_OID, value, 0, None
        node_list = []
        for i, s in enumerate(cluster.storage_list):
            with s.dm.lock:
                node_list.append(s.uuid)
                if i:
                    s.dm.holdData(*data_args(b'boo'))
                ok.append(s.getDataLockInfo())
                for i in range(3 - i):
                    s.dm.storeData(*data_args(b'!' * i))
                bad.append(s.getDataLockInfo())
                s.dm.commit()
        def check(dry_run, expected):
            cluster.neoctl.repair(node_list, bool(dry_run))
            for e, s in zip(expected, cluster.storage_list):
                while 1:
                    self.tic()
                    if s.dm._background_worker._orphan is None:
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
        def answerStoreObject(orig, conn, conflict, oid, serial):
            if not conflict:
                p.revert()
                ll()
            orig(conn, conflict, oid, serial)
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
        storage.store(oid, None, b'foo', '', txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)

        txn = transaction.Transaction()
        storage.tpc_begin(txn)
        self.assertRaises(POSException.ConflictError, storage.store,
                          oid, None, b'*' * cluster.cache_size, '', txn)

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
        for x in range(3):
            r[x] = PCounter()
        t.commit()
        for x in range(3):
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
        for x in range(3):
            r[x]._p_deactivate()
            value_list.append(r[x].value)
        self.assertEqual(value_list, list(range(3)))

    @with_cluster(replicas=1, partitions=3, storage_count=3)
    def testMasterArbitratingVote(self, cluster):
        r"""
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
                cluster.client.nm.getByUUID(s2.uuid).getConnection().close()
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

    @with_cluster(partitions=2, storage_count=2)
    def testMasterArbitratingVoteAfterFailedVoteTransaction(self, cluster):
        """
        Check that the master node arbitrates the vote when a failure happens
        at the end of the first phase (VoteTransaction).
        """
        t, c = cluster.getTransaction()
        tid = cluster.client.last_tid
        r = c.root()
        r[0] = ''
        delayed = []
        with cluster.moduloTID(1), ConnectionFilter() as f:
            @f.delayAskVoteTransaction
            def _(conn):
                delayed.append(None)
                s, = (s for s in cluster.storage_list if conn.uuid == s.uuid)
                conn, = s.getConnectionList(cluster.client)
                conn.em.wakeup(conn.close)
                return False
            self.assertRaises(NEOStorageError, t.commit)
        self.tic()
        self.assertEqual(len(delayed), 1)
        self.assertEqual(tid, cluster.client.last_tid)
        self.assertEqual(cluster.neoctl.getClusterState(),
                         ClusterStates.RUNNING)

    @with_cluster(storage_count=2, replicas=1)
    def testPartitionNotFullyWriteLocked(self, cluster):
        """
        Make sure all oids are write-locked at least once, which is not
        guaranteed by just the storage/master nodes when a readable cell
        becomes OUT_OF_DATE during a commit. This scenario is special in that
        the other readable cell was only writable at the beginning of the
        transaction and the replication finished just before the node failure.
        The test uses a conflict to detect lockless writes.
        """
        s0, s1 = cluster.storage_list
        t, c = cluster.getTransaction()
        r = c.root()
        x = r[''] = PCounterWithResolution()
        t.commit()
        s1c, = s1.getConnectionList(cluster.client)
        s0.stop()
        cluster.join((s0,))
        s0.resetNode()
        x.value += 2
        def vote(_):
            f.remove(delay)
            self.tic()
            s1.stop()
            cluster.join((s1,))
        TransactionalResource(t, 0, tpc_vote=vote)
        with ConnectionFilter() as f, cluster.newClient(1) as db:
            t2, c2 = cluster.getTransaction(db)
            c2.root()[''].value += 3
            t2.commit()
            f.delayAnswerStoreObject(lambda conn: conn is s1c)
            delay = f.delayAskFetchTransactions()
            s0.start()
            self.tic()
            self.assertRaisesRegex(NEOStorageError,
                                   '^partition 0 not fully write-locked$',
                                   t.commit)
        cluster.client._cache.clear()
        t.begin()
        x._p_deactivate()
        self.assertEqual(x.value, 3)

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
                and self.getConnectionApp(conn) is s)
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
    def thread_switcher(self, threads, order, expected,
            # XXX: sched() can be recursive while handling conflicts. In some
            #      cases, when the test is ending, it seems to work provided
            #      we ignore a few exceptions.
            allow_recurse=False):
        self.assertGreaterEqual(len(order), len(expected))
        thread_id = ThreadId()
        l = [threading.Lock() for l in range(len(threads)+1)]
        l[0].acquire()
        end = defaultdict(list)
        order = iter(order)
        expected = iter(expected)
        def sched(orig, *args, **kw):
            i = thread_id()
            e = orig.__name__
            logging.info('%s: %s%r', i, e, args)
            try:
                e = u64((args[3] if e == '_handlePacket' else kw)['oid'])
            except KeyError:
                for x in args:
                    if isinstance(x, Packet):
                        e = type(x).__name__
                        break
            try:
                j = next(order)
            except StopIteration:
                end[i].append(e)
                j = None
                try:
                    while 1:
                        try:
                            l.pop().release()
                        except threading.ThreadError:
                            if not allow_recurse:
                                raise
                except IndexError:
                    pass
            else:
                try:
                    self.assertEqual(next(expected), e)
                except StopIteration:
                    end[i].append(e)
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
                        try:
                            threads[j-1].start()
                        except RuntimeError:
                            if not allow_recurse:
                                raise
                            l[j].release()
                    if e != 'AskStoreTransaction':
                        try:
                            l[i].acquire()
                        except IndexError:
                            pass
        def _handlePacket(orig, *args):
            if args[2] is _DeadlockPacket:
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
        for x in 'abc':
            r[x] = PCounterWithResolution()
            t1.commit()
            oids.append(u64(r[x]._p_oid))
        # The test relies on the implementation-defined behavior that ZODB
        # processes oids by order of registration. It's also simpler with
        # oids from a=1 to c=3.
        self.assertEqual(oids, [1, 2, 3])
        t2, c2 = cluster.getTransaction()
        t3, c3 = cluster.getTransaction()
        changes(r, c2.root(), c3.root())
        threads = list(map(self.newPausedThread, (t2.commit, t3.commit)))
        with self.thread_switcher(threads, order, expected_packets) as end:
            t1.commit()
            threads[0].join()
            if except2 is None:
                threads[1].join()
            else:
                self.assertRaises(except2, threads[1].join)
        t3.begin()
        r = c3.root()
        self.assertEqual(expected_values, [r[x].value for x in 'abc'])
        return dict(end)

    def _testConflictDuringDeadlockAvoidance(self, change):
        def changes(r1, r2, r3):
            r1['a'].value = 1
            self.readCurrent(r1['c'])
            r2['b'].value = 2
            r2['c'].value = 3
            r3['b'].value = 4
            change(r3['a'])
        end = self._testComplexDeadlockAvoidanceWithOneStorage(changes,
            (1, 2, 2, 2, 1, 1, 2, 0, 0, 1, 2, 0, 2, 1),
            ('tpc_begin', 'tpc_begin', 'tpc_begin', 2, 1, 2, 3, 2, 1, 3, 3, 1,
            'AskStoreTransaction', 'tpc_abort'),
            [1, 2, 3], POSException.ConflictError)
        self.assertEqual(end, {1: ['AskStoreTransaction']})

    def testFailedConflictOnBigValueDuringDeadlockAvoidance(self):
        @self._testConflictDuringDeadlockAvoidance
        def change(ob):
            ob.value = '*' * ob._p_jar.db().storage._cache.max_size

    def testFailedCheckCurrentDuringDeadlockAvoidance(self):
        self._testConflictDuringDeadlockAvoidance(self.readCurrent)

    @with_cluster(replicas=1, partitions=2)
    def testNotifyReplicated(self, cluster):
        """
        Check replication while several concurrent transactions leads to
        conflict resolutions and deadlock avoidances, and in particular the
        handling of write-locks when the storage node is about to notify the
        master that partitions are replicated.

        About releasing shared write-locks:
        - The test was originally written with only t1 & t3, covering only the
          case of not releasing a write-lock that is owned by a higher TTID.
        - t4 was then added to test the opposite case ('a' on s1 by t3 & t4,
          and t4 finishes first): the write-lock is released but given
          immediately after while checking for replicated partitions.

        At last, t2 was added to trigger a deadlock when t3 is about to vote:
        - the notification from s0 is ignored
        - s1 does not notify because 'a' is still lockless
        """
        s0, s1 = cluster.storage_list
        s1.stop()
        cluster.join((s1,))
        s1.resetNode()
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'abc':
            r[x] = PCounterWithResolution()
            t1.commit()
        t3, c3 = cluster.getTransaction()
        r['a'].value += 1
        t1.commit()
        r['a'].value += 2
        r['c'].value += 3
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['a'].value += 4
        r = c3.root()
        r['b'].value += 5
        r['c'].value += 6
        r['a'].value += 7
        t4, c4 = cluster.getTransaction()
        r = c4.root()
        r['a'].value += 8
        threads = list(map(self.newPausedThread, (t2.commit, t3.commit, t4.commit)))
        deadlocks = [(3, False)] # by t1
        def t3_relock(*args, **kw):
            self.assertPartitionTable(cluster, 'UO|UO')
            f.remove(delay)
            self.tic()
            self.assertPartitionTable(cluster, 'UU|UO')
            yield 0
        def t2_store(*args, **kw):
            self.assertFalse(deadlocks)
            deadlocks.append((1, True))
            yield 3
        def t4_vote(*args, **kw):
            yield 2
            f.remove(delayDeadlock)
            self.assertFalse(deadlocks)
        def delayDeadlock(conn, packet):
            if isinstance(packet, Packets.NotifyDeadlock):
                self.assertEqual(self.getConnectionApp(conn).uuid, s0.uuid)
                oid, delay = deadlocks.pop()
                self.assertEqual(u64(packet._args[1]), oid)
                return delay
        with ConnectionFilter() as f, \
             self.thread_switcher(threads,
                (1, 2, 3, 2, 2, 2, 0, 3, 0, 2, t3_relock, 3,
                 1, t2_store, t4_vote, 2, 2),
                ('tpc_begin', 'tpc_begin', 'tpc_begin', 'tpc_begin',
                 2, 3, 1, 1, 1, 3, 3, 'AskStoreTransaction',
                 1, 1, 'AskStoreTransaction'), allow_recurse=True) as end:
            delay = f.delayAskFetchTransactions()
            f.add(delayDeadlock)
            s1.start()
            self.tic()
            t1.commit()
            for t in threads:
                t.join()
        t4.begin()
        self.assertPartitionTable(cluster, 'UU|UU')
        self.assertEqual([22, 5, 9], [r[x].value for x in 'abc'])
        self.assertEqual(end.pop(3), [1])
        self.assertEqual(sorted(end), [1, 2])
        with s1.dm.lock:
            self.assertFalse(s1.dm.getOrphanList())

    @with_cluster(replicas=1)
    def testNotifyReplicated2(self, cluster):
        """
        See comment in about using 'discard' instead of 'remove'
        in TransactionManager._notifyReplicated of the storage node.
        """
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
        t2, c2 = cluster.getTransaction()
        r = c2.root()
        r['a'].value += 2
        r['b'].value += 4
        thread = self.newPausedThread(t2.commit)
        def t2_b(*args, **kw):
            self.assertPartitionTable(cluster, 'UO')
            f.remove(delay)
            self.tic()
            self.assertPartitionTable(cluster, 'UO')
            yield 0
        def t1_vote(*args, **kw):
            self.tic()
            self.assertPartitionTable(cluster, 'UO')
            yield 1
        with ConnectionFilter() as f, \
             self.thread_switcher((thread,),
                 (1, 0, 1, 1, t2_b, t1_vote, 1, 1),
                 ('tpc_begin', 'tpc_begin', 1, 1, 2, 'AskStoreTransaction',
                  1,  'AskStoreTransaction',
                  )) as end:
            delay = f.delayAskFetchTransactions()
            s1.start()
            self.tic()
            t1.commit()
            thread.join()
        t2.begin()
        self.assertPartitionTable(cluster, 'UU')
        self.assertEqual([3, 4], [r[x].value for x in 'ab'])
        self.assertFalse(end)

    @with_cluster(replicas=1)
    def testLateLocklessWrite(self, cluster):
        s0, s1 = cluster.storage_list
        s1.stop()
        cluster.join((s1,))
        s1.resetNode()
        t1, c1 = cluster.getTransaction()
        ob = c1.root()[''] = PCounterWithResolution()
        t1.commit()
        ob.value += 1
        t2, c2 = cluster.getTransaction()
        ob = c2.root()['']
        ob.value += 2
        t2.commit()
        def resolve(orig, *args):
            f.remove(d)
            return orig(*args)
        with ConnectionFilter() as f, \
             Patch(PCounterWithResolution, _p_resolveConflict=resolve):
            f.delayAskFetchTransactions()
            d = f.delayAnswerStoreObject(lambda conn:
                self.getConnectionApp(conn) is s1)
            s1.start()
            self.tic()
            t1.commit()
        t2.begin()
        self.tic()
        self.assertEqual(ob.value, 3)
        self.assertPartitionTable(cluster, 'UU')

    @with_cluster(start_cluster=0, storage_count=2, replicas=1)
    def testLocklessWriteDuringConflictResolution(self, cluster):
        """
        This test reproduces a scenario during which the storage node didn't
        check for conflicts when notifying the master that a partition is
        replicated. The consequence was that it kept having write locks in a
        weird state, relying on replicas to do the checking. While this may
        work in itself, the code was hazardous and the client can't easily
        discard such "positive" answers, or process them early enough.

        The scenario focuses on transaction t2 (storing oid 1)
        and node s1 (initially replicating the only partition):
        1. t2 stores: conflict on s0, lockless write on s1
        2. t1 stores: locked on s0, lockless write on s1
        3. t2 resolves: waiting for write-lock on s0, packet to s1 delayed
        4. Partition 0 replicated. Multiple lockless writes block notification
           to master.
        5. t1 commits: t2 conflicts on s0, a single lockless write remains
           on s1, and s1 notifies the master that it is UP_TO_DATE
        6. s1 receives the second store from t2: answer delayed
        7. while t2 is resolving the conflict: answer from s1 processed, s0 down
        8. conflict on s1
        9. t2 resolves a last time and votes

        What happened before:
        5. t1 still has the lock
        6. locked ("Transaction storing more than once")
        9. t2 already processed a successful store from s1 and crashes
           (KeyError in Transaction.written)

        Now that the storage nodes discards lockless writes that actually
        conflict:
        5. t1 does not have the lock anymore
        6. conflict
        9. just after vote, t2 aborts because the only storage node that's
           RUNNING from the beginning got lockless writes (note that the client
           currently tracks them as a list of partitions: if it was a list
           of oids, it could finish the transaction because oid 1 ended up
           being stored normally)
        """
        s0, s1 = cluster.storage_list
        cluster.start(storage_list=(s0,))
        t1, c1 = cluster.getTransaction()
        x1 = c1.root()[''] = PCounterWithResolution()
        t1.commit()
        x1.value += 1
        with cluster.newClient(1) as db, ConnectionFilter() as f:
            client = db.storage.app
            delayReplication = f.delayAnswerFetchObjects()
            delayed = []
            delayStore = f.delayAskStoreObject(lambda conn:
                conn.uuid in delayed and
                self.getConnectionApp(conn) is client)
            delayStored = f.delayAnswerStoreObject(lambda conn:
                conn.uuid == client.uuid and
                self.getConnectionApp(conn).uuid in delayed)
            def load(orig, oid, at_tid, before_tid):
                if delayed:
                    p.revert()
                    f.remove(delayStored)
                    s0.stop()
                    cluster.join((s0,))
                    self.tic()
                return orig(oid, at_tid, before_tid)
            s1.start()
            self.tic()
            cluster.neoctl.enableStorageList([s1.uuid])
            cluster.neoctl.tweakPartitionTable()
            self.tic()
            t2, c2 = cluster.getTransaction(db)
            x2 = c2.root()['']
            x2.value += 2
            t1.commit()
            x1.value += 4
            def tic2(*args, **kw):
                yield 0
                self.tic()
            def t2_resolve(*args, **kw):
                delayed.append(s1.uuid)
                f.remove(delayReplication)
                self.tic()
                yield 0
                self.tic()
            commit2 = self.newPausedThread(t2.commit)
            with Patch(client, _loadFromStorage=load) as p, \
                 self.thread_switcher((commit2,),
                    (1, 1, tic2, 1, t2_resolve, 1, 1, 1),
                    ('tpc_begin', 'tpc_begin', 1, 1, 1, 'AskStoreTransaction',
                     1, 'AskStoreTransaction')) as end:
                t1.commit()
                f.remove(delayStore)
                self.assertRaisesRegex(NEOStorageError,
                                       '^partition 0 not fully write-locked$',
                                       commit2.join)
            t2.begin()
            self.assertEqual(x2.value, 5)
        self.assertPartitionTable(cluster, 'OU')
        self.assertEqual(end, {1: [
            'AskVoteTransaction', # no such packet sent in reality
                # (the fact that it appears here is only an artefact
                #  between implementation detail and thread_switcher)
            'tpc_abort']})

    @with_cluster(partitions=2, storage_count=2)
    def testUnstore(self, cluster):
        """
        Show that when resolving a conflict after a lockless write, the storage
        can't easily discard the data of the previous store, as it would make
        internal data inconsistent. This is currently protected by a assertion
        when trying to notifying the master that the replication is finished.
        """
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        for x in 'ab':
            r[x] = PCounterWithResolution()
            t1.commit()
        cluster.neoctl.setNumReplicas(1)
        self.tic()
        s0, s1 = cluster.sortStorageList()
        t1, c1 = cluster.getTransaction()
        r = c1.root()
        r._p_changed = 1
        r['b'].value += 1
        with ConnectionFilter() as f:
            delayReplication = f.delayAnswerFetchObjects()
            cluster.neoctl.tweakPartitionTable()
            self.tic()
            t2, c2 = cluster.getTransaction()
            x = c2.root()['b']
            x.value += 2
            t2.commit()
            delayStore = f.delayAnswerStoreObject()
            delayFinish = f.delayAskFinishTransaction()
            x.value += 3
            commit2 = self.newPausedThread(t2.commit)
            def t2_b(*args, **kw):
                yield 1
                self.tic()
                f.remove(delayReplication)
                f.remove(delayStore)
                self.tic()
            def t1_resolve(*args, **kw):
                yield 0
                self.tic()
                f.remove(delayFinish)
            with self.thread_switcher((commit2,),
                (1, 0, 0, 1, t2_b, 0, t1_resolve),
                ('tpc_begin', 'tpc_begin', 0, 2, 2, 'AskStoreTransaction')
                ) as end:
                t1.commit()
                commit2.join()
        t1.begin()
        self.assertEqual(c1.root()['b'].value, 6)
        self.assertPartitionTable(cluster, 'UU|UU')
        self.assertEqual(end, {0: [2, 2, 'AskStoreTransaction']})
        with s1.dm.lock:
            self.assertFalse(s1.dm.getOrphanList())

    @with_cluster(start_cluster=0, master_count=3)
    def testElection(self, cluster):
        m0, m1, m2 = cluster.master_list
        cluster.start(master_list=(m0,), recovering=True)
        getClusterState = cluster.neoctl.getClusterState
        getPrimary = cluster.neoctl.getPrimary
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
        self.assertRaises(NotReadyException, getPrimary)
        self.tic()
        self.assertEqual(getClusterState(), ClusterStates.RUNNING)
        self.assertEqual(getPrimary(), m2.uuid)
        self.assertTrue(m2.primary)
        # Check for proper update of node ids on first NotifyNodeInformation.
        stop(m2)
        m0.start()
        updates = []
        def update(orig, app, timestamp, node_list):
            updates.append(len(node_list))
            # Make sure m2 is processed first. Its nid changes from M3 to M2
            # and we want to cover that the correct Node object is updated
            # (the one that is found by addr).
            orig(app, timestamp, sorted(node_list, reverse=1))
        with Patch(cluster.storage.nm, update=update):
            with ConnectionFilter() as f:
                f.add(lambda conn, packet:
                    isinstance(packet, Packets.RequestIdentification)
                    and packet._args[0] == NodeTypes.STORAGE)
                self.tic()
                m2.start()
                self.tic()
            self.assertEqual(updates, [])
            self.tic()
            self.assertEqual(updates, [4, 1]) # 3M+S, S
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
                and packet._args[0] == NodeTypes.MASTER)
            cluster.start(recovering=True)
            neoctl = cluster.neoctl
            getClusterState = neoctl.getClusterState
            getStorageList = lambda: neoctl.getNodeList(NodeTypes.STORAGE)
            self.assertEqual(getClusterState(), ClusterStates.RECOVERING)
            self.assertEqual(1, len(getStorageList()))
        with Patch(EventHandler, protocolError=lambda *_: sys.exit()):
            self.tic()
        with self.expectedFailure(): \
        self.assertEqual(neoctl.getClusterState(), ClusterStates.RUNNING)
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
        big_id_list = (b'\x7c' * 8, b'\x7e' * 8), (b'\x7b' * 8, b'\x7d' * 8)
        for i in 0, 1:
            dm = cluster.storage_list[i].dm
            oid, tid = big_id_list[i]
            with dm.lock:
                for j, expected in (
                        (1 - i, (dm.getLastTID(u64(MAX_TID)), dm.getLastIDs())),
                        (i, (u64(tid), (tid, oid)))):
                    oid, tid = big_id_list[j]
                    # Somehow we abuse 'storeTransaction' because we ask it to
                    # write data for unassigned partitions. This is not checked
                    # so for the moment, the test works.
                    dm.storeTransaction(tid, ((oid, None, None),),
                                        ((oid,), b'', b'', b'', 0, tid), False)
                    self.assertEqual(expected,
                        (dm.getLastTID(u64(MAX_TID)), dm.getLastIDs()))

    def testStorageUpgrade(self):
        path = os.path.join(os.path.dirname(__file__),
                            self._testMethodName + '-%s',
                            's%s.sql')
        dump_dict = {}
        def switch(s):
            dm = s.dm
            with dm.lock:
                dm.commit()
                dump_dict[s.uuid] = dm.dump()
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
            storage.store(p64(0), None, b'foo', '', txn)
            storage.tpc_vote(txn)
            storage.tpc_finish(txn)
            self.tic()
            switch(s1)
            switch(s2)
            cluster.stop()
            for i, s in zip(nid_list, cluster.storage_list):
                self.assertMultiLineEqual(s.dm.dump(), dump_dict[i])

    def testProtocolVersionMismatch(self):
        def ConnectorClass(cls, conn, *args):
            connector = cls(*args)
            x = connector.queued[0]
            connector.queued[0] = x[:-1] + six.int2byte(six.byte2int(x[-1:])^1)
            return connector
        with Patch(ClientConnection, ConnectorClass=ConnectorClass), \
             self.getLoopbackConnection() as conn:
            for _ in range(9):
                if conn.isClosed():
                    break
                conn.em.poll(1)
            else:
                self.fail('%r not closed' % conn)

    @with_cluster(partitions=2, storage_count=2)
    def testAnswerInformationLockedDuringRecovery(self, cluster):
        """
        This test shows a possible wrong behavior of nodes about the handling
        of answer packets. Here, after going back to RECOVERING state whereas
        it was waiting for a transaction to be locked, it will anyway process
        the AnswerInformationLocked packet: if the cluster state was not
        checked, it would send a NotifyUnlockInformation packet, but the
        latter would be unexpected for the storage (InitializationHandler),
        causing an extra storage-master disconnection.

        This would be a minor issue. However, the current design of connection
        handlers requires careful coding for every requests: for the case that
        is tested here, it would still work because the transaction manager is
        only reset after recovery and AnswerInformationLocked can't arrive
        later.

        It would be less error-prone if pending requests were marked to discard
        their answers once it makes no sense anymore to process them. Provided
        that handlers can be carefully split, a possible solution is that
        switching handler could first consist in changing the handlers of all
        pending requests to one that ignore answers.
        """
        s0, s1 = cluster.sortStorageList()
        t, c = cluster.getTransaction()
        c.root()._p_changed = 1
        s0m = s0.master_conn
        def reconnect(orig):
            p.revert()
            self.assertFalse(s0m.isClosed())
            f.remove(delay)
            self.tic()
            return orig()
        with Patch(cluster.client, _connectToPrimaryNode=reconnect) as p, \
             s0.filterConnection(cluster.master) as f, cluster.moduloTID(0):
            @f.delayAnswerInformationLocked
            def delay(_):
                s1.em.wakeup(s1.master_conn.close)
            t.commit()
        self.tic()
        self.assertEqual(cluster.neoctl.getClusterState(),
                         ClusterStates.RUNNING)
        self.assertFalse(s0m.isClosed())

    @with_cluster(storage_count=2, replicas=1)
    def testStorageGettingReadyDuringRecovery(self, cluster):
        """
        Similar to testAnswerInformationLockedDuringRecovery in that a late
        non-RECOVERING packet is unexpected when the cluster becomes
        non-operational and results in extra storage-master disconnection.

        Here, the same connection is closed from both sides at the same time:
        M -> S: unexpected AskRecovery in MasterOperationHandler
        S -> M: unexpected NotifyReady in RecoveryManager

        Contrary to the processing of answering packets, unexpected requests
        & notifications are immediately rejected and a less subjects to buggy
        code. No reason to make this test pass without a more beautiful way
        of how packets are processed in general.
        """
        s0, s1 = cluster.storage_list
        s0m = s0.master_conn
        cluster.neoctl.killNode(s1.uuid)
        cluster.join((s1,))
        s1.resetNode()
        t, c = cluster.getTransaction()
        c.root()._p_changed = 1
        t.commit()
        self.assertPartitionTable(cluster, 'UO')
        with ConnectionFilter() as f:
            @f.delayNotifyReady
            def delay(_):
                s0.em.wakeup(s0.master_conn.close)
            @f.delayAskRecovery
            def _(_):
                s0.em.wakeup(lambda: f.discard(delay))
                return False
            s1.start()
            self.assertFalse(s0m.isClosed())
            self.tic()
        self.assertEqual(cluster.neoctl.getClusterState(),
                         ClusterStates.RUNNING)
        self.assertPartitionTable(cluster, 'UU')
        with self.expectedFailure(): \
        self.assertFalse(s0m.isClosed())

    @with_cluster()
    def testUnsetUpsteam(self, cluster):
        self.assertRaises(SystemExit, cluster.neoctl.setClusterState,
                          ClusterStates.STARTING_BACKUP)

    @with_cluster()
    def testNeoctlVsDisconnectingMaster(self, cluster):
        def setClusterState(orig, conn, *args):
            conn.close()
        with Patch(cluster.master.administration_handler,
                   setClusterState=setClusterState):
            self.assertRaises(RuntimeError, cluster.neoctl.setClusterState,
                              ClusterStates.STARTING_BACKUP)

    @with_cluster()
    def testTpcBeginWithInvalidTID(self, cluster):
        storage = cluster.getZODBStorage()
        txn = transaction.Transaction()
        storage.tpc_begin(txn)
        storage.store(ZERO_OID, None, b'foo', '', txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)
        self.assertRaises(NEOStorageError, storage.tpc_begin,
                          transaction.Transaction(), tid)
        new_tid = add64(tid, 1)
        txn = transaction.Transaction()
        storage.tpc_begin(txn, new_tid)
        storage.store(ZERO_OID, tid, b'bar', '', txn)
        storage.tpc_vote(txn)
        self.assertEqual(add64(tid, 1), storage.tpc_finish(txn))

    @with_cluster()
    def testCorruptedData(self, cluster):
        def holdData(orig, *args):
            args = list(args)
            args[2] = b'!' + args[2]
            return orig(*args)
        data = b'foo' * 10
        tid = None
        for compress in False, True:
            with cluster.newClient(ignore_wrong_checksum=True,
                                   compress=compress) as client, \
                 Patch(cluster.storage.dm, holdData=holdData):
                storage = cluster.getZODBStorage(client=client)
                txn = transaction.Transaction()
                storage.tpc_begin(txn)
                storage.store(ZERO_OID, tid, data, '', txn)
                storage.tpc_vote(txn)
                tid = storage.tpc_finish(txn)
                storage._cache.clear()
                self.assertEqual((b'' if compress else b'!' + data, tid),
                                 storage.load(ZERO_OID))
            with self.assertRaises(exception.NEOStorageWrongChecksum) as cm:
                cluster.client.load(ZERO_OID)
            self.assertEqual(tid, cm.exception.args[1])
