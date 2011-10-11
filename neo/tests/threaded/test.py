#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
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

import threading
import transaction
from thread import get_ident
from persistent import Persistent
from neo.storage.transactions import TransactionManager, \
    DelayedError, ConflictError
from neo.lib.connection import MTClientConnection
from neo.lib.protocol import NodeStates, Packets, ZERO_TID
from neo.tests.threaded import NEOCluster, NEOThreadedTest, \
    Patch, ConnectionFilter
from neo.lib.util import makeChecksum
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
            for data in 'foo', '', 'foo':
                checksum = makeChecksum(data)
                oid = storage.new_oid()
                txn = transaction.Transaction()
                storage.tpc_begin(txn)
                r1 = storage.store(oid, None, data, '', txn)
                r2 = storage.tpc_vote(txn)
                data_info[checksum] = 1
                self.assertEqual(data_info, cluster.storage.getDataLockInfo())
                serial = storage.tpc_finish(txn)
                data_info[checksum] = 0
                self.assertEqual(data_info, cluster.storage.getDataLockInfo())
                self.assertEqual((data, serial), storage.load(oid, ''))
                storage._cache.clear()
                self.assertEqual((data, serial), storage.load(oid, ''))
                self.assertEqual((data, serial), storage.load(oid, ''))
        finally:
            cluster.stop()

    def testStorageDataLock(self):
        cluster = NEOCluster()
        try:
            cluster.start()
            storage = cluster.getZODBStorage()
            data_info = {}

            data = 'foo'
            checksum = makeChecksum(data)
            oid = storage.new_oid()
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            r1 = storage.store(oid, None, data, '', txn)
            r2 = storage.tpc_vote(txn)
            tid = storage.tpc_finish(txn)
            data_info[checksum] = 0
            storage.sync()

            txn = [transaction.Transaction() for x in xrange(3)]
            for t in txn:
                storage.tpc_begin(t)
                storage.store(tid and oid or storage.new_oid(),
                              tid, data, '', t)
                tid = None
            for t in txn:
                storage.tpc_vote(t)
            data_info[checksum] = 3
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[1])
            storage.sync()
            data_info[checksum] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            tid1 = storage.tpc_finish(txn[2])
            data_info[checksum] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())

            storage.tpc_abort(txn[0])
            storage.sync()
            data_info[checksum] -= 1
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())
        finally:
            cluster.stop()

    def testDelayedUnlockInformation(self):
        except_list = []
        def delayUnlockInformation(conn, packet):
            return isinstance(packet, Packets.NotifyUnlockInformation)
        def onStoreObject(orig, tm, ttid, serial, oid, *args):
            if oid == resume_oid and delayUnlockInformation in master_storage:
                master_storage.remove(delayUnlockInformation)
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
            master_storage = cluster.master.filterConnection(cluster.storage)
            try:
                resume_oid = None
                master_storage.add(delayUnlockInformation,
                    Patch(TransactionManager, storeObject=onStoreObject))
                t.commit()
                resume_oid = ob._p_oid
                ob._p_changed = 1
                t.commit()
                self.assertFalse(delayUnlockInformation in master_storage)
            finally:
                master_storage()
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

            p = (Patch(TransactionManager, storeObject=onStoreObject),
                 Patch(MTClientConnection, ask=onAsk))
            try:
                t = self.newThread(t1.commit)
                t2.commit()
                t.join()
            finally:
                del p
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
        cluster.start()
        try:
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
        cluster = NEOCluster()
        try:
            cluster.start()
            cluster.db # open DB
            cluster.client.setPoll(0)
            storage, = cluster.client.nm.getStorageList()
            conn = storage.getConnection()
            self.assertFalse(conn.isClosed())
            getCellSortKey = cluster.client.cp.getCellSortKey
            self.assertEqual(getCellSortKey(storage), CELL_CONNECTED)
            cluster.neoctl.dropNode(cluster.storage.uuid)
            self.assertFalse(cluster.client.nm.getStorageList())
            self.assertTrue(conn.isClosed())
            self.assertEqual(getCellSortKey(storage), CELL_GOOD)
            # XXX: the test originally checked that 'unregister' method
            #      was called (even if it's useless in this case),
            #      but we would need an API to do that easily.
            self.assertFalse(cluster.client.dispatcher.registered(conn))
        finally:
            cluster.stop()

    def testRestartWithMissingStorage(self, fast_startup=False):
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
            cluster.start(storage_list=(s1,), fast_startup=fast_startup)
            self.assertEqual(NodeStates.UNKNOWN, cluster.getNodeState(s2))
        finally:
            cluster.stop()

    def testRestartWithMissingStorageFastStartup(self):
        self.testRestartWithMissingStorage(True)

    def testVerificationCommitUnfinishedTransactions(self, fast_startup=False):
        """ Verification step should commit unfinished transactions """
        # translated from neo.tests.functional.testCluster.ClusterTests
        cluster = NEOCluster()
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            c.root()[0] = 'ok'
            t.commit()
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
            cluster.start(fast_startup=fast_startup)
            t, c = cluster.getTransaction()
            # transaction should be verified and commited
            self.assertEqual(c.root()[0], 'ok')
            self.assertEqual(data_info, cluster.storage.getDataLockInfo())
        finally:
            cluster.stop()

    def testVerificationCommitUnfinishedTransactionsFastStartup(self):
        self.testVerificationCommitUnfinishedTransactions(True)

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

    # The following 2 tests fail because the same queue is used for
    # AskTIDs(From) requests and reconnections. The same bug affected
    # history() before df47e5b1df8eabbff1383348b6b8c476bca0c328

    def testStorageReconnectDuringTransactionLog(self):
        cluster = NEOCluster(storage_count=2, partitions=2)
        try:
            cluster.start()
            t, c = cluster.getTransaction()
            while cluster.client.cp.connection_dict:
                cluster.client.cp._dropConnections()
            tid, (t1,) = cluster.client.transactionLog(
                ZERO_TID, c.root()._p_serial, 10)
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
