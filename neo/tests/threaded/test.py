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

from persistent import Persistent
from neo.lib.protocol import NodeStates, ZERO_TID
from neo.tests.threaded import NEOCluster, NEOThreadedTest
from neo.client.pool import CELL_CONNECTED, CELL_GOOD

class PCounter(Persistent):
    value = 0

class PCounterWithResolution(PCounter):
    def _p_resolveConflict(self, old, saved, new):
        new['value'] += saved['value'] - old.get('value', 0)
        return new

class Test(NEOThreadedTest):

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
        finally:
            cluster.stop()
        cluster.reset()
        # XXX: (obj|trans) become t(obj|trans)
        cluster.storage.switchTables()
        try:
            cluster.start(fast_startup=fast_startup)
            t, c = cluster.getTransaction()
            # transaction should be verified and commited
            self.assertEqual(c.root()[0], 'ok')
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
