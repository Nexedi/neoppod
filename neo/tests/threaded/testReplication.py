#
# Copyright (C) 2012-2017  Nexedi SA
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

import random, sys, threading, time
import transaction
from ZODB.POSException import ReadOnlyError, POSKeyError
import unittest
from collections import defaultdict
from functools import wraps
from itertools import product
from neo.lib import logging
from neo.client.exception import NEOStorageError
from neo.master.handlers.backup import BackupHandler
from neo.storage.checker import CHECK_COUNT
from neo.storage.database.manager import DatabaseManager
from neo.storage import replicator
from neo.lib.connector import SocketConnector
from neo.lib.connection import ClientConnection
from neo.lib.protocol import CellStates, ClusterStates, Packets, \
    ZERO_OID, ZERO_TID, MAX_TID, uuid_str
from neo.lib.util import add64, p64, u64
from .. import expectedFailure, Patch, TransactionalResource
from . import ConnectionFilter, NEOCluster, NEOThreadedTest, \
    predictable_random, with_cluster
from .test import PCounter, PCounterWithResolution # XXX


def backup_test(partitions=1, upstream_kw={}, backup_kw={}):
    def decorator(wrapped):
        def wrapper(self):
            with NEOCluster(partitions=partitions, **upstream_kw) as upstream:
                upstream.start()
                with NEOCluster(partitions=partitions, upstream=upstream,
                                **backup_kw) as backup:
                    backup.start()
                    backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                    self.tic()
                    wrapped(self, backup)
        return wraps(wrapped)(wrapper)
    return decorator


class ReplicationTests(NEOThreadedTest):

    def checksumPartition(self, storage, partition, max_tid=MAX_TID):
        dm = storage.dm
        args = partition, None, ZERO_TID, max_tid
        return dm.checkTIDRange(*args), \
            dm.checkSerialRange(min_oid=ZERO_OID, *args)

    def checkPartitionReplicated(self, source, destination, partition, **kw):
        self.assertEqual(self.checksumPartition(source, partition, **kw),
                         self.checksumPartition(destination, partition, **kw))

    def checkBackup(self, cluster, **kw):
        upstream_pt = cluster.upstream.primary_master.pt
        pt = cluster.primary_master.pt
        np = pt.getPartitions()
        self.assertEqual(np, upstream_pt.getPartitions())
        checked = 0
        source_dict = {x.uuid: x for x in cluster.upstream.storage_list}
        for storage in cluster.storage_list:
            self.assertFalse(storage.dm._uncommitted_data)
            self.assertEqual(np, storage.pt.getPartitions())
            for partition in pt.getAssignedPartitionList(storage.uuid):
                cell_list = upstream_pt.getCellList(partition, readable=True)
                source = source_dict[random.choice(cell_list).getUUID()]
                self.checkPartitionReplicated(source, storage, partition, **kw)
                checked += 1
        return checked

    def checkReplicas(self, cluster):
        pt = cluster.primary_master.pt
        storage_dict = {x.uuid: x for x in cluster.storage_list}
        for offset in xrange(pt.getPartitions()):
            checksum_list = [
                self.checksumPartition(storage_dict[x.getUUID()], offset)
                for x in pt.getCellList(offset)]
            self.assertEqual(1, len(set(checksum_list)),
                             (offset, checksum_list))

    def testBackupNormalCase(self):
        np = 7
        nr = 2
        check_dict = dict.fromkeys(xrange(np))
        with NEOCluster(partitions=np, replicas=nr-1, storage_count=3
                        ) as upstream:
            upstream.start()
            importZODB = upstream.importZODB()
            importZODB(3)
            def delaySecondary(conn, packet):
                if isinstance(packet, Packets.Replicate):
                    tid, upstream_name, source_dict = packet.decode()
                    return not upstream_name and all(source_dict.itervalues())
            with NEOCluster(partitions=np, replicas=nr-1, storage_count=5,
                            upstream=upstream) as backup:
                backup.start()
                # Initialize & catch up.
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup))
                # Normal case, following upstream cluster closely.
                importZODB(17)
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup))

                # Check that a backup cluster can be restarted.
                backup.stop()
                backup.start()
                self.assertEqual(backup.neoctl.getClusterState(),
                                 ClusterStates.BACKINGUP)
                importZODB(17)
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup))
                backup.neoctl.checkReplicas(check_dict, ZERO_TID, None)
                self.tic()
                # Stop backing up, nothing truncated.
                backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup))
                self.assertEqual(backup.neoctl.getClusterState(),
                                 ClusterStates.RUNNING)

                # Restart and switch to BACKINGUP mode again.
                backup.stop()
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()

                # Leave BACKINGUP mode when 1 replica is late. The cluster
                # remains in STOPPING_BACKUP state until it catches up.
                with backup.master.filterConnection(*backup.storage_list) as f:
                    f.add(delaySecondary)
                    while not f.filtered_count:
                        importZODB(1)
                    self.tic()
                    backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
                    self.tic()
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup,
                    max_tid=backup.last_tid))

                # Again but leave BACKINGUP mode when a storage node is
                # receiving data from the upstream cluster.
                backup.stop()
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                with ConnectionFilter() as f:
                    f.delayAddObject(lambda conn: conn.getUUID() is None)
                    while not f.filtered_count:
                        importZODB(1)
                    self.tic()
                    backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
                    self.tic()
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup,
                    max_tid=backup.last_tid))

                storage = upstream.getZODBStorage()

                # Check that replication from upstream is resumed even if
                # upstream is idle.
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                x = backup.master.backup_app.primary_partition_dict
                new_oid_storage = x[0]
                with upstream.moduloTID(next(p for p, n in x.iteritems()
                                               if n is not new_oid_storage)), \
                     ConnectionFilter() as f:
                    f.delayAddObject()
                    # Transaction that touches 2 primary cells on 2 different
                    # nodes.
                    txn = transaction.Transaction()
                    tid = storage.load(ZERO_OID)[1]
                    storage.tpc_begin(txn)
                    storage.store(ZERO_OID, tid, '', '', txn)
                    storage.tpc_vote(txn)
                    storage.tpc_finish(txn)
                    self.tic()
                    # Stop when exactly 1 of the 2 cells is synced with
                    # upstream.
                    backup.stop()
                backup.start()
                self.assertEqual(np*nr, self.checkBackup(backup,
                    max_tid=backup.last_tid))

                # Check that replication to secondary cells is resumed even if
                # upstream is idle.
                with backup.master.filterConnection(*backup.storage_list) as f:
                    f.add(delaySecondary)
                    txn = transaction.Transaction()
                    storage.tpc_begin(txn)
                    storage.tpc_finish(txn)
                    self.tic()
                    backup.stop()
                backup.start()
                self.assertEqual(np*nr, self.checkBackup(backup,
                    max_tid=backup.last_tid))


    @predictable_random()
    def testBackupNodeLost(self):
        """Check backup cluster can recover after random connection loss

        - backup master disconnected from upstream master
        - primary storage disconnected from backup master
        - non-primary storage disconnected from backup master
        """
        np = 4
        check_dict = dict.fromkeys(xrange(np))
        from neo.master.backup_app import random
        def fetchObjects(orig, min_tid=None, min_oid=ZERO_OID):
            if min_tid is None:
                counts[0] += 1
                if counts[0] > 1:
                    orig.im_self.app.master_conn.close()
            return orig(min_tid, min_oid)
        def onTransactionCommitted(orig, txn):
            counts[0] += 1
            if counts[0] > 1:
                node_list = orig.im_self.nm.getClientList(only_identified=True)
                node_list.remove(txn.getNode())
                node_list[0].getConnection().close()
            return orig(txn)
        with NEOCluster(partitions=np, replicas=0, storage_count=1) as upstream:
            upstream.start()
            importZODB = upstream.importZODB(random=random)
            # Do not start with an empty DB so that 'primary_dict' below is not
            # empty on the first iteration.
            importZODB(1)
            with NEOCluster(partitions=np, replicas=2, storage_count=4,
                            upstream=upstream) as backup:
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                storage_list = [x.uuid for x in backup.storage_list]
                slave = set(xrange(len(storage_list))).difference
                for event in xrange(10):
                    logging.info("event=%s", event)
                    counts = [0]
                    if event == 5:
                        p = Patch(upstream.master.tm,
                            _on_commit=onTransactionCommitted)
                    else:
                        primary_dict = defaultdict(list)
                        for k, v in sorted(backup.master.backup_app
                                           .primary_partition_dict.iteritems()):
                            primary_dict[storage_list.index(v._uuid)].append(k)
                        if event % 2:
                            storage = slave(primary_dict).pop()
                        else:
                            storage, partition_list = primary_dict.popitem()
                        # Populate until the found storage performs
                        # a second replication partially and aborts.
                        p = Patch(backup.storage_list[storage].replicator,
                                  fetchObjects=fetchObjects)
                    with p:
                        importZODB(lambda x: counts[0] > 1)
                    if event > 5:
                        backup.neoctl.checkReplicas(check_dict, ZERO_TID, None)
                    self.tic()
                    self.assertEqual(np*3, self.checkBackup(backup))

    @backup_test()
    def testBackupUpstreamStorageDead(self, backup):
        upstream = backup.upstream
        with ConnectionFilter() as f:
            f.delayInvalidateObjects()
            upstream.importZODB()(1)
        count = [0]
        def _connect(orig, conn):
            count[0] += 1
            orig(conn)
        with Patch(ClientConnection, _connect=_connect):
            upstream.storage.listening_conn.close()
            self.tic(step=2)
            self.assertEqual(count[0], 0)
            t = SocketConnector.CONNECT_LIMIT = .5
            t += time.time()
            self.tic()
            # 1st attempt failed, 2nd is deferred
            self.assertEqual(count[0], 2)
            self.tic(check_timeout=(backup.storage,))
            # 2nd failed, 3rd deferred
            self.assertEqual(count[0], 4)
            self.assertLessEqual(t, time.time())

    @backup_test()
    def testBackupDelayedUnlockTransaction(self, backup):
        """
        Check that a backup storage node is put on hold by upstream if
        the requested transaction is still locked. Such case happens when
        the backup cluster reacts very quickly to a new transaction.
        """
        upstream = backup.upstream
        t1, c1 = upstream.getTransaction()
        ob = c1.root()[''] = PCounterWithResolution()
        t1.commit()
        ob.value += 2
        t2, c2 = upstream.getTransaction()
        c2.root()[''].value += 3
        self.tic()
        with upstream.master.filterConnection(upstream.storage) as f:
            delay = f.delayNotifyUnlockInformation()
            t1.commit()
            self.tic()
            def storeObject(orig, *args, **kw):
                p.revert()
                f.remove(delay)
                return orig(*args, **kw)
            with Patch(upstream.storage.tm, storeObject=storeObject) as p:
                t2.commit()
        self.tic()
        t1.begin()
        self.assertEqual(5, ob.value)
        self.assertEqual(1, self.checkBackup(backup))

    @with_cluster()
    def testBackupEarlyInvalidation(self, upstream):
        """
        The backup master must ignore notifications before being fully
        initialized.
        """
        with NEOCluster(upstream=upstream) as backup:
                backup.start()
                with ConnectionFilter() as f:
                    f.delayAskPartitionTable(lambda conn:
                        isinstance(conn.getHandler(), BackupHandler))
                    backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                    upstream.importZODB()(1)
                    self.tic()
                self.tic()
                self.assertTrue(backup.master.is_alive())

    @backup_test()
    def testCreationUndone(self, backup):
        """
        Check both IStorage.history and replication when the DB contains a
        deletion record.

        XXX: This test reveals that without --dedup, the replication does not
             preserve the deduplication that is done by the 'undo' code.
        """
        storage = backup.upstream.getZODBStorage()
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
        self.tic()
        self.assertEqual(1, self.checkBackup(backup))

    @backup_test()
    def testBackupTid(self, backup):
        """
        Check that the backup cluster does not claim it has all the data just
        after it came back whereas new transactions were committed during its
        absence.
        """
        importZODB = backup.upstream.importZODB()
        importZODB(1)
        self.tic()
        last_tid = backup.upstream.last_tid
        self.assertEqual(last_tid, backup.backup_tid)
        backup.stop()
        importZODB(1)
        with ConnectionFilter() as f:
            f.delayAskFetchTransactions()
            backup.start()
            self.assertEqual(last_tid, backup.backup_tid)
        self.tic()
        self.assertEqual(1, self.checkBackup(backup))

    @with_cluster(start_cluster=0, partitions=3, replicas=1, storage_count=3)
    def testSafeTweak(self, cluster):
        """
        Check that tweak always tries to keep a minimum of (replicas + 1)
        readable cells, otherwise we have less/no redundancy as long as
        replication has not finished.
        """
        def changePartitionTable(orig, *args):
            orig(*args)
            sys.exit()
        s0, s1, s2 = cluster.storage_list
        cluster.start([s0, s1])
        s2.start()
        self.tic()
        cluster.enableStorageList([s2])
        # 2 UP_TO_DATE cells become FEEDING:
        # they are "normally" (see below) dropped only when the replication
        # is done, so that 1 storage can still die without data loss.
        with Patch(s0.dm, changePartitionTable=changePartitionTable):
            cluster.neoctl.tweakPartitionTable()
            self.tic()
        self.assertEqual(cluster.neoctl.getClusterState(),
                         ClusterStates.RUNNING)
        # 1 of the FEEDING cells was actually discarded immediately when it got
        # out-of-date, so that we don't end up with too many up-to-date cells.
        s0.resetNode()
        s0.start()
        self.tic()
        self.assertPartitionTable(cluster, 'UU.|U.U|.UU', sort_by_nid=True)

    @with_cluster(start_cluster=0, partitions=3, replicas=1, storage_count=3)
    def testReplicationAbortedBySource(self, cluster):
        """
        Check that a feeding node aborts replication when its partition is
        dropped, and that the out-of-date node finishes to replicate from
        another source.
        Here are the different states of partitions over time:
          pt: 0: U|U|U
          pt: 0: UO.|U.O|FOO
          pt: 0: UU.|U.O|FOO
          pt: 0: UU.|U.U|FOO # nodes 1 & 2 replicate from node 0
          pt: 0: UU.|U.U|.OU # here node 0 lost partition 2
                             # and node 1 must switch to node 2
          pt: 0: UU.|U.U|.UU
        """
        def delayAskFetch(conn, packet):
            return isinstance(packet, delayed) and \
                   packet.decode()[0] == offset and \
                   conn in s1.getConnectionList(s0)
        def changePartitionTable(orig, ptid, cell_list):
            if (offset, s0.uuid, CellStates.DISCARDED) in cell_list:
                connection_filter.remove(delayAskFetch)
                # XXX: this is currently not done by
                #      default for performance reason
                orig.im_self.dropPartitions((offset,))
            return orig(ptid, cell_list)
        np = cluster.num_partitions
        s0, s1, s2 = cluster.storage_list
        for delayed in Packets.AskFetchTransactions, Packets.AskFetchObjects:
            if cluster.started:
                cluster.stop(1)
            if 1:
                cluster.start([s0])
                cluster.populate([range(np*2)] * np)
                s1.start()
                s2.start()
                self.tic()
                cluster.neoctl.enableStorageList([s1.uuid, s2.uuid])
                cluster.neoctl.tweakPartitionTable()
                offset, = [offset for offset, row in enumerate(
                                      cluster.master.pt.partition_list)
                                  for cell in row if cell.isFeeding()]
                with ConnectionFilter() as connection_filter:
                    connection_filter.add(delayAskFetch,
                        Patch(s0.dm, changePartitionTable=changePartitionTable))
                    self.tic()
                    self.assertEqual(1, connection_filter.filtered_count)
                self.tic()
                self.checkPartitionReplicated(s1, s2, offset)

    @with_cluster(start_cluster=0, partitions=2, storage_count=2)
    def testClientReadingDuringTweak(self, cluster):
        def sync(orig):
            m2c.remove(delay)
            orig()
        s0, s1 = cluster.storage_list
        if 1:
            cluster.start([s0])
            storage = cluster.getZODBStorage()
            oid = p64(1)
            txn = transaction.Transaction()
            storage.tpc_begin(txn)
            storage.store(oid, None, 'foo', '', txn)
            storage.tpc_finish(txn)
            storage._cache.clear()
            s1.start()
            self.tic()
            cluster.neoctl.enableStorageList([s1.uuid])
            cluster.neoctl.tweakPartitionTable()
            with cluster.master.filterConnection(cluster.client) as m2c:
                delay = m2c.delayNotifyPartitionChanges()
                self.tic()
                with Patch(cluster.client, sync=sync):
                    self.assertEqual('foo', storage.load(oid)[0])
                self.assertNotIn(delay, m2c)

    @with_cluster(start_cluster=False, storage_count=3, partitions=3)
    def testAbortingReplication(self, cluster):
        s1, s2, s3 = cluster.storage_list
        cluster.start((s1, s2))
        t, c = cluster.getTransaction()
        r = c.root()
        for x in 'ab':
            r[x] = PCounter()
        t.commit()
        cluster.stop(replicas=1)
        cluster.start((s1, s2))
        with ConnectionFilter() as f:
            f.delayAddObject()
            cluster.neoctl.tweakPartitionTable()
            s3.start()
            self.tic()
            cluster.neoctl.enableStorageList((s3.uuid,))
            cluster.neoctl.tweakPartitionTable()
            self.tic()
        self.tic()
        for s in cluster.storage_list:
            self.assertTrue(s.is_alive())
        self.checkReplicas(cluster)

    def testTopology(self):
        """
        In addition to MasterPartitionTableTests.test_19_topology, this checks
        correct propagation of the paths from storage nodes to tweak().
        """
        with Patch(DatabaseManager, getTopologyPath=lambda *_: next(topology)):
            for topology, expected in (
                    (iter("0" * 9),
                        'UU.......|..UU.....|....UU...|'
                        '......UU.|U.......U|.UU......|'
                        '...UU....|.....UU..|.......UU'),
                    (product("012", "012"),
                        'U..U.....|.U....U..|..U.U....|'
                        '.....U.U.|U.......U|.U.U.....|'
                        '..U...U..|....U..U.|.....U..U'),
                ):
                with NEOCluster(replicas=1, partitions=9,
                                storage_count=9) as cluster:
                    for i, s in enumerate(cluster.storage_list, 1):
                        s.uuid = i
                    cluster.start()
                    self.assertPartitionTable(cluster, expected)

    @with_cluster(start_cluster=0, replicas=1, storage_count=4, partitions=2)
    def testTweakVsReplication(self, cluster, done=False):
        S = cluster.storage_list
        cluster.start(S[:1])
        t, c = cluster.getTransaction()
        ob = c.root()[''] = PCounterWithResolution()
        t.commit()
        self.assertEqual(1, u64(ob._p_oid))
        for s in S[1:]:
            s.start()
        self.tic()
        def tweak():
            self.tic()
            self.assertFalse(delay_list)
            self.assertPartitionTable(cluster, 'UU|UO')
            f.delayAskFetchObjects()
            cluster.enableStorageList(S[2:])
            cluster.neoctl.tweakPartitionTable()
            self.tic()
            self.assertPartitionTable(cluster, 'UU..|F.OO')
        with ConnectionFilter() as f, cluster.moduloTID(1), \
             Patch(S[1].replicator,
                   _nextPartitionSortKey=lambda orig, offset: offset):
            delay_list = [1, 0]
            delay = (f.delayNotifyReplicationDone if done else
                     f.delayAnswerFetchObjects)(lambda _: delay_list.pop())
            cluster.enableStorageList((S[1],))
            cluster.neoctl.tweakPartitionTable()
            ob._p_changed = 1
            if done:
                tweak()
                t.commit()
            else:
                t2, c2 = cluster.getTransaction()
                c2.root()['']._p_changed = 1
                l = threading.Lock(); l.acquire()
                TransactionalResource(t2, 0, tpc_vote=lambda _: l.release())
                t2 = self.newPausedThread(t2.commit)
                self.tic()
                @TransactionalResource(t, 0)
                def tpc_vote(_):
                    t2.start()
                    l.acquire()
                    f.remove(delay)
                    tweak()
                t.commit()
                t2.join()
            cluster.neoctl.dropNode(S[2].uuid)
            cluster.neoctl.dropNode(S[3].uuid)
            cluster.neoctl.tweakPartitionTable()
            if done:
                f.remove(delay)
            self.tic()
            self.assertPartitionTable(cluster, 'UU|UO')
        self.tic()
        self.assertPartitionTable(cluster, 'UU|UU')
        self.checkReplicas(cluster)

    def testTweakVsReplicationDone(self):
        self.testTweakVsReplication(True)

    @with_cluster(start_cluster=0, storage_count=2, partitions=2)
    def testCommitVsDiscardedCell(self, cluster):
        s0, s1 = cluster.storage_list
        cluster.start((s0,))
        t, c = cluster.getTransaction()
        ob = c.root()[''] = PCounterWithResolution()
        t.commit()
        self.assertEqual(1, u64(ob._p_oid))
        s1.start()
        self.tic()
        nonlocal_ = []
        with ConnectionFilter() as f:
            delay = f.delayNotifyReplicationDone()
            cluster.enableStorageList((s1,))
            cluster.neoctl.tweakPartitionTable()
            self.tic()
            self.assertPartitionTable(cluster, 'U.|FO')
            t2, c2 = cluster.getTransaction()
            c2.root()[''].value += 3
            l = threading.Lock(); l.acquire()
            @TransactionalResource(t2, 0)
            def tpc_vote(_):
                self.tic()
                l.release()
            t2 = self.newPausedThread(t2.commit)
            @TransactionalResource(t, 0, tpc_finish=lambda _:
                f.remove(nonlocal_.pop(0)))
            def tpc_vote(_):
                t2.start()
                l.acquire()
                nonlocal_.append(f.delayNotifyPartitionChanges())
                f.remove(delay)
                self.tic()
                self.assertPartitionTable(cluster, 'U.|.U', cluster.master)
                nonlocal_.append(cluster.master.pt.getID())
            ob.value += 2
            t.commit()
            t2.join()
        self.tic()
        self.assertPartitionTable(cluster, 'U.|.U')
        self.assertEqual(cluster.master.pt.getID(), nonlocal_.pop())
        t.begin()
        self.assertEqual(ob.value, 5)
        # get the second to last tid (for which ob=2)
        tid2 = s1.dm.getObject(ob._p_oid, None, ob._p_serial)[0]
        # s0 must not have committed anything for partition 1
        with s0.dm.replicated(1):
            self.assertFalse(s0.dm.getObject(ob._p_oid, tid2))

    @with_cluster(start_cluster=0, storage_count=2, partitions=2)
    def testDropPartitions(self, cluster, disable=False):
        s0, s1 = cluster.storage_list
        cluster.start(storage_list=(s0,))
        t, c = cluster.getTransaction()
        c.root()[''] = PCounter()
        t.commit()
        s1.start()
        self.tic()
        self.assertEqual(3, s0.sqlCount('obj'))
        cluster.enableStorageList((s1,))
        cluster.neoctl.tweakPartitionTable()
        self.tic()
        self.assertEqual(1, s1.sqlCount('obj'))
        # Deletion should start as soon as the cell is discarded, as a
        # background task, instead of doing it during initialization.
        count = s0.sqlCount('obj')
        s0.stop()
        cluster.join((s0,))
        s0.resetNode()
        s0.start()
        self.tic()
        self.assertEqual(2, s0.sqlCount('obj'))
        expectedFailure(self.assertEqual)(2, count)

    @with_cluster(replicas=1)
    def testResumingReplication(self, cluster):
        """
        Check from where replication resumes for an OUT_OF_DATE cell that has
        a hole, which is possible because OUT_OF_DATE cells are writable.
        """
        ask = []
        def logReplication(conn, packet):
            if isinstance(packet, (Packets.AskFetchTransactions,
                                   Packets.AskFetchObjects)):
                ask.append(packet.decode()[2:])
        def getTIDList():
            return [t.tid for t in c.db().storage.iterator()]
        s0, s1 = cluster.storage_list
        t, c = cluster.getTransaction()
        r = c.root()
        # s1 is UP_TO_DATE and it has the initial transaction.
        # Let's outdate it: replication will have to resume just after this
        # transaction, regardless of future written transactions.
        # To make sure, we get a hole in the cell, we block replication.
        s1.stop()
        cluster.join((s1,))
        r._p_changed = 1
        t.commit()
        s1.resetNode()
        with Patch(replicator.Replicator, connected=lambda *_: None):
            s1.start()
            self.tic()
            r._p_changed = 1
            t.commit()
            self.tic()
            s1.stop()
            cluster.join((s1,))
        tids = getTIDList()
        s1.resetNode()
        # Initialization done. Now we check that replication is correct
        # and efficient.
        with ConnectionFilter() as f:
            f.add(logReplication)
            s1.start()
            self.tic()
        self.assertEqual([], cluster.getOutdatedCells())
        s0.stop()
        cluster.join((s0,))
        self.assertEqual(tids, getTIDList())
        t0_next = add64(tids[0], 1)
        self.assertEqual(ask, [
            (t0_next, tids[2], tids[2:]),
            (t0_next, tids[2], ZERO_OID, {tids[2]: [ZERO_OID]}),
        ])

    @backup_test(2, backup_kw=dict(replicas=1))
    def testResumingBackupReplication(self, backup):
        upstream = backup.upstream
        t, c = upstream.getTransaction()
        r = c.root()
        r[1] = PCounter()
        t.commit()
        r[2] = ob = PCounter()
        tids = []
        def newTransaction():
            r._p_changed = ob._p_changed = 1
            with upstream.moduloTID(0):
                t.commit()
            self.tic()
            tids.append(r._p_serial)
        def getTIDList(storage):
            return storage.dm.getReplicationTIDList(tids[0], MAX_TID, 9, 0)
        newTransaction()
        self.assertEqual(u64(ob._p_oid), 2)
        getBackupTid = backup.master.pt.getBackupTid

        # Check when an OUT_OF_DATE cell has more data than an UP_TO_DATE one.
        primary = backup.master.backup_app.primary_partition_dict[0]._uuid
        slave, primary = sorted(backup.storage_list,
            key=lambda x: x.uuid == primary)
        with ConnectionFilter() as f:
            @f.delayAnswerFetchTransactions
            def delay(conn, x={None: 0, primary.uuid: 0}):
                return x.pop(conn.getUUID(), 1)
            newTransaction()
            self.assertEqual(getBackupTid(), tids[1])
            primary.stop()
            backup.join((primary,))
            primary.resetNode()
            primary.start()
            self.tic()
            primary, slave = slave, primary
            self.assertEqual(tids, getTIDList(slave))
            self.assertEqual(tids[:1], getTIDList(primary))
            self.assertEqual(getBackupTid(), add64(tids[1], -1))
            self.assertEqual(f.filtered_count, 3)
        self.tic()
        self.assertEqual(4, self.checkBackup(backup))
        self.assertEqual(getBackupTid(min), tids[1])

        # Check that replication resumes from the maximum possible tid
        # (for UP_TO_DATE cells of a backup cluster). More precisely:
        # - cells are handled independently (done here by blocking replication
        #   of partition 1 to keep the backup TID low)
        # - trans and obj are also handled independently (with FETCH_COUNT=1,
        #   we interrupt replication of obj in the middle of a transaction)
        slave.stop()
        backup.join((slave,))
        ask = []
        def delayReplicate(conn, packet):
            if isinstance(packet, Packets.AskFetchObjects):
                if len(ask) == 6:
                    return True
            elif not isinstance(packet, Packets.AskFetchTransactions):
                return
            ask.append(packet.decode())
        conn, = upstream.master.getConnectionList(backup.master)
        with ConnectionFilter() as f, Patch(replicator.Replicator,
                _nextPartitionSortKey=lambda orig, self, offset: offset):
            f.add(delayReplicate)
            delayReconnect = f.delayAskLastTransaction()
            conn.close()
            newTransaction()
            newTransaction()
            newTransaction()
            self.assertFalse(ask)
            self.assertEqual(f.filtered_count, 1)
            with Patch(replicator, FETCH_COUNT=1):
                f.remove(delayReconnect)
                self.tic()
            t1_next = add64(tids[1], 1)
            self.assertEqual(ask, [
                # trans
                (0, 1, t1_next, tids[4], []),
                (0, 1, tids[3], tids[4], []),
                (0, 1, tids[4], tids[4], []),
                # obj
                (0, 1, t1_next, tids[4], ZERO_OID, {}),
                (0, 1, tids[2], tids[4], p64(2), {}),
                (0, 1, tids[3], tids[4], ZERO_OID, {}),
            ])
            del ask[:]
            max_ask = None
            backup.stop()
            newTransaction()
            backup.start((primary,))
            n = replicator.FETCH_COUNT
            t4_next = add64(tids[4], 1)
            self.assertEqual(ask, [
                (0, n, t4_next, tids[5], []),
                (0, n, tids[3], tids[5], ZERO_OID, {tids[3]: [ZERO_OID]}),
                (1, n, t1_next, tids[5], []),
                (1, n, t1_next, tids[5], ZERO_OID, {}),
            ])
        self.tic()
        self.assertEqual(2, self.checkBackup(backup))

    @with_cluster(start_cluster=0, replicas=1)
    def testStoppingDuringReplication(self, cluster):
        """
        When a node is stopped while it is replicating obj from ZERO_TID,
        check that replication does not resume from the beginning.
        """
        s1, s2 = cluster.storage_list
        cluster.start(storage_list=(s1,))
        t, c = cluster.getTransaction()
        r = c.root()
        r._p_changed = 1
        t.commit()
        ltid = r._p_serial
        trans = []
        obj = []
        with ConnectionFilter() as f, Patch(replicator, FETCH_COUNT=1):
            @f.add
            def delayReplicate(conn, packet):
                if isinstance(packet, Packets.AskFetchTransactions):
                    trans.append(packet.decode()[2])
                elif isinstance(packet, Packets.AskFetchObjects):
                    if obj:
                        return True
                    obj.append(packet.decode()[2])
            s2.start()
            self.tic()
            cluster.neoctl.enableStorageList([s2.uuid])
            cluster.neoctl.tweakPartitionTable()
            self.tic()
            self.assertEqual(trans, [ZERO_TID, ltid])
            self.assertEqual(obj, [ZERO_TID])
            self.assertPartitionTable(cluster, 'UO')
            s2.stop()
            cluster.join((s2,))
            s2.resetNode()
            del trans[:], obj[:]
            s2.start()
            self.tic()
            self.assertEqual(trans, [ltid])
            self.assertEqual(obj, [ltid])
            self.assertPartitionTable(cluster, 'UU')

    @with_cluster(start_cluster=0, replicas=1, partitions=2)
    def testReplicationBlockedByUnfinished1(self, cluster,
                                            delay_replication=False):
        s0, s1 = cluster.storage_list
        cluster.start(storage_list=(s0,))
        storage = cluster.getZODBStorage()
        oid = storage.new_oid()
        with ConnectionFilter() as f, cluster.moduloTID(1 - u64(oid) % 2):
            if delay_replication:
                delay_replication = f.delayAnswerFetchObjects()
            tid = None
            expected = 'U|U'
            for n in xrange(3):
                # On second iteration, the transaction will block replication
                # until tpc_finish.
                # We do a last iteration as a quick check that the cluster
                # remains functional after such a scenario.
                txn = transaction.Transaction()
                storage.tpc_begin(txn)
                tid = storage.store(oid, tid, str(n), '', txn)
                if n == 1:
                    # Start the outdated storage.
                    s1.start()
                    self.tic()
                    cluster.enableStorageList((s1,))
                    cluster.neoctl.tweakPartitionTable()
                    expected = 'UO|UO'
                self.tic()
                self.assertPartitionTable(cluster, expected)
                storage.tpc_vote(txn)
                self.assertPartitionTable(cluster, expected)
                tid = storage.tpc_finish(txn)
                if n == 1:
                    if delay_replication:
                        self.tic()
                        self.assertPartitionTable(cluster, expected)
                        f.remove(delay_replication)
                        delay_replication = None
                    self.tic() # replication resumes and ends
                    expected = 'UU|UU'
                self.assertPartitionTable(cluster, expected)
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)
        self.checkPartitionReplicated(s0, s1, 0)

    def testReplicationBlockedByUnfinished2(self):
        self.testReplicationBlockedByUnfinished1(True)

    @with_cluster(partitions=5, replicas=2, storage_count=3)
    def testCheckReplicas(self, cluster):
        from neo.storage import checker
        def corrupt(offset):
            s0, s1, s2 = (storage_dict[cell.getUUID()]
                for cell in cluster.master.pt.getCellList(offset, True))
            logging.info('corrupt partition %u of %s',
                         offset, uuid_str(s1.uuid))
            s1.dm.deleteObject(p64(np+offset), p64(corrupt_tid))
            return s0.uuid
        def check(expected_state, expected_count):
            self.assertEqual(expected_count, len([None
              for row in cluster.neoctl.getPartitionRowList()[1]
              for cell in row[1]
              if cell[1] == CellStates.CORRUPTED]))
            self.assertEqual(expected_state, cluster.neoctl.getClusterState())
        np = cluster.num_partitions
        tid_count = np * 3
        corrupt_tid = tid_count // 2
        check_dict = dict.fromkeys(xrange(np))
        with Patch(checker, CHECK_COUNT=2):
            cluster.populate([range(np*2)] * tid_count)
            storage_dict = {x.uuid: x for x in cluster.storage_list}
            cluster.neoctl.checkReplicas(check_dict, ZERO_TID, None)
            self.tic()
            check(ClusterStates.RUNNING, 0)
            source = corrupt(0)
            cluster.neoctl.checkReplicas(check_dict, p64(corrupt_tid+1), None)
            self.tic()
            check(ClusterStates.RUNNING, 0)
            cluster.neoctl.checkReplicas({0: source}, ZERO_TID, None)
            self.tic()
            check(ClusterStates.RUNNING, 1)
            corrupt(1)
            cluster.neoctl.checkReplicas(check_dict, p64(corrupt_tid+1), None)
            self.tic()
            check(ClusterStates.RUNNING, 1)
            cluster.neoctl.checkReplicas(check_dict, ZERO_TID, None)
            self.tic()
            check(ClusterStates.RECOVERING, 4)

    @backup_test()
    def testBackupReadOnlyAccess(self, backup):
        """Check backup cluster can be used in read-only mode by ZODB clients"""
        B = backup
        U = B.upstream
        Z = U.getZODBStorage()
        #Zb = B.getZODBStorage()    # XXX see below about invalidations

        oid_list = []
        tid_list = []

        # S -> Sb link stops working during [cutoff, recover) test iterations
        cutoff  = 4
        recover = 7
        def delayReplication(conn, packet):
            return isinstance(packet, Packets.AnswerFetchTransactions)

        with ConnectionFilter() as f:
            for i in xrange(10):
                if i == cutoff:
                    f.add(delayReplication)
                if i == recover:
                    # .remove() removes the filter and retransmits all packets
                    # that were queued once first filtered packed was detected
                    # on a connection.
                    f.remove(delayReplication)

                # commit new data to U
                txn = transaction.Transaction()
                txn.note(u'test transaction %s' % i)
                Z.tpc_begin(txn)
                oid = Z.new_oid()
                Z.store(oid, None, '%s-%i' % (oid, i), '', txn)
                Z.tpc_vote(txn)
                tid = Z.tpc_finish(txn)
                oid_list.append(oid)
                tid_list.append(tid)

                # make sure data propagated to B  (depending on cutoff)
                self.tic()
                if cutoff <= i < recover:
                    self.assertLess(B.backup_tid, U.last_tid)
                else:
                    self.assertEqual(B.backup_tid, U.last_tid)
                self.assertEqual(B.last_tid,   U.last_tid)
                self.assertEqual(1, self.checkBackup(B, max_tid=B.backup_tid))

                # read data from B and verify it is what it should be
                # XXX we open new ZODB storage every time because invalidations
                # are not yet implemented in read-only mode.
                Zb = B.getZODBStorage()
                for j, oid in enumerate(oid_list):
                    if cutoff <= i < recover and j >= cutoff:
                        self.assertRaises(POSKeyError, Zb.load, oid, '')
                    else:
                        data, serial = Zb.load(oid, '')
                        self.assertEqual(data, '%s-%s' % (oid, j))
                        self.assertEqual(serial, tid_list[j])

                # verify how transaction log & friends behave under potentially
                # not-yet-fully fetched backup state (transactions committed at
                # [cutoff, recover) should not be there; otherwise transactions
                # should be fully there)
                Zb = B.getZODBStorage()
                Btxn_list = list(Zb.iterator())
                self.assertEqual(len(Btxn_list), cutoff if cutoff <= i < recover
                                                 else i+1)
                for j, txn in enumerate(Btxn_list):
                    self.assertEqual(txn.tid, tid_list[j])
                    self.assertEqual(txn.description, 'test transaction %i' % j)
                    obj, = txn
                    self.assertEqual(obj.oid, oid_list[j])
                    self.assertEqual(obj.data, '%s-%s' % (obj.oid, j))

                # TODO test askObjectHistory once it is fixed

                # try to commit something to backup storage and make sure it is
                # really read-only
                Zb._cache.max_size = 0     # make store() do work in sync way
                txn = transaction.Transaction()
                self.assertRaises(ReadOnlyError, Zb.tpc_begin, txn)
                self.assertRaises(ReadOnlyError, Zb.new_oid)
                self.assertRaises(ReadOnlyError, Zb.store, oid_list[-1],
                                            tid_list[-1], 'somedata', '', txn)
                # tpc_vote first checks whether there were store replies -
                # thus not ReadOnlyError
                self.assertRaises(NEOStorageError, Zb.tpc_vote, txn)

                # close storage because client app is otherwise shared in
                # threaded tests and we need to refresh last_tid on next run
                # (XXX see above about invalidations not working)
                Zb.close()


if __name__ == "__main__":
    unittest.main()
