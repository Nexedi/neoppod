# -*- coding: utf-8 -*-
#
# Copyright (C) 2012-2019  Nexedi SA
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
from ZODB.POSException import (
    POSKeyError, ReadOnlyError, StorageTransactionError)
from collections import defaultdict
from functools import wraps
from itertools import product
from neo.lib import logging
from neo.master.handlers.backup import BackupHandler
from neo.storage.checker import CHECK_COUNT
from neo.storage.database.manager import DatabaseManager
from neo.storage import replicator
from neo.lib.connector import SocketConnector
from neo.lib.connection import ClientConnection
from neo.lib.protocol import CellStates, ClusterStates, NodeStates, Packets, \
    ZERO_OID, ZERO_TID, MAX_TID, uuid_str
from neo.lib.util import add64, p64, u64, parseMasterList
from .. import Patch, TransactionalResource
from . import ConnectionFilter, LockLock, NEOCluster, NEOThreadedTest, \
    predictable_random, with_cluster
from .test import PCounter, PCounterWithResolution # XXX


def backup_test(partitions=1, upstream_kw={}, backup_kw={}):
    def decorator(wrapped):
        def wrapper(self):
            with NEOCluster(partitions=partitions, backup_count=1,
                            **upstream_kw) as upstream:
                upstream.start()
                name, = upstream.backup_list
                with NEOCluster(partitions=partitions, upstream=upstream,
                                name=name, **backup_kw) as backup:
                    self.assertMonitor(upstream, 2, 'RECOVERING',
                                       (backup, None))
                    backup.start()
                    backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                    self.tic()
                    wrapped(self, backup)
        return wraps(wrapped)(wrapper)
    return decorator


class ReplicationTests(NEOThreadedTest):

    def checkBackup(self, cluster, **kw):
        upstream_pt = cluster.upstream.primary_master.pt
        pt = cluster.primary_master.pt
        np = pt.getPartitions()
        self.assertEqual(np, upstream_pt.getPartitions())
        checked = 0
        source_dict = {x.uuid: x for x in cluster.upstream.storage_list}
        for storage in cluster.storage_list:
            with storage.dm.lock:
                self.assertFalse(storage.dm._uncommitted_data)
                if storage.pt is None:
                    storage.loadPartitionTable()
            self.assertEqual(np, storage.pt.getPartitions())
            for partition in pt.getReadableOffsetList(storage.uuid):
                cell_list = upstream_pt.getCellList(partition, readable=True)
                source = source_dict[random.choice(cell_list).getUUID()]
                self.checkPartitionReplicated(source, storage, partition, **kw)
                checked += 1
        return checked

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
                    tid, upstream_name, source_dict = packet._args
                    return not upstream_name and all(source_dict.itervalues())
            with NEOCluster(partitions=np, replicas=nr-1, storage_count=5,
                            upstream=upstream, backup_initially=True) as backup:
                state_list = []
                def changeClusterState(orig, state):
                    state_list.append(state)
                    orig(state)
                with Patch(backup.master, changeClusterState=changeClusterState):
                    # Initialize & catch up.
                    backup.start()
                    self.tic()
                # Check that backup cluster goes straight to BACKINGUP.
                self.assertEqual(state_list, [
                    ClusterStates.RECOVERING,
                    ClusterStates.VERIFYING,
                    ClusterStates.STARTING_BACKUP,
                    ClusterStates.BACKINGUP])

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
                node_list.remove(txn.node)
                node_list[0].getConnection().close()
            return orig(txn)
        with NEOCluster(partitions=np, replicas=0, storage_count=1) as upstream:
            upstream.start()
            importZODB = upstream.importZODB(random=random)
            # Do not start with an empty DB so that 'primary_dict' below is not
            # empty on the first iteration.
            importZODB(1)

            # --- ASIDE ---
            # Check that master crashes when started with --backup but without
            # upstream (-C,--upstream-cluster and -M,--upstream-masters) info.
            with NEOCluster(partitions=np, replicas=2, storage_count=4,
                            backup_initially=True) as backup:
                exitmsg = []
                def exit(orig, msg):
                    exitmsg.append(msg)
                    orig(msg)
                state_list = []
                def changeClusterState(orig, state):
                    state_list.append(state)
                    orig(state)
                m = backup.master
                with Patch(sys, exit=exit), Patch(
                        m, changeClusterState=changeClusterState):
                    self.assertRaises(AssertionError, backup.start)
                backup.join((m,))
                self.assertEqual(exitmsg, [m.no_upstream_msg])
                self.assertEqual(state_list, [
                    ClusterStates.RECOVERING,
                    ClusterStates.VERIFYING])
                del state_list[:]
                # Now check that restarting the master with upstream info and
                # with --backup makes the cluster go to BACKINGUP.
                m.resetNode(
                    upstream_cluster=upstream.name,
                    upstream_masters=parseMasterList(upstream.master_nodes))
                backup.upstream = upstream
                with Patch(m, changeClusterState=changeClusterState):
                    # Initialize & catch up.
                    m.start()
                    self.tic()
                # Check that backup cluster goes straight to BACKINGUP.
                self.assertEqual(state_list, [
                    ClusterStates.RECOVERING,
                    ClusterStates.VERIFYING,
                    ClusterStates.STARTING_BACKUP,
                    ClusterStates.BACKINGUP])
                # --- END ASIDE ---

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
            f.delayInvalidatePartitions()
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
            warning, problem, msg = upstream.neoctl.getMonitorInformation()
            self.assertEqual(warning, (backup.name,))
            self.assertFalse(problem)
            self.assertTrue(msg.endswith('lag=ε'), msg)
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
        warning, problem, msg = upstream.neoctl.getMonitorInformation()
        self.assertFalse(warning)
        self.assertFalse(problem)
        self.assertTrue(msg.endswith('lag=0.0'), msg)

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

    @with_cluster(master_count=2)
    def testBackupFromUpstreamWithSecondaryMaster(self, upstream):
        """
        Check that the backup master reacts correctly when connecting first
        to a secondary master of the upstream cluster.
        """
        with NEOCluster(upstream=upstream) as backup:
            primary = upstream.primary_master
            m, = (m for m in upstream.master_list if m is not primary)
            backup.master.resetNode(upstream_masters=[m.server])
            backup.start()
            backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
            self.tic()
            self.assertEqual(backup.neoctl.getClusterState(),
                             ClusterStates.BACKINGUP)

    @backup_test()
    def testCreationUndone(self, backup):
        """
        Check both IStorage.history and replication when the DB contains a
        deletion record.
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
        for cluster in backup, backup.upstream:
            self.assertEqual(1, cluster.storage.sqlCount('data'))

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
                   packet._args[0] == offset and \
                   conn in s1.getConnectionList(s0)
        def changePartitionTable(orig, app, ptid, num_replicas, cell_list):
            if (offset, s0.uuid, CellStates.DISCARDED) in cell_list:
                connection_filter.remove(delayAskFetch)
            return orig(app, ptid, num_replicas, cell_list)
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
        cluster.neoctl.setNumReplicas(1)
        self.tic()
        cluster.stop()
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
            for s in S[2:]:
                cluster.neoctl.killNode(s.uuid)
                cluster.neoctl.dropNode(s.uuid)
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
        with s1.dm.lock:
            tid2 = s1.dm.getObject(ob._p_oid, None, ob._p_serial)[0]
        # s0 must not have committed anything for partition 1
        with s0.dm.lock, s0.dm.replicated(1):
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
        cluster.ticAndJoinStorageTasks()
        self.assertEqual(1, s1.sqlCount('obj'))
        self.assertEqual(2, s0.sqlCount('obj'))

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
                ask.append(packet._args[2:])
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
            (t0_next, tids[2], tids[2:], True),
            (t0_next, tids[2], ZERO_OID, {tids[2]: [ZERO_OID]}),
        ])

    @backup_test(2, backup_kw=dict(replicas=1))
    def testResumingBackupReplication(self, backup):
        upstream = backup.upstream
        for monitor in 'RECOVERING', 'VERIFYING', 'RUNNING':
            monitor += '; UP_TO_DATE=2'
            self.assertMonitor(upstream, 2, monitor, (backup, None))
        self.assertMonitor(upstream, 0, monitor,
                           (backup, 'BACKINGUP; UP_TO_DATE=4;'))
        def checkMonitor():
            self.assertMonitor(upstream, 2, monitor,
                (backup, 'BACKINGUP; OUT_OF_DATE=2, UP_TO_DATE=2; DOWN=1;'))
            self.assertNoMonitorInformation(upstream)
            warning, problem, _ = upstream.neoctl.getMonitorInformation()
            self.assertFalse(warning)
            self.assertEqual(problem, (backup.name,))
            warning, problem, _ = backup.neoctl.getMonitorInformation()
            self.assertFalse(warning)
            self.assertEqual(problem, (None,))

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
            with storage.dm.lock:
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
            self.assertNoMonitorInformation(upstream)
            primary.stop()
            backup.join((primary,))
            primary.resetNode()
            checkMonitor()
            primary.start()
            self.tic()
            self.assertMonitor(upstream, 1, monitor,
                (backup, 'BACKINGUP; OUT_OF_DATE=2, UP_TO_DATE=2; ltid='))
            warning, problem, _ = backup.neoctl.getMonitorInformation()
            self.assertEqual(warning, (None,))
            self.assertFalse(problem)
            primary, slave = slave, primary
            self.assertEqual(tids, getTIDList(slave))
            self.assertEqual(tids[:1], getTIDList(primary))
            self.assertEqual(getBackupTid(), add64(tids[1], -1))
            self.assertEqual(f.filtered_count, 3)
        self.tic()
        self.assertEqual(4, self.checkBackup(backup))
        self.assertEqual(getBackupTid(min), tids[1])

        self.assertMonitor(upstream, 1, monitor,
            (backup, 'BACKINGUP; OUT_OF_DATE=1, UP_TO_DATE=3; ltid='))
        self.assertMonitor(upstream, 0, monitor,
                           (backup, 'BACKINGUP; UP_TO_DATE=4;'))

        # Check that replication resumes from the maximum possible tid
        # (for UP_TO_DATE cells of a backup cluster). More precisely:
        # - cells are handled independently (done here by blocking replication
        #   of partition 1 to keep the backup TID low)
        # - trans and obj are also handled independently (with FETCH_COUNT=1,
        #   we interrupt replication of obj in the middle of a transaction)
        slave.stop()
        backup.join((slave,))
        checkMonitor()
        ask = []
        def delayReplicate(conn, packet):
            if isinstance(packet, Packets.AskFetchObjects):
                if len(ask) == 6:
                    return True
            elif not isinstance(packet, Packets.AskFetchTransactions):
                return
            ask.append(packet._args)
        conn, = upstream.master.getConnectionList(backup.master)
        admins = upstream.admin, backup.admin
        with ConnectionFilter() as f, Patch(replicator.Replicator,
                _nextPartitionSortKey=lambda orig, self, offset: offset):
            f.add(delayReplicate)
            delayReconnect = f.delayAskLastTransaction(lambda conn:
                self.getConnectionApp(conn) not in admins)
            # Without the following delay, the upstream admin may be notified
            # that the backup is back in BACKINGUP state before getting the
            # last tid (from the upstream master); note that in such case,
            # we would have 2 consecutive identical notifications.
            delayMonitor = f.delayNotifyMonitorInformation(
                lambda _, x=iter((0,)): next(x, 1))
            conn.close()
            newTransaction()
            self.assertMonitor(upstream, 2, monitor, (backup,
                'STARTING_BACKUP; OUT_OF_DATE=2, UP_TO_DATE=2; DOWN=1'))
            f.remove(delayMonitor)
            newTransaction()
            checkMonitor()
            newTransaction()
            self.assertFalse(ask)
            self.assertEqual(f.filtered_count, 2)
            with Patch(replicator, FETCH_COUNT=1):
                f.remove(delayReconnect)
                self.tic()
            t1_next = add64(tids[1], 1)
            self.assertEqual(ask, [
                # trans
                (0, 1, t1_next, tids[4], [], True),
                (0, 1, tids[3], tids[4], [], False),
                (0, 1, tids[4], tids[4], [], False),
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
                (0, n, t4_next, tids[5], [], True),
                (0, n, tids[3], tids[5], ZERO_OID, {tids[3]: [ZERO_OID]}),
                (1, n, t1_next, tids[5], [], True),
                (1, n, t1_next, tids[5], ZERO_OID, {}),
            ])
        self.tic()
        self.assertEqual(2, self.checkBackup(backup))
        checkMonitor()

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
                    trans.append(packet._args[2])
                elif isinstance(packet, Packets.AskFetchObjects):
                    if obj:
                        return True
                    obj.append(packet._args[2])
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

    @with_cluster(partitions=6, storage_count=5, start_cluster=0)
    def testSplitAndMakeResilientUsingClone(self, cluster):
        """
        Test cloning of storage nodes using --new-nid instead NEO replication.
        """
        s0 = cluster.storage_list[0]
        s12 = cluster.storage_list[1:3]
        s34 = cluster.storage_list[3:]
        cluster.start(storage_list=(s0,))
        cluster.importZODB()(6)
        for s in s12:
            s.start()
            self.tic()
        drop_list = [s0.uuid]
        self.assertRaises(SystemExit, cluster.neoctl.tweakPartitionTable,
                          drop_list)
        cluster.enableStorageList(s12)
        def expected(changed):
            s0 = 1, CellStates.UP_TO_DATE
            s = CellStates.OUT_OF_DATE if changed else CellStates.UP_TO_DATE
            return changed, 3 * ((s0, (2, s)), (s0, (3, s)))
        for dry_run in True, False:
            self.assertEqual(expected(True),
                cluster.neoctl.tweakPartitionTable(drop_list, dry_run))
            self.tic()
        self.assertEqual(expected(False),
            cluster.neoctl.tweakPartitionTable(drop_list))
        for s, d in zip(s12, s34):
            s.stop()
            cluster.join((s,))
            s.resetNode()
            with d.dm.lock:
                d.dm.restore(s.dm.dump())
            d.resetNode(new_nid=True)
            s.start()
            d.start()
            self.tic()
            self.assertEqual(cluster.getNodeState(s), NodeStates.RUNNING)
            self.assertEqual(cluster.getNodeState(d), NodeStates.DOWN)
            cluster.join((d,))
            d.resetNode(new_nid=False)
            d.start()
        self.tic()
        self.checkReplicas(cluster)
        expected = '|'.join(['UU.U.|U.U.U'] * 3)
        self.assertPartitionTable(cluster, expected)
        cluster.neoctl.setNumReplicas(1)
        cluster.neoctl.tweakPartitionTable(drop_list)
        self.tic()
        self.assertPartitionTable(cluster, expected)
        s0.stop()
        cluster.join((s0,))
        cluster.neoctl.dropNode(s0.uuid)
        expected = '|'.join(['U.U.|.U.U'] * 3)
        self.assertPartitionTable(cluster, expected)

    @with_cluster(partitions=3, replicas=1, storage_count=3)
    def testAdminOnerousOperationCondition(self, cluster):
        s = cluster.storage_list[2]
        cluster.neoctl.killNode(s.uuid)
        tweak = cluster.neoctl.tweakPartitionTable
        self.assertRaises(SystemExit, tweak)
        self.assertRaises(SystemExit, tweak, dry_run=True)
        self.assertTrue(tweak((s.uuid,))[0])
        self.tic()
        cluster.neoctl.dropNode(s.uuid)
        s = cluster.storage_list[1]
        self.assertRaises(SystemExit, cluster.neoctl.dropNode, s.uuid)

    @backup_test()
    def testUpstreamStartWithDownstreamRunning(self, backup):
        upstream = backup.upstream
        upstream.getTransaction() # "initial database creation" commit
        self.tic()
        backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
        self.tic()
        self.assertEqual(backup.neoctl.getClusterState(),
                         ClusterStates.RUNNING)
        admin = upstream.admin
        admin.stop()
        storage = upstream.storage
        storage.stop()
        upstream.join((admin, storage))
        admin.resetNode()
        admin.start()
        self.tic()
        upstream.resetNeoCTL()
        self.assertEqual(upstream.neoctl.getClusterState(),
                         ClusterStates.RECOVERING)

    @with_cluster(partitions=5, replicas=2, storage_count=3)
    def testCheckReplicas(self, cluster, corrupted_state=False):
        from neo.storage import checker
        def corrupt(offset):
            s0, s1, s2 = (storage_dict[cell.getUUID()]
                for cell in cluster.master.pt.getCellList(offset, True))
            logging.info('corrupt partition %u of %s',
                         offset, uuid_str(s1.uuid))
            with s1.dm.lock:
                s1.dm.deleteObject(p64(np+offset), p64(corrupt_tid))
            return s0.uuid
        def check(expected_state, expected_count):
            self.assertEqual(expected_count if corrupted_state else 0, sum(
                cell[1] == CellStates.CORRUPTED
                for row in cluster.neoctl.getPartitionRowList()[2]
                for cell in row))
            self.assertEqual(cluster.neoctl.getClusterState(),
                expected_state if corrupted_state else ClusterStates.RUNNING)
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

    def testCheckReplicasCorruptedState(self):
        from neo.master.handlers import storage
        with Patch(storage, EXPERIMENTAL_CORRUPTED_STATE=True):
            self.testCheckReplicas(True)

    @backup_test()
    def testBackupReadOnlyAccess(self, backup):
        """Check backup cluster can be used in read-only mode by ZODB clients"""
        B = backup
        U = B.upstream
        Z = U.getZODBStorage()
        with B.newClient() as client, self.assertRaises(ReadOnlyError):
            client.last_tid
        #Zb = B.getZODBStorage(read_only=True) # XXX see below about invalidations

        oid_list = []
        tid_list = []

        # S -> Sb link stops working during [cutoff, recover) test iterations
        cutoff  = 4
        recover = 7
        loop = 10
        def delayReplication(conn, packet):
            return isinstance(packet, Packets.AnswerFetchTransactions)

        with ConnectionFilter() as f:
            for i in xrange(loop):
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
                Zb = B.getZODBStorage(read_only=True)
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
                txn = transaction.Transaction()
                self.assertRaises(ReadOnlyError, Zb.tpc_begin, txn)
                self.assertRaises(ReadOnlyError, Zb.new_oid)
                # tpc_vote first checks whether the transaction has begun -
                # thus not ReadOnlyError
                self.assertRaises(StorageTransactionError, Zb.tpc_vote, txn)

                if i == loop // 2:
                    # Check that we survive a disconnection from upstream
                    # when we are serving clients. The client must be
                    # disconnected before leaving BACKINGUP state.
                    conn, = U.master.getConnectionList(B.master)
                    conn.close()
                    self.tic()

                # (XXX see above about invalidations not working)
                Zb.close()

    @backup_test()
    def testBackupPack(self, backup):
        """Check asynchronous replication during a pack"""
        upstream = backup.upstream
        importZODB = upstream.importZODB()
        importZODB(10)
        tid = upstream.last_tid
        importZODB(10)
        def _task_pack(orig, *args):
            ll()
            orig(*args)
        with LockLock() as ll:
            with Patch(backup.storage.dm._background_worker,
                       _task_pack=_task_pack):
                upstream.client.pack(tid)
                self.tic()
                ll()
            importZODB(10)
            upstream.ticAndJoinStorageTasks()
        backup.ticAndJoinStorageTasks()
        self.assertEqual(1, self.checkBackup(backup))

    @backup_test()
    def testUpstreamTruncated(self, backup):
        upstream = backup.upstream
        importZODB = upstream.importZODB()
        importZODB(10)
        tid1 = upstream.last_tid
        importZODB(10)
        tid2 = upstream.last_tid
        self.tic()
        getBackupState = backup.neoctl.getClusterState
        self.assertEqual(getBackupState(), ClusterStates.BACKINGUP)
        self.assertEqual(backup.last_tid, tid2)
        upstream.neoctl.truncate(tid1)
        self.tic()
        self.assertEqual(getBackupState(), ClusterStates.RUNNING)
        self.assertEqual(backup.last_tid, tid2)
        backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
        self.tic()
        self.assertEqual(getBackupState(), ClusterStates.RUNNING)
        self.assertEqual(backup.last_tid, tid2)
        backup.neoctl.truncate(tid1)
        self.tic()
        backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
        self.tic()
        self.assertEqual(getBackupState(), ClusterStates.BACKINGUP)
        self.assertEqual(backup.last_tid, tid1)

    @backup_test(3)
    def testDeleteObject(self, backup):
        upstream = backup.upstream
        storage = upstream.getZODBStorage()
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
                    self.tic()
                    self.assertEqual(3, self.checkBackup(backup))
                    if clear_cache:
                        storage._cache.clear()
                self.assertRaises(POSKeyError, storage.load, oid, '')
