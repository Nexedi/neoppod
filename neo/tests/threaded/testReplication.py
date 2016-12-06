#
# Copyright (C) 2012-2016  Nexedi SA
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

import random
import sys
import time
import transaction
from ZODB.POSException import ReadOnlyError, POSKeyError
import unittest
from collections import defaultdict
from functools import wraps
from neo.lib import logging
from neo.client.exception import NEOStorageError
from neo.master.handlers.backup import BackupHandler
from neo.storage.checker import CHECK_COUNT
from neo.storage.replicator import Replicator
from neo.lib.connector import SocketConnector
from neo.lib.connection import ClientConnection
from neo.lib.event import EventManager
from neo.lib.protocol import CellStates, ClusterStates, Packets, \
    ZERO_OID, ZERO_TID, MAX_TID, uuid_str
from neo.lib.util import p64
from .. import expectedFailure, Patch
from . import ConnectionFilter, NEOCluster, NEOThreadedTest, predictable_random


def backup_test(partitions=1, upstream_kw={}, backup_kw={}):
    def decorator(wrapped):
        def wrapper(self):
            upstream = NEOCluster(partitions, **upstream_kw)
            try:
                upstream.start()
                backup = NEOCluster(partitions, upstream=upstream, **backup_kw)
                try:
                    backup.start()
                    backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                    self.tic()
                    wrapped(self, backup)
                finally:
                    backup.stop()
            finally:
                upstream.stop()
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

    def testBackupNormalCase(self):
        np = 7
        nr = 2
        check_dict = dict.fromkeys(xrange(np))
        upstream = NEOCluster(partitions=np, replicas=nr-1, storage_count=3)
        try:
            upstream.start()
            importZODB = upstream.importZODB()
            importZODB(3)
            backup = NEOCluster(partitions=np, replicas=nr-1, storage_count=5,
                                upstream=upstream)
            try:
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
            finally:
                backup.stop()
            backup.reset()
            try:
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
            finally:
                backup.stop()
            def delaySecondary(conn, packet):
                if isinstance(packet, Packets.Replicate):
                    tid, upstream_name, source_dict = packet.decode()
                    return not upstream_name and all(source_dict.itervalues())
            backup.reset()
            try:
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
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
            finally:
                backup.stop()
            backup.reset()
            try:
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                with ConnectionFilter() as f:
                    f.add(lambda conn, packet: conn.getUUID() is None and
                        isinstance(packet, Packets.AddObject))
                    while not f.filtered_count:
                        importZODB(1)
                    self.tic()
                    backup.neoctl.setClusterState(ClusterStates.STOPPING_BACKUP)
                    self.tic()
                self.tic()
                self.assertEqual(np*nr, self.checkBackup(backup,
                    max_tid=backup.last_tid))
            finally:
                backup.stop()
        finally:
            upstream.stop()

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
        upstream = NEOCluster(partitions=np, replicas=0, storage_count=1)
        try:
            upstream.start()
            importZODB = upstream.importZODB(random=random)
            # Do not start with an empty DB so that 'primary_dict' below is not
            # empty on the first iteration.
            importZODB(1)
            backup = NEOCluster(partitions=np, replicas=2, storage_count=4,
                                upstream=upstream)
            try:
                backup.start()
                backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                self.tic()
                storage_list = [x.uuid for x in backup.storage_list]
                slave = set(xrange(len(storage_list))).difference
                for event in xrange(10):
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
            finally:
                backup.stop()
        finally:
            upstream.stop()

    @backup_test()
    def testBackupUpstreamMasterDead(self, backup):
        """Check proper behaviour when upstream master is unreachable

        More generally, this checks that when a handler raises when a connection
        is closed voluntarily, the connection is in a consistent state and can
        be, for example, closed again after the exception is caught, without
        assertion failure.
        """
        conn, = backup.master.getConnectionList(backup.upstream.master)
        # trigger ping
        self.assertFalse(conn.isPending())
        conn.onTimeout()
        self.assertTrue(conn.isPending())
        # force ping to have expired
        # connection will be closed before upstream master has time
        # to answer
        def _poll(orig, self, blocking):
            if backup.master.em is self:
                p.revert()
                conn._next_timeout = 0
                conn.onTimeout()
            else:
                orig(self, blocking)
        with Patch(EventManager, _poll=_poll) as p:
            self.tic()
        new_conn, = backup.master.getConnectionList(backup.upstream.master)
        self.assertIsNot(new_conn, conn)

    @backup_test()
    def testBackupUpstreamStorageDead(self, backup):
        upstream = backup.upstream
        with ConnectionFilter() as f:
            f.add(lambda conn, packet:
                isinstance(packet, Packets.InvalidateObjects))
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
            self.assertTrue(t <= time.time())

    @backup_test()
    def testBackupDelayedUnlockTransaction(self, backup):
        """
        Check that a backup storage node is put on hold by upstream if
        the requested transaction is still locked. Such case happens when
        the backup cluster reacts very quickly to a new transaction.
        """
        upstream = backup.upstream
        with upstream.master.filterConnection(upstream.storage) as f:
            f.add(lambda conn, packet:
                isinstance(packet, Packets.NotifyUnlockInformation))
            upstream.importZODB()(1)
            self.tic()
        self.tic()
        self.assertEqual(1, self.checkBackup(backup))

    def testBackupEarlyInvalidation(self):
        """
        The backup master must ignore notification before being fully
        initialized.
        """
        upstream = NEOCluster()
        try:
            upstream.start()
            backup = NEOCluster(upstream=upstream)
            try:
                backup.start()
                with ConnectionFilter() as f:
                    f.add(lambda conn, packet:
                        isinstance(packet, Packets.AskPartitionTable) and
                        isinstance(conn.getHandler(), BackupHandler))
                    backup.neoctl.setClusterState(ClusterStates.STARTING_BACKUP)
                    upstream.importZODB()(1)
                    self.tic()
                self.tic()
                self.assertTrue(backup.master.isAlive())
            finally:
                backup.stop()
        finally:
            upstream.stop()

    def testSafeTweak(self):
        """
        Check that tweak always tries to keep a minimum of (replicas + 1)
        readable cells, otherwise we have less/no redundancy as long as
        replication has not finished.
        """
        def changePartitionTable(orig, *args):
            orig(*args)
            sys.exit()
        cluster = NEOCluster(partitions=3, replicas=1, storage_count=3)
        s0, s1, s2 = cluster.storage_list
        try:
            cluster.start([s0, s1])
            s2.start()
            self.tic()
            cluster.enableStorageList([s2])
            # 2 UP_TO_DATE cells should become FEEDING,
            # and be dropped only when the replication is done,
            # so that 1 storage can still die without data loss.
            with Patch(s0.dm, changePartitionTable=changePartitionTable):
                cluster.neoctl.tweakPartitionTable()
                self.tic()
            expectedFailure(self.assertEqual)(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)
        finally:
            cluster.stop()

    def testReplicationAbortedBySource(self):
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
        np = 3
        cluster = NEOCluster(partitions=np, replicas=1, storage_count=3)
        s0, s1, s2 = cluster.storage_list
        for delayed in Packets.AskFetchTransactions, Packets.AskFetchObjects:
            try:
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
            finally:
                cluster.stop()
            cluster.reset(True)

    def testClientReadingDuringTweak(self):
        # XXX: Currently, the test passes because data of dropped cells are not
        #      deleted while the cluster is operational: this is only done
        #      during the RECOVERING phase. But we'll want to be able to free
        #      disk space without service interruption, and for this the client
        #      may have to retry reading data from the new cells. If s0 deleted
        #      all data for partition 1, the test would fail with a POSKeyError.
        cluster = NEOCluster(partitions=2, storage_count=2)
        s0, s1 = cluster.storage_list
        try:
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
                m2c.add(lambda conn, packet:
                    isinstance(packet, Packets.NotifyPartitionChanges))
                self.tic()
                self.assertEqual('foo', storage.load(oid)[0])
        finally:
            cluster.stop()

    def testResumingReplication(self):
        cluster = NEOCluster(replicas=1)
        try:
            s0, s1 = cluster.storage_list
            cluster.start(storage_list=(s0,))
            t, c = cluster.getTransaction()
            r = c.root()
            r._p_changed = 1
            t.commit()
            s1.start()
            self.tic()
            with Patch(Replicator, connected=lambda *_: None):
                cluster.enableStorageList((s1,))
                cluster.neoctl.tweakPartitionTable()
                r._p_changed = 1
                t.commit()
                self.tic()
                s1.stop()
                cluster.join((s1,))
            t0, t1, t2 = c.db().storage.iterator()
            s1.resetNode()
            s1.start()
            self.tic()
            self.assertEqual([], cluster.getOutdatedCells())
            s0.stop()
            cluster.join((s0,))
            t0, t1, t2 = c.db().storage.iterator()
        finally:
            cluster.stop()

    def testCheckReplicas(self):
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
        np = 5
        tid_count = np * 3
        corrupt_tid = tid_count // 2
        check_dict = dict.fromkeys(xrange(np))
        cluster = NEOCluster(partitions=np, replicas=2, storage_count=3)
        try:
            checker.CHECK_COUNT = 2
            cluster.start()
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
        finally:
            checker.CHECK_COUNT = CHECK_COUNT
            cluster.stop()

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
                txn.note('test transaction %i' % i)
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
                Zb._cache._max_size = 0     # make store() do work in sync way
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
