#
# Copyright (C) 2021  Nexedi SA
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

import random, threading, unittest
from bisect import bisect
from collections import defaultdict, deque
from contextlib import contextmanager
from time import time
import transaction
from persistent import Persistent
from ZODB.POSException import UndoError
from neo.client.exception import NEOStorageError, NEOUndoPackError
from neo.lib import logging
from neo.lib.protocol import ClusterStates, Packets
from neo.lib.util import add64, p64
from neo.storage.database.manager import BackgroundWorker
from .. import consume, Patch
from . import ConnectionFilter, NEOThreadedTest, with_cluster

class PCounter(Persistent):
    value = 0

class PackTests(NEOThreadedTest):

    @contextmanager
    def assertPackOperationCount(self, cluster, *counts):
        packs = defaultdict(dict)
        def _pack(orig, dm, offset, *args):
            p = packs[dm.getUUID()]
            tid = args[1]
            try:
                tids = p[offset]
            except KeyError:
                p[offset] = [tid]
            else:
                self.assertLessEqual(tids[-1], tid)
                tids.append(tid)
            return orig(dm, offset, *args)
        storage_list = cluster.storage_list
        cls, = {type(s.dm) for s in storage_list}
        with Patch(cls, _pack=_pack):
            yield
            cluster.ticAndJoinStorageTasks()
        self.assertSequenceEqual(counts,
            tuple(sum(len(set(x)) for x in packs.pop(s.uuid, {}).itervalues())
                  for s in storage_list))
        self.assertFalse(packs)

    def countAskPackOrders(self, connection_filter):
        counts = defaultdict(int)
        @connection_filter.add
        def _(conn, packet):
            if isinstance(packet, Packets.AskPackOrders):
                counts[self.getConnectionApp(conn).uuid] += 1
        return counts

    def populate(self, cluster):
        t, c = cluster.getTransaction()
        r = c.root()
        for x in 'ab', 'ac', 'ab', 'bd', 'c', 'bc', 'ad':
            for x in x:
                try:
                    r[x].value += 1
                except KeyError:
                    r[x] = PCounter()
            t.commit()
            yield cluster.client.last_tid
        c.close()

    def assertPopulated(self, c):
        r = c.root()
        self.assertEqual([3, 3, 2, 1], [r[x].value for x in 'abcd'])

    @with_cluster(partitions=3, replicas=1, storage_count=3)
    def testOutdatedNodeIsBack(self, cluster):
        client = cluster.client
        s0 = cluster.storage_list[0]
        populate = self.populate(cluster)
        tid = consume(populate, 3)
        with self.assertPackOperationCount(cluster, 0, 4, 4), \
             ConnectionFilter() as f:
            counts = self.countAskPackOrders(f)
            def _worker(orig, self, weak_app):
                if weak_app() is s0:
                    logging.info("do not pack partitions %s",
                        ', '.join(map(str, self._pack_set)))
                    self._stop = True
                orig(self, weak_app)
            with Patch(BackgroundWorker, _worker=_worker):
                client.pack(tid)
                tid = consume(populate, 2)
                client.pack(tid)
                last_pack_id = client.last_tid
                s0.stop()
                cluster.join((s0,))
        # First storage node stops any pack-related work after the first
        # response to AskPackOrders. Other storage nodes process a pack order
        # for all cells before asking the master for the next pack order.
        self.assertEqual(counts, {s.uuid: 1 if s is s0 else 2
                                  for s in cluster.storage_list})
        s0.resetNode()
        with ConnectionFilter() as f, \
             self.assertPackOperationCount(cluster, 4, 0, 0):
            counts = self.countAskPackOrders(f)
            deque(populate, 0)
            s0.start()
        # The master queries 2 storage nodes for old pack orders and remember
        # those that s0 has not completed. s0 processes all orders for the first
        # replicated cell and ask them again when the second is up-to-date.
        self.assertIn(counts.pop(s0.uuid), (2, 3, 4))
        self.assertEqual(counts, {cluster.master.uuid: 2})
        t, c = cluster.getTransaction()
        r = c.root()
        def check(*values):
            t.begin()
            self.assertSequenceEqual(values, [r[x].value for x in 'abcd'])
            self.checkReplicas(cluster)
        check(3, 3, 2, 1)
        # Also check truncation vs pack.
        self.assertRaises(SystemExit, cluster.neoctl.truncate,
                          add64(last_pack_id,-1))
        cluster.neoctl.truncate(last_pack_id)
        self.tic()
        check(2, 2, 1, 0)

    @with_cluster(replicas=1)
    def testValueSerialVsReplication(self, cluster):
        t, c = cluster.getTransaction()
        ob = c.root()[''] = PCounter()
        t.commit()
        s0 = cluster.storage_list[0]
        s0.stop()
        cluster.join((s0,))
        ob.value += 1
        t.commit()
        ob.value += 1
        t.commit()
        s0.resetNode()
        with ConnectionFilter() as f:
            f.delayAskFetchTransactions()
            s0.start()
            c.db().undo(ob._p_serial, t.get())
            t.commit()
            c.db().storage.pack(time(), None)
            self.tic()
        cluster.ticAndJoinStorageTasks()
        self.checkReplicas(cluster)

    @with_cluster()
    def _testValueSerialMultipleUndo(self, cluster, race, *undos):
        t, c = cluster.getTransaction()
        r = c.root()
        ob = r[''] = PCounter()
        t.commit()
        tids = []
        for x in xrange(2):
            ob.value += 1
            t.commit()
            tids.append(ob._p_serial)
        db = c.db()
        def undo(i):
            db.undo(tids[i], t.get())
            t.commit()
            tids.append(db.lastTransaction())
        undo(-1)
        for i in undos:
            undo(i)
        if race:
            l1 = threading.Lock(); l1.acquire()
            l2 = threading.Lock(); l2.acquire()
            def _task_pack(orig, *args):
                l1.acquire()
                orig(*args)
                l2.release()
            def answerObjectUndoSerial(orig, *args, **kw):
                orig(*args, **kw)
                l1.release()
                l2.acquire()
            with Patch(cluster.client.storage_handler,
                       answerObjectUndoSerial=answerObjectUndoSerial), \
                 Patch(BackgroundWorker, _task_pack=_task_pack):
                cluster.client.pack(tids[-1])
                self.tic()
                self.assertRaises(NEOUndoPackError, undo, 2)
        else:
            cluster.client.pack(tids[-1])
            cluster.ticAndJoinStorageTasks()
            undo(2) # empty transaction

    def testValueSerialMultipleUndo1(self):
        self._testValueSerialMultipleUndo(False, 0, -1)

    def testValueSerialMultipleUndo2(self):
        self._testValueSerialMultipleUndo(True, -1, 1)

    @with_cluster(partitions=3)
    def testPartial(self, cluster):
        N = 256
        T = 40
        rnd = random.Random(0)
        t, c = cluster.getTransaction()
        r = c.root()
        for i in xrange(T):
            for x in xrange(40):
                x = rnd.randrange(0, N)
                try:
                    r[x].value += 1
                except KeyError:
                    r[x] = PCounter()
            t.commit()
            if i == 30:
                self.assertEqual(len(r), N-1)
                tid = c.db().lastTransaction()
        self.assertEqual(len(r), N)
        oids = []
        def tids(oid, pack=False):
            tids = [x['tid'] for x in c.db().history(oid, T)]
            self.assertLess(len(tids), T)
            tids.reverse()
            if pack:
                oids.append(oid)
                return tids[bisect(tids, tid)-1:]
            return tids
        expected = [tids(r._p_oid, True)]
        for x in xrange(N):
            expected.append(tids(r[x]._p_oid, x % 2))
        self.assertNotEqual(sorted(oids), oids)
        client = c.db().storage.app
        client.wait_for_pack = True
        with self.assertPackOperationCount(cluster, 3):
            client.pack(tid, oids)
        result = [tids(r._p_oid)]
        for x in xrange(N):
            result.append(tids(r[x]._p_oid))
        self.assertEqual(expected, result)

    @with_cluster(partitions=2, storage_count=2)
    def testDisablePack(self, cluster):
        s0, s1 = cluster.sortStorageList()
        def reset0(**kw):
            s0.stop()
            cluster.join((s0,))
            s0.resetNode(**kw)
            s0.start()
            cluster.ticAndJoinStorageTasks()
            self.assertEqual(cluster.neoctl.getClusterState(),
                             ClusterStates.RUNNING)
        reset0(disable_pack=True)
        populate = self.populate(cluster)
        client = cluster.client
        client.wait_for_pack = True
        tid = consume(populate, 3)
        client.pack(tid)
        tid = consume(populate, 2)
        client.pack(tid)
        deque(populate, 0)
        t, c = cluster.getTransaction()
        history = c.db().history
        def check(*counts):
            c.cacheMinimize()
            client._cache.clear()
            self.assertPopulated(c)
            self.assertSequenceEqual(counts,
                [len(history(p64(i), 10)) for i in xrange(5)])
        check(4, 2, 4, 2, 2)
        reset0(disable_pack=False)
        check(1, 2, 2, 2, 2)

    @with_cluster()
    def testInvalidPackTID(self, cluster):
        deque(self.populate(cluster), 0)
        client = cluster.client
        client.wait_for_pack = True
        tid = client.last_tid
        self.assertRaises(NEOStorageError, client.pack, add64(tid, 1))
        client.pack(tid)
        t, c = cluster.getTransaction()
        self.assertPopulated(c)


if __name__ == "__main__":
    unittest.main()
