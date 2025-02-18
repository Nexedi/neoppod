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

from __future__ import print_function
import random, thread, threading, unittest
from bisect import bisect
from collections import defaultdict, deque
from contextlib import contextmanager
from itertools import count
from logging import NullHandler
from time import time
from Queue import Queue
import transaction
from persistent import Persistent
from ZODB.POSException import POSKeyError, ReadOnlyError, UndoError
from ZODB.utils import newTid
from neo.client.exception import NEOStorageError, NEOUndoPackError
from neo.client.Storage import Storage
from neo.lib import logging
from neo.lib.protocol import ClusterStates, Packets, ZERO_TID
from neo.lib.util import add64, p64, u64
from neo.scripts import reflink
from neo.storage.database.manager import BackgroundWorker
from .. import consume, Patch, TransactionalResource
from . import ConnectionFilter, NEOCluster, NEOThreadedTest, with_cluster

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


class GCTests(NEOThreadedTest):

    @classmethod
    def setUpClass(cls):
        super(GCTests, cls).setUpClass()
        reflink.logging_handler = NullHandler()
        reflink.print = lambda *args, **kw: None

    @with_cluster(serialized=False)
    def test1(self, cluster):
        def check(*objs):
            cluster.emptyCache(conn)
            t.begin()
            for ob in all_:
                if ob in objs:
                    ob._p_activate()
                else:
                    self.assertRaises(POSKeyError, ob._p_activate)
        def commit(orig, *args):
            orig(*args)
            committed.release()
            track.acquire()
            if stop:
                thread.exit()
        commit_patch = Patch(reflink.Changeset, commit=commit)
        def reflink_run():
            reflink.main(['-v', reflink_cluster.zurl(), 'run',
                          '-p', '0', '-i', '1e-9',
                          cluster.zurl()])
        committed = threading.Lock()
        track = threading.Lock()
        with commit_patch, committed, track, NEOCluster() as reflink_cluster:
            stop = False
            start = time()
            reflink_cluster.start()
            reflink_thread = self.newThread(reflink_run)
            t, conn = cluster.getTransaction()
            committed.acquire()
            r = conn.root()[''] = PCounter()
            t.commit()
            track.release()
            committed.acquire()

            b = PCounter()
            c = PCounter()
            r.x = b, c
            z = c.x = PCounter()
            l = [PCounter(), PCounter()]
            l[0].x = l[1], l[1]
            l[1].x = l[0], l[0]
            b.x = l[0]
            t.commit()
            track.release()
            committed.acquire()

            l.append(PCounter())
            l[0].x = l[2], l[1]
            l[1].x = l[0], l[2]
            l[2].x = l[1], l[0]
            l[1].y = y = PCounter()
            d = PCounter()
            r.x = b, d
            d.x = z
            t.commit()
            track.release()
            committed.acquire()

            all_ = [b, c, d, y, z]
            all_ += l

            # GC commit
            track.release()
            committed.acquire()
            check(b, d, y, z, *l)
            tid0 = cluster.last_tid

            r.x = d
            t.commit()
            track.release()
            committed.acquire()

            tid1 = cluster.last_tid
            self.assertEqual(tid1, reflink_cluster.last_tid)
            # GC commit
            track.release()
            committed.acquire()
            final = d, z
            check(*final)

            stop = True
            track.release()
            reflink_thread.join()
            tid2 = cluster.last_tid
            self.assertLess(tid1, tid2)
            self.assertEqual(tid2, reflink_cluster.last_tid)

            end = time()
            for x in reflink_cluster.client.iterator():
                x = x.extension['time']
                self.assertLess(start, x)
                start = x
            self.assertLess(x, end)

        cluster.neoctl.truncate(tid1)
        self.tic()

        with NEOCluster() as reflink_cluster:
            reflink_cluster.start()
            loid = cluster.master.tm.getLastOID()
            reflink.main([reflink_cluster.zurl(), 'bootstrap', hex(u64(tid0))])
            stop = False
            with commit_patch, committed, track:
                reflink_thread = self.newThread(reflink_run)
                committed.acquire()

                for i in xrange(-u64(loid), 2):
                    track.release()
                    committed.acquire()
                    if not i:
                        self.assertEqual(tid1, reflink_cluster.last_tid)
                check(*final)

                stop = True
                track.release()
                reflink_thread.join()
            tid3 = cluster.last_tid
            self.assertLess(tid2, tid3)
            self.assertEqual(tid3, reflink_cluster.last_tid)

    @with_cluster()
    def test2(self, cluster, full=False):
        """
        Several cases, 1 line per transaction,
        XY means thats X has reference to Y and X!Y deletes it,
        a digit refers to an object that is equivalent to the oid 0:
        0. 0a,0b
           ab,ba,0!a
           -> a marked as orphan but evaluated during GC as being part of
              a cycle that is not orphan
        1. 1a,1b
           ab,ba,1!a
           a!b
        2. 2a,2b,ab,ba
           2!a,2!b
           -> a and/or b must be marked as orphan
        3. (packed)
           ab
           ba
           3b
        4. (packed)
           same as 3 but no more ref to b
        5. (packed)
           a
           ba
           5b
        6. 6a,a6
           6!a
        7. 7a,7b,ab,ba,ac
           7!a
           b!a
        8. 8a,ab,ba
           8!a
           partial GC of only a (--max-txn-size exceeded)
        """
        t, conn = cluster.getTransaction()
        r = conn.root()
        for i in xrange(9):
            r[i] = PCounter()
        t.commit()

        a0 = r[0].a = PCounter()
        b0 = r[0].b = PCounter()
        a1 = r[1].a = PCounter()
        b1 = r[1].b = PCounter()
        a2 = r[2].a = PCounter()
        b2 = r[2].b = PCounter()
        a2.x = b2
        b2.x = a2
        b3 = r[3].b = PCounter()
        a3 = b3.x = PCounter()
        a3.x = b3
        b4 = r[4].b = PCounter()
        a4 = b4.x = PCounter()
        a4.x = b4
        b5 = r[5].b = PCounter()
        a5 = b5.x = PCounter()
        a6 = r[6].a = PCounter()
        a6.x = r[6]
        a7 = r[7].a = PCounter()
        b7 = r[7].b = PCounter()
        c7 = a7.y = PCounter()
        a7.x = b7
        b7.x = a7
        a8 = r[8].a = PCounter()
        b8 = a8.x = PCounter()
        b8.x = a8
        t.commit()

        b3._p_changed = b4._p_changed = b5._p_changed = 1
        t.commit()
        r[3]._p_changed = 1
        del r[4].b
        t.commit()
        tid = cluster.last_tid

        a0.x = b0
        b0.x = a0
        a1.x = b1
        b1.x = a1
        del r[0].a, r[1].a, r[2].a, r[2].b, r[6].a, r[7].a, r[8].a
        t.commit()

        del a1.x, b7.x
        def txn_meta(txn):
            try:
                return txn.data(conn)
            except KeyError:
                return txn
        TransactionalResource(t, 1, tpc_begin=lambda txn:
            conn.db().storage.deleteObject(a8._p_oid, a8._p_serial,
                                           txn_meta(txn)))
        t.commit()

        client = cluster.client
        client.wait_for_pack = True
        client.pack(tid)

        with NEOCluster() as reflink_cluster:
            reflink_cluster.start()
            args = ['-v', reflink_cluster.zurl(), 'run',
                    '-p', '0', '-1', cluster.zurl()]
            if full:
                args.append('-f')
            reflink.main(args)

        cluster.emptyCache(conn)
        t.begin()
        for x in a0, b0, a1, b1, a3, b3, a5, b5, b7:
            x._p_activate()
        for x in a2, b2, a4, b4, a6, a7, c7, a8, b8:
            self.assertRaises(POSKeyError, x._p_activate)

    def test3(self):
        self.test2(True)

    @with_cluster()
    def test4(self, cluster, jobs=None):
        t, conn = cluster.getTransaction()
        r = conn.root()
        a = r.x = PCounter()
        z = r.z = PCounter()
        t.commit()
        b = r.x = PCounter()
        b.x = a
        t.commit()
        c = r.x = PCounter()
        c.x = b
        t.commit()
        tid0 = cluster.last_tid
        assert a._p_oid < b._p_oid < c._p_oid
        del r.x
        t.commit()

        class tpc_finish(Exception):
            def __new__(cls, orig, storage, *args):
                tid = orig(storage, *args)
                if maybeAbort(tid):
                    raise Exception.__new__(cls)
                return tid
        with NEOCluster() as reflink_cluster:
            reflink_cluster.start()
            args = ['-v', reflink_cluster.zurl(), 'bootstrap', hex(u64(tid0))]
            reflink.main(args)
            args[2:] = 'run', '-m', '1', '-p', '0', cluster.zurl()
            fgc_args = args[:2]
            fgc_args += 'gc', '-f', args[-1] + '?read_only=true'
            if jobs:
                fgc_args[4:4] = args[5:5] = '-j', str(jobs)
            with Patch(Storage, tpc_finish=tpc_finish):
                for x in a, b:
                    maybeAbort = cluster.last_tid.__lt__
                    # Interrupt after 1 commit of the bootstrap GC.
                    self.assertRaises(tpc_finish, reflink.main, args)
                    cluster.emptyCache(conn)
                    t.begin()
                    self.assertRaises(POSKeyError, x._p_activate)
                    c._p_activate()
                tids = []
                def maybeAbort(tid):
                    tids.append(tid)
                    return len(tids) == 3
                del args[3:5]
                # Interrupt when it is going to wait for new transactions
                # (bootstrap completed).
                self.assertRaises(tpc_finish, reflink.main, args)
                cluster.emptyCache(conn)
                t.begin()
                self.assertRaises(POSKeyError, c._p_activate)
                z._p_activate()
                self.assertRaises(ReadOnlyError, reflink.main, fgc_args)
                del r.z
                t.commit()
                args.insert(3, '-f')
                # Interrupt when it is going to wait for new transactions.
                del tids[:]
                self.assertRaises(tpc_finish, reflink.main, args)
            cluster.emptyCache(conn)
            t.begin()
            self.assertRaises(POSKeyError, z._p_activate)
            # Now, there really should be nothing to GC anymore.
            reflink.main(fgc_args)

        cluster.emptyCache(conn)
        t.begin()
        r._p_activate()

    def test5(self):
        self.test4(2)

    @with_cluster()
    def test6(self, cluster):
        t, conn = cluster.getTransaction()
        r = conn.root()
        a = r.x = PCounter()
        t.commit()
        b = r.x = PCounter()
        b.x = a
        t.commit()
        tid0 = cluster.last_tid
        assert a._p_oid < b._p_oid
        del r.x
        t.commit()

        class _wait(Exception):
            def __init__(self, *_):
                raise self
        with NEOCluster() as reflink_cluster:
            reflink_cluster.start()
            args = ['-v', reflink_cluster.zurl(), 'run',
                    '-m', '1', '-p', '0', '-i', '1e-9', cluster.zurl()]
            with Patch(reflink.InvalidationListener, _wait=_wait):
                self.assertRaises(_wait, reflink.main, args)
            cluster.emptyCache(conn)
            t.begin()
            for x in a, b:
                self.assertRaises(POSKeyError, x._p_activate)
            gc_args = args[:2]
            gc_args += 'gc', args[-1] + '?read_only=true'
            reflink.main(gc_args)

    @with_cluster()
    def test7(self, cluster):
        """
        Should an oid be wrongly marked as orphan, the GC should keep it.
        """
        changeset = reflink.Changeset(cluster.getZODBStorage())
        for i in xrange(4):
            for z in 0, 10:
                a = p64(z+i)
                A = changeset.get(a)
                b = p64(z + (i + 1) % 4)
                B = changeset.get(b)
                self.assertFalse(A.referents or B.referrers)
                A.referents.add(b)
                B.referrers.append(a)
        tid = newTid(None)
        changeset.commit(tid)
        self.assertFalse(changeset.orphans(None))
        a = changeset.get(ZERO_TID).next_orphan = p64(2)
        A = changeset.get(a)
        A.prev_orphan = ZERO_TID
        A.next_orphan = b = add64(a, z)
        changeset.get(b).prev_orphan = a
        tid = newTid(tid)
        changeset.commit(tid)
        self.assertFalse(changeset.orphans(None))
        changeset.abort()
        for i in xrange(4):
            for z in 0, 10:
                a = p64(z + i)
                A = changeset.get(a)
                b = p64(z + (i - 1) % 4)
                B = changeset.get(b)
                A.referents.add(b)
                reflink.insort(B.referrers, a)
        tid = newTid(tid)
        changeset.commit(tid)
        self.assertEqual(map(p64, xrange(z, z+4)),
                         sorted(changeset.orphans(None)))
        changeset.abort()
        a = p64(2)
        b = p64(z)
        changeset.get(a).referents.add(b)
        reflink.insort(changeset.get(b).referrers, a)
        tid = newTid(tid)
        changeset.commit(tid)
        self.assertFalse(changeset.orphans(None))


if __name__ == "__main__":
    unittest.main()
