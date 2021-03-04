#
# Copyright (C) 2014-2019  Nexedi SA
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

from cPickle import Pickler, Unpickler
from cStringIO import StringIO
from itertools import izip_longest
import os, random, shutil, threading, time, unittest
import transaction, ZODB
from persistent import Persistent
from neo.client.exception import NEOPrimaryMasterLost
from neo.lib import logging
from neo.lib.protocol import MAX_TID
from neo.lib.util import cached_property, p64, u64
from neo.master.transactions import TransactionManager
from neo.storage.database import getAdapterKlass, importer, manager
from neo.storage.database.importer import \
    Repickler, TransactionRecord, WriteBack
from .. import expectedFailure, getTempDirectory, random_tree, \
    Patch, TransactionalResource, getTransactionMetaData
from . import NEOCluster, NEOThreadedTest
from ZODB import serialize
from ZODB.DB import TransactionalUndo
from ZODB.FileStorage import FileStorage
from ZODB.POSException import POSKeyError

class Equal:

    _recurse = {}

    def __hash__(self):
        return 1

    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __repr__(self):
        return "<%s(%s)>" % (self.__class__.__name__,
            ", ".join("%s=%r" % k for k in self.__dict__.iteritems()))

class Reduce(Equal, object):

    state = None

    def __init__(self, *args):
        self.args = args
        self._l = []
        self._d = []

    def append(self, item):
        self._l.append(item)

    def extend(self, item):
        self._l.extend(item)

    def __setitem__(self, *args):
        self._d.append(args)

    def __setstate__(self, state):
        self.state = state

    def __reduce__(self):
        r = self.__class__, self.args, self.state, iter(self._l), iter(self._d)
        return r[:5 if self._d else
                  4 if self._l else
                  3 if self.state is not None else
                  2]

class Obj(Equal):

    state = None

    def __getinitargs__(self):
        return self.args

    def __init__(self, *args):
        self.args = args

    def __getstate__(self):
        return self.state

    def __setstate__(self, state):
        self.state = state

class NewObj(Obj, object):

    def __init__(self):
        pass # __getinitargs__ only work with old-style classes

class DummyRepickler(Repickler):

    def __init__(self):
        Repickler.__init__(self, None)

    _changed = True

    def __setattr__(self, name, value):
        if name != "_changed":
            self.__dict__[name] = value


class ImporterTests(NEOThreadedTest):

    def testRepickler(self):
        r2 = Obj("foo")
        r2.__setstate__("bar")
        r2 = Reduce(r2)
        r3 = Reduce(1, 2)
        r3.__setstate__(NewObj())
        r4 = Reduce()
        r4.args = r2.args
        r4.__setstate__("bar")
        r4.extend("!!!")
        r5 = Reduce()
        r5.append("!!!")
        r5["foo"] = "bar"
        state = {r2: r3, r4: r5}
        p = StringIO()
        Pickler(p, 1).dump(Obj).dump(state)
        p = p.getvalue()
        r = DummyRepickler()(p)
        load = Unpickler(StringIO(r)).load
        self.assertIs(Obj, load())
        self.assertDictEqual(state, load())

    @cached_property
    def getFS(self):
        fs_dir = os.path.join(getTempDirectory(), self.id())
        shutil.rmtree(fs_dir, 1) # for --loop
        os.mkdir(fs_dir)
        def getFS(db='root'):
            path = os.path.join(fs_dir, '%s.fs' % db)
            return path, {
                "storage": "<filestorage>\npath %s\n</filestorage>" % path
            }
        return getFS

    def getData(self, tree=random_tree.generateTree(random.Random(0))):
        txn_size = 10
        i = len(tree) // 3
        assert i > txn_size
        before_tree = tree[:i]
        after_tree = tree[i:]
        def beforeCheck(h, count=52):
            self.assertEqual(count, h())
            self.assertEqual('1d4ff03730fe6bcbf235e3739fbe5f5b', h.hexdigest())
        def finalCheck(r):
            h = random_tree.hashTree(r)
            self.assertEqual(93, h())
            self.assertEqual('6bf0f0cb2d6c1aae9e52c412ef0e25b6', h.hexdigest())
        return (
            beforeCheck,
            lambda r, *f: random_tree.importTree(r, before_tree, txn_size, *f),
            finalCheck,
            lambda r: random_tree.importTree(r, after_tree, txn_size),
            )

    def _importFromFileStorage(self, multi=(),
                               root_filter=None, sub_filter=None):
        beforeCheck, before, finalCheck, after = self.getData()
        iter_list = []
        db_list = []
        # Setup several FileStorage databases.
        for i, db in enumerate(('root',) + multi):
            fs_path, cfg = self.getFS(db)
            c = ZODB.DB(FileStorage(fs_path)).open()
            r = c.root()['tree'] = random_tree.Node()
            transaction.commit()
            iter_list.append(before(r, sub_filter(db) if i else root_filter))
            db_list.append((db, r, cfg))
        # Populate FileStorage databases.
        for i, iter_list in enumerate(izip_longest(*iter_list)):
            for r in iter_list:
                if r:
                    transaction.commit()
        # Get oids of mount points and close.
        zodb = []
        importer = {'zodb': zodb}
        for db, r, cfg in db_list:
            if db == 'root':
                if multi:
                    for x in multi:
                        cfg['_%s' % x] = str(u64(r[x]._p_oid))
                else:
                    beforeCheck(random_tree.hashTree(r))
                    importer['writeback'] = 'true'
            else:
                cfg["oid"] = str(u64(r[db]._p_oid))
                db = '_%s' % db
            r._p_jar.db().close()
            zodb.append((db, cfg))
        del db_list, iter_list
        #del zodb[0][1][zodb.pop()[0]]
        # Start NEO cluster with transparent import.
        with NEOCluster(importer=importer, partitions=2) as cluster:
            # Suspend import for a while, so that import
            # is finished in the middle of the below 'for' loop.
            # Use a slightly different main loop for storage so that it
            # does not import data too fast and we test read/write access
            # by the client during the import.
            dm = cluster.storage.dm
            def doOperation(app):
                del dm.doOperation
                try:
                    while True:
                        if app.task_queue:
                            app.task_queue[-1].next()
                        app._poll()
                except StopIteration:
                    app.task_queue.pop()
            dm.doOperation = doOperation
            cluster.start()
            t, c = cluster.getTransaction()
            r = c.root()['tree']
            # Test retrieving of an object from ZODB when next serial is in NEO.
            r._p_changed = 1
            t.commit()
            t.begin()
            storage = c.db().storage
            storage._cache.clear()
            storage.loadBefore(r._p_oid, r._p_serial)
            ##
            self.assertRaisesRegexp(NotImplementedError, " getObjectHistory$",
                                    c.db().history, r._p_oid)
            h = random_tree.hashTree(r)
            h(30)
            logging.info("start migration")
            dm.doOperation(cluster.storage)
            # Adjust if needed. Must remain > 0.
            beforeCheck(h, 22)
            # New writes after the switch to NEO.
            last_import = -1
            for i, r in enumerate(after(r)):
                c.readCurrent(r)
                t.commit()
                if cluster.storage.dm._import:
                    last_import = i
            for x in 0, 1:
                undo = TransactionalUndo(c.db(), [storage.lastTransaction()])
                txn = transaction.Transaction()
                undo.tpc_begin(txn)
                undo.commit(txn)
                undo.tpc_vote(txn)
                undo.tpc_finish(txn)
            self.tic()
            # Same as above. We want last_import smaller enough compared to i
            assert i < last_import * 3 < 2 * i, (last_import, i)
            self.assertFalse(cluster.storage.dm._import)
            storage._cache.clear()
            finalCheck(r)
            if dm._writeback:
                dm.commit()
                dm._writeback.wait()
        if dm._writeback:
            db = ZODB.DB(FileStorage(fs_path, read_only=True))
            finalCheck(db.open().root()['tree'])
            db.close()

    @unittest.skipUnless(importer.FORK, 'no os.fork')
    def testMultiProcessWriteBack(self):
        self._importFromFileStorage()

    def testThreadedWritebackAndDBReconnection(self):
        # Also check reconnection to the underlying DB for relevant backends.
        tid_list = []
        def __init__(orig, tr, db, tid):
            orig(tr, db, tid)
            tid_list.append(tid)
        def fetchObject(orig, db, *args):
            if len(tid_list) == 5:
                if isinstance(db, getAdapterKlass('MySQL')):
                    from neo.tests.storage.testStorageMySQL import ServerGone
                    with ServerGone(db):
                        orig(db, *args)
                    self.fail()
                else:
                    tid_list.append(None)
                    p.revert()
            return orig(db, *args)
        def sleep(orig, seconds):
            self.assertEqual(len(tid_list), 5)
            p.revert()
        with Patch(importer, FORK=False), \
             Patch(TransactionRecord, __init__=__init__), \
             Patch(manager.DatabaseManager, fetchObject=fetchObject), \
             Patch(time, sleep=sleep) as p:
            self._importFromFileStorage()
            self.assertFalse(p.applied)
        self.assertEqual(len(tid_list), 13)

    def testThreadedWritebackWithUnbalancedPartitions(self):
        N = 7
        nonlocal_ = [0]
        def committed(orig, self):
            if nonlocal_[0] > N:
                orig(self)
        def _nextTID(orig, self, *args):
            if args:
                return orig(self, *args)
            nonlocal_[0] += 1
            return orig(self, p64(nonlocal_[0] == N), 2)
        with Patch(importer, FORK=False), \
             Patch(TransactionManager, _nextTID=_nextTID), \
             Patch(WriteBack, chunk_size=N-2), \
             Patch(WriteBack, committed=committed):
            self._importFromFileStorage()
        self.assertEqual(nonlocal_[0], 12)

    def testMerge(self):
        multi = 1, 2, 3
        self._importFromFileStorage(multi,
            (lambda path: path[0] not in multi or len(path) == 1),
            (lambda db: lambda path: path[0] in (db, 4)))

    if getattr(serialize, '_protocol', 1) > 1:
        # XXX: With ZODB5, we should at least keep a working test that does not
        #      merge several DB.
        testMerge = expectedFailure(NEOPrimaryMasterLost)(testMerge)

    def testIncremental(self):
        """
        This reproduces an undocumented way to speed up the import of a single
        ZODB by doing most of the work before switching to NEO.
        """
        beforeCheck, before, finalCheck, after = self.getData()
        fs_path, cfg = self.getFS()
        c = ZODB.DB(FileStorage(fs_path)).open()
        r = c.root()['tree'] = random_tree.Node()
        transaction.commit()
        for _ in before(r):
            transaction.commit()
        c.db().close()
        importer = {'zodb': [('root', cfg)]}
        # Start NEO cluster with transparent import.
        with NEOCluster(importer=importer, partitions=2) as cluster:
            s = cluster.storage
            l = threading.Lock()
            l.acquire()
            def _finished(orig):
                orig()
                l.release()
            with Patch(s.dm, _finished=_finished):
                cluster.start()
                l.acquire()
            t, c = cluster.getTransaction()
            r = c.root()['tree']
            beforeCheck(random_tree.hashTree(r))
            c = ZODB.DB(FileStorage(fs_path)).open()
            for _ in after(c.root()['tree']):
                transaction.commit()
            c.db().close()
            # TODO: Add a storage option that only does this and exits.
            #       Such command would also check that there's no data after
            #       what's already imported.
            s.dm.setConfiguration('zodb', None)
            s.stop()
            cluster.join((s,))
            s.resetNode()
            with Patch(s.dm, _finished=_finished):
                s.start()
                self.tic()
                l.acquire()
            t.begin()
            finalCheck(r)

    def testDeleteAndUndo(self):
        fs_path, cfg = self.getFS()
        c = ZODB.DB(FileStorage(fs_path)).open()
        s = c.db().storage
        r = c.root()
        tid = r._p_serial
        r[''] = delete = Persistent()
        transaction.commit()
        self.assertEqual(delete._p_oid, p64(1))
        del r['']
        TransactionalResource(transaction, 0, commit=lambda txn:
            s.deleteObject(delete._p_oid, delete._p_serial,
                           getTransactionMetaData(txn, c)))
        transaction.commit()
        r[''] = undo = Persistent()
        transaction.commit()
        c.db().undo(s.undoLog(last=1)[0]['id'])
        transaction.commit()
        self.assertEqual(undo._p_oid, p64(2))
        def check():
            self.assertIsNone(s.loadBefore(delete._p_oid, tid))
            for oid in delete._p_oid, undo._p_oid, p64(3):
                self.assertRaises(POSKeyError, s.loadBefore, oid, MAX_TID)
        check() # FileStorage
        c.db().close()
        importer = {'zodb': [('root', cfg)]}
        with NEOCluster(importer=importer) as cluster:
            storage = cluster.storage
            dm = storage.dm
            with storage.patchDeferred(dm._finished):
                with storage.patchDeferred(dm.doOperation):
                    cluster.start()
                    s = cluster.getZODBStorage()
                    check() # before import
                self.tic()
                check() # imported, Importer getObject
            self.tic()
            check() # imported, direct getObject


if __name__ == "__main__":
    unittest.main()
