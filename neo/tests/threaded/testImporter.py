#
# Copyright (C) 2014-2015  Nexedi SA
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

from collections import deque
from cPickle import Pickler, Unpickler
from cStringIO import StringIO
from itertools import islice, izip_longest
import os, time, unittest
import neo, transaction, ZODB
from neo.lib import logging
from neo.lib.util import u64
from neo.storage.database.importer import Repickler
from ..fs2zodb import Inode
from .. import getTempDirectory
from . import NEOCluster, NEOThreadedTest
from ZODB.FileStorage import FileStorage


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

    def test(self):
        importer = []
        fs_dir = os.path.join(getTempDirectory(), self.id())
        os.mkdir(fs_dir)
        src_root, = neo.__path__
        fs_list = "root", "client", "master", "tests"
        def root_filter(name):
            if not name.endswith(".pyc"):
                i = name.find(os.sep)
                return i < 0 or name[:i] not in fs_list
        def sub_filter(name):
            return lambda n: n[-4:] != '.pyc' and \
                n.split(os.sep, 1)[0] in (name, "scripts")
        conn_list = []
        iter_list = []
        # Setup several FileStorage databases.
        for i, name in enumerate(fs_list):
            fs_path = os.path.join(fs_dir, name + ".fs")
            c = ZODB.DB(FileStorage(fs_path)).open()
            r = c.root()["neo"] = Inode()
            transaction.commit()
            conn_list.append(c)
            iter_list.append(r.treeFromFs(src_root, 10,
                sub_filter(name) if i else root_filter))
            importer.append((name, {
                "storage": "<filestorage>\npath %s\n</filestorage>" % fs_path
                }))
        # Populate FileStorage databases.
        for iter_list in izip_longest(*iter_list):
            for i in iter_list:
                if i:
                    transaction.commit()
        del iter_list
        # Get oids of mount points and close.
        for (name, cfg), c in zip(importer, conn_list):
            r = c.root()["neo"]
            if name == "root":
                for name in fs_list[1:]:
                    cfg[name] = str(u64(r[name]._p_oid))
            else:
                cfg["oid"] = str(u64(r[name]._p_oid))
            c.db().close()
        #del importer[0][1][importer.pop()[0]]
        # Start NEO cluster with transparent import of a multi-base ZODB.
        cluster = NEOCluster(compress=False, importer=importer)
        try:
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
            r = c.root()["neo"]
            self.assertRaisesRegexp(NotImplementedError, " getObjectHistory$",
                                    c.db().history, r._p_oid)
            i = r.walk()
            next(islice(i, 9, None))
            dm.doOperation(cluster.storage) # resume
            deque(i, maxlen=0)
            last_import = None
            for i, r in enumerate(r.treeFromFs(src_root, 10)):
                t.commit()
                if cluster.storage.dm._import:
                    last_import = i
            self.tic()
            self.assertTrue(last_import and not cluster.storage.dm._import)
            i = len(src_root) + 1
            self.assertEqual(sorted(r.walk()), sorted(
                (x[i:] or '.', sorted(y), sorted(z))
                for x, y, z in os.walk(src_root)))
            t.commit()
        finally:
            cluster.stop()


if __name__ == "__main__":
    unittest.main()
