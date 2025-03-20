#
# Copyright (C) 2009-2019  Nexedi SA
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

from neo.client.app import Application as ClientApplication, TXN_PACK_DESC
from .. import DB_PREFIX, Patch
functional = int(os.getenv('NEO_TEST_ZODB_FUNCTIONAL', 0))
if functional:
    from ..functional import NEOCluster, NEOFunctionalTest as TestCase
else:
    from ..threaded import NEOCluster, NEOThreadedTest
    x = dict.fromkeys(x for x in dir(NEOThreadedTest) if x.startswith('check'))
    assert x
    TestCase = type('', (NEOThreadedTest,), x)
    del x

class ZODBTestCase(TestCase):

    def undoLog(orig, *args, **kw):
        return [txn for txn in orig(*args, **kw)
                    if txn['description'] != TXN_PACK_DESC]

    _patches = (
        Patch(ClientApplication, undoLog=undoLog),
        Patch(ClientApplication, wait_for_pack=True),
    )
    del undoLog

    def setUp(self):
        super(ZODBTestCase, self).setUp()
        storages = int(os.getenv('NEO_TEST_ZODB_STORAGES', 1))
        kw = {
            'master_count': int(os.getenv('NEO_TEST_ZODB_MASTERS', 1)),
            'replicas': int(os.getenv('NEO_TEST_ZODB_REPLICAS', 0)),
            'partitions': int(os.getenv('NEO_TEST_ZODB_PARTITIONS', 1)),
            'db_list': ['%s%u' % (DB_PREFIX, i) for i in xrange(storages)],
        }
        if functional:
            kw['temp_dir'] = self.getTempDirectory()
        self.neo = NEOCluster(**kw)
        for p in self._patches:
            p.apply()

    def __init__(self, methodName):
        super(ZODBTestCase, self).__init__(methodName)
        test = getattr(self, methodName).__func__
        def runTest():
            failed = True
            try:
                self.neo.start()
                self.open()
                test(self)
                if functional:
                    dm = self._getDatabaseManager()
                    try:
                        dm.lock.acquire()
                        @self.neo.expectCondition
                        def _(last_try):
                            dm.commit()
                            dm.setup()
                            x = dm.nonempty('todel'), dm._uncommitted_data
                            return not any(x), x
                        orphan = dm.getOrphanList()
                    finally:
                        dm.close()
                else:
                    self.neo.ticAndJoinStorageTasks()
                    with self.neo.storage.ignoreDmLock(True) as dm:
                        orphan = dm.getOrphanList()
                failed = False
            finally:
                self.close()
                if functional:
                    self.neo.stop(ignore_errors=failed)
                else:
                    self.neo.stop(None)
            self.assertFalse(orphan)
        setattr(self, methodName, runTest)

    def _tearDown(self, success):
        for p in self._patches:
            p.revert()
        del self.neo
        super(ZODBTestCase, self)._tearDown(success)

    assertEquals = failUnlessEqual = TestCase.assertEqual
    assertNotEquals = failIfEqual = TestCase.assertNotEqual

    if functional:
        def _getDatabaseManager(self):
            return self.neo.getSQLConnection(*self.neo.db_list)

    def open(self, **kw):
        self._open(_storage=self.neo.getZODBStorage(**kw))

    def _open(self, **kw):
        self.close()
        (attr, value), = kw.iteritems()
        setattr(self, attr, value)
        def close():
            getattr(self, attr).close()
            delattr(self, attr)
            del self.close
        self.close = close

    def close(self):
        pass
