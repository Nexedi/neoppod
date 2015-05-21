#
# Copyright (C) 2009-2015  Nexedi SA
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

from .. import DB_PREFIX
functional = int(os.getenv('NEO_TEST_ZODB_FUNCTIONAL', 0))
if functional:
    from ..functional import NEOCluster, NEOFunctionalTest as TestCase
else:
    from ..threaded import NEOCluster, NEOThreadedTest as TestCase

class ZODBTestCase(TestCase):

    def setUp(self, cluster_kw={}):
        super(ZODBTestCase, self).setUp()
        storages = int(os.getenv('NEO_TEST_ZODB_STORAGES', 1))
        kw = {
            'master_count': int(os.getenv('NEO_TEST_ZODB_MASTERS', 1)),
            'replicas': int(os.getenv('NEO_TEST_ZODB_REPLICAS', 0)),
            'partitions': int(os.getenv('NEO_TEST_ZODB_PARTITIONS', 1)),
            'db_list': ['%s%u' % (DB_PREFIX, i) for i in xrange(storages)],
        }
        kw.update(cluster_kw)
        if functional:
            kw['temp_dir'] = self.getTempDirectory()
        self.neo = NEOCluster(**kw)
        self.neo.start()
        self.open()

    def _tearDown(self, success):
        self._storage.cleanup()
        self.neo.stop()
        del self.neo, self._storage
        super(ZODBTestCase, self)._tearDown(success)

    assertEquals = failUnlessEqual = TestCase.assertEqual
    assertNotEquals = failIfEqual = TestCase.assertNotEqual

    def open(self, **kw):
        self._storage = self.neo.getZODBStorage(**kw)
