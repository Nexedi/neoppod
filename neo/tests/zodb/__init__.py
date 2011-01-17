#
# Copyright (C) 2009-2010  Nexedi SA
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

import os
import unittest

from neo.tests.functional import NEOCluster, NEOFunctionalTest
import neo.lib

class ZODBTestCase(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        masters = int(os.environ.get('NEO_TEST_ZODB_MASTERS', 1))
        storages = int(os.environ.get('NEO_TEST_ZODB_STORAGES', 1))
        replicas = int(os.environ.get('NEO_TEST_ZODB_REPLICAS', 0))
        partitions = int(os.environ.get('NEO_TEST_ZODB_PARTITIONS', 1))
        self.neo = NEOCluster(
            db_list=['test_neo%d' % x for x in xrange(storages)],
            partitions=partitions,
            replicas=replicas,
            master_node_count=masters,
            temp_dir=self.getTempDirectory(),
        )
        self.neo.start()
        self._storage = self.neo.getZODBStorage()

    def tearDown(self):
        self._storage.cleanup()
        self.neo.stop()
        # Deconfigure client logger
        neo.setupLog('CLIENT')
        NEOFunctionalTest.tearDown(self)

    def open(self, read_only=False):
        # required for some tests (see PersitentTests), no-op for NEO ?
        self._storage._is_read_only = read_only

