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

import unittest
import ZODB

from neo.tests.functional import NEOCluster, NEOFunctionalTest

class ZODBTestCase(NEOFunctionalTest):

    def setUp(self):
        NEOFunctionalTest.setUp(self)
        self.neo = NEOCluster(['test_neo1'],
                partitions=1, replicas=0, port_base=20000,
                master_node_count=1, temp_dir=self.getTempDirectory())
        self.neo.stop()
        self.neo.setupDB()
        self.neo.start()
        self._storage = self.neo.getZODBStorage()
        self._db = ZODB.DB(self._storage)

    def tearDown(self):
        self._db.close()
        self._storage.cleanup()
        self.neo.stop()

    def open(self, read_only=False):
        # required for some tests (see PersitentTests), no-op for NEO ?
        self._storage._is_read_only = read_only

