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
import unittest
import ZODB

from ZODB.tests.RecoveryStorage import RecoveryStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from ..functional import NEOCluster
from . import ZODBTestCase

class RecoveryTests(ZODBTestCase, StorageTestBase, RecoveryStorage):

    def setUp(self):
        super(RecoveryTests, self).setUp()
        dst_temp_dir = self.getTempDirectory() + '-dst'
        if not os.path.exists(dst_temp_dir):
            os.makedirs(dst_temp_dir)
        self.neo_dst = NEOCluster(['test_neo1-dst'], partitions=1, replicas=0,
                master_count=1, temp_dir=dst_temp_dir)
        self.neo_dst.stop()
        self.neo_dst.setupDB()
        self.neo_dst.start()
        self._dst = self.neo.getZODBStorage()
        self._dst_db = ZODB.DB(self._dst)

    def _tearDown(self, success):
        super(RecoveryTests, self)._tearDown(success)
        self._dst_db.close()
        self._dst.cleanup()
        self.neo_dst.stop()

if __name__ == "__main__":
    suite = unittest.makeSuite(RecoveryTests, 'check')
    unittest.main(defaultTest='suite')

