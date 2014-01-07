
#
# Copyright (C) 2009-2014  Nexedi SA
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

import unittest
try:
    from ZODB.tests.PackableStorage import PackableStorageWithOptionalGC
except ImportError:
    from ZODB.tests.PackableStorage import PackableStorage as \
        PackableStorageWithOptionalGC
from ZODB.tests.PackableStorage import PackableUndoStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from . import ZODBTestCase

class PackableTests(ZODBTestCase, StorageTestBase,
        PackableStorageWithOptionalGC, PackableUndoStorage):

    def setUp(self):
        super(PackableTests, self).setUp(cluster_kw={'adapter': 'MySQL'})

if __name__ == "__main__":
    suite = unittest.makeSuite(PackableTests, 'check')
    unittest.main(defaultTest='suite')

