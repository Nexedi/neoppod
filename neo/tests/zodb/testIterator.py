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

import unittest
from ZODB.tests.IteratorStorage import IteratorStorage
from ZODB.tests.IteratorStorage import ExtendedIteratorStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from . import ZODBTestCase

class IteratorTests(ZODBTestCase, StorageTestBase, IteratorStorage,
        ExtendedIteratorStorage):
    pass

if __name__ == "__main__":
    suite = unittest.makeSuite(IteratorTests, 'check')
    unittest.main(defaultTest='suite')

