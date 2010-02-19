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
from ZODB.tests.ReadOnlyStorage import ReadOnlyStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from neo.tests.zodb import ZODBTestCase

class ReadOnlyTests(ZODBTestCase, StorageTestBase, ReadOnlyStorage):
    pass

if __name__ == "__main__":
    suite = unittest.makeSuite(ReadOnlyTests, 'check')
    unittest.main(defaultTest='suite')

