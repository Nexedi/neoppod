
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

from ZODB.tests.PackableStorage import \
    PackableStorageWithOptionalGC, PackableUndoStorage
from ZODB.tests.StorageTestBase import StorageTestBase

from .. import expectedFailure, Patch
from . import ZODBTestCase

class PackableTests(ZODBTestCase, StorageTestBase,
        PackableStorageWithOptionalGC, PackableUndoStorage):

    testPackAllRevisions = expectedFailure()
    testPackUndoLog = expectedFailure()

    def testPackAllRevisionsNoGC(orig):
        def pack(orig, t, referencesf, gc):
            assert referencesf is not None
            assert gc is False
            return orig(t)
        def test(self):
            with Patch(self._storage, pack=pack):
                orig(self)
        return test
