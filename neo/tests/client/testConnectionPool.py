#
# Copyright (C) 2009-2017  Nexedi SA
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

import time, unittest
from ..mock import Mock

from .. import NeoUnitTestBase
from neo.client.app import ConnectionPool
from neo.client.exception import NEOStorageError
from neo.client import pool
from neo.lib.util import p64

class ConnectionPoolTests(NeoUnitTestBase):

    # TODO: test getConnForNode (requires splitting complex functionalities)

    def test_CellSortKey(self):
        cp = ConnectionPool(None)
        node_uuid_1 = self.getStorageUUID()
        node_uuid_2 = self.getStorageUUID()
        node_uuid_3 = self.getStorageUUID()
        # We are connected to node 1
        cp.connection_dict[node_uuid_1] = None
        def uuid_now(func, uuid, now):
            pool.time = Mock({'time': now})
            try:
                return func(Mock({'getUUID': uuid}))
            finally:
                pool.time = time
        # A connection to node 3 failed, will be forgotten at 5
        uuid_now(cp.notifyFailure, node_uuid_3, 5 - pool.MAX_FAILURE_AGE)
        def getCellSortKey(*args):
            return uuid_now(cp.getCellSortKey, *args)
        # At 0, key values are not ambiguous
        self.assertTrue(getCellSortKey(node_uuid_1, 0) < getCellSortKey(
            node_uuid_2, 0) < getCellSortKey(node_uuid_3, 0))
        # At 10, nodes 2 and 3 have the same key value
        self.assertTrue(getCellSortKey(node_uuid_1, 10) < getCellSortKey(
            node_uuid_2, 10))
        self.assertEqual(getCellSortKey(node_uuid_2, 10), getCellSortKey(
            node_uuid_3, 10))

    def test_iterateForObject_noStorageAvailable(self):
        # no node available
        oid = p64(1)
        app = Mock()
        app.pt = Mock({'getCellList': []})
        pool = ConnectionPool(app)
        self.assertRaises(NEOStorageError, pool.iterateForObject(oid).next)

if __name__ == '__main__':
    unittest.main()

