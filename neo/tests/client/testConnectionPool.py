#
# Copyright (C) 2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import unittest
from mock import Mock

from neo.tests import NeoTestBase
from neo.client.app import ConnectionPool

class ConnectionPoolTests(NeoTestBase):

    def test_removeConnection(self):
        app = None
        pool = ConnectionPool(app)
        test_node_uuid = self.getNewUUID()
        other_node_uuid = test_node_uuid
        while other_node_uuid == test_node_uuid:
            other_node_uuid = self.getNewUUID()
        test_node = Mock({'getUUID': test_node_uuid})
        other_node = Mock({'getUUID': other_node_uuid})
        # Test sanity check
        self.assertEqual(getattr(pool, 'connection_dict', None), {})
        # Call must not raise if node is not known
        self.assertEqual(len(pool.connection_dict), 0)
        pool.removeConnection(test_node)
        # Test that removal with another uuid doesn't affect entry
        pool.connection_dict[test_node_uuid] = None
        self.assertEqual(len(pool.connection_dict), 1)
        pool.removeConnection(other_node)
        self.assertEqual(len(pool.connection_dict), 1)
        # Test that removeConnection works
        pool.removeConnection(test_node)
        self.assertEqual(len(pool.connection_dict), 0)

    # TODO: test getConnForNode (requires splitting complex functionalities)

if __name__ == '__main__':
    unittest.main()

