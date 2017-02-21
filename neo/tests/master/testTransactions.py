#
# Copyright (C) 2006-2017  Nexedi SA
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
from ..mock import Mock
from struct import pack
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes
from neo.lib.util import packTID, unpackTID, addTID
from neo.master.transactions import TransactionManager

class testTransactionManager(NeoUnitTestBase):

    def makeNode(self, node_type):
        uuid = self.getNewUUID(node_type)
        node = Mock({'getUUID': uuid, '__hash__': uuid, '__repr__': 'FakeNode'})
        return uuid, node

    def testTIDUtils(self):
        """
        Tests packTID/unpackTID/addTID.
        """
        min_tid = pack('!LL', 0, 0)
        min_unpacked_tid = ((1900, 1, 1, 0, 0), 0)
        max_tid = pack('!LL', 2**32 - 1, 2 ** 32 - 1)
        # ((((9917 - 1900) * 12 + (10 - 1)) * 31 + (14 - 1)) * 24 + 4) * 60 +
        # 15 == 2**32 - 1
        max_unpacked_tid = ((9917, 10, 14, 4, 15), 2**32 - 1)

        self.assertEqual(unpackTID(min_tid), min_unpacked_tid)
        self.assertEqual(unpackTID(max_tid), max_unpacked_tid)
        self.assertEqual(packTID(*min_unpacked_tid), min_tid)
        self.assertEqual(packTID(*max_unpacked_tid), max_tid)

        self.assertEqual(addTID(min_tid, 1), pack('!LL', 0, 1))
        self.assertEqual(addTID(pack('!LL', 0, 2**32 - 1), 1),
            pack('!LL', 1, 0))
        self.assertEqual(addTID(pack('!LL', 0, 2**32 - 1), 2**32 + 1),
            pack('!LL', 2, 0))
        # Check impossible dates are avoided (2010/11/31 doesn't exist)
        self.assertEqual(
            unpackTID(addTID(packTID((2010, 11, 30, 23, 59), 2**32 - 1), 1)),
            ((2010, 12, 1, 0, 0), 0))

    def testClientDisconectsAfterBegin(self):
        client_uuid1, node1 = self.makeNode(NodeTypes.CLIENT)
        tm = TransactionManager(None)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tm.begin(node1, 0, tid1)
        tm.clientLost(node1)
        self.assertTrue(tid1 not in tm)

if __name__ == '__main__':
    unittest.main()
