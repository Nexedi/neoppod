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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import unittest

from mock import Mock
from neo.dispatcher import Dispatcher

class DispatcherTests(unittest.TestCase):

    def setUp(self):
        self.dispatcher = Dispatcher()

    def testRegister(self):
        conn = object()
        self.dispatcher.register(conn, 1, 0)
        self.assertEqual(self.dispatcher.pop(conn, 1, None), 0)
        self.assertEqual(self.dispatcher.pop(conn, 2, 3), 3)

    def testUnregister(self):
        conn = object()
        queue = Mock()
        self.dispatcher.register(conn, 2, queue)
        self.dispatcher.unregister(conn)
        self.assertEqual(len(queue.mockGetNamedCalls('put')), 1)
        self.assertEqual(self.dispatcher.pop(conn, 2, 3), 3)

    def testRegistered(self):
        conn1 = object()
        conn2 = object()
        self.assertFalse(self.dispatcher.registered(conn1))
        self.assertFalse(self.dispatcher.registered(conn2))
        self.dispatcher.register(conn1, 1, Mock())
        self.assertTrue(self.dispatcher.registered(conn1))
        self.assertFalse(self.dispatcher.registered(conn2))
        self.dispatcher.register(conn2, 2, Mock())
        self.assertTrue(self.dispatcher.registered(conn1))
        self.assertTrue(self.dispatcher.registered(conn2))
        self.dispatcher.unregister(conn1)
        self.assertFalse(self.dispatcher.registered(conn1))
        self.assertTrue(self.dispatcher.registered(conn2))
        self.dispatcher.unregister(conn2)
        self.assertFalse(self.dispatcher.registered(conn1))
        self.assertFalse(self.dispatcher.registered(conn2))


if __name__ == '__main__':
    unittest.main()

