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

from mock import Mock
from neo.dispatcher import Dispatcher
from Queue import Queue

class DispatcherTests(unittest.TestCase):

    def setUp(self):
        self.dispatcher = Dispatcher()

    def testRegister(self):
        conn = object()
        queue = Queue()
        MARKER = object()
        self.dispatcher.register(conn, 1, queue)
        self.assertTrue(queue.empty())
        self.assertTrue(self.dispatcher.dispatch(conn, 1, MARKER))
        self.assertFalse(queue.empty())
        self.assertTrue(queue.get(block=False) is MARKER)
        self.assertTrue(queue.empty())
        self.assertFalse(self.dispatcher.dispatch(conn, 2, None))

    def testUnregister(self):
        conn = object()
        queue = Mock()
        self.dispatcher.register(conn, 2, queue)
        self.dispatcher.unregister(conn)
        self.assertEqual(len(queue.mockGetNamedCalls('put')), 1)
        self.assertFalse(self.dispatcher.dispatch(conn, 2, None))

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

    def testPending(self):
        conn1 = object()
        conn2 = object()
        class Queue(object):
            _empty = True

            def empty(self):
                return self._empty

            def put(self, value):
                pass
        queue1 = Queue()
        queue2 = Queue()
        self.dispatcher.register(conn1, 1, queue1)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.dispatcher.register(conn2, 2, queue1)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.dispatcher.register(conn2, 3, queue2)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.assertTrue(self.dispatcher.pending(queue2))

        self.dispatcher.dispatch(conn1, 1, None)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.assertTrue(self.dispatcher.pending(queue2))
        self.dispatcher.dispatch(conn2, 2, None)
        self.assertFalse(self.dispatcher.pending(queue1))
        self.assertTrue(self.dispatcher.pending(queue2))

        queue1._empty = False
        self.assertTrue(self.dispatcher.pending(queue1))
        queue1._empty = True

        self.dispatcher.register(conn1, 4, queue1)
        self.dispatcher.register(conn2, 5, queue1)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.assertTrue(self.dispatcher.pending(queue2))

        self.dispatcher.unregister(conn2)
        self.assertTrue(self.dispatcher.pending(queue1))
        self.assertFalse(self.dispatcher.pending(queue2))
        self.dispatcher.unregister(conn1)
        self.assertFalse(self.dispatcher.pending(queue1))
        self.assertFalse(self.dispatcher.pending(queue2))

    def testForget(self):
        conn = object()
        queue = Queue()
        MARKER = object()
        # Register an expectation
        self.dispatcher.register(conn, 1, queue)
        # ...and forget about it
        self.dispatcher.forget(conn, 1)
        # If forgotten twice, it must raise a KeyError
        self.assertRaises(KeyError, self.dispatcher.forget, conn, 1)
        # Event arrives, return value must be True (it was expected)
        self.assertTrue(self.dispatcher.dispatch(conn, 1, MARKER))
        # ...but must not have reached the queue
        self.assertTrue(queue.empty())

if __name__ == '__main__':
    unittest.main()

