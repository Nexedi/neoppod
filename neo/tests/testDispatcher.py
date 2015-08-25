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

from mock import Mock
from . import NeoTestBase
from neo.lib.dispatcher import Dispatcher, ForgottenPacket
from Queue import Queue
import unittest

class DispatcherTests(NeoTestBase):

    def setUp(self):
        NeoTestBase.setUp(self)
        self.dispatcher = Dispatcher()

    def testRegister(self):
        conn = object()
        queue = Queue()
        MARKER = object()
        self.dispatcher.register(conn, 1, queue)
        self.assertTrue(queue.empty())
        self.assertTrue(self.dispatcher.dispatch(conn, 1, MARKER, {}))
        self.assertFalse(queue.empty())
        self.assertEqual(queue.get(block=False), (conn, MARKER, {}))
        self.assertTrue(queue.empty())
        self.assertFalse(self.dispatcher.dispatch(conn, 2, None, {}))

    def testUnregister(self):
        conn = object()
        queue = Mock()
        self.dispatcher.register(conn, 2, queue)
        self.dispatcher.unregister(conn)
        self.assertEqual(len(queue.mockGetNamedCalls('put')), 1)
        self.assertFalse(self.dispatcher.dispatch(conn, 2, None, {}))

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

        self.dispatcher.dispatch(conn1, 1, None, {})
        self.assertTrue(self.dispatcher.pending(queue1))
        self.assertTrue(self.dispatcher.pending(queue2))
        self.dispatcher.dispatch(conn2, 2, None, {})
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
        # ...and forget about it, returning registered queue
        forgotten_queue = self.dispatcher.forget(conn, 1)
        self.assertTrue(queue is forgotten_queue, (queue, forgotten_queue))
        # A ForgottenPacket must have been put in the queue
        queue_conn, packet, kw = queue.get(block=False)
        self.assertTrue(isinstance(packet, ForgottenPacket), packet)
        # ...with appropriate packet id
        self.assertEqual(packet.getId(), 1)
        # ...and appropriate connection
        self.assertTrue(conn is queue_conn, (conn, queue_conn))
        # If forgotten twice, it must raise a KeyError
        self.assertRaises(KeyError, self.dispatcher.forget, conn, 1)
        # Event arrives, return value must be True (it was expected)
        self.assertTrue(self.dispatcher.dispatch(conn, 1, MARKER, {}))
        # ...but must not have reached the queue
        self.assertTrue(queue.empty())

        # Register an expectation
        self.dispatcher.register(conn, 1, queue)
        # ...and forget about it
        self.dispatcher.forget(conn, 1)
        queue.get(block=False)
        # No exception must happen if connection is lost.
        self.dispatcher.unregister(conn)
        # Forgotten message's queue must not have received a "None"
        self.assertTrue(queue.empty())

if __name__ == '__main__':
    unittest.main()

