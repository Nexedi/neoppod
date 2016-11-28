#
# Copyright (C) 2009-2016  Nexedi SA
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

from . import NeoTestBase
from neo.lib.dispatcher import Dispatcher, ForgottenPacket
from Queue import Queue
import unittest

class DispatcherTests(NeoTestBase):

    def setUp(self):
        NeoTestBase.setUp(self)
        self.dispatcher = Dispatcher()

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

