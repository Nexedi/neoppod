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

from neo.client.dispatcher import Dispatcher

class DispatcherTests(unittest.TestCase):

    def test_register(self):
        dispatcher = Dispatcher()
        conn = []
        other_conn = []
        queue = []
        test_message_id = 1
        class Packet:
            def __init__(self, msg_id):
                self.msg_id = msg_id

            def getId(self):
                return self.msg_id
        packet = Packet(test_message_id)
        other_packet = Packet(test_message_id + 1)
        # Check that unregistered message is detected as unregistered
        self.assertEqual(dispatcher.getQueue(conn, packet), None)
        self.assertFalse(dispatcher.registered(conn))
        # Register (must not raise)
        dispatcher.register(conn, test_message_id, queue)
        # Check that connection is detected as registered
        self.assertTrue(dispatcher.registered(conn))
        # Check that variations don't get detected as registered
        self.assertFalse(dispatcher.registered(other_conn))
        self.assertEqual(dispatcher.getQueue(other_conn, packet), None)
        self.assertEqual(dispatcher.getQueue(conn, other_packet), None)
        self.assertEqual(dispatcher.getQueue(other_conn, other_packet), None)
        # Check that queue is detected as registered.
        # This unregisters the message...
        self.assertTrue(dispatcher.getQueue(conn, packet) is queue)
        # so check again that unregistered message is detected as unregistered
        self.assertEqual(dispatcher.getQueue(conn, packet), None)
        self.assertFalse(dispatcher.registered(conn))

if __name__ == '__main__':
    unittest.main()

