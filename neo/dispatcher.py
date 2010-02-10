#
# Copyright (C) 2006-2010  Nexedi SA
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

from neo.locking import Lock
MARKER = []
EMPTY = {}

# This class is thread safe:
# - pop:
#   We don't modify outer mapping.
#   Inner mapping if accessed at most once by using a python primitive, so GIL
#   ensures atomicity.
# - register:
#   We protect modification in outer mapping by a lock.
#   Inner mapping is accessed at most once by using a python primitive ('='
#   operator), so GIL ensures atomicity.
# - unregister:
#   We protect modification in outer mapping by a lock.
#   Inner mapping access is done after detaching it from outer mapping in a
#   thread-safe manner, so access doesn't need to worry about concurency.
# - registered:
#   Nothing is modified in any structure, so there is not much to worry about
#   concurency here. Note though that, by nature (read-only), this method can
#   cause concurency problems in caller, depending on how it interprets the
#   return value.

class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self):
        self.message_table = {}
        lock = Lock()
        self.message_table_lock_acquire = lock.acquire
        self.message_table_lock_release = lock.release

    def pop(self, conn, msg_id, default=MARKER):
        """Retrieve register-time provided payload."""
        result = self.message_table.get(id(conn), EMPTY).pop(msg_id, default)
        if default is MARKER:
            raise KeyError, (id(conn), msg_id)
        return result

    def register(self, conn, msg_id, payload):
        """Register an expectation for a reply."""
        self.message_table_lock_acquire()
        try:
            message_table = self.message_table.setdefault(id(conn), {})
        finally:
            self.message_table_lock_release()
        message_table[msg_id] = payload

    def unregister(self, conn):
        """ Unregister a connection and put fake packet in queues to unlock
        threads excepting responses from that connection """
        self.message_table_lock_acquire()
        try:
            message_table = self.message_table.pop(id(conn), EMPTY)
        finally:
            self.message_table_lock_release()
        for queue in message_table.itervalues():
            queue.put((conn, None))

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        return len(self.message_table.get(id(conn), EMPTY)) != 0

