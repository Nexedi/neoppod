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

def giant_lock(func):
    def wrapped(self, *args, **kw):
        self.lock_acquire()
        try:
            return func(self, *args, **kw)
        finally:
            self.lock_release()
    return wrapped

class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self):
        self.message_table = {}
        lock = Lock()
        self.lock_acquire = lock.acquire
        self.lock_release = lock.release

    @giant_lock
    def pop(self, conn, msg_id, default=MARKER):
        """Retrieve register-time provided payload."""
        result = self.message_table.get(id(conn), EMPTY).pop(msg_id, default)
        if result is MARKER:
            raise KeyError, (id(conn), msg_id)
        return result

    @giant_lock
    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply."""
        self.message_table.setdefault(id(conn), {})[msg_id] = queue

    def unregister(self, conn):
        """ Unregister a connection and put fake packet in queues to unlock
        threads excepting responses from that connection """
        self.lock_acquire()
        try:
            message_table = self.message_table.pop(id(conn), EMPTY)
        finally:
            self.lock_release()
        notified_set = set()
        for queue in message_table.itervalues():
            queue_id = id(queue)
            if queue_id not in notified_set:
                queue.put((conn, None))
                notified_set.add(queue_id)

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        return len(self.message_table.get(id(conn), EMPTY)) != 0

