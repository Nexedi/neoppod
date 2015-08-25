#
# Copyright (C) 2006-2015  Nexedi SA
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

from functools import wraps
from .locking import Lock, Empty
EMPTY = {}
NOBODY = []

class ForgottenPacket(object):
    """
      Instances of this class will be pushed to queue when an expected answer
      is being forgotten. Its purpose is similar to pushing "None" when
      connection is closed, but the meaning is different.
    """
    def __init__(self, msg_id):
        self.msg_id = msg_id

    def getId(self):
        return self.msg_id

def giant_lock(func):
    def wrapped(self, *args, **kw):
        self.lock_acquire()
        try:
            return func(self, *args, **kw)
        finally:
            self.lock_release()
    return wraps(func)(wrapped)

class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self):
        self.message_table = {}
        self.queue_dict = {}
        lock = Lock()
        self.lock_acquire = lock.acquire
        self.lock_release = lock.release

    @giant_lock
    def dispatch(self, conn, msg_id, packet, kw):
        """
        Retrieve register-time provided queue, and put conn and packet in it.
        """
        queue = self.message_table.get(id(conn), EMPTY).pop(msg_id, None)
        if queue is None:
            return False
        elif queue is NOBODY:
            return True
        self._decrefQueue(queue)
        queue.put((conn, packet, kw))
        return True

    def _decrefQueue(self, queue):
        queue_id = id(queue)
        queue_dict = self.queue_dict
        if queue_dict[queue_id] == 1:
            queue_dict.pop(queue_id)
        else:
            queue_dict[queue_id] -= 1

    def _increfQueue(self, queue):
        queue_id = id(queue)
        queue_dict = self.queue_dict
        try:
            queue_dict[queue_id] += 1
        except KeyError:
            queue_dict[queue_id] = 1

    @giant_lock
    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply."""
        self.message_table.setdefault(id(conn), {})[msg_id] = queue
        self._increfQueue(queue)

    def unregister(self, conn):
        """ Unregister a connection and put fake packet in queues to unlock
        threads excepting responses from that connection """
        self.lock_acquire()
        try:
            message_table = self.message_table.pop(id(conn), EMPTY)
        finally:
            self.lock_release()
        notified_set = set()
        _decrefQueue = self._decrefQueue
        for queue in message_table.itervalues():
            if queue is NOBODY:
                continue
            queue_id = id(queue)
            if queue_id not in notified_set:
                queue.put((conn, None, None))
                notified_set.add(queue_id)
            _decrefQueue(queue)

    @giant_lock
    def forget(self, conn, msg_id):
        """ Forget about a specific message for a specific connection.
        Actually makes it "expected by nobody", so we know we can ignore it,
        and not detect it as an error. """
        message_table = self.message_table[id(conn)]
        queue = message_table[msg_id]
        if queue is NOBODY:
            raise KeyError, 'Already expected by NOBODY: %r, %r' % (
                conn, msg_id)
        queue.put((conn, ForgottenPacket(msg_id), None))
        self.queue_dict[id(queue)] -= 1
        message_table[msg_id] = NOBODY
        return queue

    @giant_lock
    def forget_queue(self, queue, flush_queue=True):
        """
        Forget all pending messages for given queue.
        Actually makes them "expected by nobody", so we know we can ignore
        them, and not detect it as an error.
        flush_queue (boolean, default=True)
            All packets in queue get flushed.
        """
        # XXX: expensive lookup: we iterate over the whole dict
        found = 0
        for message_table in self.message_table.itervalues():
            for msg_id, t_queue in message_table.iteritems():
                if queue is t_queue:
                    found += 1
                    message_table[msg_id] = NOBODY
        refcount = self.queue_dict.pop(id(queue), 0)
        if refcount != found:
            raise ValueError('We hit a refcount bug: %s queue uses ' \
                'expected, %s found' % (refcount, found))
        if flush_queue:
            get = queue.get
            while True:
                try:
                    get(block=False)
                except Empty:
                    break

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        return len(self.message_table.get(id(conn), EMPTY)) != 0

    @giant_lock
    def pending(self, queue):
        return not queue.empty() or self.queue_dict.get(id(queue), 0) > 0

