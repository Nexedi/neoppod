#
# Copyright (C) 2006-2019  Nexedi SA
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

from collections import defaultdict
from .locking import Lock, Empty
EMPTY = {}
NOBODY = []

@apply
class _ConnectionClosed(object):

    handler_method_name = 'connectionClosed'
    _args = ()

    class getId(object):
        def __eq__(self, other):
            return True


class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self):
        self.message_table = defaultdict(dict)
        self.queue_dict = defaultdict(int)
        self.lock = Lock()

    def _decrefQueue(self, queue_id):
        queue_dict = self.queue_dict
        count = queue_dict.get(queue_id) - 1
        if count:
            queue_dict[queue_id] = count
        else:
            del queue_dict[queue_id]

    # For poll thread (connection lock already taken).

    def dispatch(self, conn, msg_id, packet, kw):
        """
        Retrieve register-time provided queue, and put conn and packet in it.
        """
        with self.lock:
            queue = self.message_table.get(id(conn), EMPTY).pop(msg_id, None)
            if queue is None:
                return False
            elif queue is NOBODY:
                return True
            # Queue before decref so that pending() does not need to lock
            # (imagine it's called between the following 2 lines).
            queue.put((conn, packet, kw))
            self._decrefQueue(id(queue))
        return True

    def unregister(self, conn):
        """ Unregister a connection and put fake packet in queues to unlock
        threads expecting responses from that connection """
        notified_set = set()
        _decrefQueue = self._decrefQueue
        with self.lock:
            message_table = self.message_table.pop(id(conn), EMPTY)
            for queue in message_table.itervalues():
                if queue is NOBODY:
                    continue
                queue_id = id(queue)
                # decref after like in dispatch()
                if queue_id not in notified_set:
                    queue.put((conn, _ConnectionClosed, EMPTY))
                    notified_set.add(queue_id)
                _decrefQueue(queue_id)

    # For worker threads.

    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply."""
        with self.lock:
            self.message_table[id(conn)][msg_id] = queue
            self.queue_dict[id(queue)] += 1

    def forget_queue(self, queue, flush_queue=True):
        """
        Forget all pending messages for given queue.
        Actually makes them "expected by nobody", so we know we can ignore
        them, and not detect it as an error.
        flush_queue (boolean, default=True)
            All packets in queue get flushed.
        """
        found = 0
        with self.lock:
            for message_table in self.message_table.itervalues():
                for msg_id, t_queue in message_table.iteritems():
                    if queue is t_queue:
                        found += 1
                        message_table[msg_id] = NOBODY
        # No other thread is going to handle this queue
        # so we don't need the lock anymore.
        refcount = self.queue_dict.pop(id(queue), 0)
        assert refcount == found, (refcount, found)
        if flush_queue:
            get = queue.get
            while True:
                try:
                    get(block=False)
                except Empty:
                    break

    def pending(self, queue):
        # Check queue_dict first so that we don't need to lock
        # (imagine other methods are called between the 2 conditions).
        return self.queue_dict.get(id(queue), 0) > 0 or not queue.empty()

