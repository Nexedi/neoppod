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
from neo.profiling import profiler_decorator
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
    return wrapped

class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self, poll_thread=None):
        self.message_table = {}
        self.queue_dict = {}
        lock = Lock()
        self.lock_acquire = lock.acquire
        self.lock_release = lock.release
        self.poll_thread = poll_thread

    @giant_lock
    @profiler_decorator
    def dispatch(self, conn, msg_id, data):
        """Retrieve register-time provided queue, and put data in it."""
        queue = self.message_table.get(id(conn), EMPTY).pop(msg_id, None)
        if queue is None:
            return False
        elif queue is NOBODY:
            return True
        self.queue_dict[id(queue)] -= 1
        queue.put(data)
        return True

    def needPollThread(self):
        thread = self.poll_thread
        # If thread has been stopped, wait for it to stop
        # Note: This is not, ironically, thread safe: if one thread is
        # stopping poll thread while we are checking its state here, a
        # race condition will occur. If safety is required, locks should
        # be added to control the access to thread's "start", "stopping"
        # and "stop" methods.
        if thread.stopping():
            # XXX: ideally, we should wake thread up here, to be sure not
            # to wait forever.
            thread.join()
        if not thread.isAlive():
            thread.start()

    @giant_lock
    @profiler_decorator
    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply."""
        if self.poll_thread is not None:
            self.needPollThread()
        self.message_table.setdefault(id(conn), {})[msg_id] = queue
        queue_dict = self.queue_dict
        key = id(queue)
        try:
            queue_dict[key] += 1
        except KeyError:
            queue_dict[key] = 1

    @profiler_decorator
    def unregister(self, conn):
        """ Unregister a connection and put fake packet in queues to unlock
        threads excepting responses from that connection """
        self.lock_acquire()
        try:
            message_table = self.message_table.pop(id(conn), EMPTY)
        finally:
            self.lock_release()
        notified_set = set()
        queue_dict = self.queue_dict
        for queue in message_table.itervalues():
            if queue is NOBODY:
                continue
            queue_id = id(queue)
            if queue_id not in notified_set:
                queue.put((conn, None))
                notified_set.add(queue_id)
            queue_dict[queue_id] -= 1

    @giant_lock
    @profiler_decorator
    def forget(self, conn, msg_id):
        """ Forget about a specific message for a specific connection.
        Actually makes it "expected by nobody", so we know we can ignore it,
        and not detect it as an error. """
        message_table = self.message_table[id(conn)]
        queue = message_table[msg_id]
        if queue is NOBODY:
            raise KeyError, 'Already expected by NOBODY: %r, %r' % (
                conn, msg_id)
        queue.put((conn, ForgottenPacket(msg_id)))
        self.queue_dict[id(queue)] -= 1
        message_table[msg_id] = NOBODY
        return queue

    @profiler_decorator
    def registered(self, conn):
        """Check if a connection is registered into message table."""
        return len(self.message_table.get(id(conn), EMPTY)) != 0

    @giant_lock
    @profiler_decorator
    def pending(self, queue):
        return not queue.empty() or self.queue_dict.get(id(queue), 0) > 0

