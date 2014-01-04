#
# Copyright (C) 2011  Nexedi SA
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

import threading
from neo.lib.locking import Lock, Empty
from collections import deque
from ZODB.POSException import StorageTransactionError

class SimpleQueue(object):
    """
    Similar to Queue.Queue but with simpler locking scheme, reducing lock
    contention on "put" (benchmark shows 60% less time spent in "put").
    As a result:
    - only a single consumer possible ("get" vs. "get" race condition)
    - only a single producer possible ("put" vs. "put" race condition)
    - no blocking size limit possible
    - no consumer -> producer notifications (task_done/join API)

    Queue is on the critical path: any moment spent here increases client
    application wait for object data, transaction completion, etc.
    As we have a single consumer (client application's thread) and a single
    producer (lib.dispatcher, which can be called from several threads but
    serialises calls internally) for each queue, Queue.Queue's locking scheme
    can be relaxed to reduce latency.
    """
    __slots__ = ('_lock', '_unlock', '_popleft', '_append', '_queue')
    def __init__(self):
        lock = Lock()
        self._lock = lock.acquire
        self._unlock = lock.release
        self._queue = queue = deque()
        self._popleft = queue.popleft
        self._append = queue.append

    def get(self, block):
        if block:
            self._lock(False)
        while True:
            try:
                return self._popleft()
            except IndexError:
                if not block:
                    raise Empty
                self._lock()

    def put(self, item):
        self._append(item)
        self._lock(False)
        self._unlock()

    def empty(self):
        return not self._queue

class ThreadContainer(threading.local):

    def __init__(self):
        self.queue = SimpleQueue()
        self.answer = None

class TransactionContainer(dict):

    def pop(self, txn):
        return dict.pop(self, id(txn), None)

    def get(self, txn):
        try:
            return self[id(txn)]
        except KeyError:
            raise StorageTransactionError("unknown transaction %r" % txn)

    def new(self, txn):
        key = id(txn)
        if key in self:
            raise StorageTransactionError("commit of transaction %r"
                                          " already started" % txn)
        context = self[key] = {
            'queue': SimpleQueue(),
            'txn': txn,
            'ttid': None,
            'data_dict': {},
            'data_size': 0,
            'cache_dict': {},
            'cache_size': 0,
            'object_base_serial_dict': {},
            'object_serial_dict': {},
            'object_stored_counter_dict': {},
            'conflict_serial_dict': {},
            'resolved_conflict_serial_dict': {},
            'involved_nodes': set(),
        }
        return context
