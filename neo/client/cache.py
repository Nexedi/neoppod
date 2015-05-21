#
# Copyright (C) 2011-2015  Nexedi SA
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

import math
from bisect import insort

class CacheItem(object):

    __slots__ = ('oid', 'tid', 'next_tid', 'data',
                 'counter', 'level', 'expire',
                 'prev', 'next')

    def __repr__(self):
        s = ''
        for attr in self.__slots__:
            try:
                value = getattr(self, attr)
                if value:
                    if attr in ('prev', 'next'):
                        s += ' %s=<...>' % attr
                        continue
                    elif attr == 'data':
                        value = '...'
                s += ' %s=%r' % (attr, value)
            except AttributeError:
                pass
        return '<%s%s>' % (self.__class__.__name__, s)

    def __lt__(self, other):
        return self.tid < other.tid

class ClientCache(object):
    """In-memory pickle cache based on Multi-Queue cache algorithm

      Multi-Queue algorithm for Second Level Buffer Caches:
      http://www.usenix.org/event/usenix01/full_papers/zhou/zhou_html/index.html

      Quick description:
      - There are multiple "regular" queues, plus a history queue
      - The queue to store an object in depends on its access frequency
      - The queue an object is in defines its lifespan (higher-index queue eq.
        longer lifespan)
        -> The more often an object is accessed, the higher lifespan it will
           have
      - Upon cache or history hit, object frequency is increased and object
        might get moved to longer-lived queue
      - Each access "ages" objects in cache, and an aging object is moved to
        shorter-lived queue as it ages without being accessed, or in the
        history queue if it's really too old.
    """

    __slots__ = ('_life_time', '_max_history_size', '_max_size',
                 '_queue_list', '_oid_dict', '_time', '_size', '_history_size')

    def __init__(self, life_time=10000, max_history_size=100000,
                                        max_size=20*1024*1024):
        self._life_time = life_time
        self._max_history_size = max_history_size
        self._max_size = max_size
        self.clear()

    def clear(self):
        """Reset cache"""
        self._queue_list = [None] # first is history
        self._oid_dict = {}
        self._time = 0
        self._size = 0
        self._history_size = 0

    def _iterQueue(self, level):
        """for debugging purpose"""
        if level < len(self._queue_list):
            item = head = self._queue_list[level]
            if item:
                while 1:
                    yield item
                    item = item.next
                    if item is head:
                        break

    def _add(self, item):
        level = item.level
        try:
            head = self._queue_list[level]
        except IndexError:
            assert len(self._queue_list) == level
            self._queue_list.append(item)
            item.prev = item.next = item
        else:
            if head:
                item.prev = tail = head.prev
                tail.next = head.prev = item
                item.next = head
            else:
                self._queue_list[level] = item
                item.prev = item.next = item
        if level:
            item.expire = self._time + self._life_time
        else:
            self._size -= len(item.data)
            item.data = None
            if self._history_size < self._max_history_size:
                self._history_size += 1
            else:
                self._remove(head)
                item_list = self._oid_dict[head.oid]
                item_list.remove(head)
                if not item_list:
                    del self._oid_dict[head.oid]

    def _remove(self, item):
        level = item.level
        if level is not None:
            item.level = level - 1
            next = item.next
            if next is item:
                self._queue_list[level] = next = None
            else:
                item.prev.next = next
                next.prev = item.prev
                if self._queue_list[level] is item:
                    self._queue_list[level] = next
            return next

    def _fetched(self, item, _log=math.log):
        self._remove(item)
        item.counter = counter = item.counter + 1
        # XXX It might be better to adjust the level according to the object
        # size. See commented factor for example.
        item.level = 1 + int(_log(counter, 2)
                             # * (1.01 - float(len(item.data)) / self._max_size)
                            )
        self._add(item)

        self._time = time = self._time + 1
        for head in self._queue_list[1:]:
            if head and head.expire < time:
                self._remove(head)
                self._add(head)
                break

    def _load(self, oid, before_tid=None):
        item_list = self._oid_dict.get(oid)
        if item_list:
            if before_tid:
                for item in reversed(item_list):
                    if item.tid < before_tid:
                        next_tid = item.next_tid
                        if next_tid and next_tid < before_tid:
                            break
                        return item
            else:
                item = item_list[-1]
                if not item.next_tid:
                    return item

    def load(self, oid, before_tid=None):
        """Return a revision of oid that was current before given tid"""
        item = self._load(oid, before_tid)
        if item:
            data = item.data
            if data is not None:
                self._fetched(item)
                return data, item.tid, item.next_tid

    def store(self, oid, data, tid, next_tid):
        """Store a new data record in the cache"""
        size = len(data)
        max_size = self._max_size
        if size < max_size:
            item = self._load(oid, next_tid)
            if item:
                assert item.tid == tid and item.next_tid == next_tid
                if item.level: # already stored
                    assert item.data == data
                    return
                assert not item.data
                self._history_size -= 1
            else:
                item = CacheItem()
                item.oid = oid
                item.tid = tid
                item.next_tid = next_tid
                item.counter = 0
                item.level = None
                try:
                    item_list = self._oid_dict[oid]
                except KeyError:
                    self._oid_dict[oid] = [item]
                else:
                    if next_tid:
                        insort(item_list, item)
                    else:
                        prev = item_list[-1]
                        item.counter = prev.counter
                        prev.counter = 0
                        if prev.level > 1:
                            self._fetched(prev)
                        item_list.append(item)
            item.data = data
            self._fetched(item)
            self._size += size
            if max_size < self._size:
                for head in self._queue_list[1:]:
                    while head:
                        next = self._remove(head)
                        head.level = 0
                        self._add(head)
                        if self._size <= max_size:
                            return
                        head = next

    def invalidate(self, oid, tid):
        """Mark data record as being valid only up to given tid"""
        try:
            item = self._oid_dict[oid][-1]
        except KeyError:
            pass
        else:
            if item.next_tid is None:
                item.next_tid = tid
            else:
                assert item.next_tid <= tid, (item, oid, tid)

    def clear_current(self):
        oid_list = []
        for oid, item_list in self._oid_dict.items():
            item = item_list[-1]
            if item.next_tid is None:
                self._remove(item)
                del item_list[-1]
                # We don't preserve statistics of removed items. This could be
                # done easily when previous versions are cached, by copying
                # counters, but it would not be fair for other oids, so it's
                # probably not worth it.
                if not item_list:
                    del self._oid_dict[oid]
                oid_list.append(oid)
        return oid_list


def test(self):
    cache = ClientCache()
    self.assertEqual(cache.load(1, 10), None)
    self.assertEqual(cache.load(1, None), None)
    cache.invalidate(1, 10)
    data = '5', 5, 10
    # 2 identical stores happens if 2 threads got a cache miss at the same time
    cache.store(1, *data)
    cache.store(1, *data)
    self.assertEqual(cache.load(1, 10), data)
    self.assertEqual(cache.load(1, None), None)
    data = '15', 15, None
    cache.store(1, *data)
    self.assertEqual(cache.load(1, None), data)
    self.assertEqual(cache.clear_current(), [1])
    self.assertEqual(cache.load(1, None), None)
    cache.store(1, *data)
    cache.invalidate(1, 20)
    self.assertEqual(cache.clear_current(), [])
    self.assertEqual(cache.load(1, 20), ('15', 15, 20))
    cache.store(1, '10', 10, 15)
    cache.store(1, '20', 20, 21)
    self.assertEqual([5, 10, 15, 20], [x.tid for x in cache._oid_dict[1]])

if __name__ == '__main__':
    import unittest
    unittest.TextTestRunner().run(type('', (unittest.TestCase,), {
        'runTest': test})())
