#
# Copyright (C) 2011-2017  Nexedi SA
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

from __future__ import division
from BTrees.LOBTree import LOBTree
from gc import get_referents
from struct import Struct
from sys import getsizeof

s = Struct('d')
pack_double = s.pack
unpack_double = s.unpack
s = Struct('q')
pack_long = s.pack
unpack_long = s.unpack
del s

def internalSizeOfBTree(x):
  module = type(x).__module__
  seen = set()
  left = [x]
  size = 0
  while left:
    x = left.pop()
    seen.add(x)
    size += getsizeof(x)
    left.extend(x for x in get_referents(x)
      if type(x).__module__ == module and x not in seen)
  return size

class CacheItem(object):

    __slots__ = 'oid', 'tid', 'next_tid', 'data', 'counter', 'expire'

    def __repr__(self):
        s = ''
        for attr in self.__slots__:
            try:
                value = getattr(self, attr)
                if attr == 'data':
                    s += ' len(%s)=%s' % (attr, len(value))
                    continue
                if attr == 'expire':
                    value = unpack_double(pack_long(value))[0]
                s += ' %s=%r' % (attr, value)
            except AttributeError:
                pass
        return '<%s%s>' % (self.__class__.__name__, s)

    def __lt__(self, other):
        return self.tid < other.tid

class ClientCache(object):
    """In-memory pickle cache based on LFRU cache algorithm

      This Least Frequent Recently Used implementation is adapted to handle
      records of different sizes. This is possible thanks to a B+Tree: the use
      of such a complex structure for a cache is quite unusual for a cache
      but we use a C implementation that's relatively fast compared to the
      cost of a cache miss.

      This algorithm adapts well regardless its maximum allowed size,
      without any tweak.
    """

    __slots__ = ('max_size', '_oid_dict', '_size', '_added', '_items',
                 '_nhit', '_nmiss')

    def __init__(self, max_size=20*1024*1024):
        self.max_size = max_size
        self.clear()

    def clear(self):
        """Reset cache"""
        self._oid_dict = {}
        self._size = self._nhit = self._nmiss = 0
        # Make sure to never produce negative keys, else
        # we could not manipulate them when encoded as integers.
        self._added = self.max_size
        self._items = LOBTree()

    def __repr__(self):
        nload = self._nhit + self._nmiss
        return ("<%s #loads=%s #oids=%s size=%s #items=%s"
                " btree_overhead=%s (max_size=%s)>") % (
            self.__class__.__name__,
            nload and '%s (%.3g%% hit)' % (nload, 100 * self._nhit / nload),
            len(self._oid_dict), self._size, len(self._items),
            internalSizeOfBTree(self._items),
            self.max_size)

    def _load(self, oid, before_tid=None):
        item_list = self._oid_dict.get(oid)
        if item_list:
            if before_tid:
                for item in item_list:
                    if item.tid < before_tid:
                        next_tid = item.next_tid
                        if next_tid and next_tid < before_tid:
                            break
                        return item
            else:
                item = item_list[0]
                if not item.next_tid:
                    return item

    def load(self, oid, before_tid):
        """Return a revision of oid that was current before given tid"""
        item = self._load(oid, before_tid)
        if item:
            del self._items[item.expire]
            item.counter += 1
            self._add(item)
            self._nhit += 1
            return item.data, item.tid, item.next_tid
        self._nmiss += 1

    def _forget(self, item):
        items = self._oid_dict[item.oid]
        items.remove(item)
        if not items:
            del self._oid_dict[item.oid]
        self._size -= len(item.data)
        del self._items[item.expire]

    def _add(self, item):
        # The initial idea was to compute keys as follows:
        #    (added - size) * item.counter
        # However, after running for a long time, this tends to degenerate:
        # - size become more and more negligible over time
        # - objects that are most often accessed become impossible to remove,
        #   making the cache too slow to adapt after a change of workload
        # - 64 bits is not enough
        # This was solved in several ways, by using the following formula:
        #    min_key - size + (added - min_key) * item.counter
        # and doubles.
        # BTrees does not have an optimized class for doubles so we encode
        # them as integers, which preserve the same order as long as they're
        # positive (hence some extra tweak to avoid negative numbers in some
        # rare cases) and it becomes easier to compute the next double
        # (+1 instead of libm.nextafter). The downside is that conversion
        # between double and long is a bit expensive in Python.
        added = self._added
        items = self._items
        try:
            x = items.minKey()
        except ValueError:
            x = added
        else:
            # Most of the time, the smallest key is smaller than `added`. In
            # the very rare case it isn't, make sure to produce a positive key.
            x = min(added, unpack_double(pack_long(x))[0])
        size = len(item.data)
        expire = unpack_long(pack_double(
            x - size + (added - x) * item.counter
            ))[0]
        for x in items.iterkeys(expire):
            if x != expire:
                break
            expire += 1
        self._added = added + size
        item.expire = expire
        items[expire] = item

    def store(self, oid, data, tid, next_tid):
        """Store a new data record in the cache"""
        size = len(data)
        max_size = self.max_size
        if size < max_size:
            i = 0
            try:
                items = self._oid_dict[oid]
            except KeyError:
                items = self._oid_dict[oid] = []
                counter = 1
            else:
                for item in items:
                    if item.tid < tid:
                        assert None is not item.next_tid <= tid
                        break
                    if item.tid == tid:
                        # We don't handle late invalidations for cached oids,
                        # because the caller is not supposed to explicitly asks
                        # for tids after app.last_tid (and the cache should be
                        # empty when app.last_tid is still None).
                        assert item.next_tid == next_tid and item.data == data
                        return
                    i += 1
                if next_tid:
                    counter = 1
                else:
                    counter = item.counter
                    if counter != 1:
                        del self._items[item.expire]
                        item.counter = 1
                        self._add(item)
            item = CacheItem()
            item.oid = oid
            item.tid = tid
            item.next_tid = next_tid
            item.data = data
            item.counter = counter
            items.insert(i, item)
            self._size += size
            self._add(item)
            while max_size < self._size:
                items = self._items
                self._forget(items[items.minKey()])

    def invalidate(self, oid, tid):
        """Mark data record as being valid only up to given tid"""
        items = self._oid_dict.get(oid)
        if items:
            item = items[0]
            if item.next_tid is None:
                item.next_tid = tid
            else:
                assert item.next_tid <= tid, (item, oid, tid)

    def clear_current(self):
        for oid, items in self._oid_dict.items():
            item = items[0]
            if item.next_tid is None:
                self._forget(item)


def test(self):
    orig_add = ClientCache._add
    def _add(cache, item):
        orig_add(cache, item)
        self.assertLessEqual(0, cache._items.minKey())
    ClientCache._add = _add

    cache = ClientCache()
    repr(cache)
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
    cache.clear_current()
    self.assertEqual(cache._size, 1)
    self.assertEqual(cache.load(1, None), None)
    cache.store(1, *data)
    cache.invalidate(1, 20)
    self.assertEqual(cache._size, 3)
    cache.clear_current()
    self.assertEqual(cache._size, 3)
    self.assertEqual(cache.load(1, 20), ('15', 15, 20))
    cache.store(1, '10', 10, 15)
    cache.store(1, '20', 20, 21)
    self.assertEqual([20, 15, 10, 5], [x.tid for x in cache._oid_dict[1]])
    self.assertRaises(AssertionError, cache.store, 1, '20', 20, None)
    repr(cache)
    cache = ClientCache(10)
    data1 = "x", 1, None
    cache.store(1, "x", 1, None)
    repr(*cache._oid_dict[1])
    data = "xxxxx", 1, None
    cache.store(2, *data)
    cache.store(3, *data)
    self.assertEqual(cache.load(1, None), data1)
    self.assertEqual(cache.load(2, None), None) # bigger records removed faster
    self.assertEqual(cache.load(3, None), data)
    self.assertEqual(cache._size, 6)
    cache.clear_current()
    for oid in 0, 1:
        cache.store(oid, 'x', 1, None)
        cache.load(oid, None)
        cache.load(oid, None)
    cache.load(0, None)

if __name__ == '__main__':
    import unittest
    unittest.TextTestRunner().run(type('', (unittest.TestCase,), {
        'runTest': test})())
