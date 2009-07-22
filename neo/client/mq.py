##############################################################################
#
# Copyright (c) 2005 Nexedi SARL and Contributors. All Rights Reserved.
#                    Yoshinori Okuji <yo@nexedi.com>
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsability of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# garantees and support are strongly adviced to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
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
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
##############################################################################

"""
Multi-Queue Cache Algorithm.
"""

from math import log

class Element(object):
    """
      This class defines an element of a FIFO buffer.
    """
    pass
      
class FIFO(object):
    """
      This class implements a FIFO buffer.
    """
    
    def __init__(self):
        self._head = None
        self._tail = None
        self._len = 0
      
    def __len__(self):
        return self._len
      
    def append(self):
        element = Element()
        element.next = None
        element.prev = self._tail
        if self._tail is not None:
            self._tail.next = element
        self._tail = element
        if self._head is None:
            self._head = element
        self._len += 1
        return element

    def head(self):
        return self._head
      
    def tail(self):
        return self._tail
            
    def shift(self):
        element = self._head
        if element is None:
            return None
        del self[element]
        del element.next
        del element.prev
        return element
      
    def __delitem__(self, element):
        if element.next is None:
            self._tail = element.prev
        else:
            element.next.prev = element.prev
        
        if element.prev is None:
            self._head = element.next
        else:
            element.prev.next = element.next
          
        self._len -= 1      
    
class Data(object):
    """
      Data for each element in a FIFO buffer.
    """
    pass
      
def sizeof(o):
    """This function returns the estimated size of an object."""
    if isinstance(o, tuple):
        return sum((len(s)+16 for s in o))
    else:
        return len(o)+16

class MQ(object):
    """
      This class manages cached data by a variant of Multi-Queue.
      
      This class caches various sizes of objects. Here are some considerations:
      
      - Expired objects are not really deleted immediately. But if GC is invoked too often,
        it degrades the performance significantly.
        
      - If large objects are cached, the number of cached objects decreases. This might affect
        the cache hit ratio. It might be better to tweak a buffer level according to the size of
        an object.
        
      - Stored values must be strings.
      
      - The size calculation is not accurate.
    """
    
    def __init__(self, life_time=10000, buffer_levels=9, max_history_size=100000, max_size=20*1024*1024):
        self._history_buffer = FIFO()
        self._cache_buffers = []
        for level in range(buffer_levels):
            self._cache_buffers.append(FIFO())
        self._data = {}
        self._time = 0
        self._life_time = life_time
        self._buffer_levels = buffer_levels
        self._max_history_size = max_history_size
        self._max_size = max_size
        self._size = 0
      
    def has_key(self, id):
        if id in self._data:
            data = self._data[id]
            if data.level >= 0:
                return 1
        return 0
      
    __contains__ = has_key
    
    def fetch(self, id):
        """
          Fetch a value associated with the id.
        """
        data = self._data[id]
        if data.level >= 0:
            value = data.value
            self._size -= sizeof(value) # XXX inaccurate
            self.store(id, value)
            return value
        raise KeyError(id)
    
    __getitem__ = fetch
    
    def get(self, id, d=None):
        try:
            return self.fetch(id)
        except KeyError:
            return d
    
    def _evict(self, id):
        """
          Evict an element to the history buffer.
        """
        data = self._data[id]
        self._size -= sizeof(data.value) # XXX inaccurate
        del self._cache_buffers[data.level][data.element]
        element = self._history_buffer.append()
        data.level = -1
        data.element = element
        del data.value
        del data.expire_time
        element.data = data
        if len(self._history_buffer) > self._max_history_size:
            element = self._history_buffer.shift()
            del self._data[element.data.id]
        
    def store(self, id, value):
        cache_buffers = self._cache_buffers

        try:
            data = self._data[id]
            level, element, counter = data.level, data.element, data.counter + 1
            if level >= 0:
                del cache_buffers[level][element]
            else:
                del self._history_buffer[element]
        except KeyError:
            counter = 1
          
        # XXX It might be better to adjust the level according to the object size.
        level = min(int(log(counter, 2)), self._buffer_levels - 1)
        element = cache_buffers[level].append()
        data = Data()
        data.id = id
        data.expire_time = self._time + self._life_time
        data.level = level
        data.element = element
        data.value = value
        data.counter = counter
        element.data = data
        self._data[id] = data
        self._size += sizeof(value) # XXX inaccurate
        del value
        
        self._time += 1

        # Expire old elements.
        time = self._time
        for level in xrange(self._buffer_levels):
            cache_buffer = cache_buffers[level]
            head = cache_buffer.head()
            if head is not None and head.data.expire_time < time:
                del cache_buffer[head]
                data = head.data
                if level > 0:
                    new_level = level - 1
                    element = cache_buffers[new_level].append()
                    element.data = data
                    data.expire_time = time + self._life_time
                    data.level = new_level
                    data.element = element
                else:
                    self._evict(data.id)
            
        # Limit the size.
        size = self._size
        max_size = self._max_size
        if size > max_size:
            for cache_buffer in cache_buffers:
                while size > max_size:
                    element = cache_buffer.shift()
                    if element is None:
                        break
                    data = element.data
                    del self._data[data.id]
                    size -= sizeof(data.value) # XXX inaccurate
                    del data.value
                if size <= max_size:
                    break
                self._size = size
      
    __setitem__ = store
    
    def invalidate(self, id):
        if id in self._data:
            data = self._data[id]
            if data.level >= 0:
                del self._cache_buffers[data.level][data.element]
                self._evict(id)
                return
        raise KeyError, "%s was not found in the cache" % id

    __delitem__ = invalidate
  
  
# Here is a test.
if __name__ == '__main__':
    import hotshot, hotshot.stats

    def test():
        cache = MQ(life_time=100, buffer_levels=9, max_history_size=10000, 
                   max_size=2*1024*1024)

        for i in xrange(10000):
            assert cache.get(i) is None, '%d should not be present' % i

        for i in xrange(10000):
            cache[i] = str(i)
            assert cache.get(i) == str(i), '%d does not exist' % i

        for i in xrange(10000 - 100 - 1):
            assert cache.get(i) is None, '%d should not be present' % i

        for i in xrange(10):
            cache[i] = str(i)

        for j in xrange(1000):
            for i in xrange(10):
                assert cache.get(i) == str(i), '%d does not exist' % i

        for i in xrange(10,500):
            cache[i] = str(i)

        for i in xrange(10):
            assert cache.get(i) == str(i), '%d does not exist' % i

    prof = hotshot.Profile("mq.prof")
    prof.runcall(test)
    prof.close()
    stats = hotshot.stats.load("mq.prof")
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(20)
