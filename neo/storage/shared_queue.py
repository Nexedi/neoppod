#
# Copyright (C) 2018-2019  Nexedi SA
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

from msgpack import Packer, Unpacker
from neo import *

class Queue(object):
    """Unidirectional pipe for asynchronous and fast exchange of big amounts
    of data between 2 processes.

    It is implemented using shared memory, a few locks and msgpack
    serialization. While the latter is faster than C pickle, it was mainly
    chosen for its streaming API while deserializing, which greatly reduces
    the locking overhead for the consumer process.

    There is no mechanism to end a communication, so this information must be
    in the exchanged data, for example by choosing a marker object like None:
    - the last object sent by the producer is this marker
    - the consumer stops iterating when it gets this marker

    As long as there are data being exchanged, the 2 processes can't change
    roles (producer/consumer).
    """

    def __init__(self, max_size):
        from multiprocessing import Lock, RawArray, RawValue
        self._max_size = max_size
        self._array = RawArray('c', max_size)
        self._pos = RawValue('L')
        self._size = RawValue('L')
        self._locks = Lock(), Lock(), Lock()

    def __repr__(self):
        return "<%s pos=%s size=%s max_size=%s>" % (self.__class__.__name__,
            self._pos.value, self._size.value, self._max_size)

    def __iter__(self):
        """Iterate endlessly over all objects sent by the producer

        Internally, this method uses a receiving buffer that is lost if
        interrupted (GeneratorExit). If this buffer was not empty, the queue
        is left in a inconsistent state and this method can't be called again.

        So the correct way to split a loop is to first get an iterator
        explicitly:
            iq = iter(queue)
            for x in iq:
                if ...:
                    break
            for x in iq:
                ...
        """
        unpacker = Unpacker(use_list=False, raw=True)
        feed = unpacker.feed
        max_size = self._max_size
        array = self._array
        pos = self._pos
        size = self._size
        lock, get_lock, put_lock = self._locks
        left = 0
        while 1:
            for data in unpacker:
                yield data
            while 1:
                with lock:
                    p = pos.value
                    s = size.value
                if s:
                    break
                get_lock.acquire()
            e = p + s
            if e < max_size:
                feed(array[p:e])
            else:
                feed(array[p:])
                e -= max_size
                feed(array[:e])
            with lock:
                pos.value = e
                n = size.value
                size.value = n - s
            if n == max_size:
                put_lock.acquire(0)
                put_lock.release()

    def __call__(self, iterable):
        """Fill the queue with given objects

        Hoping than msgpack.Packer gets a streaming API, 'iterable' should not
        be split (i.e. this method should be called only once, like __iter__).
        """
        pack = Packer(use_bin_type=True).pack
        max_size = self._max_size
        array = self._array
        pos = self._pos
        size = self._size
        lock, get_lock, put_lock = self._locks
        left = 0
        for data in iterable:
            data = pack(data)
            n = len(data)
            i = 0
            while 1:
                if not left:
                    while 1:
                        with lock:
                            p = pos.value
                            j = size.value
                        left = max_size - j
                        if left:
                            break
                        put_lock.acquire()
                    p += j
                    if p >= max_size:
                        p -= max_size
                e = min(p + min(n, left), max_size)
                j = e - p
                array[p:e] = data[i:i+j]
                n -= j
                i += j
                with lock:
                    p = pos.value
                    s = size.value
                    j += s
                    size.value = j
                if not s:
                    get_lock.acquire(0)
                    get_lock.release()
                p += j
                if p >= max_size:
                    p -= max_size
                left = max_size - j
                if not n:
                    break


def test(self):
    import multiprocessing, random, sys, threading
    from traceback import print_tb
    r = list(range(50))
    random.shuffle(r)
    for P in threading.Thread, multiprocessing.Process:
        q = Queue(23)
        def t():
            for n in range(len(r)):
                yield b'.' * n
            yield
            for n in r:
                yield b'.' * n
        i = j = 0
        p = P(target=q, args=(t(),))
        p.daemon = 1
        p.start()
        try:
            q = iter(q)
            for i, x in enumerate(q):
                if x is None:
                    break
                self.assertEqual(x, b'.' * i)
            self.assertEqual(i, len(r))
            for j in r:
                self.assertEqual(next(q), b'.' * j)
        except KeyboardInterrupt:
            print_tb(sys.exc_info()[2])
            self.fail((i, j))
        p.join()

if __name__ == '__main__':
    import unittest
    unittest.TextTestRunner().run(type('', (unittest.TestCase,), {
        'runTest': test})())
