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

import hashlib, random
from collections import deque
from itertools import islice
from persistent import Persistent
from BTrees.IOBTree import IOBTree
from .stat_zodb import _DummyData

def generateTree(random=random):
    tree = []
    N = 5
    fifo = deque()
    path = ()
    size = lambda: max(int(random.gauss(40,30)), 0)
    while 1:
        tree.extend(path + (i, size())
            for i in xrange(-random.randrange(N), 0))
        n = N * (1 - len(path)) + random.randrange(N)
        for i in xrange(n):
            fifo.append(path + (i,))
        try:
            path = fifo.popleft()
        except IndexError:
            break
    change = tree
    while change:
        change = [x[:-1] + (size(),) for x in change if random.randrange(2)]
        tree += change
    random.shuffle(tree)
    return tree

class Leaf(Persistent):
    pass

Node = IOBTree

def importTree(root, tree, yield_interval=None, filter=None):
    n = 0
    for path in tree:
        node = root
        for i, x in enumerate(path[:-1], 1):
            if filter and not filter(path[:i]):
                break
            if x < 0:
                try:
                    node = node[x]
                except KeyError:
                    node[x] = node = Leaf()
                node.data = bytes(_DummyData(random.Random(path), path[-1]))
            else:
                try:
                    node = node[x]
                    continue
                except KeyError:
                    node[x] = node = Node()
            n += 1
            if n == yield_interval:
                n = 0
                yield root
    if n:
        yield root

class hashTree(object):

    _hash = None
    _new = hashlib.md5

    def __init__(self, node):
        s = [((), node)]
        def walk():
            h = self._new()
            update = h.update
            while s:
                top, node = s.pop()
                try:
                    update('%s %s %s\n' % (top, len(node.data),
                        self._new(node.data).hexdigest()))
                    yield
                except AttributeError:
                    update('%s %s\n' % (top, tuple(node.keys())))
                    yield
                    for k, v in reversed(node.items()):
                        s.append((top + (k,), v))
            del self._walk
            self._hash = h
        self._walk = walk()

    def __getattr__(self, attr):
        return getattr(self._hash, attr)

    def __call__(self, n=None):
        if n is None:
            return sum(1 for _ in self._walk)
        next(islice(self._walk, n - 1, None))
