#
# Copyright (C) 2014-2015  Nexedi SA
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

import os, stat, time
from persistent import Persistent
from persistent.TimeStamp import TimeStamp
from BTrees.OOBTree import OOBTree

class Inode(OOBTree):

    data = None

    def __init__(self, up=None, mode=stat.S_IFDIR):
        self[os.pardir] = self if up is None else up
        self.mode = mode
        self.mtime = time.time()

    def __getstate__(self):
        return Persistent.__getstate__(self), OOBTree.__getstate__(self)

    def __setstate__(self, state):
        Persistent.__setstate__(self, state[0])
        OOBTree.__setstate__(self, state[1])

    def edit(self, data=None, mtime=None):
        fmt = stat.S_IFMT(self.mode)
        if data is None:
            assert fmt == stat.S_IFDIR, oct(fmt)
        else:
            assert fmt == stat.S_IFREG or fmt == stat.S_IFLNK, oct(fmt)
            if self.data != data:
                self.data = data
        if self.mtime != mtime:
            self.mtime = mtime or time.time()

    def root(self):
        try:
            self = self[os.pardir]
        except KeyError:
            return self
        return self.root()

    def traverse(self, path, followlinks=True):
        path = iter(path.split(os.sep) if isinstance(path, basestring) and path
                    else path)
        for d in path:
            if not d:
                return self.root().traverse(path, followlinks)
            if d != os.curdir:
                d = self[d]
                if followlinks and stat.S_ISLNK(d.mode):
                    d = self.traverse(d.data, True)
                return d.traverse(path, followlinks)
        return self

    def inodeFromFs(self, path):
        s = os.lstat(path)
        mode = s.st_mode
        name = os.path.basename(path)
        try:
            i = self[name]
            assert stat.S_IFMT(i.mode) == stat.S_IFMT(mode)
            changed = False
        except KeyError:
            i = self[name] = self.__class__(self, mode)
            changed = True
        i.edit(open(path).read() if stat.S_ISREG(mode) else
                      os.readlink(p) if stat.S_ISLNK(mode) else
                      None, s.st_mtime)
        return changed or i._p_changed

    def treeFromFs(self, path, yield_interval=None, filter=None):
        prefix_len = len(path) + len(os.sep)
        n = 0
        for dirpath, dirnames, filenames in os.walk(path):
            inodeFromFs = self.traverse(dirpath[prefix_len:]).inodeFromFs
            for names in dirnames, filenames:
                skipped = []
                for j, name in enumerate(names):
                    p = os.path.join(dirpath, name)
                    if filter and not filter(p[prefix_len:]):
                        skipped.append(j)
                    elif inodeFromFs(p):
                        n += 1
                        if n == yield_interval:
                            n = 0
                            yield self
                while skipped:
                    del names[skipped.pop()]
        if n:
            yield self

    def walk(self):
        s = [(None, self)]
        while s:
            top, self = s.pop()
            dirs = []
            nondirs = []
            for name, inode in self.iteritems():
                if name != os.pardir:
                    (dirs if stat.S_ISDIR(inode.mode) else nondirs).append(name)
            yield top or os.curdir, dirs, nondirs
            for name in dirs:
                s.append((os.path.join(top, name) if top else name, self[name]))
