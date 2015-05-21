#
# Copyright (C) 2015-2015 Nexedi SA
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
#

def speedupFileStorageTxnLookup():
    """Speed up lookup of start position when instanciating an iterator

    FileStorage does not index the file positions of transactions.
    With this patch, we use the existing {oid->file_pos} index to bisect the
    the closest file position to start iterating.
    """
    from array import array
    from bisect import bisect
    from collections import defaultdict
    from ZODB.FileStorage.FileStorage import FileStorage, FileIterator

    typecode = 'L' if array('I').itemsize < 4 else 'I'

    class Start(object):

        def __init__(self, read_data_header, h, tid):
            self.read_data_header = read_data_header
            self.h = h << 32
            self.tid = tid

        def __lt__(self, l):
            return self.tid < self.read_data_header(self.h | l).tid

    def iterator(self, start=None, stop=None):
        if start:
            try:
                index = self._tidindex
            except AttributeError:
                # Cache a sorted list of all the file pos from oid index.
                # To reduce memory usage, the list is splitted in arrays of
                # low order 32-bit words.
                tindex = defaultdict(lambda: array(typecode))
                for x in self._index.itervalues():
                    tindex[x >> 32].append(x & 0xffffffff)
                index = self._tidindex = []
                for h, l in sorted(tindex.iteritems()):
                    x = array('I')
                    x.fromlist(sorted(l))
                    l = self._read_data_header(h << 32 | x[0])
                    index.append((l.tid, h, x))
            x = bisect(index, (start,)) - 1
            if x >= 0:
                x, h, index = index[x]
                x = self._read_data_header
                h = x(h << 32 | index[bisect(index, Start(x, h, start)) - 1])
                return FileIterator(self._file_name, start, stop, h.tloc)
        return FileIterator(self._file_name, start, stop)

    FileStorage.iterator = iterator
