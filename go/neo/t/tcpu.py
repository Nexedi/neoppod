#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017-2018  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
"""tcpu - cpu-related benchmarks"""

from __future__ import print_function

import sys
import hashlib
from zlib import crc32, adler32

from golang import testing

# adler32 in hashlib interface
class Adler32Hasher:
    name = "adler32"

    def __init__(self):
        self.h = adler32('')

    def update(self, data):
        self.h = adler32(data, self.h)

    def hexdigest(self):
        return '%08x' % (self.h & 0xffffffff)

# crc32 in hashlib interface
class CRC32Hasher:
    name = "crc32"

    def __init__(self):
        self.h = crc32('')

    def update(self, data):
        self.h = crc32(data, self.h)

    def hexdigest(self):
        return '%08x' % (self.h & 0xffffffff)

# fmtsize formats size in human readable form
_unitv = "BKMGT" # (2^10)^i represents by corresponding char suffix
def fmtsize(size):
    order = 1<<10
    norder = 0
    while size and (size % order) == 0 and (norder + 1 < len(_unitv)):
        size //= order
        norder += 1

    return "%d%s" % (size, _unitv[norder])

def prettyarg(arg):
    try:
        arg = int(arg)
    except ValueError:
        return arg     # return as it is - e.g. "null-4K"
    else:
        return fmtsize(arg)


# benchit benchmarks benchf(bencharg)
def benchit(benchf, bencharg):
    def _(b):
        benchf(b, bencharg)
    r = testing.benchmark(_)

    benchname = benchf.__name__
    if benchname.startswith('bench_'):
        benchname = benchname[len('bench_'):]

    print('Benchmark%s/py/%s %d\t%.3f µs/op' %
                (benchname, prettyarg(bencharg), r.N, r.T * 1E6 / r.N))


def _bench_hasher(b, h, blksize):
    blksize = int(blksize)
    data = '\0'*blksize

    b.reset_timer()

    n = b.N
    i = 0
    while i < n:
        h.update(data)
        i += 1


def bench_adler32(b, blksize):  _bench_hasher(b, Adler32Hasher(), blksize)
def bench_crc32(b, blksize):    _bench_hasher(b, CRC32Hasher(), blksize)
def bench_sha1(b, blksize):     _bench_hasher(b, hashlib.sha1(), blksize)


def main():
    bench    = sys.argv[1]
    bencharg = sys.argv[2]

    benchf = globals()['bench_%s' % bench]
    benchit(benchf, bencharg)

if __name__ == '__main__':
    main()
