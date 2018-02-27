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
import tzodb
import zlib
from time import time
from math import ceil, log10
import socket

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


# ---- 8< ---- from wendelin.core/t/py.bench

# benchmarking timer/request passed to benchmarks as fixture
# similar to https://golang.org/pkg/testing/#B
class B:

    def __init__(self):
        self.N = 1              # default when func does not accept `b` arg
        self._t_start = None    # t of timer started; None if timer is currently stopped
        self.reset_timer()

    def reset_timer(self):
        self._t_total = 0.

    def start_timer(self):
        if self._t_start is not None:
            return

        self._t_start = time()

    def stop_timer(self):
        if self._t_start is None:
            return

        t = time()
        self._t_total += t - self._t_start
        self._t_start = None

    def total_time(self):
        return self._t_total

# benchit runs benchf auto-adjusting whole runing time to ttarget
def benchit(benchf, bencharg, ttarget = 1.):
    b = B()
    b.N = 0
    t = 0.
    while t < (ttarget * 0.9):
        if b.N == 0:
            b.N = 1
        else:
            n = b.N * (ttarget / t)     # exact how to adjust b.N to reach ttarget
            order = int(log10(n))       # n = k·10^order, k ∈ [1,10)
            k = float(n) / (10**order)
            k = ceil(k)                 # lift up k to nearest int
            b.N = int(k * 10**order)    # b.N = int([1,10))·10^order

        b.reset_timer()
        b.start_timer()
        benchf(b, bencharg)
        b.stop_timer()
        t = b.total_time()

    hostname = socket.gethostname()
    benchname = benchf.__name__
    if benchname.startswith('bench_'):
        benchname = benchname[len('bench_'):]

    print('Benchmark%s/%s/py/%s %d\t%.3f µs/op' %
                (hostname, benchname, prettyarg(bencharg), n, t * 1E6 / n))

# ---- 8< ----

def _bench_hasher(b, h, blksize):
    blksize = int(blksize)
    data = '\0'*blksize

    b.reset_timer()

    n = b.N
    i = 0
    while i < n:
        h.update(data)
        i += 1


def bench_adler32(b, blksize):  _bench_hasher(b, tzodb.Adler32Hasher(), blksize)
def bench_crc32(b, blksize):    _bench_hasher(b, tzodb.CRC32Hasher(), blksize)
def bench_sha1(b, blksize):     _bench_hasher(b, hashlib.sha1(), blksize)


def readfile(path):
    with open(path, 'r') as f:
        return f.read()

def bench_unzlib(b, zfile):
    zdata = readfile('testdata/zlib/%s' % zfile)
    b.reset_timer()

    n = b.N
    i = 0
    while i < n:
        zlib.decompress(zdata)
        i += 1


def main():
    bench    = sys.argv[1]
    bencharg = sys.argv[2]

    benchf = globals()['bench_%s' % bench]
    benchit(benchf, bencharg)

if __name__ == '__main__':
    main()