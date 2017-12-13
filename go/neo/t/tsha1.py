#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2017  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
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
"""tsha1 - benchmark sha1"""

from __future__ import print_function

import sys
import hashlib
from time import time
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


def main():
    blksize = int(sys.argv[1])
    data = '\0'*blksize

    h = hashlib.sha1()
    tstart = time()

    n = int(1E6)
    if blksize > 1024:
        n = n * 1024 / blksize   # assumes 1K ~= 1μs

    i = 0
    while i < n:
        h.update(data)
        i += 1

    tend = time()
    dt = tend - tstart

    hostname = socket.gethostname()
    print('Benchmark%s/sha1/py/%s %d\t%.3f µs/op' % (hostname, fmtsize(blksize), n, dt * 1E6 / n))

if __name__ == '__main__':
    main()
