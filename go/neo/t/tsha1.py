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

    print('sha1(%dB) ~= %.1fμs  x=tsha1.py' % (blksize, dt * 1E6 / n))

if __name__ == '__main__':
    main()
