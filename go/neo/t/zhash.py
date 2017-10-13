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
"""zhash - compute hash of whole latest objects stream in a ZODB database"""

from __future__ import print_function

import zodbtools.util
from ZODB.POSException import POSKeyError
from ZODB.utils import p64, u64

import hashlib
from zlib import crc32, adler32
import sys
from time import time
from getopt import getopt, GetoptError

# hasher that discards data
class NullHasher:
    name = "null"

    def update(self, data):
        pass

    def hexdigest(self):
        return "00"

# adler32 in hashlib interface
class Adler32Hasher:
    name = "adler32"

    def __init__(self):
        self.h = adler32('')

    def update(self, data):
        self.h = adler32(data, self.h)

    def hexdigest(self):
        return '%x' % (self.h & 0xffffffff)

# crc32 in hashlib interface
class CRC32Hasher:
    name = "crc32"

    def __init__(self):
        self.h = crc32('')

    def update(self, data):
        self.h = crc32(data, self.h)

    def hexdigest(self):
        return '%x' % (self.h & 0xffffffff)

# {} name -> hasher
hashRegistry = {
    "null":     NullHasher,
    "adler32":  Adler32Hasher,
    "crc32":    CRC32Hasher,
    "sha1":     hashlib.sha1,
    "sha256":   hashlib.sha256,
    "sha512":   hashlib.sha512,
}

def usage(w):
    print(\
"""Usage: zhash [options] url

options:

    --null          don't compute hash - just read data
    --adler32       compute Adler32 checksum
    --crc32         compute CRC32 checksum
    --sha1          compute SHA1 cryptographic hash
    --sha256        compute SHA256 cryptographic hash
    --sha512        compute SHA512 cryptographic hash

    --bench=<topic> use benchmarking format for output
""", file=w)

def main():
    try:
        optv, argv = getopt(sys.argv[1:], "h", ["help", "bench="] + hashRegistry.keys())
    except GetoptError as e:
        print("E: %s" % e, file=sys.stderr)
        usage(sys.stderr)
        exit(1)

    bench=None
    for opt, arg in optv:
        if opt in ("-h", "--help"):
            print(__doc__)
            usage(sys.stdout)
            sys.exit()

        if opt in ("--bench"):
            bench=arg
            continue

        opt = opt.lstrip("-")
        hctor = hashRegistry[opt]
        h = hctor()

    if len(argv) != 1:
        print(__doc__)
        usage(sys.stderr)
        sys.exit(1)

    url = argv[0]

    stor = zodbtools.util.storageFromURL(url, read_only=True)
    last_tid = stor.lastTransaction()
    before = p64(u64(last_tid) + 1)

    tstart = time()

    oid = 0
    nread = 0
    while 1:
        try:
            data, serial, _ = stor.loadBefore(p64(oid), before)
        except POSKeyError:
            break

        h.update(data)

        #print('%s @%s\tsha1: %s' % (oid, u64(serial), h.hexdigest()), file=sys.stderr)
        #print('\tdata: %s' % (data.encode('hex'),), file=sys.stderr)

        nread += len(data)
        oid += 1

    tend = time()
    dt = tend - tstart

    x = "zhash.py"
    if bench is None:
        print('%s:%s   ; oid=0..%d  nread=%d  t=%.3fs (%.1fμs / object)  x=%s' % \
                (h.name, h.hexdigest(), oid-1, nread, dt, dt * 1E6 / oid, x))
    else:
        topic = bench % x
        print('Benchmark%s 1 %.1f µs/object\t# %s:%s  oid=0..%d  nread=%d  t=%.3fs' % \
                (topic, dt * 1E6 / oid, h.name, h.hexdigest(), oid-1, nread, dt))

if __name__ == '__main__':
    main()
