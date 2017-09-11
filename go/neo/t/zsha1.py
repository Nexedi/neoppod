#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""zsha1 - compute sha1 of whole latest objects stream in a ZODB database"""

from __future__ import print_function

import zodbtools.util
from ZODB.POSException import POSKeyError
from ZODB.utils import p64, u64

import hashlib
from zlib import crc32, adler32
import sys
from time import time

# crc32 in hashlib interface
class CRC32Hasher:

    name = "crc32"

    def __init__(self):
        self.h = crc32('')

    def update(self, data):
        self.h = crc32(data, self.h)

    def hexdigest(self):
        return '%x' % (self.h & 0xffffffff)

# adler32 in hashlib interface
class Adler32Hasher:

    name = "adler32"

    def __init__(self):
        self.h = adler32('')

    def update(self, data):
        self.h = adler32(data, self.h)

    def hexdigest(self):
        return '%x' % (self.h & 0xffffffff)

def main():
    url = sys.argv[1]

    stor = zodbtools.util.storageFromURL(url, read_only=True)
    last_tid = stor.lastTransaction()
    before = p64(u64(last_tid) + 1)

    for zzz in range(10):
        #m = hashlib.sha1()
        m = CRC32Hasher()
        #m = Adler32Hasher()

        tstart = time()

        oid = 0
        nread = 0
        while 1:
            try:
                data, serial, _ = stor.loadBefore(p64(oid), before)
            except POSKeyError:
                break

            m.update(data)

            #print('%s @%s\tsha1: %s' % (oid, u64(serial), m.hexdigest()), file=sys.stderr)
            #print('\tdata: %s' % (data.encode('hex'),), file=sys.stderr)

            nread += len(data)
            oid += 1

        tend = time()
        dt = tend - tstart

        print('%s:%s   ; oid=0..%d  nread=%d  t=%.3fs (%.1fμs / object)  x=zsha1.py' % \
                (m.name, m.hexdigest(), oid-1, nread, dt, dt * 1E6 / oid))


if __name__ == '__main__':
    main()
