#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
""", file=w)

def main():
    try:
        optv, argv = getopt(sys.argv[1:], "h", ["help"] + hashRegistry.keys())
    except GetoptError as e:
        print("E: %s" % e, file=sys.stderr)
        usage(sys.stderr)
        exit(1)

    for opt, _ in optv:
        if opt in ("-h", "--help"):
            print(__doc__)
            usage(sys.stdout)
            sys.exit()

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

    for zzz in range(1):
        tstart = time()

        # vvv h.reset() XXX temp
        try:
            h = h.__class__()
        except:
            h = hashlib.new(h.name)

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

        print('%s:%s   ; oid=0..%d  nread=%d  t=%.3fs (%.1fμs / object)  x=zhash.py' % \
                (h.name, h.hexdigest(), oid-1, nread, dt, dt * 1E6 / oid))


if __name__ == '__main__':
    main()
