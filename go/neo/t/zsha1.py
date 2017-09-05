#!/usr/bin/env python
"""zsha1 - compute sha1 of whole latest objects stream in a ZODB database"""

from __future__ import print_function

import zodbtools.util
from ZODB.POSException import POSKeyError
from ZODB.utils import p64, u64

import hashlib
import sys
from time import time

def main():
    url = sys.argv[1]

    stor = zodbtools.util.storageFromURL(url, read_only=True)
    last_tid = stor.lastTransaction()
    before = p64(u64(last_tid) + 1)

    tstart = time()
    m = hashlib.sha1()

    oid = 0
    nread = 0
    while 1:
        try:
            data, serial, _ = stor.loadBefore(p64(oid), before)
        except POSKeyError:
            break

        m.update(data)

        print('%s @%s\tsha1: %s' % (oid, u64(serial), m.hexdigest()), file=sys.stderr)

        nread += len(data)
        oid += 1

    tend = time()

    print('%s   ; oid=0..%d  nread=%d  t=%.3fs' % \
            (m.hexdigest(), oid-1, nread, tend - tstart))


if __name__ == '__main__':
    main()
