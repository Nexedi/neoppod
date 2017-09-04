#!/usr/bin/env python
"""zsha1 - compute sha1 of whole latest objects stream in a ZODB database"""

import zodbtools.util
from ZODB.POSException import POSKeyError
from ZODB.utils import p64, u64

import hashlib
import sys

def main():
    url = sys.argv[1]

    stor = zodbtools.util.storageFromURL(url, read_only=True)
    last_tid = stor.lastTransaction()
    before = p64(u64(last_tid) + 1)

    m = hashlib.sha1()

    oid = 0
    while 1:
        try:
            data, serial, _ = stor.loadBefore(p64(oid), before)
        except POSKeyError:
            break

        #print('%s @%s' % (oid, u64(serial)))
        m.update(data)

        oid += 1

    print('%s   ; oid=0..%d' % (m.hexdigest(), oid-1))


if __name__ == '__main__':
    main()
