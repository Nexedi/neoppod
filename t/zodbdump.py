#!/usr/bin/env python
# TODO copyright
"""zodbdump - Tool to dump content from a ZODB

TODO format

txn <tid> (<status>)
user <user|encode?>
description <description|encode?>
extension <extension|encode?>
obj <oid> (delete | from <tid> | <size> (sha1) LF <content>) LF     XXX do we really need back <tid>
---- // ----
LF
txn ...

"""

import ZODB.config
import hashlib
import sys

def ashex(s):
    return s.encode('hex')

def sha1(data):
    m = hashlib.sha1()
    m.update(data)
    return m.digest()


def dump(stor, tidmin, tidmax):
    first = True

    for txn in stor.iterator(tidmin, tidmax):   # TODO tid -> 0, inf
        if not first:
            print
        first = False

        print 'txn %s (%s)' % (ashex(txn.tid), txn.status) # XXX hex, status=?
        print 'user: %r' % (txn.user,)                     # XXX encode
        print 'description:', txn.description       # XXX encode
        print 'extension:', txn.extension           # XXX dict, encode

        objv = []
        for obj in txn:
            assert obj.tid == txn.tid       # XXX == txn.tid
            assert obj.version == ''        # XXX must be == ''

            objv.append(obj)

        objv.sort(key = lambda obj: obj.oid)    # XXX int vs packed ?
        for obj in objv:
            entry = 'obj %s ' % ashex(obj.oid)             # XXX hex
            if obj.data is None:
                entry += 'delete'

            # was undo and data taken from obj.data_txn
            elif obj.data_txn is not None:
                entry += 'from %s' % obj.data_txn   # XXX hex

            else:
                entry += '%s %i' % (ashex(sha1(obj.data)), len(obj.data))
                #entry += '%i %s' % (len(obj.data), ashex(sha1(obj.data)))
                #entry += '%i' % len(obj.data)
                #entry += '%i\n' % len(obj.data)
                #entry += obj.data

            print entry


def mkneostor():
    from neo.client.Storage import Storage as NEOStorage
    etc1 = 'etc1'
    """
    'master_nodes':     '[2001:67c:1254:e:20::3977]:2051',      # M on webr-wneo-*1*
    'name':             'woelfel-munich-clone',

    'ca':       etc1 + '/ca.crt',
    'cert':     etc1 + '/neo.crt',
    'key':      etc1 + '/neo.key',
    """

    import subprocess
    M1 = subprocess.check_output("neoctl -a 127.0.0.1:5551 print node |grep MASTER |awk '{print $5}'", shell=True)
    M2 = subprocess.check_output("neoctl -a 127.0.0.1:5552 print node |grep MASTER |awk '{print $5}'", shell=True)
    kw = {
        # N1
        #'master_nodes':     M1,
        #'name':             'neo1',

        # N2
        'master_nodes':     M2,
        'name':             'neo2',

        'read_only':        True,

    }

    stor = NEOStorage(**kw)
    return stor



def main():
    # TODO parse options
    #zstor  = sys.argv[1]
    #tidrange = sys.argv[2]
    #tidmax = ...
    #stor = ZODB.config.storageFromFile(zstor)  TODO

    # XXX temp
    """
    from ZODB.FileStorage import FileStorage
    stor = FileStorage(zstor, read_only=True)
    """
    stor = mkneostor()

    # XXX temp, better -> 0, inf
    tidmin = None
    tidmax = None

    dump(stor, tidmin, tidmax)

if __name__ == '__main__':
    main()
