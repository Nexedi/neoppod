#!/usr/bin/env python

from ZODB import DB
from neo.client.Storage import Storage
from neo.lib import logging
from time import sleep


#etc1 = '/srv/slapgrid/slappart9/srv/runner/instance/slappart1/etc'
etc1 = 'etc1'

def main():
    logging.backlog(max_size=None, max_packet=None) # log everything & without bufferring

    kw = {
            #'master_nodes':     '[2001:67c:1254:e:20::3977]:2051',      # M on webr-wneo-*1*
            #'name':             'woelfel-munich-clone',

            'master_nodes':     '[2001:67c:1254:e:21::ffa]:2051',       # M on webr-wneo-2
            'name':             'woelfel-munich-clone-backup-comp-2591',
            'read_only':        True,

            'logfile':  'x.log',

            'ca':       etc1 + '/ca.crt',
            'cert':     etc1 + '/neo.crt',
            'key':      etc1 + '/neo.key',
    }

    print 'aaa'
    stor = Storage(**kw)
    print 'bbb'
    db = DB(stor)
    print 'ccc'
    conn = db.open()
    print 'ddd'
    root = conn.root()
    print 'eee'
    print root
    print 'fff'

    while 1:
        sleep(1)


if __name__ == '__main__':
    main()

