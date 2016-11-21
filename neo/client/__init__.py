##############################################################################
#
# Copyright (C) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

def patch():
    from hashlib import md5
    from ZODB.Connection import Connection

    H = lambda f: md5(f.func_code.co_code).hexdigest()

    # Allow serial to be returned as late as tpc_finish
    #
    # This makes possible for storage to allocate serial inside tpc_finish,
    # removing the requirement to serialise second commit phase (tpc_vote
    # to tpc_finish/tpc_abort).

    h = H(Connection.tpc_finish)

    def tpc_finish(self, transaction):
        """Indicate confirmation that the transaction is done."""

        def callback(tid):
            if self._mvcc_storage:
                # Inter-connection invalidation is not needed when the
                # storage provides MVCC.
                return
            d = dict.fromkeys(self._modified)
            self._db.invalidate(tid, d, self)
#       It's important that the storage calls the passed function
#       while it still has its lock.  We don't want another thread
#       to be able to read any updated data until we've had a chance
#       to send an invalidation message to all of the other
#       connections!
        # <patch>
        serial = self._storage.tpc_finish(transaction, callback)
        if serial is not None:
            assert isinstance(serial, str), repr(serial)
            for oid_iterator in (self._modified, self._creating):
                for oid in oid_iterator:
                    obj = self._cache.get(oid, None)
                    # Ignore missing objects and don't update ghosts.
                    if obj is not None and obj._p_changed is not None:
                        obj._p_changed = 0
                        obj._p_serial = serial
        # </patch>
        self._tpc_cleanup()

    global OLD_ZODB
    OLD_ZODB = h in (
        'ab9b1b8d82c40e5fffa84f7bc4ea3a8b', # Python 2.7
        )

    if OLD_ZODB:
        Connection.tpc_finish = tpc_finish
    elif hasattr(Connection, '_handle_serial'): # merged upstream ?
        assert hasattr(Connection, '_warn_about_returned_serial')

    # sync() is used to provide a "network barrier", which is required for
    # NEO & ZEO to make sure our view of the storage includes all changes done
    # so far by other clients. But a round-trip to the server introduces
    # latency so it must not be done when it's not useful. Note also that a
    # successful commit (which ends with a response from the master) already
    # acts as a "network barrier".
    # BBB: What this monkey-patch does has been merged in ZODB5.
    if not hasattr(Connection, '_flush_invalidations'):
        return

    assert H(Connection.afterCompletion) in (
        'cd3a080b80fd957190ff3bb867149448', # Python 2.7
        )

    def afterCompletion(self, *ignored):
        self._readCurrent.clear()
        # PATCH: do not call sync()
        self._flush_invalidations()
    Connection.afterCompletion = afterCompletion

patch()

import app # set up signal handlers early enough to do it in the main thread
