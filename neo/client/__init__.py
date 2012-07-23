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

# At the moment, no ZODB release include the following patches.
# Later, this must be replaced by some detection mechanism.
if 1:

    from ZODB.Connection import Connection

    # Allow serial to be returned as late as tpc_finish
    #
    # This makes possible for storage to allocate serial inside tpc_finish,
    # removing the requirement to serialise second commit phase (tpc_vote
    # to tpc_finish/tpc_abort).

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
        self._tpc_cleanup()

    Connection.tpc_finish = tpc_finish

    ###

    try:
        if Connection._nexedi_fix != 5:
            raise Exception("A different ZODB fix is already applied")
    except AttributeError:
        Connection._nexedi_fix = 5

        # Whenever an connection is opened (and there's usually an existing one
        # in DB pool that can be reused) whereas the transaction is already
        # started, we must make sure that proper storage setup is done by
        # calling Connection.newTransaction.
        # For example, there's no open transaction when a ZPublisher/Publish
        # transaction begins.

        def open(self, *args, **kw):
            def _flush_invalidations():
                acquire = self._db._a
                try:
                    self._db._r() # this is a RLock
                except RuntimeError:
                    acquire = lambda: None
                try:
                    del self._flush_invalidations
                    self.newTransaction()
                finally:
                    acquire()
                    self._flush_invalidations = _flush_invalidations
            self._flush_invalidations = _flush_invalidations
            try:
                Connection_open(self, *args, **kw)
            finally:
                del self._flush_invalidations

        Connection_open = Connection.open
        Connection.open = open

    # IStorage implementations usually need to provide a "network barrier",
    # at least for NEO & ZEO, to make sure we have an up-to-date view of
    # the storage. It's unclear whether sync() is a good place to do this
    # because a round-trip to the server introduces latency and we prefer
    # it's not done when it's not useful.
    # For example, we know we are up-to-date after a successful commit,
    # so this should not be done in afterCompletion(), and anyway, we don't
    # know any legitimate use of DB access outside a transaction.

    def afterCompletion(self, *ignored):
        try:
            self._readCurrent.clear()
        except AttributeError: # BBB: ZODB < 3.10
            pass
        # PATCH: do not call sync()
        self._flush_invalidations()
    Connection.afterCompletion = afterCompletion
