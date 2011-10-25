##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
            # BBB: _mvcc_storage not supported on older ZODB
            if getattr(self, '_mvcc_storage', False):
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
                    self._db._r()                      # this is a RLock
                except (AssertionError, RuntimeError): # old Python uses assert
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
        try:
            Connection_open = Connection._setDB
            Connection._setDB = open
        except AttributeError: # recent ZODB
            Connection_open = Connection.open
            Connection.open = open

        # Storage.sync usually implements a "network barrier" (at least
        # in NEO, but ZEO should be fixed to do the same), which is quite
        # slow so we prefer to not call it where it's not useful.
        # I don't know any legitimate use of DB access outside a transaction.

        # But old versions of ERP5 (before 2010-10-29 17:15:34) and maybe other
        # applications do not always call 'transaction.begin()' when they should
        # so this patch disabled as a precaution, at least as long as we support
        # old software. This should also be discussed on zodb-dev ML first.

        def afterCompletion(self, *ignored):
            try:
                self._readCurrent.clear()
            except AttributeError: # old ZODB (e.g. ZODB 3.4)
                pass
            self._flush_invalidations()
        #Connection.afterCompletion = afterCompletion


    class _DB(object):
        """
        Wrapper to DB instance that properly initialize Connection objects
        with NEO storages.
        It forces the connection to always create a new instance of the
        storage, for compatibility with ZODB 3.4, and because we don't
        implement IMVCCStorage completely.
        """

        def __new__(cls, db, connection):
            if db._storage.__class__.__module__ != 'neo.client.Storage':
                return db
            self = object.__new__(cls)
            self._db = db
            self._connection = connection
            return self

        def __getattr__(self, attr):
            result = getattr(self._db, attr)
            if attr in ('storage', '_storage'):
                result = result.new_instance()
                self._connection._db = self._db
                setattr(self, attr, result)
            return result

    try:
        Connection_setDB = Connection._setDB
    except AttributeError: # recent ZODB
        Connection_init = Connection.__init__
        Connection.__init__ = lambda self, db, *args, **kw: \
            Connection_init(self, _DB(db, self), *args, **kw)
    else: # old ZODB (e.g. ZODB 3.4)
        Connection._setDB = lambda self, odb, *args, **kw: \
            Connection_setDB(self, _DB(odb, self), *args, **kw)

        from ZODB.DB import DB
        DB_invalidate = DB.invalidate
        DB.invalidate = lambda self, tid, oids, *args, **kw: \
            DB_invalidate(self, tid, dict.fromkeys(oids, None), *args, **kw)
