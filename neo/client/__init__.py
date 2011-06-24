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

# NEO requires ZODB to allow TID to be returned as late as tpc_finish.
# At the moment, no ZODB release include this patch.
# Later, this must be replaced by some detection mechanism.
needs_patch = True

if needs_patch:
    from ZODB.Connection import Connection

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
