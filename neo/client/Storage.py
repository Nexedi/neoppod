#
# Copyright (C) 2006-2010  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from ZODB import BaseStorage, ConflictResolution, POSException
from zope.interface import implements
import ZODB.interfaces

import neo.lib
from functools import wraps
from neo.lib import setupLog
from neo.lib.util import add64
from neo.lib.protocol import ZERO_TID
from neo.client.app import Application
from neo.client.exception import NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError

def check_read_only(func):
    def wrapped(self, *args, **kw):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return func(self, *args, **kw)
    return wraps(func)(wrapped)

def old_history_api(func):
    try:
        if ZODB.interfaces.IStorage['history'].positional[1] != 'version':
            return func # ZODB >= 3.9
    except KeyError: # ZODB < 3.8
        pass
    def history(self, oid, version=None, *args, **kw):
        if version is None:
            return func(self, oid, *args, **kw)
        raise ValueError('Versions are not supported')
    return wraps(func)(history)

class Storage(BaseStorage.BaseStorage,
              ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    # Stores the highest TID visible for current transaction.
    # First call sets this snapshot by asking master node most recent
    # committed TID.
    # As a (positive) side-effect, this forces us to handle all pending
    # invalidations, so we get a very recent view of the database (which is
    # good when multiple databases are used in the same program with some
    # amount of referential integrity).
    # Should remain None when not bound to a connection,
    # so that it always read the last revision.
    _snapshot_tid = None

    implements(*filter(None, (
        ZODB.interfaces.IStorage,
        # "restore" missing for the moment, but "store" implements this
        # interface.
        # ZODB.interfaces.IStorageRestoreable,
        # XXX: imperfect iterator implementation:
        # - start & stop are not handled (raises if either is not None)
        # - transaction isolation is not done
        # ZODB.interfaces.IStorageIteration,
        ZODB.interfaces.IStorageUndoable,
        getattr(ZODB.interfaces, 'IExternalGC', None), # XXX ZODB < 3.9
        getattr(ZODB.interfaces, 'ReadVerifyingStorage', None), # XXX ZODB 3.9
    )))

    def __init__(self, master_nodes, name, read_only=False,
            compress=None, logfile=None, verbose=False, _app=None, **kw):
        """
        Do not pass those parameters (used internally):
        _app
        _cache
        """
        if compress is None:
            compress = True
        setupLog('CLIENT', filename=logfile, verbose=verbose)
        BaseStorage.BaseStorage.__init__(self, 'NEOStorage(%s)' % (name, ))
        # Warning: _is_read_only is used in BaseStorage, do not rename it.
        self._is_read_only = read_only
        if _app is None:
            _app = Application(master_nodes, name, compress=compress)
        self.app = _app
        # Used to clone self (see new_instance & IMVCCStorage definition).
        self._init_args = (master_nodes, name)
        self._init_kw = {
            'read_only': read_only,
            'compress': compress,
            'logfile': logfile,
            'verbose': verbose,
            '_app': _app,
        }

    @property
    def _cache(self):
        return self.app._cache

    def load(self, oid, version=''):
        # In order to know if it was safe to get the last revision of an object
        # instead of using loadBefore(), ZODB.Connection._setstate relies on
        # the fact that retrieving data from a remote storage forces incoming
        # invalidations to be received.
        # But in NEO, invalidations are not received from the same network
        # connection that the one used to retrieve data.
        # So we must implement load() like a loadBefore().
        # XXX: interface definition states that version parameter is
        # mandatory, while some ZODB tests do not provide it. For now, make
        # it optional.
        assert version == '', 'Versions are not supported'
        try:
            return self.app.load(oid, None, self._snapshot_tid)[:2]
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    @check_read_only
    def new_oid(self):
        return self.app.new_oid()

    @check_read_only
    def tpc_begin(self, transaction, tid=None, status=' '):
        """
        Note: never blocks in NEO.
        """
        return self.app.tpc_begin(transaction=transaction, tid=tid,
                status=status)

    @check_read_only
    def tpc_vote(self, transaction):
        return self.app.tpc_vote(transaction=transaction,
            tryToResolveConflict=self.tryToResolveConflict)

    @check_read_only
    def tpc_abort(self, transaction):
        return self.app.tpc_abort(transaction=transaction)

    def tpc_finish(self, transaction, f=None):
        tid = self.app.tpc_finish(transaction=transaction,
            tryToResolveConflict=self.tryToResolveConflict, f=f)
        # XXX: Note that when undoing changes, the following is useless because
        #      a temporary Storage object is used to commit.
        #      See also testZODB.NEOZODBTests.checkMultipleUndoInOneTransaction
        if self._snapshot_tid:
            self._snapshot_tid = add64(tid, 1)
        return tid

    @check_read_only
    def store(self, oid, serial, data, version, transaction):
        assert version == '', 'Versions are not supported'
        return self.app.store(oid=oid, serial=serial,
            data=data, version=version, transaction=transaction)

    @check_read_only
    def deleteObject(self, oid, serial, transaction):
        self.app.store(oid=oid, serial=serial, data='', version=None,
            transaction=transaction)

    # mutliple revisions
    def loadSerial(self, oid, serial):
        try:
            return self.app.load(oid, serial)[0]
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def loadBefore(self, oid, tid):
        try:
            return self.app.load(oid, None, tid)
        except NEOStorageDoesNotExistError:
            raise POSException.POSKeyError(oid)
        except NEOStorageNotFoundError:
            return None

    def iterator(self, start=None, stop=None):
        # Iterator lives in its own transaction, so get a fresh snapshot.
        snapshot_tid = self.lastTransaction()
        if stop is None:
            stop = snapshot_tid
        else:
            stop = min(snapshot_tid, stop)
        return self.app.iterator(start, stop)

    # undo
    @check_read_only
    def undo(self, transaction_id, txn):
        return self.app.undo(self._snapshot_tid, undone_tid=transaction_id,
            txn=txn, tryToResolveConflict=self.tryToResolveConflict)


    @check_read_only
    def undoLog(self, first=0, last=-20, filter=None):
        return self.app.undoLog(first, last, filter)

    def supportsUndo(self):
        return True

    def supportsTransactionalUndo(self):
        return True

    @check_read_only
    def abortVersion(self, src, transaction):
        return self.app.abortVersion(src, transaction)

    @check_read_only
    def commitVersion(self, src, dest, transaction):
        return self.app.commitVersion(src, dest, transaction)

    def loadEx(self, oid, version):
        try:
            data, serial, _ = self.app.load(oid, None, self._snapshot_tid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)
        return data, serial, ''

    def __len__(self):
        return self.app.getStorageSize()

    def registerDB(self, db, limit=None):
        self.app.registerDB(db, limit)

    @old_history_api
    def history(self, oid, *args, **kw):
        try:
            return self.app.history(oid, *args, **kw)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def sync(self, force=True):
        # Increment by one, as we will use this as an excluded upper
        # bound (loadBefore).
        self._snapshot_tid = add64(self.lastTransaction(), 1)

    def copyTransactionsFrom(self, source, verbose=False):
        """ Zope compliant API """
        return self.app.importFrom(source, None, None,
                self.tryToResolveConflict)

    def importFrom(self, source, start=None, stop=None):
        """ Allow import only a part of the source storage """
        return self.app.importFrom(source, start, stop,
                self.tryToResolveConflict)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        raise NotImplementedError

    def pack(self, t, referencesf, gc=False):
        if gc:
            neo.lib.logging.warning(
                'Garbage Collection is not available in NEO, '
                'please use an external tool. Packing without GC.')
        self.app.pack(t)

    def lastSerial(self):
        # seems unused
        raise NotImplementedError

    def lastTransaction(self):
        # Used in ZODB unit tests
        return self.app.lastTransaction()

    def _clear_temp(self):
        raise NotImplementedError

    def set_max_oid(self, possible_new_max_oid):
        # seems used only by FileStorage
        raise NotImplementedError

    def cleanup(self):
        # Used in unit tests to remove local database files.
        # We have no such thing, so make this method a no-op.
        pass

    def close(self):
        self.app.close()

    def getTid(self, oid):
        try:
            return self.app.getLastTID(oid)
        except NEOStorageNotFoundError:
            raise KeyError

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self.app.checkCurrentSerialInTransaction(oid, serial, transaction)

    def new_instance(self):
        return Storage(*self._init_args, **self._init_kw)
