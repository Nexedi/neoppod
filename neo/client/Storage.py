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

from neo import setupLog
from neo.client.app import Application
from neo.client.exception import NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError

def check_read_only(func):
    def wrapped(self, *args, **kw):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return func(self, *args, **kw)
    return wrapped

class DummyCache(object):
    def __init__(self, app):
        self.app = app

    def clear(self):
        self.app.mq_cache.clear()

class Storage(BaseStorage.BaseStorage,
              ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    implements(
        ZODB.interfaces.IStorage,
        # "restore" missing for the moment, but "store" implements this
        # interface.
        # ZODB.interfaces.IStorageRestoreable,
        # XXX: imperfect iterator implementation:
        # - start & stop are not handled (raises if either is not None)
        # - transaction isolation is not done
        # ZODB.interfaces.IStorageIteration,
        ZODB.interfaces.IStorageUndoable,
    )

    def __init__(self, master_nodes, name, connector=None, read_only=False,
                 compress=None, logfile=None, verbose=False, **kw):
        if compress is None:
            compress = True
        setupLog('CLIENT', filename=logfile, verbose=verbose)
        BaseStorage.BaseStorage.__init__(self, 'NEOStorage(%s)' % (name, ))
        # Warning: _is_read_only is used in BaseStorage, do not rename it.
        self._is_read_only = read_only
        self.app = Application(master_nodes, name, connector,
            compress=compress)
        self._cache = DummyCache(self.app)

    def load(self, oid, version):
        assert version == '', 'Versions are not supported'
        try:
            return self.app.load(oid=oid)
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
        return self.app.tpc_finish(transaction=transaction,
            tryToResolveConflict=self.tryToResolveConflict, f=f)

    @check_read_only
    def store(self, oid, serial, data, version, transaction):
        assert version == '', 'Versions are not supported'
        return self.app.store(oid=oid, serial=serial,
            data=data, version=version, transaction=transaction)

    @check_read_only
    def deleteObject(oid, serial, transaction):
        self.app.store(oid=oid, serial=serial, data='', version=None,
            transaction=transaction)

    # mutliple revisions
    def loadSerial(self, oid, serial):
        try:
            return self.app.loadSerial(oid=oid, serial=serial)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def loadBefore(self, oid, tid):
        try:
            return self.app.loadBefore(oid=oid, tid=tid)
        except NEOStorageDoesNotExistError:
            raise POSException.POSKeyError(oid)
        except NEOStorageNotFoundError:
            return None

    def iterator(self, start=None, stop=None):
        return self.app.iterator(start, stop)

    # undo
    @check_read_only
    def undo(self, transaction_id, txn):
        return self.app.undo(undone_tid=transaction_id, txn=txn,
            tryToResolveConflict=self.tryToResolveConflict)


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
            return self.app.loadEx(oid=oid, version=version)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def __len__(self):
        return self.app.getStorageSize()

    def registerDB(self, db, limit=None):
        self.app.registerDB(db, limit)

    def history(self, oid, version=None, size=1, filter=None):
        return self.app.history(oid, version, size, filter)

    def sync(self):
        self.app.sync()

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
            neo.logging.warning('Garbage Collection is not available in NEO, '
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

    def getTID(self, oid):
        return self.app.getLastTID(oid)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self.app.checkCurrentSerialInTransaction(oid, serial, transaction)

