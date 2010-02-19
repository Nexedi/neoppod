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

from neo.client.app import Application
from neo.client.exception import NEOStorageNotFoundError

class Storage(BaseStorage.BaseStorage,
              ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    __name__ = 'NEOStorage'

    def __init__(self, master_nodes, name, connector=None, read_only=False,
                 **kw):
        BaseStorage.BaseStorage.__init__(self, name)
        self._is_read_only = read_only
        self.app = Application(master_nodes, name, connector)

    def load(self, oid, version=None):
        try:
            return self.app.load(oid=oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.new_oid()

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.tpc_begin(transaction=transaction, tid=tid,
                status=status)

    def tpc_vote(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.tpc_vote(transaction=transaction,
            tryToResolveConflict=self.tryToResolveConflict)

    def tpc_abort(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.tpc_abort(transaction=transaction)

    def tpc_finish(self, transaction, f=None):
        return self.app.tpc_finish(transaction=transaction, f=f)

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.store(oid=oid, serial=serial,
            data=data, version=version, transaction=transaction,
            tryToResolveConflict=self.tryToResolveConflict)

    def getSerial(self, oid):
        try:
            return self.app.getSerial(oid = oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    # mutliple revisions
    def loadSerial(self, oid, serial):
        try:
            return self.app.loadSerial(oid=oid, serial=serial)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, serial)

    def loadBefore(self, oid, tid):
        try:
            return self.app.loadBefore(oid=oid, tid=tid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, tid)

    def iterator(self, start=None, stop=None):
        return self.app.iterator(start, stop)

    # undo
    def undo(self, transaction_id, txn):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.undo(transaction_id=transaction_id, txn=txn,
            tryToResolveConflict=self.tryToResolveConflict)


    def undoLog(self, first, last, filter):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.undoLog(first, last, filter)

    def supportsUndo(self):
        return True

    def supportsTransactionalUndo(self):
        return True

    def abortVersion(self, src, transaction):
        return '', []

    def commitVersion(self, src, dest, transaction):
        return '', []

    def __len__(self):
        return self.app.getStorageSize()

    def registerDB(self, db, limit):
        self.app.registerDB(db, limit)

    def history(self, oid, version, length=1, filter=None):
        return self.app.history(oid, version, length, filter)

    def sync(self):
        self.app.sync()

#    def restore(self, oid, serial, data, version, prev_txn, transaction):
#        raise NotImplementedError

    def pack(self, t, referencesf):
        raise NotImplementedError

    def lastSerial(self):
        # seems unused
        raise NotImplementedError

    def lastTransaction(self):
        # seems unused
        raise NotImplementedError

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
        # The purpose of this method is unclear, the NEO implementation may
        # stop the client node or ask the primary master to shutdown/freeze the
        # cluster. For now make this a no-op.
        pass

