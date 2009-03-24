#
# Copyright (C) 2006-2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from Queue import Queue
from threading import Lock
from ZODB import BaseStorage, ConflictResolution, POSException
from ZODB.utils import oid_repr, p64, u64
import logging

from neo.client.dispatcher import Dispatcher
from neo.client.app import Application
from neo.client.exception import NEOStorageConflictError, NEOStorageNotFoundError
from neo.event import EventManager
from neo.util import dump
from neo import connector as Connector

class Storage(BaseStorage.BaseStorage,
              ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    __name__ = 'NEOStorage'

    def __init__(self, master_nodes, name, connector, read_only=False, **kw):
        self._is_read_only = read_only
        # Transaction must be under protection of lock
        l = Lock()
        self._txn_lock_acquire = l.acquire
        self._txn_lock_release = l.release
        # Create the event manager
        em = EventManager()
        # Create dispatcher thread
        dispatcher = Dispatcher(em)
        dispatcher.setDaemon(True)
        connector_handler = getattr(Connector, connector)
        self.app = Application(master_nodes, name, em, dispatcher,
                               connector_handler)
        # Connect to primary master node
        dispatcher.connectToPrimaryMasterNode(self.app, self.app.connector_handler)
        # Start dispatcher
        dispatcher.start()

    def load(self, oid, version=None):
        try:
            return self.app.load(oid=oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def close(self):
        return self.app.close()

    def cleanup(self):
        # Used in unit tests to remove local database files.
        # We have no such thing, so make this method a no-op.
        pass

    def lastSerial(self):
        # does not seem to be used
        raise NotImplementedError

    def lastTransaction(self):
        # does not seem to be used
        raise NotImplementedError

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.new_oid()

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._txn_lock_acquire()
        return self.app.tpc_begin(transaction=transaction, tid=tid, status=status)

    def tpc_vote(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.tpc_vote(transaction=transaction)

    def tpc_abort(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            return self.app.tpc_abort(transaction=transaction)
        finally:
            self._txn_lock_release()

    def tpc_finish(self, transaction, f=None):
        try:
            return self.app.tpc_finish(transaction=transaction, f=f)
        finally:
            self._txn_lock_release()

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            return self.app.store(oid = oid, serial = serial,
                                   data = data, version = version,
                                   transaction = transaction)
        except NEOStorageConflictError:
            if self.app.conflict_serial <= self.app.tid:
                # Try to resolve conflict only if conflicting serial is older
                # than the current transaction ID
                new_data = self.tryToResolveConflict(oid,
                                                     self.app.conflict_serial,
                                                     serial, data)
                if new_data is not None:
                    # Try again after conflict resolution
                    self.store(oid, self.app.conflict_serial,
                               new_data, version, transaction)
                    return ConflictResolution.ResolvedSerial
            raise POSException.ConflictError(oid=oid,
                                             serials=(self.app.tid,
                                                      serial),data=data)

    def _clear_temp(self):
        raise NotImplementedError

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
        raise NotImplementedError

    # undo
    def undo(self, transaction_id, txn):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            return self.app.undo(transaction_id = transaction_id,
                                 txn = txn, wrapper = self)
        except NEOStorageConflictError:
            raise POSException.ConflictError


    def undoLog(self, first, last, filter):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.undoLog(first, last, filter)

    def supportsUndo(self):
        return 0

    def abortVersion(self, src, transaction):
        return '', []

    def commitVersion(self, src, dest, transaction):
        return '', []

    def set_max_oid(self, possible_new_max_oid):
        # seems to be only use by FileStorage
        raise NotImplementedError

    def copyTransactionsFrom(self, other, verbose=0):
        raise NotImplementedError

    def __len__(self):
        # XXX bogus but how to implement this?
        return 0

    def registerDB(self, db, limit):
        self.app.registerDB(db, limit)

    def history(self, oid, version, length=1, filter=None):
        return self.app.history(oid, version, length, filter)

    def sync(self):
        self.app.sync()

