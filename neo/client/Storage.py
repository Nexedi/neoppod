#
# Copyright (C) 2006-2017  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from ZODB import BaseStorage, ConflictResolution, POSException
from zope.interface import implementer
import ZODB.interfaces

from neo.lib import logging
from .app import Application
from .exception import NEOStorageNotFoundError, NEOStorageDoesNotExistError

def raiseReadOnlyError(*args, **kw):
    raise POSException.ReadOnlyError()

@implementer(
        ZODB.interfaces.IStorage,
        # ZODB.interfaces.IStorageRestoreable,
        ZODB.interfaces.IStorageIteration,
        ZODB.interfaces.IStorageUndoable,
        ZODB.interfaces.IExternalGC,
        ZODB.interfaces.ReadVerifyingStorage,
    )
class Storage(BaseStorage.BaseStorage,
              ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    def __init__(self, master_nodes, name, read_only=False,
            compress=None, logfile=None, _app=None, **kw):
        """
        Do not pass those parameters (used internally):
        _app
        """
        if compress is None:
            compress = True
        if logfile:
            logging.setup(logfile)
        BaseStorage.BaseStorage.__init__(self, 'NEOStorage(%s)' % (name, ))
        # Warning: _is_read_only is used in BaseStorage, do not rename it.
        self._is_read_only = read_only
        if read_only:
            for method_id in (
                        'new_oid',
                        'tpc_begin',
                        'tpc_vote',
                        'tpc_abort',
                        'store',
                        'deleteObject',
                        'undo',
                        'undoLog',
                    ):
                setattr(self, method_id, raiseReadOnlyError)
        if _app is None:
            ssl = [kw.pop(x, None) for x in ('ca', 'cert', 'key')]
            _app = Application(master_nodes, name, compress=compress,
                               ssl=ssl if any(ssl) else None, **kw)
        self.app = _app

    @property
    def _cache(self):
        return self.app._cache

    def load(self, oid, version=''):
        # XXX: interface definition states that version parameter is
        # mandatory, while some ZODB tests do not provide it. For now, make
        # it optional.
        assert version == '', 'Versions are not supported'
        try:
            return self.app.load(oid)[:2]
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def new_oid(self):
        return self.app.new_oid()

    def tpc_begin(self, transaction, tid=None, status=' '):
        """
        Note: never blocks in NEO.
        """
        return self.app.tpc_begin(self, transaction, tid, status)

    def tpc_vote(self, transaction):
        return self.app.tpc_vote(transaction)

    def tpc_abort(self, transaction):
        return self.app.tpc_abort(transaction)

    def tpc_finish(self, transaction, f=None):
        return self.app.tpc_finish(transaction, f)

    def store(self, oid, serial, data, version, transaction):
        assert version == '', 'Versions are not supported'
        return self.app.store(oid, serial, data, version, transaction)

    def deleteObject(self, oid, serial, transaction):
        self.app.store(oid, serial, None, None, transaction)

    # multiple revisions
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

    @property
    def iterator(self):
        return self.app.iterator

    # undo
    def undo(self, transaction_id, txn):
        return self.app.undo(transaction_id, txn)

    def undoLog(self, first=0, last=-20, filter=None):
        return self.app.undoLog(first, last, filter)

    def supportsUndo(self):
        return True

    def loadEx(self, oid, version):
        try:
            data, serial, _ = self.app.load(oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)
        return data, serial, ''

    def __len__(self):
        return self.app.getObjectCount()

    def registerDB(self, db, limit=None):
        self.app.registerDB(db, limit)

    def history(self, oid, *args, **kw):
        try:
            return self.app.history(oid, *args, **kw)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)

    def sync(self):
        return self.app.sync()

    def copyTransactionsFrom(self, source, verbose=False):
        """ Zope compliant API """
        return self.importFrom(source)

    def importFrom(self, source, start=None, stop=None, preindex=None):
        """ Allow import only a part of the source storage """
        return self.app.importFrom(self, source, start, stop, preindex)

    def pack(self, t, referencesf, gc=False):
        if gc:
            logging.warning('Garbage Collection is not available in NEO,'
                ' please use an external tool. Packing without GC.')
        self.app.pack(t)

    def lastSerial(self):
        # seems unused
        raise NotImplementedError

    def lastTransaction(self):
        # Used in ZODB unit tests
        return self.app.last_tid

    def _clear_temp(self):
        raise NotImplementedError

    def set_max_oid(self, possible_new_max_oid):
        # seems used only by FileStorage
        raise NotImplementedError

    def close(self):
        # WARNING: This does not handle the case where an app is shared by
        #          several Storage instances, but this is something that only
        #          happens in threaded tests (and this method is not called on
        #          extra Storages).
        app = self.app
        if app is not None:
            self.app = None
            app.close()

    def getTid(self, oid):
        try:
            return self.app.getLastTID(oid)
        except NEOStorageNotFoundError:
            raise KeyError

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        self.app.checkCurrentSerialInTransaction(oid, serial, transaction)
