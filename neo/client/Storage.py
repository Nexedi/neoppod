#
# Copyright (C) 2006-2019  Nexedi SA
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
from ZODB.POSException import ConflictError, UndoError
from zope.interface import implementer
import ZODB.interfaces

from neo.lib import logging
from neo.lib.util import tidFromTime
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
        except Exception:
            logging.exception('oid=%r', oid)
            raise

    def new_oid(self):
        try:
            return self.app.new_oid()
        except Exception:
            logging.exception('')
            raise

    def tpc_begin(self, transaction, tid=None, status=' '):
        """
        Note: never blocks in NEO.
        """
        try:
            return self.app.tpc_begin(self, transaction, tid, status)
        except Exception:
            logging.exception('transaction=%r, tid=%r', transaction, tid)
            raise

    def tpc_vote(self, transaction):
        try:
            return self.app.tpc_vote(transaction)
        except ConflictError:
            raise
        except Exception:
            logging.exception('transaction=%r', transaction)
            raise

    def tpc_abort(self, transaction):
        try:
            return self.app.tpc_abort(transaction)
        except Exception:
            logging.exception('transaction=%r', transaction)
            raise

    def tpc_finish(self, transaction, f=None):
        try:
            return self.app.tpc_finish(transaction, f)
        except Exception:
            logging.exception('transaction=%r', transaction)
            raise

    def store(self, oid, serial, data, version, transaction):
        assert version == '', 'Versions are not supported'
        try:
            return self.app.store(oid, serial, data, version, transaction)
        except ConflictError:
            raise
        except Exception:
            logging.exception('oid=%r, serial=%r, transaction=%r',
                              oid, serial, transaction)
            raise

    def deleteObject(self, oid, serial, transaction):
        try:
            self.app.store(oid, serial, None, None, transaction)
        except Exception:
            logging.exception('oid=%r, serial=%r, transaction=%r',
                              oid, serial, transaction)
            raise

    # multiple revisions
    def loadSerial(self, oid, serial):
        try:
            return self.app.load(oid, serial)[0]
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)
        except Exception:
            logging.exception('oid=%r, serial=%r', oid, serial)
            raise

    def loadBefore(self, oid, tid):
        try:
            return self.app.load(oid, None, tid)
        except NEOStorageDoesNotExistError:
            raise POSException.POSKeyError(oid)
        except NEOStorageNotFoundError:
            return None
        except Exception:
            logging.exception('oid=%r, tid=%r', oid, tid)
            raise

    @property
    def iterator(self):
        return self.app.iterator

    # undo
    def undo(self, transaction_id, txn):
        try:
            return self.app.undo(transaction_id, txn)
        except (ConflictError, UndoError):
            raise
        except Exception:
            logging.exception('transaction_id=%r, txn=%r', transaction_id, txn)
            raise

    def undoLog(self, first=0, last=-20, filter=None):
        try:
            return self.app.undoLog(first, last, filter)
        except Exception:
            logging.exception('first=%r, last=%r', first, last)
            raise

    def supportsUndo(self):
        return True

    def loadEx(self, oid, version):
        try:
            data, serial, _ = self.app.load(oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)
        except Exception:
            logging.exception('oid=%r', oid)
            raise
        return data, serial, ''

    def __len__(self):
        try:
            return self.app.getObjectCount()
        except Exception:
            logging.exception('')
            raise

    def registerDB(self, db, limit=None):
        self.app.registerDB(db, limit)

    def history(self, oid, *args, **kw):
        try:
            return self.app.history(oid, *args, **kw)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError(oid)
        except Exception:
            logging.exception('oid=%r', oid)
            raise

    def sync(self):
        return self.app.sync()

    def copyTransactionsFrom(self, source, verbose=False):
        """ Zope compliant API """
        try:
            return self.app.importFrom(self, source)
        except Exception:
            logging.exception('source=%r', source)
            raise

    def pack(self, t, referencesf, gc=False):
        if gc:
            logging.warning('Garbage Collection is not available in NEO,'
                ' please use an external tool. Packing without GC.')
        try:
            self.app.pack(tidFromTime(t))
        except Exception:
            logging.exception('pack_time=%r', t)
            raise

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
        except Exception:
            logging.exception('oid=%r', oid)
            raise

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        try:
            self.app.checkCurrentSerialInTransaction(oid, serial, transaction)
        except Exception:
            logging.exception('oid=%r, serial=%r, transaction=%r',
                              oid, serial, transaction)
            raise
