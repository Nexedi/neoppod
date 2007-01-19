
from ZODB import BaseStorage, ConflictResolution, POSException
from ZODB.utils import p64, u64, cp, z64

class NEOStorageError(POSException.StorageError):
    pass

class NEOStorageConflictError(NEOStorageError):  
    pass

class NEOStorageNotFoundError(NEOStorageError):
    pass

class NEOStorage(BaseStorage.BaseStorage,
                 ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""
  
    def __init__(self, master_addr, master_port, read_only=False, **kw):
        self._is_read_only = read_only
        from neo.client.app import Application # here to prevent recursive import
        self.app = Application(master_addr, master_port)

    def load(self, oid, version=None):
        try:
            self.app.load(oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid)
          
    def close(self):
        self.app.close()

    def cleanup(self):
        self.app.cleanup()
  
    def lastSerial(self):
        self.app.lastSerial()
  
    def lastTransaction(self):
        self.app.lastTransaction()

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.new_oid()

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.tpc_begin(transaction, tid, status)

    def tpc_vote(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.tpc_vote(transaction)
  
    def tpc_abort(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.tpc_abort(transaction)
  
    def tpc_finish(self, transaction, f=None):
        self.app.tpc_finish(transaction, f)

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            self.app.store(oid, serial, data, version, transaction)
        except NEOStorageConflictError:
            new_data = self.app.tryToResolveConflict(oid, self.app.tid,
                                                     serial, data)
            if new_data is not None:
                # try again after conflict resolution
                self.store(oid, serial, new_data, version, transaction)
            else:
                raise POSException.ConflictError(oid=oid,
                                                 serials=(self.app.tid,
                                                          serial),data=data)
          
    def _clear_temp(self):
        self.app._clear_temp()

    def getSerial(self, oid):
        try:
            self.app.getSerial(oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid)
    
    # mutliple revisions
    def loadSerial(self, oid, serial):
        try:
            self.app.loadSerial(oid,serial)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, serial)

    def loadBefore(self, oid, tid):
        try:
            self.app.loadBefore(self, oid, tid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, tid)
  
    def iterator(self, start=None, stop=None):
        raise NotImplementedError

    # undo  
    def undo(self, transaction_id, txn):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.undo(transaction_id, txn)

    def undoInfo(self, first=0, last=-20, specification=None):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self.app.undoInfo(first, last, specification)
    
    def undoLog(self, first, last, filter):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        # This should not be used by ZODB
        # instead it should use undoInfo
        # Look at ZODB/interface.py for more info
        if filter is not None:            
            return []
        else:
            return self.undoInfo(first, last)

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
