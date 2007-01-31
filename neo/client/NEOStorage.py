from Queue import Queue
from threading import Lock
from ZODB import BaseStorage, ConflictResolution, POSException
from ZODB.utils import oid_repr, p64, u64

from neo.client.dispatcher import Dispatcher
from neo.event import EventManager

class NEOStorageError(POSException.StorageError):
    pass

class NEOStorageConflictError(NEOStorageError):
    pass

class NEOStorageNotFoundError(NEOStorageError):
    pass

class NEOStorage(BaseStorage.BaseStorage,
                 ConflictResolution.ConflictResolvingStorage):
    """Wrapper class for neoclient."""

    __name__ = 'NEOStorage'

    def __init__(self, master_addr, master_port, name, read_only=False, **kw):
        self._is_read_only = read_only
        # Transaction must be under protection of lock
        l = Lock()
        self._txn_lock_acquire = l.acquire
        self._txn_lock_release = l.release
        # Create two queue for message between thread and dispatcher
        # - message queue is for message that has to be send to other node
        # through the dispatcher
        # - request queue is for message receive from other node which have to
        # be processed
        message_queue = Queue()
        request_queue = Queue()
        # Create the event manager
        em = EventManager()
        # Create dispatcher thread
        dispatcher = Dispatcher(em, message_queue, request_queue)
        dispatcher.setDaemon(True)
        dispatcher.start()
        # Import here to prevent recursive import
        from neo.client.app import Application
        self.app = Application(master_addr, master_port, name, em, dispatcher,
                               message_queue, request_queue)

    def load(self, oid, version=None):
        try:
            return self.app.process_method('load', oid=u64(oid))
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid)

    def close(self):
        return self.app.process_method('close')

    def cleanup(self):
        raise NotImplementedError

    def lastSerial(self):
        # does not seem to be used
        raise NotImplementedError

    def lastTransaction(self):
        # does not seem to be used
        raise NotImplementedError

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.process_method('new_oid')

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._txn_lock_acquire()
        return self.app.process_method('tpc_begin', transaction=transaction, tid=tid, status=status)

    def tpc_vote(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.app.process_method('tpc_vote', transaction=transaction)

    def tpc_abort(self, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            return self.app.process_method('tpc_abort', transaction=transaction)
        except:
            self._txn_lock_release()

    def tpc_finish(self, transaction, f=None):
        try:
            return self.app.process_method('tpc_finish', transaction=transaction, f=f)
        except:
            self._txn_lock_release()

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        try:
            return self.app.process_method('store', oid=oid, serial=serial, data=data,
                                           version=version, transaction=transaction)
        except NEOStorageConflictError, conflict_serial:
            if conflict_serial <= self.app.tid:
                # Try to resolve conflict only if conflicting serial is older
                # than the current transaction ID
                new_data = self.tryToResolveConflict(oid, self.app.tid,
                                                         serial, data)
                if new_data is not None:
                    # Try again after conflict resolution
                    return self.store(oid, serial, new_data, version, transaction)
            raise POSException.ConflictError(oid=oid,
                                             serials=(self.app.tid,
                                                      serial),data=data)

    def _clear_temp(self):
        raise NotImplementedError

    def getSerial(self, oid):
        try:
            return self.app.process_method('getSerial', oid=oid)
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid)

    # mutliple revisions
    def loadSerial(self, oid, serial):
        try:
            return self.app.process_method('loadSerial', oid=u64(oid), serial=u64(serial))
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, serial)

    def loadBefore(self, oid, tid):
        try:
            return self.app.process_method('loadBefore', oid=u64(oid), tid=u64(tid))
        except NEOStorageNotFoundError:
            raise POSException.POSKeyError (oid, tid)

    def iterator(self, start=None, stop=None):
        raise NotImplementedError

    # undo
    def undo(self, transaction_id, txn):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._txn_lock_acquire()
        try:
            return self.app.process_method('undo', transaction_id=transaction_id, txn=txn, wrapper=self)
        except:
            self._txn_lock_release()

    def undoLog(self, first, last, filter):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self.undoLog(first, last, filter)

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
