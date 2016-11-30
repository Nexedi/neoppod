#
# Copyright (C) 2010-2016  Nexedi SA
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

from time import time
from neo.lib import logging
from neo.lib.util import dump
from neo.lib.protocol import ProtocolError, uuid_str, ZERO_TID

class ConflictError(Exception):
    """
        Raised when a resolvable conflict occurs
        Argument: tid of locking transaction or latest revision
    """

    def __init__(self, tid):
        Exception.__init__(self)
        self._tid = tid

    def getTID(self):
        return self._tid


class DelayedError(Exception):
    """
        Raised when an object is locked by a previous transaction
    """

class NotRegisteredError(Exception):
    """
        Raised when a ttid is not registered
    """

class Transaction(object):
    """
        Container for a pending transaction
    """
    _tid = None
    has_trans = False

    def __init__(self, uuid, ttid):
        self._uuid = uuid
        self._ttid = ttid
        self._object_dict = {}
        self._locked = False
        self._birth = time()
        self._checked_set = set()

    def __repr__(self):
        return "<%s(ttid=%r, tid=%r, uuid=%r, locked=%r, age=%.2fs) at 0x%x>" \
         % (self.__class__.__name__,
            dump(self._ttid),
            dump(self._tid),
            uuid_str(self._uuid),
            self.isLocked(),
            time() - self._birth,
            id(self))

    def addCheckedObject(self, oid):
        assert oid not in self._object_dict, dump(oid)
        self._checked_set.add(oid)

    def getTTID(self):
        return self._ttid

    def setTID(self, tid):
        assert self._tid is None, dump(self._tid)
        assert tid is not None
        self._tid = tid

    def getTID(self):
        return self._tid

    def getUUID(self):
        return self._uuid

    def lock(self):
        assert not self._locked
        self._locked = True

    def isLocked(self):
        return self._locked

    def addObject(self, oid, data_id, value_serial):
        """
            Add an object to the transaction
        """
        assert oid not in self._checked_set, dump(oid)
        self._object_dict[oid] = oid, data_id, value_serial

    def delObject(self, oid):
        try:
            return self._object_dict.pop(oid)[1]
        except KeyError:
            self._checked_set.remove(oid)

    def getObject(self, oid):
        return self._object_dict[oid]

    def getObjectList(self):
        return self._object_dict.values()

    def getOIDList(self):
        return self._object_dict.keys()

    def getLockedOIDList(self):
        return self._object_dict.keys() + list(self._checked_set)


class TransactionManager(object):
    """
        Manage pending transaction and locks
    """

    def __init__(self, app):
        self._app = app
        self._transaction_dict = {}
        self._store_lock_dict = {}
        self._load_lock_dict = {}

    def register(self, conn, ttid):
        """
            Register a transaction, it may be already registered
        """
        if ttid not in self._transaction_dict:
            uuid = conn.getUUID()
            logging.debug('Register TXN %s for %s', dump(ttid), uuid_str(uuid))
            self._transaction_dict[ttid] = Transaction(uuid, ttid)

    def getObjectFromTransaction(self, ttid, oid):
        """
            Return object data for given running transaction.
            Return None if not found.
        """
        try:
            return self._transaction_dict[ttid].getObject(oid)
        except KeyError:
            return None

    def reset(self):
        """
            Reset the transaction manager
        """
        self._transaction_dict.clear()
        self._store_lock_dict.clear()
        self._load_lock_dict.clear()

    def vote(self, ttid, txn_info=None):
        """
            Store transaction information received from client node
        """
        logging.debug('Vote TXN %s', dump(ttid))
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))
        object_list = transaction.getObjectList()
        if txn_info:
            user, desc, ext, oid_list = txn_info
            txn_info = oid_list, user, desc, ext, False, ttid
            transaction.has_trans = True
        # store metadata to temporary table
        dm = self._app.dm
        dm.storeTransaction(ttid, object_list, txn_info)
        dm.commit()

    def lock(self, ttid, tid):
        """
            Lock a transaction
        """
        logging.debug('Lock TXN %s (ttid=%s)', dump(tid), dump(ttid))
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))
        # remember that the transaction has been locked
        transaction.lock()
        self._load_lock_dict.update(
            dict.fromkeys(transaction.getOIDList(), ttid))
        # commit transaction and remember its definitive TID
        if transaction.has_trans:
            self._app.dm.lockTransaction(tid, ttid)
        transaction.setTID(tid)

    def unlock(self, ttid):
        """
            Unlock transaction
        """
        try:
            tid = self._transaction_dict[ttid].getTID()
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))
        logging.debug('Unlock TXN %s (ttid=%s)', dump(tid), dump(ttid))
        dm = self._app.dm
        dm.unlockTransaction(tid, ttid)
        self._app.em.setTimeout(time() + 1, dm.deferCommit())
        self.abort(ttid, even_if_locked=True)

    def getFinalTID(self, ttid):
        try:
            return self._transaction_dict[ttid].getTID()
        except KeyError:
            return self._app.dm.getFinalTID(ttid)

    def getLockingTID(self, oid):
        return self._store_lock_dict.get(oid)

    def lockObject(self, ttid, serial, oid, unlock=False):
        """
            Take a write lock on given object, checking that "serial" is
            current.
            Raises:
                DelayedError
                ConflictError
        """
        # check if the object if locked
        locking_tid = self._store_lock_dict.get(oid)
        if locking_tid == ttid and unlock:
            logging.info('Deadlock resolution on %r:%r', dump(oid), dump(ttid))
            # A duplicate store means client is resolving a deadlock, so
            # drop the lock it held on this object, and drop object data for
            # consistency.
            del self._store_lock_dict[oid]
            data_id = self._transaction_dict[ttid].delObject(oid)
            if data_id:
                self._app.dm.pruneData((data_id,))
            # Give a chance to pending events to take that lock now.
            self._app.executeQueuedEvents()
            # Attemp to acquire lock again.
            locking_tid = self._store_lock_dict.get(oid)
        if locking_tid is None:
            previous_serial = None
        elif locking_tid == ttid:
            # If previous store was an undo, next store must be based on
            # undo target.
            previous_serial = self._transaction_dict[ttid].getObject(oid)[2]
            if previous_serial is None:
                # XXX: use some special serial when previous store was not
                # an undo ? Maybe it should just not happen.
                logging.info('Transaction %s storing %s more than once',
                             dump(ttid), dump(oid))
        elif locking_tid < ttid:
            # We have a bigger TTID than locking transaction, so we are younger:
            # enter waiting queue so we are handled when lock gets released.
            # We also want to delay (instead of conflict) if the client is
            # so faster that it is committing another transaction before we
            # processed UnlockInformation from the master.
            logging.info('Store delayed for %r:%r by %r', dump(oid),
                    dump(ttid), dump(locking_tid))
            raise DelayedError
        else:
            # We have a smaller TTID than locking transaction, so we are older:
            # this is a possible deadlock case, as we might already hold locks
            # the younger transaction is waiting upon. Make client release
            # locks & reacquire them by notifying it of the possible deadlock.
            logging.info('Possible deadlock on %r:%r with %r',
                dump(oid), dump(ttid), dump(locking_tid))
            raise ConflictError(ZERO_TID)
        # XXX: Consider locking before reporting a conflict:
        #      - That would speed up the case of cascading conflict resolution
        #        by avoiding incremental resolution, assuming that the time to
        #        resolve a conflict is often constant: "C+A vs. B -> C+A+B"
        #        rarely costs more than "C+A vs. C+B -> C+A+B".
        #      - That would slow down of cascading unresolvable conflicts but
        #        if that happens, the application should be reviewed.
        if previous_serial is None:
            previous_serial = self._app.dm.getLastObjectTID(oid)
        if previous_serial is not None and previous_serial != serial:
            logging.info('Resolvable conflict on %r:%r',
                dump(oid), dump(ttid))
            raise ConflictError(previous_serial)
        logging.debug('Transaction %s storing %s', dump(ttid), dump(oid))
        self._store_lock_dict[oid] = ttid

    def checkCurrentSerial(self, ttid, serial, oid):
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise NotRegisteredError
        self.lockObject(ttid, serial, oid, unlock=True)
        transaction.addCheckedObject(oid)

    def storeObject(self, ttid, serial, oid, compression, checksum, data,
            value_serial, unlock=False):
        """
            Store an object received from client node
        """
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise NotRegisteredError
        self.lockObject(ttid, serial, oid, unlock=unlock)
        # store object
        if data is None:
            data_id = None
        else:
            data_id = self._app.dm.holdData(checksum, data, compression)
        transaction.addObject(oid, data_id, value_serial)

    def abort(self, ttid, even_if_locked=False):
        """
            Abort a transaction
            Releases locks held on all transaction objects, deletes Transaction
            instance, and executed queued events.
            Note: does not alter persistent content.
        """
        if ttid not in self._transaction_dict:
            # the tid may be unknown as the transaction is aborted on every node
            # of the partition, even if no data was received (eg. conflict on
            # another node)
            return
        logging.debug('Abort TXN %s', dump(ttid))
        transaction = self._transaction_dict[ttid]
        has_load_lock = transaction.isLocked()
        # if the transaction is locked, ensure we can drop it
        if has_load_lock:
            if not even_if_locked:
                return
        else:
            self._app.dm.abortTransaction(ttid)
        # unlock any object
        for oid in transaction.getLockedOIDList():
            if has_load_lock:
                lock_ttid = self._load_lock_dict.pop(oid, None)
                assert lock_ttid in (ttid, None), 'Transaction %s tried to ' \
                    'release the lock on oid %s, but it was held by %s' % (
                    dump(ttid), dump(oid), dump(lock_ttid))
            write_locking_tid = self._store_lock_dict.pop(oid)
            assert write_locking_tid == ttid, 'Inconsistent locking state: ' \
                'aborting %s:%s but %s has the lock.' % (dump(ttid), dump(oid),
                    dump(write_locking_tid))
        # remove the transaction
        del self._transaction_dict[ttid]
        # some locks were released, some pending locks may now succeed
        self._app.executeQueuedEvents()

    def abortFor(self, uuid):
        """
            Abort any non-locked transaction of a node
        """
        logging.debug('Abort for %s', uuid_str(uuid))
        # abort any non-locked transaction of this node
        for transaction in self._transaction_dict.values():
            if transaction.getUUID() == uuid:
                self.abort(transaction.getTTID())

    def isLockedTid(self, tid):
        for t in self._transaction_dict.itervalues():
            if t.isLocked() and t.getTID() <= tid:
                return True
        return False

    def loadLocked(self, oid):
        return oid in self._load_lock_dict

    def log(self):
        logging.info("Transactions:")
        for txn in self._transaction_dict.values():
            logging.info('    %r', txn)
        logging.info('  Read locks:')
        for oid, ttid in self._load_lock_dict.items():
            logging.info('    %r by %r', dump(oid), dump(ttid))
        logging.info('  Write locks:')
        for oid, ttid in self._store_lock_dict.items():
            logging.info('    %r by %r', dump(oid), dump(ttid))

    def updateObjectDataForPack(self, oid, orig_serial, new_serial, data_id):
        lock_tid = self.getLockingTID(oid)
        if lock_tid is not None:
            transaction = self._transaction_dict[lock_tid]
            if transaction.getObject(oid)[2] == orig_serial:
                if new_serial:
                    data_id = None
                else:
                    self._app.dm.holdData(data_id)
                transaction.addObject(oid, data_id, new_serial)
