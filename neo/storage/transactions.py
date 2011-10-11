#
# Copyright (C) 2010  Nexedi SA
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

from time import time
import neo.lib
from neo.lib.util import dump
from neo.lib.protocol import ZERO_TID

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


class Transaction(object):
    """
        Container for a pending transaction
    """
    _tid = None

    def __init__(self, uuid, ttid):
        self._uuid = uuid
        self._ttid = ttid
        self._object_dict = {}
        self._transaction = None
        self._locked = False
        self._birth = time()
        self._checked_set = set()

    def __repr__(self):
        return "<%s(ttid=%r, tid=%r, uuid=%r, locked=%r, age=%.2fs)> at %x" % (
            self.__class__.__name__,
            dump(self._ttid),
            dump(self._tid),
            dump(self._uuid),
            self.isLocked(),
            time() - self._birth,
            id(self),
        )

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

    def prepare(self, oid_list, user, desc, ext, packed):
        """
            Set the transaction informations
        """
        # assert self._transaction is not None
        self._transaction = (oid_list, user, desc, ext, packed)

    def addObject(self, oid, checksum, value_serial):
        """
            Add an object to the transaction
        """
        assert oid not in self._checked_set, dump(oid)
        self._object_dict[oid] = oid, checksum, value_serial

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

    def getTransactionInformations(self):
        return self._transaction


class TransactionManager(object):
    """
        Manage pending transaction and locks
    """

    def __init__(self, app):
        self._app = app
        self._transaction_dict = {}
        self._store_lock_dict = {}
        self._load_lock_dict = {}
        self._uuid_dict = {}

    def __contains__(self, ttid):
        """
            Returns True if the TID is known by the manager
        """
        return ttid in self._transaction_dict

    def register(self, uuid, ttid):
        """
            Register a transaction, it may be already registered
        """
        neo.lib.logging.debug('Register TXN %s for %s', dump(ttid), dump(uuid))
        transaction = self._transaction_dict.get(ttid, None)
        if transaction is None:
            transaction = Transaction(uuid, ttid)
            self._uuid_dict.setdefault(uuid, set()).add(transaction)
            self._transaction_dict[ttid] = transaction
        return transaction

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
        self._uuid_dict.clear()

    def lock(self, ttid, tid, oid_list):
        """
            Lock a transaction
        """
        neo.lib.logging.debug('Lock TXN %s (ttid=%s)', dump(tid), dump(ttid))
        transaction = self._transaction_dict[ttid]
        # remember that the transaction has been locked
        transaction.lock()
        for oid in transaction.getOIDList():
            self._load_lock_dict[oid] = ttid
        # check every object that should be locked
        uuid = transaction.getUUID()
        is_assigned = self._app.pt.isAssigned
        for oid in oid_list:
            if is_assigned(oid, uuid) and \
                    self._load_lock_dict.get(oid) != ttid:
                raise ValueError, 'Some locks are not held'
        object_list = transaction.getObjectList()
        # txn_info is None is the transaction information is not stored on
        # this storage.
        txn_info = transaction.getTransactionInformations()
        # store data from memory to temporary table
        self._app.dm.storeTransaction(tid, object_list, txn_info)
        # ...and remember its definitive TID
        transaction.setTID(tid)

    def getTIDFromTTID(self, ttid):
        return self._transaction_dict[ttid].getTID()

    def unlock(self, ttid):
        """
            Unlock transaction
        """
        neo.lib.logging.debug('Unlock TXN %s', dump(ttid))
        self._app.dm.finishTransaction(self.getTIDFromTTID(ttid))
        self.abort(ttid, even_if_locked=True)

    def storeTransaction(self, ttid, oid_list, user, desc, ext, packed):
        """
            Store transaction information received from client node
        """
        assert ttid in self, "Transaction not registered"
        transaction = self._transaction_dict[ttid]
        transaction.prepare(oid_list, user, desc, ext, packed)

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
            neo.lib.logging.info('Deadlock resolution on %r:%r', dump(oid),
                dump(ttid))
            # A duplicate store means client is resolving a deadlock, so
            # drop the lock it held on this object, and drop object data for
            # consistency.
            del self._store_lock_dict[oid]
            checksum = self._transaction_dict[ttid].delObject(oid)
            if checksum:
                self._app.dm.pruneData((checksum,))
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
                neo.lib.logging.info('Transaction %s storing %s more than '
                    'once', dump(ttid), dump(oid))
        elif locking_tid < ttid:
            # We have a bigger TTID than locking transaction, so we are younger:
            # enter waiting queue so we are handled when lock gets released.
            # We also want to delay (instead of conflict) if the client is
            # so faster that it is committing another transaction before we
            # processed UnlockInformation from the master.
            neo.lib.logging.info('Store delayed for %r:%r by %r', dump(oid),
                    dump(ttid), dump(locking_tid))
            raise DelayedError
        else:
            # We have a smaller TTID than locking transaction, so we are older:
            # this is a possible deadlock case, as we might already hold locks
            # the younger transaction is waiting upon. Make client release
            # locks & reacquire them by notifying it of the possible deadlock.
            neo.lib.logging.info('Possible deadlock on %r:%r with %r',
                dump(oid), dump(ttid), dump(locking_tid))
            raise ConflictError(ZERO_TID)
        if previous_serial is None:
            history_list = self._app.dm.getObjectHistory(oid)
            if history_list:
                previous_serial = history_list[0][0]
        if previous_serial is not None and previous_serial != serial:
            neo.lib.logging.info('Resolvable conflict on %r:%r',
                dump(oid), dump(ttid))
            raise ConflictError(previous_serial)
        neo.lib.logging.debug('Transaction %s storing %s',
            dump(ttid), dump(oid))
        self._store_lock_dict[oid] = ttid

    def checkCurrentSerial(self, ttid, serial, oid):
        self.lockObject(ttid, serial, oid, unlock=True)
        assert ttid in self, "Transaction not registered"
        transaction = self._transaction_dict[ttid]
        transaction.addCheckedObject(oid)

    def storeObject(self, ttid, serial, oid, compression, checksum, data,
            value_serial, unlock=False):
        """
            Store an object received from client node
        """
        self.lockObject(ttid, serial, oid, unlock=unlock)
        # store object
        assert ttid in self, "Transaction not registered"
        if data is None:
            checksum = None
        else:
            self._app.dm.storeData(checksum, data, compression)
        self._transaction_dict[ttid].addObject(oid, checksum, value_serial)

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
        neo.lib.logging.debug('Abort TXN %s', dump(ttid))
        transaction = self._transaction_dict[ttid]
        has_load_lock = transaction.isLocked()
        # if the transaction is locked, ensure we can drop it
        if has_load_lock:
            if not even_if_locked:
                return
        else:
            self._app.dm.unlockData([checksum
                for oid, checksum, value_serial in transaction.getObjectList()
                if checksum], True)
        # unlock any object
        for oid in transaction.getLockedOIDList():
            if has_load_lock:
                lock_ttid = self._load_lock_dict.pop(oid, None)
                assert lock_ttid in (ttid, None), 'Transaction %s tried to ' \
                    'release the lock on oid %s, but it was held by %s' % (
                    dump(ttid), dump(oid), dump(lock_tid))
            write_locking_tid = self._store_lock_dict.pop(oid)
            assert write_locking_tid == ttid, 'Inconsistent locking state: ' \
                'aborting %s:%s but %s has the lock.' % (dump(ttid), dump(oid),
                    dump(write_locking_tid))
        # remove the transaction
        uuid = transaction.getUUID()
        self._uuid_dict[uuid].discard(transaction)
        # clean node index if there is no more current transactions
        if not self._uuid_dict[uuid]:
            del self._uuid_dict[uuid]
        del self._transaction_dict[ttid]
        # some locks were released, some pending locks may now succeed
        self._app.executeQueuedEvents()

    def abortFor(self, uuid):
        """
            Abort any non-locked transaction of a node
        """
        neo.lib.logging.debug('Abort for %s', dump(uuid))
        # abort any non-locked transaction of this node
        for ttid in [x.getTTID() for x in self._uuid_dict.get(uuid, [])]:
            self.abort(ttid)
        # cleanup _uuid_dict if no transaction remains for this node
        transaction_set = self._uuid_dict.get(uuid)
        if transaction_set is not None and not transaction_set:
            del self._uuid_dict[uuid]

    def loadLocked(self, oid):
        return oid in self._load_lock_dict

    def log(self):
        neo.lib.logging.info("Transactions:")
        for txn in self._transaction_dict.values():
            neo.lib.logging.info('    %r', txn)
        neo.lib.logging.info('  Read locks:')
        for oid, ttid in self._load_lock_dict.items():
            neo.lib.logging.info('    %r by %r', dump(oid), dump(ttid))
        neo.lib.logging.info('  Write locks:')
        for oid, ttid in self._store_lock_dict.items():
            neo.lib.logging.info('    %r by %r', dump(oid), dump(ttid))

    def updateObjectDataForPack(self, oid, orig_serial, new_serial, checksum):
        lock_tid = self.getLockingTID(oid)
        if lock_tid is not None:
            transaction = self._transaction_dict[lock_tid]
            if transaction.getObject(oid)[2] == orig_serial:
                if new_serial:
                    checksum = None
                else:
                    self._app.dm.storeData(checksum)
                transaction.addObject(oid, checksum, new_serial)
