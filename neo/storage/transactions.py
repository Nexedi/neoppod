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
import neo
from neo.util import dump


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

    def __init__(self, uuid, tid):
        self._uuid = uuid
        self._tid = tid
        self._object_dict = {}
        self._transaction = None
        self._locked = False
        self._birth = time()
        self._checked_set = set()

    def __repr__(self):
        return "<%s(tid=%r, uuid=%r, locked=%r, age=%.2fs)> at %x" % (
            self.__class__.__name__,
            dump(self._tid),
            dump(self._uuid),
            self.isLocked(),
            time() - self._birth,
            id(self),
        )

    def addCheckedObject(self, oid):
        self._checked_set.add(oid)

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

    def addObject(self, oid, compression, checksum, data, value_serial):
        """
            Add an object to the transaction
        """
        self._object_dict[oid] = (oid, compression, checksum, data,
            value_serial)

    def getObject(self, oid):
        return self._object_dict.get(oid)

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

    def __contains__(self, tid):
        """
            Returns True if the TID is known by the manager
        """
        return tid in self._transaction_dict

    def register(self, uuid, tid):
        """
            Register a transaction, it may be already registered
        """
        transaction = self._transaction_dict.get(tid, None)
        if transaction is None:
            transaction = Transaction(uuid, tid)
            self._uuid_dict.setdefault(uuid, set()).add(transaction)
            self._transaction_dict[tid] = transaction
        return transaction

    def getObjectFromTransaction(self, tid, oid):
        """
            Return object data for given running transaction.
            Return None if not found.
        """
        result = self._transaction_dict.get(tid)
        if result is not None:
            result = result.getObject(oid)
        return result

    def reset(self):
        """
            Reset the transaction manager
        """
        self._transaction_dict.clear()
        self._store_lock_dict.clear()
        self._load_lock_dict.clear()
        self._uuid_dict.clear()

    def lock(self, tid, oid_list):
        """
            Lock a transaction
        """
        transaction = self._transaction_dict[tid]
        # remember that the transaction has been locked
        transaction.lock()
        for oid in transaction.getOIDList():
            self._load_lock_dict[oid] = tid
        # check every object that should be locked
        uuid = transaction.getUUID()
        is_assigned = self._app.pt.isAssigned
        for oid in oid_list:
            if is_assigned(oid, uuid) and self._load_lock_dict.get(oid) != tid:
                raise ValueError, 'Some locks are not held'
        object_list = transaction.getObjectList()
        # txn_info is None is the transaction information is not stored on 
        # this storage.
        txn_info = transaction.getTransactionInformations()
        # store data from memory to temporary table
        self._app.dm.storeTransaction(tid, object_list, txn_info)

    def unlock(self, tid):
        """
            Unlock transaction
        """
        self._app.dm.finishTransaction(tid)
        self.abort(tid, even_if_locked=True)

    def storeTransaction(self, tid, oid_list, user, desc, ext, packed):
        """
            Store transaction information received from client node
        """
        assert tid in self, "Transaction not registered"
        transaction = self._transaction_dict[tid]
        transaction.prepare(oid_list, user, desc, ext, packed)

    def getLockingTID(self, oid):
        return self._store_lock_dict.get(oid)

    def lockObject(self, tid, serial, oid):
        """
            Take a write lock on given object, checking that "serial" is
            current.
            Raises:
                DelayedError
                ConflictError
        """
        # check if the object if locked
        locking_tid = self._store_lock_dict.get(oid)
        if locking_tid == tid:
            neo.logging.info('Transaction %s storing %s more than once',
                dump(tid), dump(oid))
        elif locking_tid is None:
            # check if this is generated from the latest revision.
            history_list = self._app.dm.getObjectHistory(oid)
            if history_list and history_list[0][0] != serial:
                neo.logging.info('Resolvable conflict on %r:%r', dump(oid),
                        dump(tid))
                raise ConflictError(history_list[0][0])
            neo.logging.info('Transaction %s storing %s', dump(tid), dump(oid))
            self._store_lock_dict[oid] = tid
        elif locking_tid < tid:
            # a previous transaction lock this object, retry later
            neo.logging.info('Store delayed for %r:%r by %r', dump(oid),
                    dump(tid), dump(locking_tid))
            raise DelayedError
        else:
            # If a newer transaction already locks this object,
            # do not try to resolve a conflict, so return immediately.
            neo.logging.info('Unresolvable conflict on %r:%r with %r',
                dump(oid), dump(tid), dump(locking_tid))
            raise ConflictError(locking_tid)

    def checkCurrentSerial(self, tid, serial, oid):
        self.lockObject(tid, serial, oid)
        assert tid in self, "Transaction not registered"
        transaction = self._transaction_dict[tid]
        transaction.addCheckedObject(oid)

    def storeObject(self, tid, serial, oid, compression, checksum, data,
            value_serial):
        """
            Store an object received from client node
        """
        self.lockObject(tid, serial, oid)
        # store object
        assert tid in self, "Transaction not registered"
        transaction = self._transaction_dict[tid]
        transaction.addObject(oid, compression, checksum, data, value_serial)

    def abort(self, tid, even_if_locked=False):
        """
            Abort a transaction
            Releases locks held on all transaction objects, deletes Transaction
            instance, and executed queued events.
            Note: does not alter persistent content.
        """
        if tid not in self._transaction_dict:
            # the tid may be unknown as the transaction is aborted on every node
            # of the partition, even if no data was received (eg. conflict on
            # another node)
            return
        transaction = self._transaction_dict[tid]
        has_load_lock = transaction.isLocked()
        # if the transaction is locked, ensure we can drop it
        if not even_if_locked and has_load_lock:
            return
        # unlock any object
        for oid in transaction.getLockedOIDList():
            if has_load_lock:
                lock_tid = self._load_lock_dict.pop(oid, None)
                assert lock_tid in (tid, None), 'Transaction %s tried to ' \
                    'release the lock on oid %s, but it was held by %s' % (
                    dump(tid), dump(oid), dump(lock_tid))
            del self._store_lock_dict[oid]
        # remove the transaction
        uuid = transaction.getUUID()
        self._uuid_dict[uuid].discard(transaction)
        # clean node index if there is no more current transactions
        if not self._uuid_dict[uuid]:
            del self._uuid_dict[uuid]
        del self._transaction_dict[tid]
        # some locks were released, some pending locks may now succeed
        self._app.executeQueuedEvents()

    def abortFor(self, uuid):
        """
            Abort any non-locked transaction of a node
        """
        # abort any non-locked transaction of this node
        for tid in [x.getTID() for x in self._uuid_dict.get(uuid, [])]:
            self.abort(tid)
        # cleanup _uuid_dict if no transaction remains for this node
        transaction_set = self._uuid_dict.get(uuid)
        if transaction_set is not None and not transaction_set:
            del self._uuid_dict[uuid]

    def loadLocked(self, oid):
        return oid in self._load_lock_dict

    def log(self):
        neo.logging.info("Transactions:")
        for txn in self._transaction_dict.values():
            neo.logging.info('    %r', txn)
        neo.logging.info('  Read locks:')
        for oid, tid in self._load_lock_dict.items():
            neo.logging.info('    %r by %r', dump(oid), dump(tid))
        neo.logging.info('  Write locks:')
        for oid, tid in self._store_lock_dict.items():
            neo.logging.info('    %r by %r', dump(oid), dump(tid))

    def updateObjectDataForPack(self, oid, orig_serial, new_serial,
            getObjectData):
        lock_tid = self.getLockingTID(oid)
        if lock_tid is not None:
            transaction = self._transaction_dict[lock_tid]
            oid, compression, checksum, data, value_serial = \
                transaction.getObject(oid)
            if value_serial == orig_serial:
                if new_serial:
                    value_serial = new_serial
                else:
                    compression, checksum, data = getObjectData()
                    value_serial = None
                transaction.addObject(oid, compression, checksum, data,
                    value_serial)

