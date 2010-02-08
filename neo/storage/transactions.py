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

from neo import logging
from neo.util import dump


class ConflictError(Exception):
    """
        Raised when a resolvable conflict occurs
        Argument: tid of locking transaction or latest revision
    """

    def __init__(self, tid):
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

    def getTID(self):
        return self._tid

    def getUUID(self):
        return self._uuid

    def lock(self):
        assert not self._locked
        self._locked = True

    def isLocked(self):
        return self._locked

    def prepare(self, oid_list, user, desc, ext):
        """
            Set the transaction informations
        """
        # assert self._transaction is not None
        self._transaction = (oid_list, user, desc, ext)

    def addObject(self, oid, compression, checksum, data):
        """
            Add an object to the transaction
        """
        self._object_dict[oid] = (oid, compression, checksum, data)

    def getObjectList(self):
        return self._object_dict.values()

    def getOIDList(self):
        return self._object_dict.keys()

    def getTransactionInformations(self):
        assert self._transaction is not None
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
        # TODO: replace app.loid with this one:
        self._loid = None

    def __contains__(self, tid):
        """
            Returns True if the TID is known by the manager
        """
        return tid in self._transaction_dict

    def _getTransaction(self, tid, uuid):
        """
            Get or create the transaction object for this tid
        """
        transaction = self._transaction_dict.get(tid, None)
        if transaction is None:
            transaction = Transaction(uuid, tid)
            self._uuid_dict.setdefault(uuid, set()).add(transaction)
            self._transaction_dict[tid] = transaction
        return transaction

    def reset(self):
        """
            Reset the transaction manager
        """
        self._transaction_dict.clear()
        self._store_lock_dict.clear()
        self._load_lock_dict.clear()
        self._uuid_dict.clear()

    def lock(self, tid):
        """
            Lock a transaction
        """
        transaction = self._transaction_dict[tid]
        # remember that the transaction has been locked
        transaction.lock()
        for oid in transaction.getOIDList():
            self._load_lock_dict[oid] = tid
        object_list = transaction.getObjectList()
        txn_info = transaction.getTransactionInformations()
        # store data from memory to temporary table
        self._app.dm.storeTransaction(tid, object_list, txn_info)

    def unlock(self, tid):
        """
            Unlock transaction
        """
        self._app.dm.finishTransaction(tid)
        self.abort(tid, even_if_locked=True)

        # update loid if needed
        if self._loid != self._app.loid:
            args = dump(self._loid), dump(self._app.loid)
            logging.warning('Greater OID used in StoreObject : %s > %s', *args)
            self._app.loid = self._loid
            self._app.dm.setLastOID(self._app.loid)

    def storeTransaction(self, uuid, tid, oid_list, user, desc, ext):
        """
            Store transaction information received from client node
        """
        transaction = self._getTransaction(tid, uuid)
        transaction.prepare(oid_list, user, desc, ext)

    def storeObject(self, uuid, tid, serial, oid, compression, checksum, data):
        """
            Store an object received from client node
        """
        # check if the object if locked
        locking_tid = self._store_lock_dict.get(oid, None)
        if locking_tid is not None:
            if locking_tid < tid:
                # a previous transaction lock this object, retry later
                raise DelayedError
            # If a newer transaction already locks this object,
            # do not try to resolve a conflict, so return immediately.
            logging.info('unresolvable conflict in %s', dump(oid))
            raise ConflictError(locking_tid)

        # check if this is generated from the latest revision.
        history_list = self._app.dm.getObjectHistory(oid)
        if history_list and history_list[0][0] != serial:
            logging.info('resolvable conflict in %s', dump(oid))
            raise ConflictError(history_list[0][0])

        # store object
        transaction = self._getTransaction(tid, uuid)
        transaction.addObject(oid, compression, checksum, data)
        self._store_lock_dict[oid] = tid

        # update loid
        self._loid = max(oid, self._app.loid)

    def abort(self, tid, even_if_locked=True):
        """
            Abort a transaction
        """
        if tid not in self._transaction_dict:
            # XXX: this happen sometimes, explain or fix
            return
        transaction = self._transaction_dict[tid]
        has_load_lock = transaction.isLocked()
        # if the transaction is locked, ensure we can drop it
        if not even_if_locked and has_load_lock:
            return
        # unlock any object
        for oid in transaction.getOIDList():
            if has_read_lock:
                # XXX: we release locks without checking if tid owns them
                del self._load_lock_dict[oid]
            del self._store_lock_dict[oid]
        # _uuid_dict entry will be deleted at node disconnection
        self._uuid_dict[transaction.getUUID()].discard(transaction)
        del self._transaction_dict[tid]
        self._app.executeQueuedEvents()

    def abortFor(self, uuid):
        """
            Abort any non-locked transaction of a node
        """
        # abort any non-locked transaction of this node
        for tid in [x.getTID() for x in self._uuid_dict.get(uuid, [])]:
            self.abort(tid, even_if_locked=False)
        # cleanup _uuid_dict if no transaction remains for this node
        if not self._uuid_dict.get(uuid):
            del self._uuid_dict[uuid]

    def loadLocked(self, oid):
        return oid in self._load_lock_dict

