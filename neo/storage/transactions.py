#
# Copyright (C) 2010-2017  Nexedi SA
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
from neo.lib.handler import DelayEvent, EventQueue
from neo.lib.util import dump
from neo.lib.protocol import Packets, ProtocolError, NonReadableCell, \
    uuid_str, MAX_TID, ZERO_TID

class ConflictError(Exception):
    """
        Raised when a resolvable conflict occurs
        Argument: tid of locking transaction or latest revision
    """

    def __init__(self, tid):
        Exception.__init__(self)
        self.tid = tid


class NotRegisteredError(Exception):
    """
        Raised when a ttid is not registered
    """

class Transaction(object):
    """
        Container for a pending transaction
    """
    _delayed = {}
    tid = None
    voted = 0

    def __init__(self, uuid, ttid):
        self._birth = time()
        self.locking_tid = ttid
        self.uuid = uuid
        self.serial_dict = {}
        self.store_dict = {}
        # We must distinguish lockless stores from deadlocks.
        self.lockless = set()

    def __repr__(self):
        return "<%s(%s, locking_tid=%s, tid=%s, age=%.2fs) at 0x%x>" % (
            self.__class__.__name__,
            uuid_str(self.uuid),
            dump(self.locking_tid),
            dump(self.tid),
            time() - self._birth,
            id(self))

    def __lt__(self, other):
        return self.locking_tid < other.locking_tid

    def logDelay(self, ttid, locked, oid_serial):
        if self._delayed.get(oid_serial) != locked:
            if self._delayed:
                self._delayed[oid_serial] = locked
            else:
                self._delayed = {oid_serial: locked}
            logging.info('Lock delayed for %s:%s by %s',
                         dump(oid_serial[0]), dump(ttid), dump(locked))

    def store(self, oid, data_id, value_serial):
        """
            Add an object to the transaction
        """
        self.store_dict[oid] = oid, data_id, value_serial


class TransactionManager(EventQueue):
    """
        Manage pending transaction and locks
    """

    def __init__(self, app):
        EventQueue.__init__(self)
        self.read_queue = EventQueue()
        self._app = app
        self._transaction_dict = {}
        self._store_lock_dict = {}
        self._load_lock_dict = {}
        self._replicated = {}
        self._replicating = set()
        from neo.lib.util import u64
        np = app.pt.getPartitions()
        self.getPartition = lambda oid: u64(oid) % np

    def discarded(self, offset_list):
        self._replicating.difference_update(offset_list)
        for offset in offset_list:
            self._replicated.pop(offset, None)
        getPartition = self.getPartition
        for oid_dict in self._load_lock_dict, self._store_lock_dict:
            for oid in oid_dict.keys():
                if getPartition(oid) in offset_list:
                    del oid_dict[oid]
        data_id_list = []
        for transaction in self._transaction_dict.itervalues():
            serial_dict = transaction.serial_dict
            oid_list = [oid for oid in serial_dict
                if getPartition(oid) in offset_list]
            for oid in oid_list:
                del serial_dict[oid]
                try:
                    data_id_list.append(transaction.store_dict.pop(oid)[1])
                except KeyError:
                    pass
            transaction.lockless.difference_update(oid_list)
        self._app.dm.dropPartitionsTemporary(offset_list)
        self._app.dm.releaseData(data_id_list, True)
        # notifyPartitionChanges will commit
        self.executeQueuedEvents()
        self.read_queue.executeQueuedEvents()

    def readable(self, offset_list):
        for offset in offset_list:
            tid = self._replicated.pop(offset, None)
            assert tid is None, (offset, tid)

    def replicating(self, offset_list):
        self._replicating.update(offset_list)
        isdisjoint = set(offset_list).isdisjoint
        assert isdisjoint(self._replicated), (offset_list, self._replicated)
        assert isdisjoint(map(self.getPartition, self._store_lock_dict)), (
            offset_list, self._store_lock_dict)
        p = Packets.AskUnfinishedTransactions(offset_list)
        self._app.master_conn.ask(p, offset_list=offset_list)

    def replicated(self, partition, tid):
        # also called for readable cells in BACKINGUP state
        self._replicating.discard(partition)
        self._replicated[partition] = tid
        self._notifyReplicated()

    def _notifyReplicated(self):
        getPartition = self.getPartition
        store_lock_dict = self._store_lock_dict
        replicated = self._replicated
        notify = {x[0] for x in replicated.iteritems() if x[1]}
        # We sort transactions so that in case of muliple stores/checks for the
        # same oid, the lock is taken by the highest locking ttid, which will
        # delay new transactions.
        for txn, ttid in sorted((txn, ttid) for ttid, txn in
                                self._transaction_dict.iteritems()):
            if txn.locking_tid == MAX_TID:
                break # all remaining transactions are resolving a deadlock
            assert txn.lockless.issubset(txn.serial_dict), (
                txn.lockless, txn.serial_dict)
            for oid in txn.lockless:
                partition = getPartition(oid)
                if replicated.get(partition):
                    if store_lock_dict.get(oid, ttid) != ttid:
                        # We have a "multi-lock" store, i.e. an
                        # initially-lockless store to a partition that became
                        # replicated.
                        notify.discard(partition)
                    store_lock_dict[oid] = ttid
        if notify:
            # For these partitions, all oids of all pending transactions
            # are now locked normally and we don't rely anymore on other
            # readable cells to check locks: we're really up-to-date.
            for partition in notify:
                self._app.master_conn.send(Packets.NotifyReplicationDone(
                    partition, replicated[partition]))
                replicated[partition] = None
            for oid, ttid in store_lock_dict.iteritems():
                if getPartition(oid) in notify:
                    # Use 'discard' instead of 'remove', for oids that were
                    # locked after that the partition was replicated.
                    self._transaction_dict[ttid].lockless.discard(oid)

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
            return self._transaction_dict[ttid].store_dict[oid]
        except KeyError:
            return None

    def _rebase(self, transaction, ttid, locking_tid=MAX_TID):
        # With the default value of locking_tid, this marks the transaction as
        # being rebased, in case that the current lock is released (the other
        # transaction is aborted or committed) before the client sends us a new
        # locking tid: in lockObject, 'locked' will be None but we'll still
        # have to delay the store.
        transaction.locking_tid = locking_tid
        if ttid:
            # Remove store locks we have.
            # In order to keep all locking data consistent, this must be done
            # when the locking tid changes, i.e. from both 'lockObject' (for
            # the node that triggered the deadlock) and 'rebase' (for other
            # nodes).
            for oid, locked in self._store_lock_dict.items():
                # If this oid is locked by several transactions (all lockless),
                # the following condition is true if we have the highest ttid,
                # but in either case, _notifyReplicated will be called below,
                # fixing the store lock.
                if locked == ttid:
                    del self._store_lock_dict[oid]
            lockless = transaction.lockless
            # There's nothing to rebase for lockless stores to replicating
            # partitions because there's no lock taken yet. In other words,
            # rebasing such stores would do nothing. Other lockless stores
            # become normal ones: this transaction does not block anymore
            # replicated partitions from being marked as UP_TO_DATE.
            oid = [oid
                for oid in lockless.intersection(transaction.serial_dict)
                if self.getPartition(oid) not in self._replicating]
            if oid:
                lockless.difference_update(oid)
                self._notifyReplicated()
        # Some locks were released, some pending locks may now succeed.
        # We may even have delayed stores for this transaction, like the one
        # that triggered the deadlock. They must also be sorted again because
        # our locking tid has changed.
        self.sortAndExecuteQueuedEvents()

    def rebase(self, conn, ttid, locking_tid):
        self.register(conn, ttid)
        transaction = self._transaction_dict[ttid]
        if transaction.voted:
            raise ProtocolError("TXN %s already voted" % dump(ttid))
        # First, get a set copy of serial_dict before _rebase locks oids.
        lock_set = set(transaction.serial_dict)
        self._rebase(transaction, transaction.locking_tid != MAX_TID and ttid,
                     locking_tid)
        if transaction.locking_tid == MAX_TID:
            # New deadlock. There's no point rebasing objects now.
            return ()
        # We return all oids that can't be relocked trivially
        # (the client will use RebaseObject for these oids).
        lock_set -= transaction.lockless # see comment in _rebase
        recheck_set = lock_set.intersection(self._store_lock_dict)
        lock_set -= recheck_set
        for oid in lock_set:
            try:
                serial = transaction.serial_dict[oid]
            except KeyError:
                # An oid was already being rebased and delayed,
                # and it got a conflict during the above call to _rebase.
                continue
            try:
                self.lockObject(ttid, serial, oid)
            except ConflictError:
                recheck_set.add(oid)
        return recheck_set

    def vote(self, ttid, txn_info=None):
        """
            Store transaction information received from client node
        """
        logging.debug('Vote TXN %s', dump(ttid))
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))
        object_list = transaction.store_dict.itervalues()
        if txn_info:
            user, desc, ext, oid_list = txn_info
            txn_info = oid_list, user, desc, ext, False, ttid
            transaction.voted = 2
        else:
            transaction.voted = 1
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
        assert transaction.tid is None, dump(transaction.tid)
        assert ttid <= tid, (ttid, tid)
        transaction.tid = tid
        self._load_lock_dict.update(
            dict.fromkeys(transaction.store_dict, ttid))
        if transaction.voted == 2:
            self._app.dm.lockTransaction(tid, ttid)

    def unlock(self, ttid):
        """
            Unlock transaction
        """
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))
        tid = transaction.tid
        logging.debug('Unlock TXN %s (ttid=%s)', dump(tid), dump(ttid))
        dm = self._app.dm
        dm.unlockTransaction(tid, ttid,
            transaction.voted == 2,
            transaction.store_dict)
        self._app.em.setTimeout(time() + 1, dm.deferCommit())
        self.abort(ttid, even_if_locked=True)

    def getFinalTID(self, ttid):
        try:
            return self._transaction_dict[ttid].tid
        except KeyError:
            return self._app.dm.getFinalTID(ttid)

    def getLockingTID(self, oid):
        return self._store_lock_dict.get(oid)

    def lockObject(self, ttid, serial, oid):
        """
            Take a write lock on given object, checking that "serial" is
            current.
            Raises:
                DelayEvent
                ConflictError
        """
        transaction = self._transaction_dict[ttid]
        if self.getPartition(oid) in self._replicating:
            # We're out-of-date so maybe:
            # - we don't have all data to check for conflicts
            # - we missed stores/check that would lock this one
            # However, this transaction may have begun after we started to
            # replicate, and we're expected to store it in full.
            # Since there's at least 1 other (readable) cell that will do this
            # check, we accept this store/check without taking a lock.
            if transaction.locking_tid == MAX_TID:
                # Deadlock avoidance. Still no new locking_tid from the client.
                raise DelayEvent(transaction)
            transaction.lockless.add(oid)
            return
        locked = self._store_lock_dict.get(oid)
        if locked:
            other = self._transaction_dict[locked]
            if other < transaction or other.voted:
                # We have a bigger "TTID" than locking transaction, so we are
                # younger: enter waiting queue so we are handled when lock gets
                # released. We also want to delay (instead of conflict) if the
                # client is so faster that it is committing another transaction
                # before we processed UnlockInformation from the master.
                # Or the locking transaction has already voted and there's no
                # risk of deadlock if we delay.
                transaction.logDelay(ttid, locked, (oid, serial))
                # A client may have several stores delayed for the same oid
                # but this is not a problem. EventQueue processes them in order
                # and only the last one will not result in conflicts (that are
                # already resolved).
                raise DelayEvent(transaction)
            if oid in transaction.lockless:
                # This is a consequence of not having taken a lock during
                # replication. After a ConflictError, we may be asked to "lock"
                # it again. The current lock is a special one that only delays
                # new transactions.
                # For the cluster, we're still out-of-date and like above,
                # at least 1 other (readable) cell checks for conflicts.
                return
            if other is not transaction:
                # We have a smaller "TTID" than locking transaction, so we are
                # older: this is a possible deadlock case, as we might already
                # hold locks the younger transaction is waiting upon.
                logging.info('Deadlock on %s:%s with %s',
                             dump(oid), dump(ttid), dump(locked))
                # Ask master to give the client a new locking tid, which will
                # be used to ask all involved storage nodes to rebase the
                # already locked oids for this transaction.
                self._app.master_conn.send(Packets.NotifyDeadlock(
                    ttid, transaction.locking_tid))
                self._rebase(transaction, ttid)
                raise DelayEvent(transaction)
            # If previous store was an undo, next store must be based on
            # undo target.
            try:
                previous_serial = transaction.store_dict[oid][2]
            except KeyError:
                # Similarly to below for store, cascaded deadlock for
                # checkCurrentSerial is possible because rebase() may return
                # oids for which previous rebaseObject are delayed, or being
                # received, and the client will bindly resend them.
                assert oid in transaction.serial_dict, transaction
                logging.info('Transaction %s checking %s more than once',
                             dump(ttid), dump(oid))
                return True
            if previous_serial is None:
                # 2 valid cases:
                # - the previous undo resulted in a resolved conflict
                # - cascaded deadlock resolution
                # Otherwise, this should not happen. For example, when being
                # disconnected by the master because we missed a transaction,
                # a conflict may happen after a first store to us, but the
                # resolution waits for invalidations from the master (to then
                # load the saved data), which are sent after the notification
                # we are down, and the client would stop writing to us.
                logging.info('Transaction %s storing %s more than once',
                             dump(ttid), dump(oid))
                return True
        elif transaction.locking_tid == MAX_TID:
            # Deadlock avoidance. Still no new locking_tid from the client.
            raise DelayEvent(transaction)
        else:
            try:
                previous_serial = self._app.dm.getLastObjectTID(oid)
            except NonReadableCell:
                partition = self.getPartition(oid)
                if partition not in self._replicated:
                    # Either the partition is discarded or we haven't yet
                    # received the notification from the master that the
                    # partition is assigned to us. In the latter case, we're
                    # not expected to have the partition in full.
                    # We'll return a successful answer to the client, which
                    # is fine because there's at least one other cell that is
                    # readable for this oid.
                    raise
                with self._app.dm.replicated(partition):
                    previous_serial = self._app.dm.getLastObjectTID(oid)
        # Locking before reporting a conflict would speed up the case of
        # cascading conflict resolution by avoiding incremental resolution,
        # assuming that the time to resolve a conflict is often constant:
        # "C+A vs. B -> C+A+B" rarely costs more than "C+A vs. C+B -> C+A+B".
        # However, this would be against the optimistic principle of ZODB.
        if previous_serial is not None and previous_serial != serial:
            assert serial < previous_serial, (serial, previous_serial)
            logging.info('Conflict on %s:%s with %s',
                dump(oid), dump(ttid), dump(previous_serial))
            raise ConflictError(previous_serial)
        logging.debug('Transaction %s locking %s', dump(ttid), dump(oid))
        self._store_lock_dict[oid] = ttid
        return True

    def checkCurrentSerial(self, ttid, oid, serial):
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise NotRegisteredError
        locked = self.lockObject(ttid, serial, oid)
        transaction.serial_dict[oid] = serial
        if not locked:
            return ZERO_TID

    def storeObject(self, ttid, serial, oid, compression, checksum, data,
            value_serial):
        """
            Store an object received from client node
        """
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            raise NotRegisteredError
        locked = self.lockObject(ttid, serial, oid)
        transaction.serial_dict[oid] = serial
        # store object
        if data is None:
            data_id = None
        else:
            data_id = self._app.dm.holdData(checksum, oid, data, compression)
        transaction.store(oid, data_id, value_serial)
        if not locked:
            return ZERO_TID

    def rebaseObject(self, ttid, oid):
        try:
            transaction = self._transaction_dict[ttid]
        except KeyError:
            logging.info('Forget rebase of %s by %s delayed by %s',
                dump(oid), dump(ttid), dump(self.getLockingTID(oid)))
            return
        try:
            serial = transaction.serial_dict[oid]
        except KeyError:
            # There was a previous rebase for this oid, it was still delayed
            # during the second RebaseTransaction, and then a conflict was
            # reported when another transaction was committed.
            # This can also happen when a partition is dropped.
            logging.info("no oid %s to rebase for transaction %s",
                dump(oid), dump(ttid))
            return
        assert oid not in transaction.lockless, (oid, transaction.lockless)
        try:
            self.lockObject(ttid, serial, oid)
        except ConflictError, e:
            # Move the data back to the client for conflict resolution,
            # since the client may not have it anymore.
            try:
                data_id = transaction.store_dict.pop(oid)[1]
            except KeyError: # check current
                data = None
            else:
                if data_id is None:
                    data = None
                else:
                    dm = self._app.dm
                    data = dm.loadData(data_id)
                    dm.releaseData([data_id], True)
            del transaction.serial_dict[oid]
            return serial, e.tid, data

    def abort(self, ttid, even_if_locked=False):
        """
            Abort a transaction
            Releases locks held on all transaction objects, deletes Transaction
            instance, and executed queued events.
            Note: does not alter persistent content.
        """
        if ttid not in self._transaction_dict:
            assert not even_if_locked
            # See how the master processes AbortTransaction from the client.
            return
        transaction = self._transaction_dict[ttid]
        locked = transaction.tid
        # if the transaction is locked, ensure we can drop it
        if locked:
            if not even_if_locked:
                return
        else:
            logging.debug('Abort TXN %s', dump(ttid))
            dm = self._app.dm
            dm.abortTransaction(ttid)
            dm.releaseData([x[1] for x in transaction.store_dict.itervalues()],
                           True)
            dm.commit()
        # unlock any object
        for oid in transaction.serial_dict:
            if locked:
                lock_ttid = self._load_lock_dict.pop(oid, None)
                assert lock_ttid in (ttid, None), ('Transaction %s tried'
                    ' to release the lock on oid %s, but it was held by %s'
                    % (dump(ttid), dump(oid), dump(lock_ttid)))
            try:
                write_locking_tid = self._store_lock_dict[oid]
            except KeyError:
                # Lockless store (we are replicating this partition),
                # or unresolved deadlock.
                continue
            if ttid != write_locking_tid:
                if __debug__:
                    other = self._transaction_dict[write_locking_tid]
                    x = (oid, ttid, write_locking_tid,
                         self._replicated, transaction.lockless)
                lockless = oid in transaction.lockless
                # If there were multiple lockless writes, lockless can be
                # False (after a rebase) whereas the partition is still not
                # notified as replicated.
                assert oid in other.serial_dict and not (lockless and
                    not self._replicated.get(self.getPartition(oid))), x
                if not lockless:
                    assert not locked, x
                    continue # unresolved deadlock
                # Several lockless stores for this oid and among them,
                # a higher ttid is still pending.
                assert transaction < other, x
            del self._store_lock_dict[oid]
        # remove the transaction
        del self._transaction_dict[ttid]
        if self._replicated:
            self._notifyReplicated()
        # some locks were released, some pending locks may now succeed
        self.read_queue.executeQueuedEvents()
        self.executeQueuedEvents()

    def abortFor(self, uuid, even_if_voted=False):
        """
            Abort any non-locked transaction of a node
        """
        # abort any non-locked transaction of this node
        for ttid, transaction in self._transaction_dict.items():
            if transaction.uuid == uuid and (
               even_if_voted or not transaction.voted):
                self.abort(ttid)

    def isLockedTid(self, tid):
        return any(None is not t.tid <= tid
            for t in self._transaction_dict.itervalues())

    def loadLocked(self, oid):
        return oid in self._load_lock_dict

    def log(self):
        logging.info("Transactions:")
        for ttid, txn in self._transaction_dict.iteritems():
            logging.info('    %s %r', dump(ttid), txn)
        logging.info('  Read locks:')
        for oid, ttid in self._load_lock_dict.iteritems():
            logging.info('    %s by %s', dump(oid), dump(ttid))
        logging.info('  Write locks:')
        for oid, ttid in self._store_lock_dict.iteritems():
            logging.info('    %s by %s', dump(oid), dump(ttid))
        self.logQueuedEvents()
        self.read_queue.logQueuedEvents()

    def updateObjectDataForPack(self, oid, orig_serial, new_serial, data_id):
        lock_tid = self.getLockingTID(oid)
        if lock_tid is not None:
            transaction = self._transaction_dict[lock_tid]
            if transaction.store_dict[oid][2] == orig_serial:
                if new_serial:
                    data_id = None
                else:
                    self._app.dm.holdData(data_id)
                transaction.store(oid, data_id, new_serial)
