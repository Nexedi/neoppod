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

from collections import deque
from time import time
from struct import pack, unpack
from neo.lib import logging
from neo.lib.exception import ProtocolError
from neo.lib.handler import DelayEvent, EventQueue
from neo.lib.protocol import uuid_str, ZERO_OID, ZERO_TID
from neo.lib.util import dump, u64, addTID, tidFromTime

class Transaction(object):
    """
        A pending transaction
    """
    tid = None
    oid_list = ()
    failed = \
    involved = frozenset()

    def __init__(self, node, storage_readiness, ttid):
        """
            Prepare the transaction, set OIDs and UUIDs related to it
        """
        self.node = node
        self.storage_readiness = storage_readiness
        self.ttid = ttid
        self._birth = time()
        # store storage uuids that must be notified at commit
        self._notification_set = set()

    def __repr__(self):
        return "<%s(client=%r, tid=%r, invalidated=%r, storages=%r, age=%.2fs) at %x>" % (
                self.__class__.__name__,
                self.node,
                dump(self.tid),
                map(dump, self.oid_list),
                map(uuid_str, self.involved),
                time() - self._birth,
                id(self),
        )

    def registerForNotification(self, uuid):
        """
            Register a node that requires a notification at commit
        """
        self._notification_set.add(uuid)

    def getNotificationUUIDList(self):
        """
            Returns the list of nodes waiting for the transaction to be
            finished
        """
        return list(self._notification_set)

    def prepare(self, tid, oid_list, partition_list, involved, msg_id):
        self.tid = tid
        self.oid_list = oid_list
        self.partition_list = partition_list
        self.msg_id = msg_id
        self.involved = involved
        self.locking = involved.copy()

    def storageLost(self, uuid):
        """
            Given storage was lost while waiting for its lock, stop waiting
            for it.
            Does nothing if the node was not part of the transaction.
        """
        self._notification_set.discard(uuid)
        # XXX: We might lose information that a storage successfully locked
        # data but was later found to be disconnected. This loss has no impact
        # on current code, but it might be disturbing to reader or future code.
        if self.tid:
            locking = self.locking
            locking.discard(uuid)
            self.involved.discard(uuid)
            return not locking
        return False

    def clientLost(self, node):
        if self.node is node:
            if self.tid:
                self.node = None # orphan
            else:
                return True # abort
        else:
            self._notification_set.discard(node.getUUID())
        return False

    def lock(self, uuid):
        """
            Define that a node has locked the transaction
            Returns true if all nodes are locked
        """
        locking = self.locking
        locking.remove(uuid)
        return not locking


class TransactionManager(EventQueue):
    """
        Manage current transactions
    """

    def __init__(self, on_commit):
        self._on_commit = on_commit
        self.reset()

    def reset(self):
        EventQueue.__init__(self)
        # ttid -> transaction
        self._ttid_dict = {}
        self._last_oid = ZERO_OID
        self._last_tid = ZERO_TID
        # queue filled with ttids pointing to transactions with increasing tids
        self._queue = deque()

    def __getitem__(self, ttid):
        """
            Return the transaction object for this TID
        """
        try:
            return self._ttid_dict[ttid]
        except KeyError:
            raise ProtocolError("unknown ttid %s" % dump(ttid))

    def __delitem__(self, ttid):
        try:
            self._queue.remove(ttid)
        except ValueError:
            pass
        del self._ttid_dict[ttid]
        self.executeQueuedEvents()

    def __contains__(self, ttid):
        """
            Returns True if this is a pending transaction
        """
        return ttid in self._ttid_dict

    def getNextOIDList(self, num_oids):
        """ Generate a new OID list """
        oid = unpack('!Q', self._last_oid)[0] + 1
        oid_list = [pack('!Q', oid + i) for i in xrange(num_oids)]
        self._last_oid = oid_list[-1]
        return oid_list

    def setLastOID(self, oid):
        if self._last_oid < oid:
            self._last_oid = oid

    def getLastOID(self):
        return self._last_oid

    def _nextTID(self, ttid=None, divisor=None):
        """
        Compute the next TID based on the current time and check collisions.
        Also, if ttid is not None, divisor is mandatory adjust it so that
            tid % divisor == ttid % divisor
        while preserving
            min_tid < tid
        If ttid is None, divisor is ignored.
        When constraints allow, prefer decreasing generated TID, to avoid
        fast-forwarding to future dates.
        """
        tid = tidFromTime(time())
        min_tid = self._last_tid
        if tid <= min_tid:
            tid = addTID(min_tid, 1)
        if ttid is not None:
            remainder = u64(ttid) % divisor
            delta_remainder = remainder - u64(tid) % divisor
            if delta_remainder:
                tid = addTID(tid, delta_remainder)
                if tid <= min_tid:
                    tid = addTID(tid, divisor)
                assert u64(tid) % divisor == remainder, (dump(tid), remainder)
                assert min_tid < tid, (dump(min_tid), dump(tid))
        self._last_tid = tid
        return self._last_tid

    def getLastTID(self):
        """
            Returns the last TID used
        """
        return self._last_tid

    def setLastTID(self, tid):
        """
            Set the last TID, keep the previous if lower
        """
        if self._last_tid < tid:
            self._last_tid = tid

    def hasPending(self):
        """
            Returns True if some transactions are pending
        """
        return bool(self._ttid_dict)

    def registerForNotification(self, uuid):
        """
            Return the list of pending transaction IDs
        """
        # remember that this node must be notified when pending transactions
        # will be finished
        for txn in self._ttid_dict.itervalues():
            txn.registerForNotification(uuid)
        return self._ttid_dict.keys()

    def begin(self, node, storage_readiness, tid=None):
        """
            Generate a new TID
        """
        if tid is None:
            # No TID requested, generate a temporary one
            tid = self._nextTID()
        elif tid <= self._last_tid:
            raise ProtocolError(
                "new TID must be greater than the last committed one")
        else:
            # Use of specific TID requested, queue it immediately and update
            # last TID.
            self._queue.append(tid)
            self._last_tid = tid
        txn = self._ttid_dict[tid] = Transaction(node, storage_readiness, tid)
        logging.debug('Begin %s', txn)
        return tid

    def vote(self, app, ttid, uuid_list):
        """
            Check that the transaction can be voted
            when the client reports failed nodes.
        """
        txn = self[ttid]
        # The client does not know which nodes are not expected to have
        # transactions in full. Let's filter out them.
        failed = app.getStorageReadySet(txn.storage_readiness)
        failed.intersection_update(uuid_list)
        if failed:
            operational = app.pt.operational
            if not operational(failed):
                # No way to commit this transaction because there are
                # non-replicated storage nodes with failed stores.
                return False
            all_failed = failed.copy()
            for t in self._ttid_dict.itervalues():
                all_failed |= t.failed
            if not operational(all_failed):
                # Other transactions were voted and unless they're aborted,
                # we won't be able to finish this one, because that would make
                # the cluster non-operational. Let's retry later.
                raise DelayEvent
            # Allow the client to finish the transaction,
            # even if this will disconnect storage nodes.
            txn.failed = failed
        return True

    def prepare(self, app, ttid, oid_list, deleted, checked, msg_id):
        """
            Prepare a transaction to be finished
        """
        txn = self[ttid]
        pt = app.pt

        failed = txn.failed
        if failed and not pt.operational(failed):
            return None, None
        ready = app.getStorageReadySet(txn.storage_readiness)
        getPartition = pt.getPartition
        partition_set = set(map(getPartition, oid_list))
        partition_set.update(deleted)
        partition_list = list(partition_set)
        partition_set.add(getPartition(ttid))
        partition_set.update(checked)
        node_list = []
        involved = set()
        for partition in partition_set:
            for cell in pt.getCellList(partition):
                node = cell.getNode()
                if node.isIdentified():
                    uuid = node.getUUID()
                    if uuid in involved:
                        continue
                    if uuid in failed:
                        # This will commit a new PT with outdated cells before
                        # locking the transaction, which is important during
                        # the verification phase.
                        node.getConnection().close()
                    elif uuid in ready:
                        involved.add(uuid)
                        node_list.append(node)
        # A node that was not ready at the beginning of the transaction
        # can't have readable cells. And if we're still operational without
        # the 'failed' nodes, then there must still be 1 node in 'ready'
        # that is UP.
        assert node_list, (ready, failed)

        # maybe not the fastest but _queue should be often small
        if ttid in self._queue:
            tid = ttid
        else:
            tid = self._nextTID(ttid, pt.getPartitions())
            self._queue.append(ttid)
        logging.debug('Finish TXN %s for %s (was %s)',
                      dump(tid), txn.node, dump(ttid))
        txn.prepare(tid, oid_list, partition_list, involved, msg_id)
        # check if greater and foreign OID was stored
        if oid_list:
            self.setLastOID(max(oid_list))
        return tid, node_list

    def abort(self, ttid, uuid):
        """
            Abort a transaction
        """
        logging.debug('Abort TXN %s for %s', dump(ttid), uuid_str(uuid))
        txn = self[ttid]
        if txn.tid:
            raise ProtocolError("commit already requested for ttid %s"
                                % dump(ttid))
        del self[ttid]
        return txn._notification_set

    def lock(self, ttid, uuid):
        """
            Set that a node has locked the transaction.
            If transaction is completely locked, calls function given at
            instantiation time.
        """
        logging.debug('Lock TXN %s for %s', dump(ttid), uuid_str(uuid))
        if self[ttid].lock(uuid) and self._queue[0] == ttid:
            # all storage are locked and we unlock the commit queue
            self._unlockPending()

    def storageLost(self, uuid):
        """
            A storage node has been lost, don't expect a reply from it for
            current transactions
        """
        unlock = False
        for ttid, txn in self._ttid_dict.iteritems():
            if txn.storageLost(uuid) and self._queue[0] == ttid:
                unlock = True
                # do not break: we must call storageLost() on all transactions
        if unlock:
            self._unlockPending()

    def _unlockPending(self):
        """Serialize transaction unlocks

        This should rarely delay unlocks since the time needed to lock a
        transaction is roughly constant. The most common case where reordering
        is required is when some storages are already busy by other tasks.
        """
        queue = self._queue
        self._on_commit(self._ttid_dict.pop(queue.popleft()))
        while queue:
            ttid = queue[0]
            txn = self._ttid_dict[ttid]
            if txn.locking:
                break
            del queue[0], self._ttid_dict[ttid]
            self._on_commit(txn)
        self.executeQueuedEvents()

    def clientLost(self, node):
        for txn in self._ttid_dict.values():
            if txn.clientLost(node):
                tid = txn.ttid
                del self[tid]
                yield tid, txn.getNotificationUUIDList()

    def log(self):
        logging.info('Transactions:')
        for txn in self._ttid_dict.itervalues():
            logging.info('  %r', txn)
        self.logQueuedEvents()
