#
# Copyright (C) 2006-2010  Nexedi SA
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

from time import time, gmtime
from struct import pack, unpack
from datetime import timedelta, datetime
from neo.util import dump
import neo

TID_LOW_OVERFLOW = 2**32
TID_LOW_MAX = TID_LOW_OVERFLOW - 1
SECOND_PER_TID_LOW = 60.0 / TID_LOW_OVERFLOW
TID_CHUNK_RULES = (
    (-1900, 0),
    (-1, 12),
    (-1, 31),
    (0, 24),
    (0, 60),
)

def packTID(utid):
    """
    Pack given 2-tuple containing:
    - a 5-tuple containing year, month, day, hour and minute
    - seconds scaled to 60:2**32
    into a 64 bits TID.
    """
    higher, lower = utid
    assert len(higher) == len(TID_CHUNK_RULES), higher
    packed_higher = 0
    for value, (offset, multiplicator) in zip(higher, TID_CHUNK_RULES):
        assert isinstance(value, (int, long)), value
        value += offset
        assert 0 <= value, (value, offset, multiplicator)
        assert multiplicator == 0 or value < multiplicator, (value,
            offset, multiplicator)
        packed_higher *= multiplicator
        packed_higher += value
    assert isinstance(lower, (int, long)), lower
    assert 0 <= lower < TID_LOW_OVERFLOW, hex(lower)
    return pack('!LL', packed_higher, lower)

def unpackTID(ptid):
    """
    Unpack given 64 bits TID in to a 2-tuple containing:
    - a 5-tuple containing year, month, day, hour and minute
    - seconds scaled to 60:2**32
    """
    packed_higher, lower = unpack('!LL', ptid)
    higher = []
    append = higher.append
    for offset, multiplicator in reversed(TID_CHUNK_RULES):
        if multiplicator:
            packed_higher, value = divmod(packed_higher, multiplicator)
        else:
            packed_higher, value = 0, packed_higher
        append(value - offset)
    higher.reverse()
    return (tuple(higher), lower)

def addTID(ptid, offset):
    """
    Offset given packed TID.
    """
    higher, lower = unpackTID(ptid)
    high_offset, lower = divmod(lower + offset, TID_LOW_OVERFLOW)
    if high_offset:
        d = datetime(*higher) + timedelta(0, 60 * high_offset)
        higher = (d.year, d.month, d.day, d.hour, d.minute)
    return packTID((higher, lower))

class Transaction(object):
    """
        A pending transaction
    """

    def __init__(self, node, tid, oid_list, uuid_list, msg_id):
        """
            Prepare the transaction, set OIDs and UUIDs related to it
        """
        self._node = node
        self._tid = tid
        self._oid_list = oid_list
        self._msg_id = msg_id
        # uuid dict hold flag to known who has locked the transaction
        self._uuid_set = set(uuid_list)
        self._lock_wait_uuid_set = set(uuid_list)
        self._birth = time()

    def __repr__(self):
        return "<%s(client=%r, tid=%r, oids=%r, storages=%r, age=%.2fs) at %x>" % (
                self.__class__.__name__,
                self._node,
                dump(self._tid),
                [dump(x) for x in self._oid_list],
                [dump(x) for x in self._uuid_set],
                time() - self._birth,
                id(self),
        )

    def getNode(self):
        """
            Return the node that had began the transaction
        """
        return self._node

    def getTID(self):
        """
            Return the transaction ID
        """
        return self._tid

    def getMessageId(self):
        """
            Returns the packet ID to use in the answer
        """
        return self._msg_id

    def getUUIDList(self):
        """
            Returns the list of node's UUID that lock the transaction
        """
        return list(self._uuid_set)

    def getOIDList(self):
        """
            Returns the list of OIDs used in the transaction
        """

        return list(self._oid_list)

    def forget(self, uuid):
        """
            Given storage was lost while waiting for its lock, stop waiting
            for it.
            Does nothing if the node was not part of the transaction.
        """
        # XXX: We might loose information that a storage successfully locked
        # data but was later found to be disconnected. This loss has no impact
        # on current code, but it might be disturbing to reader or future code.
        self._lock_wait_uuid_set.discard(uuid)
        self._uuid_set.discard(uuid)
        return self.locked()

    def lock(self, uuid):
        """
            Define that a node has locked the transaction
            Returns true if all nodes are locked
        """
        self._lock_wait_uuid_set.remove(uuid)
        return self.locked()

    def locked(self):
        """
            Returns true if all nodes are locked
        """
        return not self._lock_wait_uuid_set


class TransactionManager(object):
    """
        Manage current transactions
    """

    def __init__(self):
        # tid -> transaction
        self._tid_dict = {}
        # node -> transactions mapping
        self._node_dict = {}
        self._last_tid = None
        self._last_oid = None

    def __getitem__(self, tid):
        """
            Return the transaction object for this TID
        """
        return self._tid_dict[tid]

    def __contains__(self, tid):
        """
            Returns True if this is a pending transaction
        """
        return tid in self._tid_dict

    def items(self):
        return self._tid_dict.items()

    def getNextOIDList(self, num_oids):
        """ Generate a new OID list """
        if self._last_oid is None:
            raise RuntimeError, 'I do not know the last OID'
        oid = unpack('!Q', self._last_oid)[0] + 1
        oid_list = [pack('!Q', oid + i) for i in xrange(num_oids)]
        self._last_oid = oid_list[-1]
        return oid_list

    def updateLastOID(self, oid_list):
        """
            Updates the last oid with the max of those supplied if greater than
            the current known, returns True if changed
        """
        max_oid = oid_list and max(oid_list) or None # oid_list might be empty
        if max_oid > self._last_oid:
            self._last_oid = max_oid
            return True
        return False

    def setLastOID(self, oid):
        self._last_oid = oid

    def getLastOID(self):
        return self._last_oid

    def _nextTID(self):
        """ Compute the next TID based on the current time and check collisions """
        tm = time()
        gmt = gmtime(tm)
        tid = packTID((
            (gmt.tm_year, gmt.tm_mon, gmt.tm_mday, gmt.tm_hour,
                gmt.tm_min),
            int((gmt.tm_sec % 60 + (tm - int(tm))) / SECOND_PER_TID_LOW)
        ))
        if self._last_tid is not None and tid <= self._last_tid:
            tid  = addTID(self._last_tid, 1)
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
        if self._last_tid is None:
            self._last_tid = tid
        else:
            self._last_tid = max(self._last_tid, tid)

    def reset(self):
        """
            Discard all manager content
            This doesn't reset the last TID.
        """
        self._tid_dict = {}
        self._node_dict = {}

    def hasPending(self):
        """
            Returns True if some transactions are pending
        """
        return bool(self._tid_dict)

    def getPendingList(self):
        """
            Return the list of pending transaction IDs
        """
        return self._tid_dict.keys()

    def begin(self):
        """
            Generate a new TID
        """
        return self._nextTID()

    def prepare(self, node, tid, oid_list, uuid_list, msg_id):
        """
            Prepare a transaction to be finished
        """
        self.setLastTID(tid)
        txn = Transaction(node, tid, oid_list, uuid_list, msg_id)
        self._tid_dict[tid] = txn
        self._node_dict.setdefault(node, {})[tid] = txn

    def remove(self, tid):
        """
            Remove a transaction, commited or aborted
        """
        node = self._tid_dict[tid].getNode()
        # remove both mappings, node will be removed in abortFor
        del self._tid_dict[tid]
        del self._node_dict[node][tid]

    def lock(self, tid, uuid):
        """
            Set that a node has locked the transaction.
            Returns True if all are now locked
        """
        assert tid in self._tid_dict, "Transaction not started"
        return self._tid_dict[tid].lock(uuid)

    def abortFor(self, node):
        """
            Abort pending transactions initiated by a node
        """
        # nothing to do
        if node not in self._node_dict:
            return
        # remove transactions
        for tid in self._node_dict[node].keys():
            del self._tid_dict[tid]
        # discard node entry
        del self._node_dict[node]

    def log(self):
        neo.logging.info('Transactions:')
        for txn in self._tid_dict.itervalues():
            neo.logging.info('  %r', txn)

