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
from neo.util import dump
from neo import logging
from neo import protocol

class Transaction(object):
    """
        A pending transaction
    """

    def __init__(self, node, tid):
        self._node = node
        self._tid = tid
        self._oid_list = []
        self._msg_id = None
        # uuid dict hold flag to known who has locked the transaction
        self._uuid_dict = {}

    def __repr__(self):
        return "<%s(node=%r, tid=%r, oids=%r, uuids=%r) at %x>" % (
                self.__class__.__name__,
                self._node,
                dump(self._tid),
                [dump(x) for x in self._oid_list],
                [dump(x) for x in self._uuid_dict],
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
        return self._uuid_dict.keys()

    def getOIDList(self):
        """
            Returns the list of OIDs used in the transaction
        """

        return list(self._oid_list)

    def prepare(self, oid_list, uuid_list, msg_id):
        """
            Prepare the transaction, set OIDs and UUIDs related to it
        """
        assert not self._oid_list
        assert not self._uuid_dict
        self._oid_list = oid_list
        self._uuid_dict = dict.fromkeys(uuid_list, False)
        self._msg_id = msg_id

    def lock(self, uuid):
        """
            Define that a node has locked the transaction
            Returns true if all nodes are locked
        """
        self._uuid_dict[uuid] = True
        return self.locked()

    def locked(self):
        """
            Returns true if all nodes are locked
        """
        return False not in self._uuid_dict.values()


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

    def _nextTID(self):
        """ Compute the next TID based on the current time and check collisions """
        tm = time()
        gmt = gmtime(tm)
        upper = ((((gmt.tm_year - 1900) * 12 + gmt.tm_mon - 1) * 31 \
                  + gmt.tm_mday - 1) * 24 + gmt.tm_hour) * 60 + gmt.tm_min
        lower = int((gmt.tm_sec % 60 + (tm - int(tm))) / (60.0 / 65536.0 / 65536.0))
        tid = pack('!LL', upper, lower)
        if self._last_tid is not None and tid <= self._last_tid:
            upper, lower = unpack('!LL', self._last_tid)
            if lower == 0xffffffff:
                # This should not happen usually.
                from datetime import timedelta, datetime
                d = datetime(gmt.tm_year, gmt.tm_mon, gmt.tm_mday,
                             gmt.tm_hour, gmt.tm_min) \
                        + timedelta(0, 60)
                upper = ((((d.year - 1900) * 12 + d.month - 1) * 31 \
                          + d.day - 1) * 24 + d.hour) * 60 + d.minute
                lower = 0
            else:
                lower += 1
            tid = pack('!LL', upper, lower)
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

    def begin(self, node, tid):
        """
            Begin a new transaction
        """
        assert node is not None
        if tid is not None and tid < self._last_tid:
            logging.warn('Transaction began with a decreased TID: %s, ' \
                'expected at least %s', tid, self._last_tid)
        if tid is None:
            # give a TID
            tid = self._nextTID()
        txn = Transaction(node, tid)
        self._tid_dict[tid] = txn
        self._node_dict.setdefault(node, {})[tid] = txn
        self.setLastTID(tid)
        return tid

    def prepare(self, tid, oid_list, uuid_list, msg_id):
        """
            Prepare a transaction to be finished
        """
        assert tid in self._tid_dict, "Transaction not started"
        txn = self._tid_dict[tid]
        txn.prepare(oid_list, uuid_list, msg_id)

    def remove(self, tid):
        """
            Remove a transaction, commited or aborted
        """
        if tid not in self._tid_dict:
            return
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

