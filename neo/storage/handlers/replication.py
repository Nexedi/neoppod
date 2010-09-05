
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

from neo import logging

from neo.handler import EventHandler
from neo.protocol import Packets, ZERO_TID, ZERO_OID
from neo import util

# TODO: benchmark how different values behave
RANGE_LENGTH = 4000
MIN_RANGE_LENGTH = 1000

"""
Replication algorythm

Purpose: replicate the content of a reference node into a replicating node,
bringing it up-to-date.
This happens both when a new storage is added to en existing cluster, as well
as when a nde was separated from cluster and rejoins it.

Replication happens per partition. Reference node can change between
partitions.

2 parts, done sequentially:
- Transaction (metadata) replication
- Object (data) replication

Both part follow the same mechanism:
- On both sides (replicating and reference), compute a checksum of a chunk
  (RANGE_LENGTH number of entries). If there is a mismatch, chunk size is
  reduced, and scan restarts from same row, until it reaches a minimal length
  (MIN_RANGE_LENGTH). Then, it replicates all rows in that chunk. If the
  content of chunks match, it moves on to the next chunk.
- Replicating a chunk starts with asking for a list of all entries (only their
  identifier) and skipping those both side have, deleting those which reference
  has and replicating doesn't, and asking individually all entries missing in
  replicating.
"""

# TODO: Make object replication get ordered by serial first and oid second, so
# changes are in a big segment at the end, rather than in many segments (one
# per object).

# TODO: To improve performance when a pack happened, the following algorithm
# should be used:
# - If reference node packed, find non-existant oids in reference node (their
#   creation was undone, and pack pruned them), and delete them.
# - Run current algorithm, starting at our last pack TID.
# - Pack partition at reference's TID.

def checkConnectionIsReplicatorConnection(func):
    def decorator(self, conn, *args, **kw):
        if self.app.replicator.current_connection is conn:
            result = func(self, conn, *args, **kw)
        else:
            # Should probably raise & close connection...
            result = None
        return result
    return decorator

def add64(packed, offset):
    """Add a python number to a 64-bits packed value"""
    return util.p64(util.u64(packed) + offset)

class ReplicationHandler(EventHandler):
    """This class handles events for replications."""

    def connectionLost(self, conn, new_state):
        logging.error('replication is stopped due to a connection lost')
        self.app.replicator.reset()

    def connectionFailed(self, conn):
        logging.error('replication is stopped due to connection failure')
        self.app.replicator.reset()

    def acceptIdentification(self, conn, node_type,
                       uuid, num_partitions, num_replicas, your_uuid):
        # set the UUID on the connection
        conn.setUUID(uuid)
        self.startReplication(conn)

    def startReplication(self, conn):
        conn.ask(self._doAskCheckTIDRange(ZERO_TID), timeout=300)

    @checkConnectionIsReplicatorConnection
    def answerTIDsFrom(self, conn, tid_list):
        app = self.app
        # If I have pending TIDs, check which TIDs I don't have, and
        # request the data.
        tid_set = frozenset(tid_list)
        my_tid_set = frozenset(app.replicator.getTIDsFromResult())
        extra_tid_set = my_tid_set - tid_set
        if extra_tid_set:
            deleteTransaction = app.dm.deleteTransaction
            for tid in extra_tid_set:
                deleteTransaction(tid)
        missing_tid_set = tid_set - my_tid_set
        for tid in missing_tid_set:
            conn.ask(Packets.AskTransactionInformation(tid), timeout=300)

    @checkConnectionIsReplicatorConnection
    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        app = self.app
        # Directly store the transaction.
        app.dm.storeTransaction(tid, (), (oid_list, user, desc, ext, packed),
            False)

    @checkConnectionIsReplicatorConnection
    def answerObjectHistoryFrom(self, conn, object_dict):
        app = self.app
        my_object_dict = app.replicator.getObjectHistoryFromResult()
        deleteObject = app.dm.deleteObject
        for oid, serial_list in object_dict.iteritems():
            # Check if I have objects, request those which I don't have.
            if oid in my_object_dict:
                my_serial_set = frozenset(my_object_dict[oid])
                serial_set = frozenset(serial_list)
                extra_serial_set = my_serial_set - serial_set
                for serial in extra_serial_set:
                    deleteObject(oid, serial)
                missing_serial_set = serial_set - my_serial_set
            else:
                missing_serial_set = serial_list
            for serial in missing_serial_set:
                conn.ask(Packets.AskObject(oid, serial, None), timeout=300)

    @checkConnectionIsReplicatorConnection
    def answerObject(self, conn, oid, serial_start,
            serial_end, compression, checksum, data, data_serial):
        app = self.app
        # Directly store the transaction.
        obj = (oid, compression, checksum, data, data_serial)
        app.dm.storeTransaction(serial_start, [obj], None, False)
        del obj
        del data

    def _doAskCheckSerialRange(self, min_oid, min_tid, length=RANGE_LENGTH):
        replicator = self.app.replicator
        partition = replicator.current_partition.getRID()
        replicator.checkSerialRange(min_oid, min_tid, length, partition)
        return Packets.AskCheckSerialRange(min_oid, min_tid, length, partition)

    def _doAskCheckTIDRange(self, min_tid, length=RANGE_LENGTH):
        replicator = self.app.replicator
        partition = replicator.current_partition.getRID()
        replicator.checkTIDRange(min_tid, length, partition)
        return Packets.AskCheckTIDRange(min_tid, length, partition)

    def _doAskTIDsFrom(self, min_tid, length):
        replicator = self.app.replicator
        partition = replicator.current_partition
        partition_id = partition.getRID()
        max_tid = partition.getCriticalTID()
        replicator.getTIDsFrom(min_tid, max_tid, length, partition_id)
        return Packets.AskTIDsFrom(min_tid, max_tid, length, partition_id)

    def _doAskObjectHistoryFrom(self, min_oid, min_serial, length):
        replicator = self.app.replicator
        partition = replicator.current_partition
        partition_id = partition.getRID()
        max_serial = partition.getCriticalTID()
        replicator.getObjectHistoryFrom(min_oid, min_serial, max_serial,
            length, partition_id)
        return Packets.AskObjectHistoryFrom(min_oid, min_serial, max_serial,
            length, partition_id)

    @checkConnectionIsReplicatorConnection
    def answerCheckTIDRange(self, conn, min_tid, length, count, tid_checksum,
            max_tid):
        app = self.app
        replicator = app.replicator
        our = replicator.getTIDCheckResult(min_tid, length)
        his = (count, tid_checksum, max_tid)
        our_count = our[0]
        our_max_tid = our[2]
        p = None
        if our != his:
            # Something is different...
            if length <= MIN_RANGE_LENGTH:
                # We are already at minimum chunk length, replicate.
                conn.ask(self._doAskTIDsFrom(min_tid, count))
            else:
                # Check a smaller chunk.
                # Note: this could be made into a real binary search, but is
                # it really worth the work ?
                # Note: +1, so we can detect we reached the end when answer
                # comes back.
                p = self._doAskCheckTIDRange(min_tid, min(length / 2,
                    count + 1))
        if p is None:
            if count == length and \
                    max_tid < replicator.current_partition.getCriticalTID():
                # Go on with next chunk
                p = self._doAskCheckTIDRange(add64(max_tid, 1))
            else:
                # If no more TID, a replication of transactions is finished.
                # So start to replicate objects now.
                p = self._doAskCheckSerialRange(ZERO_OID, ZERO_TID)
        conn.ask(p)

    @checkConnectionIsReplicatorConnection
    def answerCheckSerialRange(self, conn, min_oid, min_serial, length, count,
            oid_checksum, max_oid, serial_checksum, max_serial):
        app = self.app
        replicator = app.replicator
        our = replicator.getSerialCheckResult(min_oid, min_serial, length)
        his = (count, oid_checksum, max_oid, serial_checksum, max_serial)
        our_count = our[0]
        our_max_oid = our[2]
        our_max_serial = our[4]
        p = None
        if our != his:
            # Something is different...
            if length <= MIN_RANGE_LENGTH:
                # We are already at minimum chunk length, replicate.
                conn.ask(self._doAskObjectHistoryFrom(min_oid, min_serial,
                  count))
            else:
                # Check a smaller chunk.
                # Note: this could be made into a real binary search, but is
                # it really worth the work ?
                # Note: +1, so we can detect we reached the end when answer
                # comes back.
                p = self._doAskCheckSerialRange(min_oid, min_serial,
                    min(length / 2, count + 1))
        if p is None:
            if count == length:
                # Go on with next chunk
                p = self._doAskCheckSerialRange(max_oid, add64(max_serial, 1))
            else:
                # Nothing remains, so the replication for this partition is
                # finished.
                replicator.replication_done = True
        if p is not None:
            conn.ask(p)

