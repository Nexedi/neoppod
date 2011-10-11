
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

from functools import wraps
import neo.lib

from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets, ZERO_HASH, ZERO_TID, ZERO_OID
from neo.lib.util import add64, u64

# TODO: benchmark how different values behave
RANGE_LENGTH = 4000
MIN_RANGE_LENGTH = 1000

CHECK_CHUNK = 0
CHECK_REPLICATE = 1
CHECK_DONE = 2

"""
Replication algorithm

Purpose: replicate the content of a reference node into a replicating node,
bringing it up-to-date.
This happens both when a new storage is added to en existing cluster, as well
as when a nde was separated from cluster and rejoins it.

Replication happens per partition. Reference node can change between
partitions.

2 parts, done sequentially:
- Transaction (metadata) replication
- Object (data) replication

Both parts follow the same mechanism:
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
        if self.app.replicator.isCurrentConnection(conn):
            return func(self, conn, *args, **kw)
        # Should probably raise & close connection...
    return wraps(func)(decorator)

class ReplicationHandler(EventHandler):
    """This class handles events for replications."""

    def connectionLost(self, conn, new_state):
        replicator = self.app.replicator
        if replicator.isCurrentConnection(conn):
            if replicator.pending():
                neo.lib.logging.warning(
                    'replication is stopped due to a connection lost')
            replicator.storageLost()

    def connectionFailed(self, conn):
        neo.lib.logging.warning(
                        'replication is stopped due to connection failure')
        self.app.replicator.storageLost()

    def acceptIdentification(self, conn, node_type,
                       uuid, num_partitions, num_replicas, your_uuid):
        # set the UUID on the connection
        conn.setUUID(uuid)
        self.startReplication(conn)

    def startReplication(self, conn):
        max_tid = self.app.replicator.getCurrentCriticalTID()
        conn.ask(self._doAskCheckTIDRange(ZERO_TID, max_tid), timeout=300)

    @checkConnectionIsReplicatorConnection
    def answerTIDsFrom(self, conn, tid_list):
        assert tid_list
        app = self.app
        ask = conn.ask
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
            ask(Packets.AskTransactionInformation(tid), timeout=300)
        if len(tid_list) == MIN_RANGE_LENGTH:
            # If we received fewer, we knew it before sending AskTIDsFrom, and
            # we should have finished TID replication at that time.
            max_tid = self.app.replicator.getCurrentCriticalTID()
            ask(self._doAskCheckTIDRange(add64(tid_list[-1], 1), max_tid,
                RANGE_LENGTH))

    @checkConnectionIsReplicatorConnection
    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        app = self.app
        # Directly store the transaction.
        app.dm.storeTransaction(tid, (), (oid_list, user, desc, ext, packed),
            False)

    @checkConnectionIsReplicatorConnection
    def answerObjectHistoryFrom(self, conn, object_dict):
        assert object_dict
        app = self.app
        ask = conn.ask
        deleteObject = app.dm.deleteObject
        my_object_dict = app.replicator.getObjectHistoryFromResult()
        object_set = set()
        max_oid = max(object_dict.iterkeys())
        max_serial = max(object_dict[max_oid])
        for oid, serial_list in object_dict.iteritems():
            for serial in serial_list:
                object_set.add((oid, serial))
        my_object_set = set()
        for oid, serial_list in my_object_dict.iteritems():
            filter = lambda x: True
            if max_oid is not None:
                if oid > max_oid:
                    continue
                elif oid == max_oid:
                    filter = lambda x: x <= max_serial
            for serial in serial_list:
                if filter(serial):
                    my_object_set.add((oid, serial))
        extra_object_set = my_object_set - object_set
        for oid, serial in extra_object_set:
            deleteObject(oid, serial)
        missing_object_set = object_set - my_object_set
        for oid, serial in missing_object_set:
            if not app.dm.objectPresent(oid, serial):
                ask(Packets.AskObject(oid, serial, None), timeout=300)
        if sum((len(x) for x in object_dict.itervalues())) == MIN_RANGE_LENGTH:
            max_tid = self.app.replicator.getCurrentCriticalTID()
            ask(self._doAskCheckSerialRange(max_oid, add64(max_serial, 1),
                max_tid, RANGE_LENGTH))

    @checkConnectionIsReplicatorConnection
    def answerObject(self, conn, oid, serial_start,
            serial_end, compression, checksum, data, data_serial):
        dm = self.app.dm
        if data or checksum != ZERO_HASH:
            dm.storeData(checksum, data, compression)
        else:
            checksum = None
        # Directly store the transaction.
        obj = oid, checksum, data_serial
        dm.storeTransaction(serial_start, [obj], None, False)

    def _doAskCheckSerialRange(self, min_oid, min_tid, max_tid,
            length=RANGE_LENGTH):
        replicator = self.app.replicator
        partition = replicator.getCurrentOffset()
        neo.lib.logging.debug("Check serial range (offset=%s, min_oid=%x,"
            " min_tid=%x, max_tid=%x, length=%s)", partition, u64(min_oid),
            u64(min_tid), u64(max_tid), length)
        check_args = (min_oid, min_tid, max_tid, length, partition)
        replicator.checkSerialRange(*check_args)
        return Packets.AskCheckSerialRange(*check_args)

    def _doAskCheckTIDRange(self, min_tid, max_tid, length=RANGE_LENGTH):
        replicator = self.app.replicator
        partition = replicator.getCurrentOffset()
        neo.lib.logging.debug(
            "Check TID range (offset=%s, min_tid=%x, max_tid=%x, length=%s)",
            partition, u64(min_tid), u64(max_tid), length)
        replicator.checkTIDRange(min_tid, max_tid, length, partition)
        return Packets.AskCheckTIDRange(min_tid, max_tid, length, partition)

    def _doAskTIDsFrom(self, min_tid, length):
        replicator = self.app.replicator
        partition_id = replicator.getCurrentOffset()
        max_tid = replicator.getCurrentCriticalTID()
        replicator.getTIDsFrom(min_tid, max_tid, length, partition_id)
        neo.lib.logging.debug("Ask TIDs (offset=%s, min_tid=%x, max_tid=%x,"
            "length=%s)", partition_id, u64(min_tid), u64(max_tid), length)
        return Packets.AskTIDsFrom(min_tid, max_tid, length, [partition_id])

    def _doAskObjectHistoryFrom(self, min_oid, min_serial, length):
        replicator = self.app.replicator
        partition_id = replicator.getCurrentOffset()
        max_serial = replicator.getCurrentCriticalTID()
        replicator.getObjectHistoryFrom(min_oid, min_serial, max_serial,
            length, partition_id)
        return Packets.AskObjectHistoryFrom(min_oid, min_serial, max_serial,
            length, partition_id)

    def _checkRange(self, match, current_boundary, next_boundary, length,
            count):
        if count == 0:
            # Reference storage has no data for this chunk, stop and truncate.
            return CHECK_DONE, (current_boundary, )
        if match:
            # Same data on both sides
            if length < RANGE_LENGTH and length == count:
                # ...and previous check detected a difference - and we still
                # haven't reached the end. This means that we just check the
                # first half of a chunk which, as a whole, is different. So
                # next test must happen on the next chunk.
                recheck_min_boundary = next_boundary
            else:
                # ...and we just checked a whole chunk, move on to the next
                # one.
                recheck_min_boundary = None
        else:
            # Something is different in current chunk
            recheck_min_boundary = current_boundary
        if recheck_min_boundary is None:
            if count == length:
                # Go on with next chunk
                action = CHECK_CHUNK
                params = (next_boundary, RANGE_LENGTH)
            else:
                # No more chunks.
                action = CHECK_DONE
                params = (next_boundary, )
        else:
            # We must recheck current chunk.
            if not match and count <= MIN_RANGE_LENGTH:
                # We are already at minimum chunk length, replicate.
                action = CHECK_REPLICATE
                params = (recheck_min_boundary, )
            else:
                # Check a smaller chunk.
                # Note: +1, so we can detect we reached the end when answer
                # comes back.
                action = CHECK_CHUNK
                params = (recheck_min_boundary, max(min(length / 2, count + 1),
                                                    MIN_RANGE_LENGTH))
        return action, params

    @checkConnectionIsReplicatorConnection
    def answerCheckTIDRange(self, conn, min_tid, length, count, tid_checksum,
            max_tid):
        pkt_min_tid = min_tid
        ask = conn.ask
        app = self.app
        replicator = app.replicator
        next_tid = add64(max_tid, 1)
        action, params = self._checkRange(
            replicator.getTIDCheckResult(min_tid, length) == (
            count, tid_checksum, max_tid), min_tid, next_tid, length,
            count)
        critical_tid = replicator.getCurrentCriticalTID()
        if action == CHECK_REPLICATE:
            (min_tid, ) = params
            ask(self._doAskTIDsFrom(min_tid, count))
            if length != count:
                action = CHECK_DONE
                params = (next_tid, )
        if action == CHECK_CHUNK:
            (min_tid, count) = params
            if min_tid >= critical_tid:
                # Stop if past critical TID
                action = CHECK_DONE
                params = (next_tid, )
            else:
                ask(self._doAskCheckTIDRange(min_tid, critical_tid, count))
        if action == CHECK_DONE:
            # Delete all transactions we might have which are beyond what peer
            # knows.
            (last_tid, ) = params
            offset = replicator.getCurrentOffset()
            neo.lib.logging.debug("TID range checked (offset=%s, min_tid=%x,"
                " length=%s, count=%s, max_tid=%x, last_tid=%x,"
                " critical_tid=%x)", offset, u64(pkt_min_tid), length, count,
                u64(max_tid), u64(last_tid), u64(critical_tid))
            app.dm.deleteTransactionsAbove(app.pt.getPartitions(),
                offset, last_tid, critical_tid)
            # If no more TID, a replication of transactions is finished.
            # So start to replicate objects now.
            ask(self._doAskCheckSerialRange(ZERO_OID, ZERO_TID, critical_tid))

    @checkConnectionIsReplicatorConnection
    def answerCheckSerialRange(self, conn, min_oid, min_serial, length, count,
            oid_checksum, max_oid, serial_checksum, max_serial):
        ask = conn.ask
        app = self.app
        replicator = app.replicator
        next_params = (max_oid, add64(max_serial, 1))
        action, params = self._checkRange(
            replicator.getSerialCheckResult(min_oid, min_serial, length) == (
            count, oid_checksum, max_oid, serial_checksum, max_serial),
            (min_oid, min_serial), next_params, length, count)
        if action == CHECK_REPLICATE:
            ((min_oid, min_serial), ) = params
            ask(self._doAskObjectHistoryFrom(min_oid, min_serial, count))
            if length != count:
                action = CHECK_DONE
                params = (next_params, )
        if action == CHECK_CHUNK:
            ((min_oid, min_serial), count) = params
            max_tid = replicator.getCurrentCriticalTID()
            ask(self._doAskCheckSerialRange(min_oid, min_serial, max_tid, count))
        if action == CHECK_DONE:
            # Delete all objects we might have which are beyond what peer
            # knows.
            ((last_oid, last_serial), ) = params
            offset = replicator.getCurrentOffset()
            max_tid = replicator.getCurrentCriticalTID()
            neo.lib.logging.debug("Serial range checked (offset=%s, min_oid=%x,"
                " min_serial=%x, length=%s, count=%s, max_oid=%x,"
                " max_serial=%x, last_oid=%x, last_serial=%x, critical_tid=%x)",
                offset, u64(min_oid), u64(min_serial), length, count,
                u64(max_oid), u64(max_serial), u64(last_oid), u64(last_serial),
                u64(max_tid))
            app.dm.deleteObjectsAbove(app.pt.getPartitions(),
                offset, last_oid, last_serial, max_tid)
            # Nothing remains, so the replication for this partition is
            # finished.
            replicator.setReplicationDone()

