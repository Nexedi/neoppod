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

"""
Replication algorithm

Purpose: replicate the content of a reference node into a replicating node,
bringing it up-to-date. This happens in the following cases:
- A new storage is added to en existing cluster.
- A node was separated from cluster and rejoins it.
- In a backup cluster, the master notifies a node that new data exists upstream
  (note that in this case, the cell is always marked as UP_TO_DATE).

Replication happens per partition. Reference node can change between
partitions.

2 parts, done sequentially:
- Transaction (metadata) replication
- Object (metadata+data) replication

Both parts follow the same mechanism:
- The range of data to replicate is split into chunks of FETCH_COUNT items
  (transaction or object).
- For every chunk, the requesting node sends to seeding node the list of items
  it already has.
- Before answering, the seeding node sends 1 packet for every missing item.
  For items that are already on the replicating node, there is no check that
  values matches.
- The seeding node finally answers with the list of items to delete (usually
  empty).

Internal replication, which is similar to RAID1 (and as opposed to asynchronous
replication to a backup cluster) requires extra care with respect to
transactions. The transition of a cell from OUT_OF_DATE to UP_TO_DATE is done
is several steps.

A replicating node can not depend on other nodes to fetch the data
recently/being committed because that can not be done atomically: it could miss
writes between the processing of its request by a source node and the reception
of the answer.

Therefore, outdated cells are writable: a storage node asks the master for
transactions being committed and then it is expected to fully receive from the
client any transaction that is started after this answer.

Which has in turn other consequences:
- The client must not fail to write to a storage node after the above request
  to the master: for this, the storage must have announced it is ready, and it
  must delay identification of unknown clients (those for which it hasn't
  received yet a notification from the master).
- Writes must be accepted blindly (i.e. without taking a write-lock) when a
  storage node lacks the data to check for conflicts. This is possible because
  1 readable cell (for each partition) is enough to do these checks.
- Because the client can not reliably know if a storage node is expected to
  receive a transaction in full, all writes must succeed.
- Even if the replication is finished, we have to wait that we don't have any
  lockless writes left before announcing to the master that we're up-to-date.

To sum up:
1. ask unfinished transactions -> (last_transaction, ttid_list)
2. replicate to last_transaction
3. wait for all ttid_list to be finished -> new last_transaction
4. replicate to last_transaction
5. no lockless write anymore, except to (oid, ttid) that were already
   stored/checked without taking a lock
6. wait for all transactions with lockless writes to be finished
7. announce we're up-to-date

For any failed write, the client marks the storage node as failed and stops
writing to it for the transaction. Unless there's no failed write, vote ends
with an extra request to the master: the transaction will only succeed if the
failed nodes can be disconnected, forcing them to replicate the missing data.

TODO: Packing and replication currently fail when they happen at the same time.
"""

import random

from neo.lib import logging
from neo.lib.protocol import CellStates, NodeTypes, NodeStates, \
    Packets, INVALID_TID, ZERO_TID, ZERO_OID
from neo.lib.connection import ClientConnection, ConnectionClosed
from neo.lib.util import add64, dump, p64
from .handlers.storage import StorageOperationHandler

FETCH_COUNT = 1000


class Partition(object):

    __slots__ = 'next_trans', 'next_obj', 'max_ttid'

    def __repr__(self):
        return '<%s(%s) at 0x%x>' % (self.__class__.__name__,
            ', '.join('%s=%r' % (x, getattr(self, x)) for x in self.__slots__
                                                      if hasattr(self, x)),
            id(self))

class Replicator(object):

    # When the replication of a partition is aborted, the connection to the
    # feeding node may still be open, e.g. on PT update from the master. In
    # such case, replication is also aborted on the other side but there may
    # be a few incoming packets that must be discarded.
    _conn_msg_id = None

    current_node = None
    current_partition = None

    def __init__(self, app):
        self.app = app

    def getCurrentConnection(self):
        node = self.current_node
        if node is not None and node.isConnected(True):
            return node.getConnection()

    def isReplicatingConnection(self, conn):
        return conn is self.getCurrentConnection() and \
            conn.getPeerId() == self._conn_msg_id

    def setUnfinishedTIDList(self, max_tid, ttid_list, offset_list):
        """This is a callback from MasterOperationHandler."""
        assert self.ttid_set.issubset(ttid_list), (self.ttid_set, ttid_list)
        if ttid_list:
            self.ttid_set.update(ttid_list)
            max_ttid = max(ttid_list)
        else:
            max_ttid = None
        for offset in offset_list:
            self.partition_dict[offset].max_ttid = max_ttid
            self.replicate_dict[offset] = max_tid
        self._nextPartition()

    def transactionFinished(self, ttid, max_tid=None):
        """ Callback from MasterOperationHandler """
        try:
            self.ttid_set.remove(ttid)
        except KeyError:
            assert max_tid is None, max_tid
            return
        min_ttid = min(self.ttid_set) if self.ttid_set else INVALID_TID
        for offset, p in self.partition_dict.iteritems():
            if p.max_ttid:
                if max_tid:
                    # Filling replicate_dict while there are still unfinished
                    # transactions for this partition is not the most
                    # efficient (due to the overhead of potentially replicating
                    # the last transactions in several times), but that's a
                    # simple way to make sure it is filled even if the
                    # remaining unfinished transactions are aborted.
                    self.replicate_dict[offset] = max_tid
                if p.max_ttid < min_ttid:
                    # no more unfinished transaction for this partition
                    if not (offset == self.current_partition
                            or offset in self.replicate_dict):
                        logging.debug(
                            "All unfinished transactions have been aborted."
                            " Mark partition %u as already fully replicated",
                            offset)
                        # We don't have anymore the previous value of
                        # self.replicate_dict[offset], but p.max_ttid is not
                        # wrong. Anyway here, we're not in backup mode and this
                        # value will be ignored.
                        # XXX: see NonReadableCell.__doc__
                        self.app.tm.replicated(offset, p.max_ttid)
                    p.max_ttid = None
        self._nextPartition()

    def getBackupTID(self):
        outdated_set = set(self.app.pt.getOutdatedOffsetListFor(self.app.uuid))
        tid = INVALID_TID
        for offset, p in self.partition_dict.iteritems():
            if offset not in outdated_set:
                tid = min(tid, p.next_trans, p.next_obj)
        if ZERO_TID != tid != INVALID_TID:
            return add64(tid, -1)
        return ZERO_TID

    def updateBackupTID(self, commit=False):
        dm = self.app.dm
        tid = dm.getBackupTID()
        if tid:
            new_tid = self.getBackupTID()
            if tid != new_tid:
                dm._setBackupTID(new_tid)
                if commit:
                    dm.commit()

    def startOperation(self, backup):
        dm = self.app.dm
        if backup:
            if dm.getBackupTID():
                assert not hasattr(self, 'partition_dict'), self.partition_dict
                return
            tid = dm.getLastIDs()[0] or ZERO_TID
        else:
            tid = None
        dm._setBackupTID(tid)
        dm.commit()
        try:
            partition_dict = self.partition_dict
        except AttributeError:
            return
        for offset, next_tid in dm.iterCellNextTIDs():
            if type(next_tid) is not bytes: # readable
                p = partition_dict[offset]
                p.next_trans, p.next_obj = next_tid

    def populate(self):
        self.partition_dict = {}
        self.replicate_dict = {}
        self.source_dict = {}
        self.ttid_set = set()
        outdated_list = []
        for offset, next_tid in self.app.dm.iterCellNextTIDs():
            self.partition_dict[offset] = p = Partition()
            if type(next_tid) is bytes: # OUT_OF_DATE
                outdated_list.append(offset)
                p.next_trans = p.next_obj = next_tid
                p.max_ttid = INVALID_TID
            else: # readable
                p.next_trans, p.next_obj = next_tid or (None, None)
                p.max_ttid = None
        if outdated_list:
            self.app.tm.replicating(outdated_list)

    def notifyPartitionChanges(self, cell_list):
        """This is a callback from MasterOperationHandler."""
        abort = False
        added_list = []
        discarded_list = []
        readable_list = []
        app = self.app
        for offset, uuid, state in cell_list:
            if uuid == app.uuid:
                if state in (CellStates.DISCARDED, CellStates.CORRUPTED):
                    try:
                        del self.partition_dict[offset]
                    except KeyError:
                        continue
                    self.replicate_dict.pop(offset, None)
                    self.source_dict.pop(offset, None)
                    abort = abort or self.current_partition == offset
                    discarded_list.append(offset)
                elif state == CellStates.OUT_OF_DATE:
                    assert offset not in self.partition_dict
                    self.partition_dict[offset] = p = Partition()
                    # New cell. 0 is also what should be stored by the backend.
                    # Nothing to optimize.
                    p.next_trans = p.next_obj = ZERO_TID
                    p.max_ttid = INVALID_TID
                    added_list.append(offset)
                else:
                    assert state in (CellStates.UP_TO_DATE,
                                     CellStates.FEEDING), state
                    readable_list.append(offset)
        tm = app.tm
        if added_list:
            tm.replicating(added_list)
        if discarded_list:
            tm.discarded(discarded_list)
        if readable_list:
            tm.readable(readable_list)
        if abort:
            self.abort()

    def backup(self, tid, source_dict):
        next_tid = None
        for offset, source in source_dict.iteritems():
            if source:
                self.source_dict[offset] = source
                self.replicate_dict[offset] = tid
            elif offset != self.current_partition and \
                 offset not in self.replicate_dict:
                # The master did its best to avoid useless replication orders
                # but there may still be a few, and we may receive redundant
                # update notification of backup_tid.
                # So, we do nothing here if we are already replicating.
                p = self.partition_dict[offset]
                if not next_tid:
                    next_tid = add64(tid, 1)
                p.next_trans = p.next_obj = next_tid
        if next_tid:
            self.updateBackupTID(True)
        self._nextPartition()

    def _nextPartitionSortKey(self, offset):
        p = self.partition_dict[offset]
        return p.next_obj, bool(p.max_ttid)

    def _nextPartition(self):
        # XXX: One connection to another storage may remain open forever.
        #      All other previous connections are automatically closed
        #      after some time of inactivity.
        #      This should be improved in several ways:
        #      - Keeping connections open between 2 clusters (backup case) is
        #        quite a good thing because establishing a connection costs
        #        time/bandwidth and replication is actually never finished.
        #      - When all storages of a non-backup cluster are up-to-date,
        #        there's no reason to keep any connection open.
        if self.current_partition is not None or not self.replicate_dict:
            return
        app = self.app
        assert app.master_conn and app.operational, (
            app.master_conn, app.operational)
        # Start replicating the partition which is furthest behind,
        # to increase the overall backup_tid as soon as possible.
        # Then prefer a partition with no unfinished transaction.
        # XXX: When leaving backup mode, we should only consider UP_TO_DATE
        #      cells.
        offset = min(self.replicate_dict, key=self._nextPartitionSortKey)
        try:
            addr, name = self.source_dict[offset]
        except KeyError:
            assert app.pt.getCell(offset, app.uuid).isOutOfDate(), (
                offset, app.pt.getCell(offset, app.uuid).getState())
            node = random.choice([cell.getNode()
                for cell in app.pt.getCellList(offset, readable=True)
                if cell.getNodeState() == NodeStates.RUNNING])
            name = None
        else:
            node = app.nm.getByAddress(addr)
            if node is None:
                assert name, addr
                node = app.nm.createStorage(address=addr)
        self.current_partition = offset
        previous_node = self.current_node
        self.current_node = node
        if node.isConnected(connecting=True):
            if node.isIdentified():
                node.getConnection().asClient()
                self.fetchTransactions()
        else:
            assert name or node.getUUID() != app.uuid, "loopback connection"
            conn = ClientConnection(app, StorageOperationHandler(app), node)
            try:
                conn.ask(Packets.RequestIdentification(NodeTypes.STORAGE,
                    None if name else app.uuid, app.server, name or app.name,
                    app.id_timestamp, {}))
            except ConnectionClosed:
                if previous_node is self.current_node:
                    return
        if previous_node is not None and previous_node.isConnected():
            app.closeClient(previous_node.getConnection())

    def connected(self, node):
        if self.current_node is node and self.current_partition is not None:
            self.fetchTransactions()

    def fetchTransactions(self, min_tid=None):
        assert self.current_node.getConnection().isClient(), self.current_node
        offset = self.current_partition
        p = self.partition_dict[offset]
        if min_tid:
            # More than one chunk ? This could be a full replication so avoid
            # restarting from the beginning by committing now.
            self.app.dm.commit()
            p.next_trans = min_tid
        else:
            try:
                addr, name = self.source_dict[offset]
            except KeyError:
                pass
            else:
                if addr != self.current_node.getAddress():
                    return self.abort()
            min_tid = p.next_trans
            self.replicate_tid = self.replicate_dict.pop(offset)
            logging.debug("starting replication of <partition=%u"
                " min_tid=%s max_tid=%s> from %r", offset, dump(min_tid),
                dump(self.replicate_tid), self.current_node)
        max_tid = self.replicate_tid
        tid_list = self.app.dm.getReplicationTIDList(min_tid, max_tid,
            FETCH_COUNT, offset)
        self._conn_msg_id = self.current_node.ask(Packets.AskFetchTransactions(
            offset, FETCH_COUNT, min_tid, max_tid, tid_list))

    def fetchObjects(self, min_tid=None, min_oid=ZERO_OID):
        offset = self.current_partition
        p = self.partition_dict[offset]
        max_tid = self.replicate_tid
        dm = self.app.dm
        if min_tid:
            p.next_obj = min_tid
            self.updateBackupTID()
            dm.updateCellTID(offset, add64(min_tid, -1))
            dm.commit() # like in fetchTransactions
        else:
            min_tid = p.next_obj
            p.next_trans = add64(max_tid, 1)
        object_dict = {}
        for serial, oid in dm.getReplicationObjectList(min_tid,
                max_tid, FETCH_COUNT, offset, min_oid):
            try:
                object_dict[serial].append(oid)
            except KeyError:
                object_dict[serial] = [oid]
        self._conn_msg_id = self.current_node.ask(Packets.AskFetchObjects(
            offset, FETCH_COUNT, min_tid, max_tid, min_oid, object_dict))

    def finish(self):
        offset = self.current_partition
        tid = self.replicate_tid
        del self.current_partition, self._conn_msg_id, self.replicate_tid
        p = self.partition_dict[offset]
        p.next_obj = add64(tid, 1)
        self.updateBackupTID()
        app = self.app
        app.dm.updateCellTID(offset, tid)
        app.dm.commit()
        if p.max_ttid or offset in self.replicate_dict and \
                         offset not in self.source_dict:
            logging.debug("unfinished transactions: %r", self.ttid_set)
        else:
            app.tm.replicated(offset, tid)
        logging.debug("partition %u replicated up to %s from %r",
                      offset, dump(tid), self.current_node)
        self.getCurrentConnection().setReconnectionNoDelay()
        self._nextPartition()

    def abort(self, message=''):
        offset = self.current_partition
        if offset is None:
            return
        del self.current_partition
        self._conn_msg_id = None
        logging.warning('replication aborted for partition %u%s',
                        offset, message and ' (%s)' % message)
        if offset in self.partition_dict:
            # XXX: Try another partition if possible, to increase probability to
            #      connect to another node. It would be better to explicitly
            #      search for another node instead.
            tid = self.replicate_dict.pop(offset, None) or self.replicate_tid
            if self.replicate_dict:
                self._nextPartition()
                self.replicate_dict[offset] = tid
            else:
                self.replicate_dict[offset] = tid
                self._nextPartition()
        else: # partition removed
            self._nextPartition()

    def stop(self):
        # Close any open connection to an upstream storage,
        # possibly aborting current replication.
        node = self.current_node
        if node is not None is node.getUUID():
            offset = self.current_partition
            if offset is not None:
                logging.info('cancel replication of partition %u', offset)
                del self.current_partition
                if self._conn_msg_id is not None:
                    self.replicate_dict.setdefault(offset, self.replicate_tid)
                    del self._conn_msg_id, self.replicate_tid
                self.getCurrentConnection().close()
        # Cancel all replication orders from upstream cluster.
        for offset in self.replicate_dict.keys():
            addr, name = self.source_dict.get(offset, (None, None))
            if name:
                tid = self.replicate_dict.pop(offset)
                logging.info('cancel replication of partition %u from %r'
                             ' up to %s', offset, addr, dump(tid))
        # Make UP_TO_DATE cells really UP_TO_DATE
        self._nextPartition()
