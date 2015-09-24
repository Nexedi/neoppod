#
# Copyright (C) 2006-2015  Nexedi SA
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
- Object (data) replication

Both parts follow the same mechanism:
- The range of data to replicate is split into chunks of FETCH_COUNT items
  (transaction or object).
- For every chunk, the requesting node sends to seeding node the list of items
  it already has.
- Before answering, the seeding node sends 1 packet for every missing item.
- The seeding node finally answers with the list of items to delete (usually
  empty).

Replication is partial, starting from the greatest stored tid in the partition:
- For transactions, this tid is excluded from replication.
- For objects, this tid is included unless the storage already knows it has
  all oids for it.

There is no check that item values on both nodes matches.

TODO: Packing and replication currently fail when they happen at the same time.
"""

import random

from neo.lib import logging
from neo.lib.protocol import CellStates, NodeTypes, NodeStates, \
    Packets, INVALID_TID, ZERO_TID, ZERO_OID
from neo.lib.connection import ClientConnection
from neo.lib.util import add64, dump
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

    current_node = None
    current_partition = None

    def __init__(self, app):
        self.app = app

    def getCurrentConnection(self):
        node = self.current_node
        if node is not None and node.isConnected(True):
            return node.getConnection()

    # XXX: We can't replicate unfinished transactions but do we need such
    #      complex code ? Backup mechanism does not rely on this: instead
    #      the upstream storage delays the answer. Maybe we can do the same
    #      for internal replication.

    def setUnfinishedTIDList(self, max_tid, ttid_list, offset_list):
        """This is a callback from MasterOperationHandler."""
        if ttid_list:
            self.ttid_set.update(ttid_list)
            max_ttid = max(ttid_list)
        else:
            max_ttid = None
        for offset in offset_list:
            self.partition_dict[offset].max_ttid = max_ttid
            self.replicate_dict[offset] = max_tid
        self._nextPartition()

    def transactionFinished(self, ttid, max_tid):
        """ Callback from MasterOperationHandler """
        self.ttid_set.remove(ttid)
        min_ttid = min(self.ttid_set) if self.ttid_set else INVALID_TID
        for offset, p in self.partition_dict.iteritems():
            if p.max_ttid and p.max_ttid < min_ttid:
                p.max_ttid = None
                self.replicate_dict[offset] = max_tid
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

    def updateBackupTID(self):
        dm = self.app.dm
        tid = dm.getBackupTID()
        if tid:
            new_tid = self.getBackupTID()
            if tid != new_tid:
                dm.setBackupTID(new_tid)

    def populate(self):
        app = self.app
        pt = app.pt
        uuid = app.uuid
        self.partition_dict = p = {}
        self.replicate_dict = {}
        self.source_dict = {}
        self.ttid_set = set()
        last_tid, last_trans_dict, last_obj_dict, _ = app.dm.getLastIDs()
        next_tid = app.dm.getBackupTID() or last_tid
        next_tid = add64(next_tid, 1) if next_tid else ZERO_TID
        outdated_list = []
        for offset in xrange(pt.getPartitions()):
            for cell in pt.getCellList(offset):
                if cell.getUUID() == uuid and not cell.isCorrupted():
                    self.partition_dict[offset] = p = Partition()
                    if cell.isOutOfDate():
                        outdated_list.append(offset)
                        try:
                            p.next_trans = add64(last_trans_dict[offset], 1)
                        except KeyError:
                            p.next_trans = ZERO_TID
                        p.next_obj = last_obj_dict.get(offset, ZERO_TID)
                        p.max_ttid = INVALID_TID
                    else:
                        p.next_trans = p.next_obj = next_tid
                        p.max_ttid = None
        if outdated_list:
            self.app.master_conn.ask(Packets.AskUnfinishedTransactions(),
                                     offset_list=outdated_list)

    def notifyPartitionChanges(self, cell_list):
        """This is a callback from MasterOperationHandler."""
        abort = False
        added_list = []
        app = self.app
        last_tid, last_trans_dict, last_obj_dict, _ = app.dm.getLastIDs()
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
                elif state == CellStates.OUT_OF_DATE:
                    assert offset not in self.partition_dict
                    self.partition_dict[offset] = p = Partition()
                    try:
                        p.next_trans = add64(last_trans_dict[offset], 1)
                    except KeyError:
                        p.next_trans = ZERO_TID
                    p.next_obj = last_obj_dict.get(offset, ZERO_TID)
                    p.max_ttid = INVALID_TID
                    added_list.append(offset)
        if added_list:
            self.app.master_conn.ask(Packets.AskUnfinishedTransactions(),
                                     offset_list=added_list)
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
            self.updateBackupTID()
        self._nextPartition()

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
        # Choose a partition with no unfinished transaction if possible.
        # XXX: When leaving backup mode, we should only consider UP_TO_DATE
        #      cells.
        for offset in self.replicate_dict:
            if not self.partition_dict[offset].max_ttid:
                break
        try:
            addr, name = self.source_dict[offset]
        except KeyError:
            assert app.pt.getCell(offset, app.uuid).isOutOfDate()
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
            conn.ask(Packets.RequestIdentification(NodeTypes.STORAGE,
                None if name else app.uuid, app.server, name or app.name))
        if previous_node is not None and previous_node.isConnected():
            app.closeClient(previous_node.getConnection())

    def connected(self, node):
        if self.current_node is node and self.current_partition is not None:
            self.fetchTransactions()

    def fetchTransactions(self, min_tid=None):
        offset = self.current_partition
        p = self.partition_dict[offset]
        if min_tid:
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
        self.current_node.getConnection().ask(Packets.AskFetchTransactions(
            offset, FETCH_COUNT, min_tid, max_tid, tid_list))

    def fetchObjects(self, min_tid=None, min_oid=ZERO_OID):
        offset = self.current_partition
        p = self.partition_dict[offset]
        max_tid = self.replicate_tid
        if min_tid:
            p.next_obj = min_tid
        else:
            min_tid = p.next_obj
            p.next_trans = add64(max_tid, 1)
        object_dict = {}
        for serial, oid in self.app.dm.getReplicationObjectList(min_tid,
                max_tid, FETCH_COUNT, offset, min_oid):
            try:
                object_dict[serial].append(oid)
            except KeyError:
                object_dict[serial] = [oid]
        self.current_node.getConnection().ask(Packets.AskFetchObjects(
            offset, FETCH_COUNT, min_tid, max_tid, min_oid, object_dict))

    def finish(self):
        offset = self.current_partition
        tid = self.replicate_tid
        del self.current_partition, self.replicate_tid
        p = self.partition_dict[offset]
        p.next_obj = add64(tid, 1)
        self.updateBackupTID()
        if not p.max_ttid:
            p = Packets.NotifyReplicationDone(offset, tid)
            self.app.master_conn.notify(p)
        logging.debug("partition %u replicated up to %s from %r",
                      offset, dump(tid), self.current_node)
        self.getCurrentConnection().setReconnectionNoDelay()
        self._nextPartition()

    def abort(self, message=''):
        offset = self.current_partition
        if offset is None:
            return
        del self.current_partition
        logging.warning('replication aborted for partition %u%s',
                        offset, message and ' (%s)' % message)
        if offset in self.partition_dict:
            # XXX: Try another partition if possible, to increase probability to
            #      connect to another node. It would be better to explicitely
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

    def cancel(self):
        offset = self.current_partition
        if offset is not None:
            logging.info('cancel replication of partition %u', offset)
            del self.current_partition
            try:
                self.replicate_dict.setdefault(offset, self.replicate_tid)
                del self.replicate_tid
            except AttributeError:
                pass
            self.getCurrentConnection().close()

    def stop(self):
        # Close any open connection to an upstream storage,
        # possibly aborting current replication.
        node = self.current_node
        if node is not None is node.getUUID():
            self.cancel()
        # Cancel all replication orders from upstream cluster.
        for offset in self.replicate_dict.keys():
            addr, name = self.source_dict.get(offset, (None, None))
            if name:
                tid = self.replicate_dict.pop(offset)
                logging.info('cancel replication of partition %u from %r'
                             ' up to %s', offset, addr, dump(tid))
        # Make UP_TO_DATE cells really UP_TO_DATE
        self._nextPartition()
