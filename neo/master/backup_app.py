#
# Copyright (C) 2012-2015  Nexedi SA
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

import random, weakref
from bisect import bisect
from collections import defaultdict
from neo.lib import logging
from neo.lib.bootstrap import BootstrapManager
from neo.lib.exception import PrimaryFailure
from neo.lib.handler import EventHandler
from neo.lib.node import NodeManager
from neo.lib.protocol import CellStates, ClusterStates, \
    NodeStates, NodeTypes, Packets, uuid_str, INVALID_TID, ZERO_TID
from neo.lib.util import add64, dump
from .app import StateChangedException
from .pt import PartitionTable
from .handlers.backup import BackupHandler

"""
Backup algorithm

This implementation relies on normal storage replication.
Storage nodes that are specialised for backup are not in the same NEO cluster,
but are managed by another master in a different cluster.

When the cluster is in BACKINGUP state, its master acts like a client to the
master of the main cluster. It gets notified of new data thanks to invalidation,
and notifies in turn its storage nodes what/when to replicate.

Storages stay in UP_TO_DATE state, even if partitions are synchronized up to
different tids. Storage nodes remember they are in such state and when
switching into RUNNING state, the cluster cuts the DB at the "backup TID", which
is the last TID for which we have all data. This TID can't be guessed from
'trans' and 'obj' tables, like it is done in normal mode, so:
- The master must even notify storages of transactions that don't modify their
  partitions: see Replicate packets without any source.
- 'backup_tid' properties exist in many places, on the master and the storages,
  so that the DB can be made consistent again at any moment, without losing
  any (or little) data.

Out of backup storage nodes assigned to a partition, one is chosen as primary
for that partition. It means only this node will fetch data from the upstream
cluster, to minimize bandwidth between clusters. Other replicas will
synchronize from the primary node.

There is no UUID conflict between the 2 clusters:
- Storage nodes connect anonymously to upstream.
- Master node receives a new from upstream master and uses it only when
  communicating with it.
"""

class BackupApplication(object):

    pt = None

    def __init__(self, app, name, master_addresses):
        self.app = weakref.proxy(app)
        self.name = name
        self.nm = NodeManager()
        for master_address in master_addresses:
            self.nm.createMaster(address=master_address)

    em = property(lambda self: self.app.em)
    ssl = property(lambda self: self.app.ssl)

    def close(self):
        self.nm.close()
        del self.__dict__

    def log(self):
        self.nm.log()
        if self.pt is not None:
            self.pt.log()

    def provideService(self):
        logging.info('provide backup')
        poll = self.em.poll
        app = self.app
        pt = app.pt
        while True:
            app.changeClusterState(ClusterStates.STARTING_BACKUP)
            bootstrap = BootstrapManager(self, self.name, NodeTypes.CLIENT)
            # {offset -> node}
            self.primary_partition_dict = {}
            # [[tid]]
            self.tid_list = tuple([] for _ in xrange(pt.getPartitions()))
            try:
                while True:
                    for node in pt.getNodeSet(readable=True):
                        if not app.isStorageReady(node.getUUID()):
                            break
                    else:
                        break
                    poll(1)
                node, conn, uuid, num_partitions, num_replicas = \
                    bootstrap.getPrimaryConnection()
                try:
                    app.changeClusterState(ClusterStates.BACKINGUP)
                    del bootstrap, node
                    if num_partitions != pt.getPartitions():
                        raise RuntimeError("inconsistent number of partitions")
                    self.pt = PartitionTable(num_partitions, num_replicas)
                    conn.setHandler(BackupHandler(self))
                    conn.ask(Packets.AskNodeInformation())
                    conn.ask(Packets.AskPartitionTable())
                    conn.ask(Packets.AskLastTransaction())
                    # debug variable to log how big 'tid_list' can be.
                    self.debug_tid_count = 0
                    while True:
                        poll(1)
                except PrimaryFailure, msg:
                    logging.error('upstream master is down: %s', msg)
                finally:
                    app.backup_tid = pt.getBackupTid()
                    try:
                        conn.close()
                    except PrimaryFailure:
                        pass
                    try:
                        del self.pt
                    except AttributeError:
                        pass
            except StateChangedException, e:
                if e.args[0] != ClusterStates.STOPPING_BACKUP:
                    raise
                app.changeClusterState(*e.args)
                tid = app.backup_tid
                # Wait for non-primary partitions to catch up,
                # so that all UP_TO_DATE cells are really UP_TO_DATE.
                # XXX: Another possibility could be to outdate such cells, and
                #      they would be quickly updated at the beginning of the
                #      RUNNING phase. This may simplify code.
                # Any unfinished replication from upstream will be truncated.
                while pt.getBackupTid(min) < tid:
                    poll(1)
                last_tid = app.getLastTransaction()
                handler = EventHandler(app)
                if tid < last_tid:
                    assert tid != ZERO_TID
                    logging.warning("Truncating at %s (last_tid was %s)",
                        dump(app.backup_tid), dump(last_tid))
                # XXX: We want to go through a recovery phase in order to
                #      initialize the transaction manager, but this is only
                #      possible if storages already know that we left backup
                #      mode. To that purpose, we always send a Truncate packet,
                #      even if there's nothing to truncate.
                p = Packets.Truncate(tid)
                for node in app.nm.getStorageList(only_identified=True):
                    conn = node.getConnection()
                    conn.setHandler(handler)
                    node.setState(NodeStates.TEMPORARILY_DOWN)
                    # Packets will be sent at the beginning of the recovery
                    # phase.
                    conn.notify(p)
                    conn.abort()
                # If any error happened before reaching this line, we'd go back
                # to backup mode, which is the right mode to recover.
                del app.backup_tid
                break
            finally:
                del self.primary_partition_dict, self.tid_list
                pt.clearReplicating()

    def nodeLost(self, node):
        getCellList = self.app.pt.getCellList
        trigger_set = set()
        for offset, primary_node in self.primary_partition_dict.items():
            if primary_node is not node:
                continue
            cell_list = getCellList(offset, readable=True)
            cell = max(cell_list, key=lambda cell: cell.backup_tid)
            tid = cell.backup_tid
            self.primary_partition_dict[offset] = primary_node = cell.getNode()
            p = Packets.Replicate(tid, '', {offset: primary_node.getAddress()})
            for cell in cell_list:
                cell.replicating = tid
                if cell.backup_tid < tid:
                    logging.debug(
                        "ask %s to replicate partition %u up to %s from %s",
                        uuid_str(cell.getUUID()), offset,  dump(tid),
                        uuid_str(primary_node.getUUID()))
                    cell.getNode().getConnection().notify(p)
            trigger_set.add(primary_node)
        for node in trigger_set:
            self.triggerBackup(node)

    def invalidatePartitions(self, tid, partition_set):
        app = self.app
        prev_tid = app.getLastTransaction()
        app.setLastTransaction(tid)
        pt = app.pt
        trigger_set = set()
        untouched_dict = defaultdict(dict)
        for offset in xrange(pt.getPartitions()):
            try:
                last_max_tid = self.tid_list[offset][-1]
            except IndexError:
                last_max_tid = prev_tid
            if offset in partition_set:
                self.tid_list[offset].append(tid)
                node_list = []
                for cell in pt.getCellList(offset, readable=True):
                    node = cell.getNode()
                    assert node.isConnected(), node
                    if cell.backup_tid == prev_tid:
                        # Let's given 4 TID t0,t1,t2,t3: if a cell is only
                        # modified by t0 & t3 and has all data for t0, 4 values
                        # are possible for its 'backup_tid' until it replicates
                        # up to t3: t0, t1, t2 or t3 - 1
                        # Choosing the smallest one (t0) is easier to implement
                        # but when leaving backup mode, we would always lose
                        # data if the last full transaction does not modify
                        # all partitions. t1 is wrong for the same reason.
                        # So we have chosen the highest one (t3 - 1).
                        # t2 should also work but maybe harder to implement.
                        cell.backup_tid = add64(tid, -1)
                        logging.debug(
                            "partition %u: updating backup_tid of %r to %s",
                            offset, cell, dump(cell.backup_tid))
                    else:
                        assert cell.backup_tid < last_max_tid, (
                            cell.backup_tid, last_max_tid, prev_tid, tid)
                    if app.isStorageReady(node.getUUID()):
                        node_list.append(node)
                assert node_list
                trigger_set.update(node_list)
                # Make sure we have a primary storage for this partition.
                if offset not in self.primary_partition_dict:
                    self.primary_partition_dict[offset] = \
                        random.choice(node_list)
            else:
                # Partition not touched, so increase 'backup_tid' of all
                # "up-to-date" replicas, without having to replicate.
                for cell in pt.getCellList(offset, readable=True):
                    if last_max_tid <= cell.backup_tid:
                        cell.backup_tid = tid
                        untouched_dict[cell.getNode()][offset] = None
                    elif last_max_tid <= cell.replicating:
                        # Same for 'replicating' to avoid useless orders.
                        logging.debug("silently update replicating order"
                            " of %s for partition %u, up to %s",
                            uuid_str(cell.getUUID()), offset,  dump(tid))
                        cell.replicating = tid
        for node, untouched_dict in untouched_dict.iteritems():
            if app.isStorageReady(node.getUUID()):
                node.notify(Packets.Replicate(tid, '', untouched_dict))
        for node in trigger_set:
            self.triggerBackup(node)
        count = sum(map(len, self.tid_list))
        if self.debug_tid_count < count:
            logging.debug("Maximum number of tracked tids: %u", count)
            self.debug_tid_count = count

    def triggerBackup(self, node):
        tid_list = self.tid_list
        tid = self.app.getLastTransaction()
        replicate_list = []
        for offset, cell in self.app.pt.iterNodeCell(node):
            max_tid = tid_list[offset]
            if max_tid and self.primary_partition_dict[offset] is node and \
               max(cell.backup_tid, cell.replicating) < max_tid[-1]:
                cell.replicating = tid
                replicate_list.append(offset)
        if not replicate_list:
            return
        getCellList = self.pt.getCellList
        source_dict = {}
        address_set = set()
        for offset in replicate_list:
            cell_list = getCellList(offset, readable=True)
            random.shuffle(cell_list)
            assert cell_list, offset
            for cell in cell_list:
                addr = cell.getAddress()
                if addr in address_set:
                    break
            else:
                address_set.add(addr)
            source_dict[offset] = addr
            logging.debug("ask %s to replicate partition %u up to %s from %r",
                uuid_str(node.getUUID()), offset,  dump(tid), addr)
        node.getConnection().notify(Packets.Replicate(
            tid, self.name, source_dict))

    def notifyReplicationDone(self, node, offset, tid):
        app = self.app
        cell = app.pt.getCell(offset, node.getUUID())
        tid_list = self.tid_list[offset]
        if tid_list: # may be empty if the cell is out-of-date
                     # or if we're not fully initialized
            if tid < tid_list[0]:
                cell.replicating = tid
            else:
                try:
                    tid = add64(tid_list[bisect(tid_list, tid)], -1)
                except IndexError:
                    last_tid = app.getLastTransaction()
                    if tid < last_tid:
                        tid = last_tid
                        node.notify(Packets.Replicate(tid, '', {offset: None}))
        logging.debug("partition %u: updating backup_tid of %r to %s",
                      offset, cell, dump(tid))
        cell.backup_tid = tid
        # Forget tids we won't need anymore.
        cell_list = app.pt.getCellList(offset, readable=True)
        del tid_list[:bisect(tid_list, min(x.backup_tid for x in cell_list))]
        primary_node = self.primary_partition_dict.get(offset)
        primary = primary_node is node
        result = None if primary else app.pt.setUpToDate(node, offset)
        assert cell.isReadable()
        if result: # was out-of-date
            if primary_node is not None:
                max_tid, = [x.backup_tid for x in cell_list
                                         if x.getNode() is primary_node]
                if tid < max_tid:
                    cell.replicating = max_tid
                    logging.debug(
                        "ask %s to replicate partition %u up to %s from %s",
                        uuid_str(node.getUUID()), offset,  dump(max_tid),
                        uuid_str(primary_node.getUUID()))
                    node.notify(Packets.Replicate(max_tid, '',
                        {offset: primary_node.getAddress()}))
        else:
            if app.getClusterState() == ClusterStates.BACKINGUP:
                self.triggerBackup(node)
            if primary:
                # Notify secondary storages that they can replicate from
                # primary ones, even if they are already replicating.
                p = Packets.Replicate(tid, '', {offset: node.getAddress()})
                for cell in cell_list:
                    if max(cell.backup_tid, cell.replicating) < tid:
                        cell.replicating = tid
                        logging.debug(
                            "ask %s to replicate partition %u up to %s from %s",
                            uuid_str(cell.getUUID()), offset,
                            dump(tid), uuid_str(node.getUUID()))
                        cell.getNode().notify(p)
        return result
