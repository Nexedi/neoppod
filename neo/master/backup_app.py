##############################################################################
#
# Copyright (c) 2011 Nexedi SARL and Contributors. All Rights Reserved.
#                    Julien Muchembled <jm@nexedi.com>
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsibility of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# guarantees and support are strongly advised to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
##############################################################################

import random, weakref
from bisect import bisect
import neo.lib
from neo.lib.bootstrap import BootstrapManager
from neo.lib.connector import getConnectorHandler
from neo.lib.exception import PrimaryFailure
from neo.lib.node import NodeManager
from neo.lib.protocol import CellStates, ClusterStates, NodeTypes, Packets
from neo.lib.protocol import INVALID_TID, ZERO_TID
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
switching into RUNNING state, the cluster cuts the DB at the last TID for which
we have all data.

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

    def __init__(self, app, name, master_addresses, connector_name):
        self.app = weakref.proxy(app)
        self.name = name
        self.nm = NodeManager()
        self.connector_handler = getConnectorHandler(connector_name)
        for master_address in master_addresses:
            self.nm.createMaster(address=master_address)

    em = property(lambda self: self.app.em)

    def close(self):
        self.nm.close()
        del self.__dict__

    def log(self):
        self.nm.log()
        if self.pt is not None:
            self.pt.log()

    def provideService(self):
        neo.lib.logging.info('provide backup')
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
                node, conn, uuid, num_partitions, num_replicas = \
                    bootstrap.getPrimaryConnection(self.connector_handler)
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
                    neo.lib.logging.error('upstream master is down: %s', msg)
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
                app.changeClusterState(*e.args)
                last_tid = app.getLastTransaction()
                if last_tid < app.backup_tid:
                    neo.lib.logging.warning(
                        "Truncating at %s (last_tid was %s)",
                        dump(app.backup_tid), dump(last_tid))
                    p = Packets.AskTruncate(app.backup_tid)
                    connection_list = []
                    for node in app.nm.getStorageList(only_identified=True):
                        conn = node.getConnection()
                        conn.ask(p)
                        connection_list.append(conn)
                    for conn in connection_list:
                        while conn.isPending():
                            poll(1)
                    app.setLastTransaction(app.backup_tid)
                del app.backup_tid
                break
            finally:
                del self.primary_partition_dict, self.tid_list

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
                    neo.lib.logging.debug(
                        "ask %s to replicate partition %u up to %s from %r",
                        dump(cell.getUUID()), offset,  dump(tid),
                        dump(primary_node.getUUID()))
                    cell.getNode().getConnection().notify(p)
            trigger_set.add(primary_node)
        for node in trigger_set:
            self.triggerBackup(node)

    def invalidatePartitions(self, tid, partition_set):
        app = self.app
        prev_tid = app.getLastTransaction()
        app.setLastTransaction(tid)
        pt = app.pt
        getByUUID = app.nm.getByUUID
        trigger_set = set()
        for offset in xrange(pt.getPartitions()):
            try:
                last_max_tid = self.tid_list[offset][-1]
            except IndexError:
                last_max_tid = INVALID_TID
            if offset in partition_set:
                self.tid_list[offset].append(tid)
                node_list = []
                for cell in pt.getCellList(offset, readable=True):
                    node = cell.getNode()
                    assert node.isConnected()
                    node_list.append(node)
                    if last_max_tid <= cell.backup_tid:
                        # This is the last time we can increase
                        # 'backup_tid' without replication.
                        neo.lib.logging.debug(
                            "partition %u: updating backup_tid of %r to %s",
                            offset, cell, dump(prev_tid))
                        cell.backup_tid = prev_tid
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
                        neo.lib.logging.debug(
                            "partition %u: updating backup_tid of %r to %s",
                            offset, cell, dump(tid))
        for node in trigger_set:
            self.triggerBackup(node)
        count = sum(map(len, self.tid_list))
        if self.debug_tid_count < count:
            neo.lib.logging.debug("Maximum number of tracked tids: %u", count)
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
        getByUUID = self.nm.getByUUID
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
            neo.lib.logging.debug(
                "ask %s to replicate partition %u up to %s from %r",
                dump(node.getUUID()), offset,  dump(tid), addr)
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
                    tid = app.getLastTransaction()
        neo.lib.logging.debug("partition %u: updating backup_tid of %r to %s",
                              offset, cell, dump(tid))
        cell.backup_tid = tid
        # Forget tids we won't need anymore.
        cell_list = app.pt.getCellList(offset, readable=True)
        del tid_list[:bisect(tid_list, min(x.backup_tid for x in cell_list))]
        primary_node = self.primary_partition_dict.get(offset)
        primary = primary_node is node
        result = None if primary else app.pt.setUpToDate(node, offset)
        if app.getClusterState() == ClusterStates.BACKINGUP:
            assert not cell.isOutOfDate()
            if result: # was out-of-date
                max_tid, = [x.backup_tid for x in cell_list
                                         if x.getNode() is primary_node]
                if tid < max_tid:
                    cell.replicating = max_tid
                    neo.lib.logging.debug(
                        "ask %s to replicate partition %u up to %s from %r",
                        dump(node.getUUID()), offset,  dump(max_tid),
                        dump(primary_node.getUUID()))
                    node.getConnection().notify(Packets.Replicate(max_tid,
                        '', {offset: primary_node.getAddress()}))
            else:
                self.triggerBackup(node)
                if primary:
                    # Notify secondary storages that they can replicate from
                    # primary ones, even if they are already replicating.
                    p = Packets.Replicate(tid, '', {offset: node.getAddress()})
                    for cell in cell_list:
                        if max(cell.backup_tid, cell.replicating) < tid:
                            cell.replicating = tid
                            neo.lib.logging.debug(
                                "ask %s to replicate partition %u up to %s from"
                                " %r", dump(cell.getUUID()), offset,  dump(tid),
                                dump(node.getUUID()))
                            cell.getNode().getConnection().notify(p)
        return result
