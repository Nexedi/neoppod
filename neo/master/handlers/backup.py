#
# Copyright (C) 2012-2019  Nexedi SA
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

from ..app import StateChangedException
from neo.lib import logging
from neo.lib.exception import PrimaryFailure
from neo.lib.handler import EventHandler
from neo.lib.protocol import ClusterStates, NodeTypes, NodeStates, Packets
from neo.lib.pt import PartitionTable

class BackupHandler(EventHandler):
    """Handler dedicated to upstream master during BACKINGUP state"""

    def connectionLost(self, conn, new_state):
        if self.app.app.listening_conn: # if running
            raise PrimaryFailure('connection lost')

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        app = self.app
        pt = app.pt = object.__new__(PartitionTable)
        pt.load(ptid, num_replicas, row_list, self.app.nm)
        if pt.getPartitions() != app.app.pt.getPartitions():
            raise RuntimeError("inconsistent number of partitions")

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        self.app.pt.update(ptid, num_replicas, cell_list, self.app.nm)

    def notifyNodeInformation(self, conn, timestamp, node_list):
        super(BackupHandler, self).notifyNodeInformation(
            conn, timestamp, node_list)
        for node_type, addr, _, state, _ in node_list:
            if node_type == NodeTypes.ADMIN and state == NodeStates.RUNNING:
                self.app.notifyUpstreamAdmin(addr)

    def answerLastTransaction(self, conn, tid):
        app = self.app
        prev_tid = app.app.getLastTransaction()
        if prev_tid <= tid:
            # Since we don't know which partitions were modified during our
            # absence, we must force replication on all storages. As long as
            # they haven't done this first check, our backup tid will remain
            # inferior to this 'tid'. We don't know the real prev_tid, which is:
            #   >= app.app.getLastTransaction()
            #   < tid
            # but passing 'tid' is good enough.
            # A special case is when prev_tid == tid: even in this case, we
            # must restore the state of the backup app so that any interrupted
            # replication (internal or not) is resumed, otherwise the global
            # backup_tid could remain stuck to an old tid if upstream is idle.
            app.invalidatePartitions(tid, tid, xrange(app.pt.getPartitions()))
        else:
            logging.critical("Upstream DB truncated. Leaving backup mode"
                " in case this backup DB needs to be truncated.")
            raise StateChangedException(ClusterStates.STOPPING_BACKUP)
        app.ignore_invalidations = False

    def invalidatePartitions(self, conn, tid, partition_list):
        app = self.app
        if app.ignore_invalidations:
            return
        partition_set = set(partition_list)
        partition_set.add(app.app.pt.getPartition(tid))
        prev_tid = app.app.getLastTransaction()
        app.invalidatePartitions(tid, prev_tid, partition_set)

    # The following 2 methods:
    # - keep the PackManager up-to-date;
    # - replicate the status of pack orders when they're known after the
    #   storage nodes have fetched related transactions.

    def notifyPackSigned(self, conn, approved, rejected):
        backup_app = self.app
        if backup_app.ignore_pack_notifications:
            return
        app = backup_app.app
        packs = app.pm.packs
        ask_tid = min_tid = None
        for approved, tid in (True, approved), (False, rejected):
            for tid in tid:
                try:
                    packs[tid].approved = approved
                except KeyError:
                    if not ask_tid or tid < ask_tid:
                        ask_tid = tid
                else:
                    if not min_tid or tid < min_tid:
                        min_tid = tid
        if ask_tid:
            if min_tid is None:
                min_tid = ask_tid
            else:
                assert min_tid < ask_tid, (min_tid, ask_tid)
            conn.ask(Packets.AskPackOrders(ask_tid), min_tid=min_tid)
        elif min_tid:
            backup_app.broadcastApprovedRejected(min_tid)

    def answerPackOrders(self, conn, pack_list, min_tid):
        backup_app = self.app
        app = backup_app.app
        add = app.pm.add
        for pack_order in pack_list:
            add(*pack_order)
        backup_app.broadcastApprovedRejected(min_tid)
        backup_app.ignore_pack_notifications = False

    ###
