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

from neo.lib.exception import PrimaryFailure
from neo.lib.handler import EventHandler
from neo.lib.protocol import ZERO_TID

class BackupHandler(EventHandler):
    """Handler dedicated to upstream master during BACKINGUP state"""

    def connectionLost(self, conn, new_state):
        if self.app.app.listening_conn: # if running
            raise PrimaryFailure('connection lost')

    def answerPartitionTable(self, conn, ptid, row_list):
        self.app.pt.load(ptid, row_list, self.app.nm)

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        if self.app.pt.filled():
            self.app.pt.update(ptid, cell_list, self.app.nm)

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
            raise RuntimeError("upstream DB truncated")
        app.ignore_invalidations = False

    def invalidateObjects(self, conn, tid, oid_list):
        app = self.app
        if app.ignore_invalidations:
            return
        getPartition = app.app.pt.getPartition
        partition_set = set(map(getPartition, oid_list))
        partition_set.add(getPartition(tid))
        prev_tid = app.app.getLastTransaction()
        app.invalidatePartitions(tid, prev_tid, partition_set)
