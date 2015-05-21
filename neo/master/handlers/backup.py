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
        self.app.pt.update(ptid, cell_list, self.app.nm)

    def answerNodeInformation(self, conn):
        pass

    def notifyNodeInformation(self, conn, node_list):
        self.app.nm.update(node_list)

    def answerLastTransaction(self, conn, tid):
        app = self.app
        if tid != ZERO_TID:
            app.invalidatePartitions(tid, set(xrange(app.pt.getPartitions())))
        else: # upstream DB is empty
            assert app.app.getLastTransaction() == tid

    def invalidateObjects(self, conn, tid, oid_list):
        app = self.app
        getPartition = app.app.pt.getPartition
        partition_set = set(map(getPartition, oid_list))
        partition_set.add(getPartition(tid))
        app.invalidatePartitions(tid, partition_set)
