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

from . import BaseMasterHandler
from neo.lib import logging
from neo.lib.protocol import Packets, ProtocolError, ZERO_TID

class InitializationHandler(BaseMasterHandler):

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        app = self.app
        pt = app.pt
        pt.load(ptid, num_replicas, row_list, app.nm)
        if not pt.filled():
            raise ProtocolError('Partial partition table received')
        # Install the partition table into the database for persistence.
        cell_list = []
        unassigned = range(pt.getPartitions())
        for offset in reversed(unassigned):
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
                if cell.getUUID() == app.uuid:
                    unassigned.remove(offset)
        # delete objects database
        dm = app.dm
        if unassigned:
          if app.disable_drop_partitions:
            logging.info('partitions %r are discarded but actual deletion'
                         ' of data is disabled', unassigned)
          else:
            logging.debug('drop data for partitions %r', unassigned)
            dm.dropPartitions(unassigned)

        dm.changePartitionTable(ptid, num_replicas, cell_list, reset=True)
        dm.commit()

    def truncate(self, conn, tid):
        dm = self.app.dm
        dm._setBackupTID(None)
        dm._setTruncateTID(tid)
        dm.commit()

    def askRecovery(self, conn):
        app = self.app
        conn.answer(Packets.AnswerRecovery(
            app.pt.getID(),
            app.dm.getBackupTID(),
            app.dm.getTruncateTID()))

    def askLastIDs(self, conn):
        dm = self.app.dm
        dm.truncate()
        ltid, loid = dm.getLastIDs()
        conn.answer(Packets.AnswerLastIDs(loid, ltid))

    def askPartitionTable(self, conn):
        pt = self.app.pt
        conn.answer(Packets.AnswerPartitionTable(
            pt.getID(), pt.getReplicas(), pt.getRowList()))

    def askLockedTransactions(self, conn):
        conn.answer(Packets.AnswerLockedTransactions(
            self.app.dm.getUnfinishedTIDDict()))

    def validateTransaction(self, conn, ttid, tid):
        dm = self.app.dm
        dm.lockTransaction(tid, ttid)
        dm.unlockTransaction(tid, ttid, True, True)
        dm.commit()

    def startOperation(self, conn, backup):
        # XXX: see comment in protocol
        self.app.operational = True
        self.app.replicator.startOperation(backup)
