#
# Copyright (C) 2006-2017  Nexedi SA
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

    def sendPartitionTable(self, conn, ptid, row_list):
        app = self.app
        pt = app.pt
        pt.load(ptid, row_list, app.nm)
        if not pt.filled():
            raise ProtocolError('Partial partition table received')
        # Install the partition table into the database for persistence.
        cell_list = []
        offset_list = xrange(pt.getPartitions())
        unassigned_set = set(offset_list)
        for offset in offset_list:
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
                if cell.getUUID() == app.uuid:
                    unassigned_set.remove(offset)
        # delete objects database
        dm = app.dm
        if unassigned_set:
          if app.disable_drop_partitions:
            logging.info("don't drop data for partitions %r", unassigned_set)
          else:
            logging.debug('drop data for partitions %r', unassigned_set)
            dm.dropPartitions(unassigned_set)

        dm.changePartitionTable(ptid, cell_list, reset=True)
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
        conn.answer(Packets.AnswerPartitionTable(pt.getID(), pt.getRowList()))

    def askLockedTransactions(self, conn):
        conn.answer(Packets.AnswerLockedTransactions(
            self.app.dm.getUnfinishedTIDDict()))

    def validateTransaction(self, conn, ttid, tid):
        dm = self.app.dm
        dm.lockTransaction(tid, ttid)
        dm.unlockTransaction(tid, ttid)
        dm.commit()

    def startOperation(self, conn, backup):
        # XXX: see comment in protocol
        self.app.operational = True
        self.app.replicator.startOperation(backup)
