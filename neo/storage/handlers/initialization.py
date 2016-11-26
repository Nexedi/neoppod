#
# Copyright (C) 2006-2016  Nexedi SA
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
        pt.load(ptid, row_list, self.app.nm)
        if not pt.filled():
            raise ProtocolError('Partial partition table received')
        # Install the partition table into the database for persistence.
        cell_list = []
        num_partitions = pt.getPartitions()
        unassigned_set = set(xrange(num_partitions))
        for offset in xrange(num_partitions):
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
                if cell.getUUID() == app.uuid:
                    unassigned_set.remove(offset)
        # delete objects database
        if unassigned_set:
            logging.debug('drop data for partitions %r', unassigned_set)
            app.dm.dropPartitions(unassigned_set)

        app.dm.changePartitionTable(ptid, cell_list, reset=True)

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
        ltid, _, _, loid = dm.getLastIDs()
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
        self.app.operational = True
        # XXX: see comment in protocol
        dm = self.app.dm
        if backup:
            if dm.getBackupTID():
                return
            tid = dm.getLastIDs()[0] or ZERO_TID
        else:
            tid = None
        dm._setBackupTID(tid)
        dm.commit()
