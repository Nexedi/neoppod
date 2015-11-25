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

from . import BaseMasterHandler
from neo.lib import logging
from neo.lib.protocol import Packets, ZERO_TID
from neo.lib.exception import OperationFailure

class VerificationHandler(BaseMasterHandler):
    """This class deals with events for a verification phase."""

    def askLastIDs(self, conn):
        app = self.app
        ltid, _, _, loid = app.dm.getLastIDs()
        conn.answer(Packets.AnswerLastIDs(
            loid,
            ltid,
            app.pt.getID(),
            app.dm.getBackupTID()))

    def askPartitionTable(self, conn):
        pt = self.app.pt
        conn.answer(Packets.AnswerPartitionTable(pt.getID(), pt.getRowList()))

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        app = self.app
        if ptid <= app.pt.getID():
            # Ignore this packet.
            logging.debug('ignoring older partition changes')
            return
        # update partition table in memory and the database
        app.pt.update(ptid, cell_list, app.nm)
        app.dm.changePartitionTable(ptid, cell_list)

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
        dm.setBackupTID(tid)

    def stopOperation(self, conn):
        raise OperationFailure('operation stopped')

    def askLockedTransactions(self, conn):
        conn.answer(Packets.AnswerLockedTransactions(
            self.app.dm.getUnfinishedTIDDict()))

    def askFinalTID(self, conn, ttid):
        conn.answer(Packets.AnswerFinalTID(self.app.dm.getFinalTID(ttid)))

    def validateTransaction(self, conn, ttid, tid):
        dm = self.app.dm
        dm.lockTransaction(tid, ttid)
        dm.unlockTransaction(tid, ttid)
