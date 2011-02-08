#
# Copyright (C) 2006-2010  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import neo.lib
from neo.lib.util import dump
from neo.lib.protocol import CellStates, Packets, ProtocolError
from neo.storage.handlers import BaseMasterHandler


class MasterOperationHandler(BaseMasterHandler):
    """ This handler is used for the primary master """

    def answerLastIDs(self, conn, loid, ltid, lptid):
        self.app.replicator.setCriticalTID(ltid)

    def answerUnfinishedTransactions(self, conn, max_tid, ttid_list):
        self.app.replicator.setUnfinishedTIDList(max_tid, ttid_list)

    def notifyTransactionFinished(self, conn, ttid, max_tid):
        self.app.replicator.transactionFinished(ttid, max_tid)

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
       the information is only about changes from the previous."""
        app = self.app
        if ptid <= app.pt.getID():
            # Ignore this packet.
            neo.lib.logging.debug('ignoring older partition changes')
            return

        # update partition table in memory and the database
        app.pt.update(ptid, cell_list, app.nm)
        app.dm.changePartitionTable(ptid, cell_list)

        # Check changes for replications
        if app.replicator is not None:
            for offset, uuid, state in cell_list:
                if uuid == app.uuid:
                    # If this is for myself, this can affect replications.
                    if state == CellStates.DISCARDED:
                        app.replicator.removePartition(offset)
                    elif state == CellStates.OUT_OF_DATE:
                        app.replicator.addPartition(offset)

    def askLockInformation(self, conn, ttid, tid, oid_list):
        if not ttid in self.app.tm:
            raise ProtocolError('Unknown transaction')
        self.app.tm.lock(ttid, tid, oid_list)
        if not conn.isClosed():
            conn.answer(Packets.AnswerInformationLocked(ttid))

    def notifyUnlockInformation(self, conn, ttid):
        if not ttid in self.app.tm:
            raise ProtocolError('Unknown transaction')
        # TODO: send an answer
        self.app.tm.unlock(ttid)

    def askPack(self, conn, tid):
        app = self.app
        neo.lib.logging.info('Pack started, up to %s...', dump(tid))
        app.dm.pack(tid, app.tm.updateObjectDataForPack)
        neo.lib.logging.info('Pack finished.')
        if not conn.isClosed():
            conn.answer(Packets.AnswerPack(True))

