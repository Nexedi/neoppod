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

from neo import logging

from neo.storage.handlers import BaseMasterHandler
from neo.protocol import Packets, Errors, ProtocolError
from neo.util import dump
from neo.exception import OperationFailure

class VerificationHandler(BaseMasterHandler):
    """This class deals with events for a verification phase."""

    def askLastIDs(self, conn):
        app = self.app
        oid = app.dm.getLastOID()
        tid = app.dm.getLastTID()
        conn.answer(Packets.AnswerLastIDs(oid, tid, app.pt.getID()))

    def askPartitionTable(self, conn, offset_list):
        if not offset_list:
            # all is requested
            offset_list = xrange(0, self.app.pt.getPartitions())
        else:
            if max(offset_list) >= self.app.pt.getPartitions():
                raise ProtocolError('invalid partition table offset')

        # build a table with requested partitions
        row_list = [(offset, [(cell.getUUID(), cell.getState())
            for cell in self.app.pt.getCellList(offset)])
            for offset in offset_list]

        conn.answer(Packets.AnswerPartitionTable(self.app.pt.getID(), row_list))

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

    def startOperation(self, conn):
        self.app.operational = True

    def stopOperation(self, conn):
        raise OperationFailure('operation stopped')

    def askUnfinishedTransactions(self, conn):
        tid_list = self.app.dm.getUnfinishedTIDList()
        conn.answer(Packets.AnswerUnfinishedTransactions(tid_list))

    def askTransactionInformation(self, conn, tid):
        app = self.app
        t = app.dm.getTransaction(tid, all=True)
        if t is None:
            p = Errors.TidNotFound('%s does not exist' % dump(tid))
        else:
            p = Packets.AnswerTransactionInformation(tid, t[1], t[2], t[3],
                    t[0])
        conn.answer(p)

    def askObjectPresent(self, conn, oid, tid):
        if self.app.dm.objectPresent(oid, tid):
            p = Packets.AnswerObjectPresent(oid, tid)
        else:
            p = Errors.OidNotFound(
                          '%s:%s do not exist' % (dump(oid), dump(tid)))
        conn.answer(p)

    def deleteTransaction(self, conn, tid):
        self.app.dm.deleteTransaction(tid, all = True)

    def commitTransaction(self, conn, tid):
        self.app.dm.finishTransaction(tid)

