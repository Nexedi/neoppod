#
# Copyright (C) 2006-2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from neo import logging

from neo.storage.handlers import BaseMasterHandler
from neo import protocol
from neo.util import dump
from neo.exception import OperationFailure

class VerificationHandler(BaseMasterHandler):
    """This class deals with events for a verification phase."""

    def handleAskLastIDs(self, conn, packet):
        app = self.app
        oid = app.dm.getLastOID()
        tid = app.dm.getLastTID()
        p = protocol.answerLastIDs(oid, tid, app.ptid)
        conn.answer(p, packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        app, pt = self.app, self.app.pt
        if not offset_list:
            # all is requested
            offset_list = range(0, pt.getPartitions())
        row_list = []
        try:
            for offset in offset_list:
                row = []
                try:
                    for cell in app.pt.getCellList(offset):
                        row.append((cell.getUUID(), cell.getState()))
                except TypeError:
                    pass
                row_list.append((offset, row))
        except IndexError:
            raise protocol.ProtocolError('invalid partition table offset')

        p = protocol.answerPartitionTable(app.ptid, row_list)
        conn.answer(p, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        app = self.app
        if app.ptid >= ptid:
            # Ignore this packet.
            logging.debug('ignoring older partition changes')
            return
        app.ptid = ptid
        # update partition table in memory and the database
        app.pt.update(cell_list, app.nm)
        app.dm.changePartitionTable(ptid, cell_list)

    def handleStartOperation(self, conn, packet):
        self.app.operational = True

    def handleStopOperation(self, conn, packet):
        raise OperationFailure('operation stopped')

    def handleAskUnfinishedTransactions(self, conn, packet):
        tid_list = self.app.dm.getUnfinishedTIDList()
        p = protocol.answerUnfinishedTransactions(tid_list)
        conn.answer(p, packet)

    def handleAskTransactionInformation(self, conn, packet, tid):
        app = self.app
        t = app.dm.getTransaction(tid, all=True)
        if t is None:
            p = protocol.tidNotFound('%s does not exist' % dump(tid))
        else:
            p = protocol.answerTransactionInformation(tid, t[1], t[2], t[3], t[0])
        conn.answer(p, packet)

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        if self.app.dm.objectPresent(oid, tid):
            p = protocol.answerObjectPresent(oid, tid)
        else:
            p = protocol.oidNotFound(
                          '%s:%s do not exist' % (dump(oid), dump(tid)))
        conn.answer(p, packet)

    def handleDeleteTransaction(self, conn, packet, tid):
        self.app.dm.deleteTransaction(tid, all = True)

    def handleCommitTransaction(self, conn, packet, tid):
        self.app.dm.finishTransaction(tid)

