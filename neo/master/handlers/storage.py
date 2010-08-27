#
# Copyright (C) 2006-2010  Nexedi SA

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

from neo.protocol import ProtocolError
from neo.protocol import CellStates, Packets
from neo.master.handlers import BaseServiceHandler
from neo.exception import OperationFailure
from neo.util import dump


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn):
        node = self.app.nm.getByUUID(conn.getUUID())
        if node.isRunning():
            conn.notify(Packets.StartOperation())

    def nodeLost(self, conn, node):
        logging.info('storage node lost')
        assert not node.isRunning(), node.getState()

        if not self.app.pt.operational():
            raise OperationFailure, 'cannot continue operation'
        # this is intentionaly placed after the raise because the last cell in a
        # partition must not oudated to allows a cluster restart.
        self.app.outdateAndBroadcastPartition()
        uuid = conn.getUUID()
        for tid, transaction in self.app.tm.items():
            # if a transaction is known, this means that it's being committed
            if transaction.forget(uuid):
                self._afterLock(tid)

    def askLastIDs(self, conn):
        app = self.app
        loid = app.tm.getLastOID()
        ltid = app.tm.getLastTID()
        conn.answer(Packets.AnswerLastIDs(loid, ltid, app.pt.getID()))

    def askUnfinishedTransactions(self, conn):
        p = Packets.AnswerUnfinishedTransactions(self.app.tm.getPendingList())
        conn.answer(p)

    def answerInformationLocked(self, conn, tid):
        uuid = conn.getUUID()
        app = self.app
        tm = app.tm

        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if tid > tm.getLastTID():
            raise ProtocolError('TID too big')

        # transaction locked on this storage node
        if tm.lock(tid, uuid):
            self._afterLock(tid)

    def _afterLock(self, tid):
        # I have received all the lock answers now:
        # - send a Notify Transaction Finished to the initiated client node
        # - Invalidate Objects to the other client nodes
        app = self.app
        tm = app.tm
        t = tm[tid]
        nm = app.nm
        transaction_node = t.getNode()
        invalidate_objects = Packets.InvalidateObjects(tid, t.getOIDList())
        answer_transaction_finished = Packets.AnswerTransactionFinished(tid)
        for client_node in nm.getClientList(only_identified=True):
            c = client_node.getConnection()
            if client_node is transaction_node:
                c.answer(answer_transaction_finished, msg_id=t.getMessageId())
            else:
                c.notify(invalidate_objects)

        # - Unlock Information to relevant storage nodes.
        notify_unlock = Packets.NotifyUnlockInformation(tid)
        for storage_uuid in t.getUUIDList():
            nm.getByUUID(storage_uuid).getConnection().notify(notify_unlock)

        # remove transaction from manager
        tm.remove(tid)

    def notifyReplicationDone(self, conn, offset):
        uuid = conn.getUUID()
        node = self.app.nm.getByUUID(uuid)
        logging.debug("node %s is up for offset %s" % (dump(uuid), offset))

        # check the partition is assigned and known as outdated
        for cell in self.app.pt.getCellList(offset):
            if cell.getUUID() == uuid:
                if not cell.isOutOfDate():
                    raise ProtocolError("Non-oudated partition")
                break
        else:
            raise ProtocolError("Non-assigned partition")

        # update the partition table
        self.app.pt.setCell(offset, node, CellStates.UP_TO_DATE)
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE)]

        # If the partition contains a feeding cell, drop it now.
        for feeding_cell in self.app.pt.getCellList(offset):
            if feeding_cell.isFeeding():
                self.app.pt.removeCell(offset, feeding_cell.getNode())
                cell = (offset, feeding_cell.getUUID(), CellStates.DISCARDED)
                cell_list.append(cell)
                break
        self.app.broadcastPartitionChanges(cell_list)


