#
# Copyright (C) 2006-2009  Nexedi SA

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

from neo import protocol
from neo.protocol import UnexpectedPacketError, ProtocolError
from neo.protocol import CellStates, ErrorCodes, Packets
from neo.master.handlers import BaseServiceHandler
from neo.exception import OperationFailure
from neo.util import dump


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn):
        node = self.app.nm.getByUUID(conn.getUUID())
        if node.isRunning():
            conn.notify(Packets.NotifyLastOID(self.app.loid))
            conn.notify(Packets.StartOperation())

    def nodeLost(self, conn, node):
        logging.info('storage node lost')
        if not self.app.pt.operational():
            raise OperationFailure, 'cannot continue operation'
        # this is intentionaly placed after the raise because the last cell in a
        # partition must not oudated to allows a cluster restart.
        self.app.outdateAndBroadcastPartition()

    def askLastIDs(self, conn, packet):
        app = self.app
        conn.answer(Packets.AnswerLastIDs(app.loid, app.ltid, app.pt.getID()),
                packet.getId())

    def askUnfinishedTransactions(self, conn, packet):
        app = self.app
        p = Packets.AnswerUnfinishedTransactions(
                app.finishing_transaction_dict.keys())
        conn.answer(p, packet.getId())

    def notifyInformationLocked(self, conn, packet, tid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getByUUID(uuid)

        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            raise UnexpectedPacketError

        try:
            t = app.finishing_transaction_dict[tid]
            t.addLockedUUID(uuid)
            if t.allLocked():
                # I have received all the answers now. So send a Notify
                # Transaction Finished to the initiated client node,
                # Invalidate Objects to the other client nodes, and Unlock
                # Information to relevant storage nodes.
                for c in app.em.getConnectionList():
                    uuid = c.getUUID()
                    if uuid is not None:
                        node = app.nm.getByUUID(uuid)
                        if node.isClient():
                            if c is t.getConnection():
                                p = Packets.NotifyTransactionFinished(tid)
                                c.answer(p, t.getMessageId())
                            else:
                                p = Packets.InvalidateObjects(t.getOIDList(),
                                        tid)
                                c.notify(p)
                        elif node.isStorage():
                            if uuid in t.getUUIDSet():
                                p = Packets.UnlockInformation(tid)
                                c.notify(p)
                del app.finishing_transaction_dict[tid]
        except KeyError:
            # What is this?
            pass

    def notifyPartitionChanges(self, conn, packet, ptid, cell_list):
        # This should be sent when a cell becomes up-to-date because
        # a replication has finished.
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getByUUID(uuid)

        new_cell_list = []
        for cell in cell_list:
            if cell[2] != CellStates.UP_TO_DATE:
                logging.warn('only up-to-date state should be sent')
                continue

            if uuid != cell[1]:
                logging.warn('only a cell itself should send this packet')
                continue

            offset = cell[0]
            logging.debug("node %s is up for offset %s" %
                    (dump(node.getUUID()), offset))

            # check the storage said it is up to date for a partition it was
            # assigne to
            for xcell in app.pt.getCellList(offset):
                if xcell.getNode().getUUID() == node.getUUID() and \
                       xcell.getState() not in (CellStates.OUT_OF_DATE,
                               CellStates.UP_TO_DATE):
                    msg = "node %s telling that it is UP TO DATE for offset \
                    %s but where %s for that offset" % (dump(node.getUUID()),
                            offset, xcell.getState())
                    raise ProtocolError(msg)


            app.pt.setCell(offset, node, CellStates.UP_TO_DATE)
            new_cell_list.append(cell)

            # If the partition contains a feeding cell, drop it now.
            for feeding_cell in app.pt.getCellList(offset):
                if feeding_cell.getState() == CellStates.FEEDING:
                    app.pt.removeCell(offset, feeding_cell.getNode())
                    new_cell_list.append((offset, feeding_cell.getUUID(),
                                          CellStates.DISCARDED))
                    break

        if new_cell_list:
            ptid = app.pt.setNextID()
            app.broadcastPartitionChanges(ptid, new_cell_list)


