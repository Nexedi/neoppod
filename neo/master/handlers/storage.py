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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import logging

from neo import protocol
from neo.protocol import CLIENT_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, \
        UP_TO_DATE_STATE, FEEDING_STATE, DISCARDED_STATE, \
        STORAGE_NODE_TYPE, ADMIN_NODE_TYPE, OUT_OF_DATE_STATE, \
        HIDDEN_STATE, INTERNAL_ERROR_CODE
from neo.master.handlers import BaseServiceHandler
from neo.protocol import UnexpectedPacketError
from neo.exception import OperationFailure
from neo.util import dump


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() == RUNNING_STATE:
            conn.notify(protocol.startOperation())

    def _nodeLost(self, conn, node):
        pt = self.app.pt
        # TODO: check this, is it need ? do we have to broadcast changes ?
        pt.outdate()
        if not pt.operational():
            raise OperationFailure, 'cannot continue operation'

    def handleNotifyInformationLocked(self, conn, packet, tid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)

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
                        node = app.nm.getNodeByUUID(uuid)
                        if node.getNodeType() == CLIENT_NODE_TYPE:
                            if c is t.getConnection():
                                p = protocol.notifyTransactionFinished(tid)
                                c.notify(p, t.getMessageId())
                            else:
                                p = protocol.invalidateObjects(t.getOIDList(), tid)
                                c.notify(p)
                        elif node.getNodeType() == STORAGE_NODE_TYPE:
                            if uuid in t.getUUIDSet():
                                p = protocol.unlockInformation(tid)
                                c.notify(p)
                del app.finishing_transaction_dict[tid]
        except KeyError:
            # What is this?
            pass

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        app = self.app
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.pt.getID() < lptid:
            logging.critical('got later information in service')
            raise OperationFailure

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        # This should be sent when a cell becomes up-to-date because
        # a replication has finished.
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)

        new_cell_list = []
        for cell in cell_list:
            if cell[2] != UP_TO_DATE_STATE:
                logging.warn('only up-to-date state should be sent')
                continue

            if uuid != cell[1]:
                logging.warn('only a cell itself should send this packet')
                continue

            offset = cell[0]
            logging.debug("node %s is up for offset %s" %(dump(node.getUUID()), offset))

            # check the storage said it is up to date for a partition it was assigne to
            for xcell in app.pt.getCellList(offset):
                if xcell.getNode().getUUID() == node.getUUID() and \
                       xcell.getState() not in (OUT_OF_DATE_STATE, UP_TO_DATE_STATE):
                    msg = "node %s telling that it is UP TO DATE for offset \
                    %s but where %s for that offset" %(dump(node.getUUID()), offset, xcell.getState())
                    logging.warning(msg)
                    self.handleError(conn, packet, INTERNAL_ERROR_CODE, msg)
                    return
                    

            app.pt.setCell(offset, node, UP_TO_DATE_STATE)
            new_cell_list.append(cell)

            # If the partition contains a feeding cell, drop it now.
            for feeding_cell in app.pt.getCellList(offset):
                if feeding_cell.getState() == FEEDING_STATE:
                    app.pt.removeCell(offset, feeding_cell.getNode())
                    new_cell_list.append((offset, feeding_cell.getUUID(), 
                                          DISCARDED_STATE))
                    break

        if new_cell_list:
            ptid = app.pt.setNextID()
            app.broadcastPartitionChanges(ptid, new_cell_list)


