
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

import logging

from neo import protocol
from neo.storage.handlers.handler import BaseMasterHandler
from neo.protocol import INVALID_SERIAL, INVALID_TID, INVALID_PARTITION, \
        TEMPORARILY_DOWN_STATE, DISCARDED_STATE, OUT_OF_DATE_STATE
from neo.util import dump
from neo.node import StorageNode
from neo.exception import PrimaryFailure, OperationFailure


class MasterOperationHandler(BaseMasterHandler):
    """ This handler is used for the primary master """

    def handleStopOperation(self, conn, packet):
        raise OperationFailure('operation stopped')

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        self.app.replicator.setCriticalTID(packet, ltid)

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        self.app.replicator.setUnfinishedTIDList(tid_list)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        app = self.app
        nm = app.nm
        pt = app.pt
        if app.ptid >= ptid:
            # Ignore this packet.
            logging.info('ignoring older partition changes')
            return

        # First, change the table on memory.
        app.ptid = ptid
        for offset, uuid, state in cell_list:
            node = nm.getNodeByUUID(uuid)
            if node is None:
                node = StorageNode(uuid = uuid)
                if uuid != app.uuid:
                    node.setState(TEMPORARILY_DOWN_STATE)
                nm.add(node)
            pt.setCell(offset, node, state)

            if uuid == app.uuid:
                # If this is for myself, this can affect replications.
                if state == DISCARDED_STATE:
                    app.replicator.removePartition(offset)
                elif state == OUT_OF_DATE_STATE:
                    app.replicator.addPartition(offset)

        # Then, the database.
        app.dm.changePartitionTable(ptid, cell_list)
        logging.info('Partition table updated:')
        self.app.pt.log()

    def handleLockInformation(self, conn, packet, tid):
        app = self.app
        try:
            t = app.transaction_dict[tid]
            object_list = t.getObjectList()
            for o in object_list:
                app.load_lock_dict[o[0]] = tid

            app.dm.storeTransaction(tid, object_list, t.getTransaction())
        except KeyError:
            pass
        conn.answer(protocol.notifyInformationLocked(tid), packet)

    def handleUnlockInformation(self, conn, packet, tid):
        app = self.app
        try:
            t = app.transaction_dict[tid]
            object_list = t.getObjectList()
            for o in object_list:
                oid = o[0]
                del app.load_lock_dict[oid]
                del app.store_lock_dict[oid]

            app.dm.finishTransaction(tid)
            del app.transaction_dict[tid]

            # Now it may be possible to execute some events.
            app.executeQueuedEvents()
        except KeyError:
            pass

