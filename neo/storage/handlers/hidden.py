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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging

from neo.storage.handlers import BaseMasterHandler
from neo.protocol import NodeTypes, NodeStates, CellStates

class HiddenHandler(BaseMasterHandler):
    """This class implements a generic part of the event handlers."""

    def __init__(self, app):
        self.app = app
        BaseMasterHandler.__init__(self, app)

    def notifyNodeInformation(self, conn, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        app = self.app
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if node_type == NodeTypes.STORAGE:
                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    if state in (NodeStates.DOWN, NodeStates.TEMPORARILY_DOWN,
                            NodeStates.BROKEN):
                        conn.close()
                        erase_db = state == NodeStates.DOWN
                        self.app.shutdown(erase=erase_db)

    def requestIdentification(self, conn, node_type,
                                        uuid, address, name):
        pass

    def acceptIdentification(self, conn, node_type,
                   uuid, num_partitions, num_replicas, your_uuid):
        pass

    def answerPrimary(self, conn, primary_uuid,
                                  known_master_list):
        pass

    def askLastIDs(self, conn):
        pass

    def askPartitionTable(self, conn, offset_list):
        pass

    def sendPartitionTable(self, conn, ptid, row_list):
        pass

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

        # Check changes for replications
        for offset, uuid, state in cell_list:
            if uuid == app.uuid and app.replicator is not None:
                # If this is for myself, this can affect replications.
                if state == CellStates.DISCARDED:
                    app.replicator.removePartition(offset)
                elif state == CellStates.OUT_OF_DATE:
                    app.replicator.addPartition(offset)

    def startOperation(self, conn):
        self.app.operational = True

    def stopOperation(self, conn):
        pass

    def askUnfinishedTransactions(self, conn):
        pass

    def askTransactionInformation(self, conn, tid):
        pass

    def askObjectPresent(self, conn, oid, tid):
        pass

    def deleteTransaction(self, conn, tid):
        pass

    def commitTransaction(self, conn, tid):
        pass

    def lockInformation(self, conn, tid):
        pass

    def notifyUnlockInformation(self, conn, tid):
        pass

    def askObject(self, conn, oid, serial, tid):
        pass

    def askTIDs(self, conn, first, last, partition):
        pass

    def askObjectHistory(self, conn, oid, first, last):
        pass

    def askStoreTransaction(self, conn, tid, user, desc,
                                  ext, oid_list):
        pass

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, tid):
        pass

    def abortTransaction(self, conn, tid):
        logging.debug('ignoring abort transaction')

    def answerLastIDs(self, conn, loid, ltid, lptid):
        logging.debug('ignoring answer last ids')

    def answerUnfinishedTransactions(self, conn, tid_list):
        logging.debug('ignoring answer unfinished transactions')

    def askOIDs(self, conn, first, last, partition):
        logging.debug('ignoring ask oids')

