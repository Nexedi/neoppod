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
from neo.protocol import BROKEN_STATE, STORAGE_NODE_TYPE, DOWN_STATE, \
        TEMPORARILY_DOWN_STATE, DISCARDED_STATE, OUT_OF_DATE_STATE

class HiddenHandler(BaseMasterHandler):
    """This class implements a generic part of the event handlers."""

    def __init__(self, app):
        self.app = app
        BaseMasterHandler.__init__(self, app)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        app = self.app
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if node_type == STORAGE_NODE_TYPE:
                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, BROKEN_STATE):
                        conn.close()
                        self.app.shutdown()

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        pass

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                   uuid, address, num_partitions, num_replicas, your_uuid):
        pass

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        pass

    def handleAskLastIDs(self, conn, packet):
        pass

    def handleAskPartitionTable(self, conn, packet, offset_list):
        pass

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        pass

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
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
                if state == DISCARDED_STATE:
                    app.replicator.removePartition(offset)
                elif state == OUT_OF_DATE_STATE:
                    app.replicator.addPartition(offset)

    def handleStartOperation(self, conn, packet):
        self.app.operational = True

    def handleStopOperation(self, conn, packet):
        pass

    def handleAskUnfinishedTransactions(self, conn, packet):
        pass

    def handleAskTransactionInformation(self, conn, packet, tid):
        pass

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        pass

    def handleDeleteTransaction(self, conn, packet, tid):
        pass

    def handleCommitTransaction(self, conn, packet, tid):
        pass

    def handleLockInformation(self, conn, packet, tid):
        pass

    def handleUnlockInformation(self, conn, packet, tid):
        pass

    def handleAskObject(self, conn, packet, oid, serial, tid):
        pass

    def handleAskTIDs(self, conn, packet, first, last, partition):
        pass

    def handleAskObjectHistory(self, conn, packet, oid, first, last):
        pass

    def handleAskStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        pass

    def handleAskStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        pass

    def handleAbortTransaction(self, conn, packet, tid):
        logging.debug('ignoring abort transaction')

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        logging.debug('ignoring answer last ids')

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        logging.debug('ignoring answer unfinished transactions')

    def handleAskOIDs(self, conn, packet, first, last, partition):
        logging.debug('ignoring ask oids')

