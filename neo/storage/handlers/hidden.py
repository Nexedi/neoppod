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

from neo.storage.handlers import BaseMasterHandler
from neo.protocol import BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, \
        DOWN_STATE, TEMPORARILY_DOWN_STATE, HIDDEN_STATE, \
        DISCARDED_STATE, OUT_OF_DATE_STATE, UnexpectedPacketError
from neo.node import StorageNode
from neo import decorators


# FIXME: before move handlers, this one was inheriting from EventHandler
# instead of BaseStorageHandler
class HiddenHandler(BaseMasterHandler):
    """This class implements a generic part of the event handlers."""

    def __init__(self, app):
        self.app = app
        BaseMasterHandler.__init__(self, app)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        app = self.app
        for node_type, addr, uuid, state in node_list:
            if node_type == STORAGE_NODE_TYPE:
                if uuid == None:
                    # No interest.
                    continue

                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, BROKEN_STATE):
                        conn.close()
                        self.app.shutdown()
                    elif state == HIDDEN_STATE:
                        # I know I'm hidden
                        continue
                    else:
                        # I must be working again
                        n = app.nm.getNodeByUUID(uuid)
                        n.setState(state)


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
        nm = app.nm
        pt = app.pt
        if app.ptid >= ptid:
            # Ignore this packet.
            logging.debug('ignoring older partition changes')
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

            if uuid == app.uuid and app.replicator is not None:
                # If this is for myself, this can affect replications.
                if state == DISCARDED_STATE:
                    app.replicator.removePartition(offset)
                elif state == OUT_OF_DATE_STATE:
                    app.replicator.addPartition(offset)

        # Then, the database.
        app.dm.changePartitionTable(ptid, cell_list)
        app.pt.log()

    @decorators.client_connection_required
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
        pass

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        logging.debug('ignoring answer last ids')
        pass

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        logging.debug('ignoring answer unfinished transactions')
        pass

    def handleAskOIDs(self, conn, packet, first, last, partition):
        logging.debug('ignoring ask oids')
        pass

