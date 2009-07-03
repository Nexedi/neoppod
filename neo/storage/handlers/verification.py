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

from neo.storage.handlers.handler import StorageEventHandler
from neo.protocol import INVALID_OID, INVALID_TID, TEMPORARILY_DOWN_STATE
from neo import protocol
from neo.util import dump
from neo.node import StorageNode
from neo.exception import PrimaryFailure, OperationFailure

class VerificationEventHandler(StorageEventHandler):
    """This class deals with events for a verification phase."""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # I do not want to accept a connection at this phase, but
        # someone might mistake me as a master node.
        StorageEventHandler.connectionAccepted(self, conn, s, addr)

    def timeoutExpired(self, conn):
        # If a primary master node timeouts, I cannot continue.
        logging.critical('the primary master node times out')
        raise PrimaryFailure('the primary master node times out')
        StorageEventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        # If a primary master node closes, I cannot continue.
        logging.critical('the primary master node is dead')
        raise PrimaryFailure('the primary master node is dead')
        StorageEventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        # If a primary master node gets broken, I cannot continue.
        logging.critical('the primary master node is broken')
        raise PrimaryFailure('the primary master node is broken')
        StorageEventHandler.peerBroken(self, conn)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        app = self.app
        if app.primary_master_node.getUUID() != primary_uuid:
            raise PrimaryFailure('the primary master node seems to have changed')
        # XXX is it better to deal with known_master_list here?
        # But a primary master node is supposed not to send any info
        # with this packet, so it would be useless.

    def handleAskLastIDs(self, conn, packet):
        app = self.app
        oid = app.dm.getLastOID() or INVALID_OID
        tid = app.dm.getLastTID() or INVALID_TID
        p = protocol.answerLastIDs(oid, tid, app.ptid)
        conn.answer(p, packet)

    def handleAnswerNodeInformation(self, conn, packet, node_list):
        assert not node_list
        self.app.has_node_information = True

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        assert not row_list
        self.app.has_partition_table = True
        logging.info('Got the partition table :')
        self.app.pt.log()

    def handleAskPartitionTable(self, conn, packet, offset_list):
        app = self.app
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

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        """A primary master node sends this packet to synchronize a partition
        table. Note that the message can be split into multiple packets."""
        app = self.app
        nm = app.nm
        pt = app.pt
        if app.ptid != ptid:
            app.ptid = ptid
            pt.clear()

        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    if uuid != app.uuid:
                        node.setState(TEMPORARILY_DOWN_STATE)
                    nm.add(node)

                pt.setCell(offset, node, state)

        if pt.filled():
            # If the table is filled, I assume that the table is ready
            # to use. Thus install it into the database for persistency.
            cell_list = []
            for offset in xrange(app.num_partitions):
                for cell in pt.getCellList(offset):
                    cell_list.append((offset, cell.getUUID(), 
                                      cell.getState()))
            app.dm.setPartitionTable(ptid, cell_list)

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

        # Then, the database.
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
        if not conn.isServerConnection():
            # If this is from a primary master node, assume that it wants
            # to obtain information about the transaction, even if it has
            # not been finished.
            t = app.dm.getTransaction(tid, all = True)
        else:
            # XXX: this should never be used since we don't accept incoming
            # connections out of the operation state.
            t = app.dm.getTransaction(tid)

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

