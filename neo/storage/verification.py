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

from neo.storage.handler import StorageEventHandler
from neo.protocol import INVALID_OID, INVALID_TID, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        Packet
from neo.util import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection
from neo.exception import PrimaryFailure, OperationFailure

class VerificationEventHandler(StorageEventHandler):
    """This class deals with events for a verification phase."""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # I do not want to accept a connection at this phase, but
        # someone might mistake me as a master node.
        StorageEventHandler.connectionAccepted(self, conn, s, addr)

    def timeoutExpired(self, conn):
        if not conn.isListeningConnection():
            # If a primary master node timeouts, I cannot continue.
            logging.critical('the primary master node times out')
            raise PrimaryFailure('the primary master node times out')

        StorageEventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        if not conn.isListeningConnection():
            # If a primary master node closes, I cannot continue.
            logging.critical('the primary master node is dead')
            raise PrimaryFailure('the primary master node is dead')

        StorageEventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        if not conn.isListeningConnection():
            # If a primary master node gets broken, I cannot continue.
            logging.critical('the primary master node is broken')
            raise PrimaryFailure('the primary master node is broken')

        StorageEventHandler.peerBroken(self, conn)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        if not conn.isListeningConnection():
            self.handleUnexpectedPacket(conn, packet)
        else:
            app = self.app
            if node_type != MASTER_NODE_TYPE:
                logging.info('reject a connection from a non-master')
                conn.addPacket(Packet().notReady(packet.getId(), 'retry later'))
                conn.abort()
                return
            if name != app.name:
                logging.error('reject an alien cluster')
                conn.addPacket(Packet().protocolError(packet.getId(),
                                                      'invalid cluster name'))
                conn.abort()
                return

            addr = (ip_address, port)
            node = app.nm.getNodeByServer(addr)
            if node is None:
                node = MasterNode(server = addr, uuid = uuid)
                app.nm.add(node)
            else:
                # If this node is broken, reject it.
                if node.getUUID() == uuid:
                    if node.getState() == BROKEN_STATE:
                        p = Packet()
                        p.brokenNodeDisallowedError(packet.getId(), 'go away')
                        conn.addPacket(p)
                        conn.abort()
                        return

            # Trust the UUID sent by the peer.
            node.setUUID(uuid)
            conn.setUUID(uuid)

            p = Packet()
            p.acceptNodeIdentification(packet.getId(), STORAGE_NODE_TYPE,
                                       app.uuid, app.server[0], app.server[1],
                                       uuid)
            conn.addPacket(p)

            # Now the master node should know that I am not the right one.
            conn.abort()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        if not conn.isListeningConnection():
            app = self.app
            if app.primary_master_node.getUUID() != primary_uuid:
                raise PrimaryFailure('the primary master node seems to have changed')
            # XXX is it better to deal with known_master_list here?
            # But a primary master node is supposed not to send any info
            # with this packet, so it would be useless.
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskLastIDs(self, conn, packet):
        if not conn.isListeningConnection():
            app = self.app
            p = Packet()
            oid = app.dm.getLastOID() or INVALID_OID
            tid = app.dm.getLastTID() or INVALID_TID
            p.answerLastIDs(packet.getId(), oid, tid, app.ptid)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        if not conn.isListeningConnection():
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
                p = Packet()
                p.protocolError(packet.getId(), 
                                'invalid partition table offset')
                conn.addPacket(p)
                return

            p = Packet()
            p.answerPartitionTable(packet.getId(), app.ptid, row_list)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        """A primary master node sends this packet to synchronize a partition
        table. Note that the message can be split into multiple packets."""
        if not conn.isListeningConnection():
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
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        if not conn.isListeningConnection():
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
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStartOperation(self, conn, packet):
        if not conn.isListeningConnection():
            self.app.operational = True
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        if not conn.isListeningConnection():
            raise OperationFailure('operation stopped')
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        if not conn.isListeningConnection():
            app = self.app
            tid_list = app.dm.getUnfinishedTIDList()
            p = Packet()
            p.answerUnfinishedTransactions(packet.getId(), tid_list)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskTransactionInformation(self, conn, packet, tid):
        app = self.app
        if not conn.isListeningConnection():
            # If this is from a primary master node, assume that it wants
            # to obtain information about the transaction, even if it has
            # not been finished.
            t = app.dm.getTransaction(tid, all = True)
        else:
            t = app.dm.getTransaction(tid)

        p = Packet()
        if t is None:
            p.tidNotFound(packet.getId(), '%s does not exist' % dump(tid))
        else:
            p.answerTransactionInformation(packet.getId(), tid, 
                                           t[1], t[2], t[3], t[0])
        conn.addPacket(p)

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        if not conn.isListeningConnection():
            app = self.app
            p = Packet()
            if app.dm.objectPresent(oid, tid):
                p.answerObjectPresent(packet.getId(), oid, tid)
            else:
                p.oidNotFound(packet.getId(), 
                              '%s:%s do not exist' % (dump(oid), dump(tid)))
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleDeleteTransaction(self, conn, packet, tid):
        if not conn.isListeningConnection():
            app = self.app
            app.dm.deleteTransaction(tid, all = True)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleCommitTransaction(self, conn, packet, tid):
        if not conn.isListeningConnection():
            app = self.app
            app.dm.finishTransaction(tid)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleLockInformation(self, conn, packet, tid):
        pass

    def handleUnlockInformation(self, conn, packet, tid):
        pass
