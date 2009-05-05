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
from neo.protocol import INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection
from neo.protocol import Packet
from neo.pt import PartitionTable
from neo.storage.verification import VerificationEventHandler

class BootstrapEventHandler(StorageEventHandler):
    """This class deals with events for a bootstrap phase."""

    def connectionCompleted(self, conn):
        app = self.app
        if app.trying_master_node is None:
            # Should not happen.
            raise RuntimeError('connection completed while not trying to connect')

        p = Packet()
        msg_id = conn.getNextId()
        p.requestNodeIdentification(msg_id, STORAGE_NODE_TYPE, app.uuid,
                                    app.server[0], app.server[1], app.name)
        conn.addPacket(p)
        conn.expectMessage(msg_id)
        StorageEventHandler.connectionCompleted(self, conn)

    def connectionFailed(self, conn):
        app = self.app
        if app.trying_master_node is None:
            # Should not happen.
            raise RuntimeError('connection failed while not trying to connect')

        if app.trying_master_node is app.primary_master_node:
            # Tried to connect to a primary master node and failed.
            # So this would effectively mean that it is dead.
            app.primary_master_node = None

        app.trying_master_node = None

        StorageEventHandler.connectionFailed(self, conn)

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # I do not want to accept a connection at this phase, but
        # someone might mistake me as a master node.
        StorageEventHandler.connectionAccepted(self, conn, s, addr)

    def timeoutExpired(self, conn):
        if not conn.isListeningConnection():
            app = self.app
            if app.trying_master_node is app.primary_master_node:
                # If a primary master node timeouts, I should not rely on it.
                app.primary_master_node = None

            app.trying_master_node = None

        StorageEventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        if not conn.isListeningConnection():
            app = self.app
            if app.trying_master_node is app.primary_master_node:
                # If a primary master node closes, I should not rely on it.
                app.primary_master_node = None

            app.trying_master_node = None

        StorageEventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        if not conn.isListeningConnection():
            app = self.app
            if app.trying_master_node is app.primary_master_node:
                # If a primary master node gets broken, I should not rely
                # on it.
                app.primary_master_node = None

            app.trying_master_node = None

        StorageEventHandler.peerBroken(self, conn)

    def handleNotReady(self, conn, packet, message):
        if not conn.isListeningConnection():
            app = self.app
            if app.trying_master_node is not None:
                app.trying_master_node = None

        conn.close()

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
                                       0, 0, uuid)
            conn.addPacket(p)

            # Now the master node should know that I am not the right one.
            conn.abort()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        if conn.isListeningConnection():
            self.handleUnexpectedPacket(conn, packet)
        else:
            app = self.app
            node = app.nm.getNodeByServer(conn.getAddress())
            if node_type != MASTER_NODE_TYPE:
                # The peer is not a master node!
                logging.error('%s:%d is not a master node', ip_address, port)
                app.nm.remove(node)
                conn.close()
                return
            if conn.getAddress() != (ip_address, port):
                # The server address is different! Then why was
                # the connection successful?
                logging.error('%s:%d is waiting for %s:%d',
                              conn.getAddress()[0], conn.getAddress()[1], 
                              ip_address, port)
                app.nm.remove(node)
                conn.close()
                return

            if app.num_partitions is None:
                app.num_partitions = num_partitions
                app.num_replicas = num_replicas
                app.pt = PartitionTable(num_partitions, num_replicas)
                app.loadPartitionTable()
                app.ptid = app.dm.getPTID()
            elif app.num_partitions != num_partitions:
                raise RuntimeError('the number of partitions is inconsistent')
            elif app.num_replicas != num_replicas:
                raise RuntimeError('the number of replicas is inconsistent')

            if your_uuid != INVALID_UUID and app.uuid != your_uuid:
                # got an uuid from the primary master
                app.uuid = your_uuid

            conn.setUUID(uuid)
            node.setUUID(uuid)

            # Ask a primary master.
            msg_id = conn.getNextId()
            conn.addPacket(Packet().askPrimaryMaster(msg_id))
            conn.expectMessage(msg_id)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        if conn.isListeningConnection():
            self.handleUnexpectedPacket(conn, packet)
        else:
            app = self.app
            # Register new master nodes.
            for ip_address, port, uuid in known_master_list:
                addr = (ip_address, port)
                n = app.nm.getNodeByServer(addr)
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)

                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)

            if primary_uuid != INVALID_UUID:
                primary_node = app.nm.getNodeByUUID(primary_uuid)
                if primary_node is None:
                    # I don't know such a node. Probably this information
                    # is old. So ignore it.
                    pass
                else:
                    app.primary_master_node = primary_node
                    if app.trying_master_node is primary_node:
                        # I am connected to the right one.
                        logging.info('connected to a primary master node')
                        # This is a workaround to prevent handling of
                        # packets for the verification phase.
                        handler = VerificationEventHandler(app)
                        conn.setHandler(handler)
                    else:
                        app.trying_master_node = None
                        conn.close()
            else:
                if app.primary_master_node is not None:
                    # The primary master node is not a primary master node
                    # any longer.
                    app.primary_master_node = None

                app.trying_master_node = None
                conn.close()

    def handleAskLastIDs(self, conn, packet):
        logging.info('got ask last ids')
        pass

    def handleAskPartitionTable(self, conn, packet, offset_list):
        pass

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        pass

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        pass

    def handleStartOperation(self, conn, packet):
        pass

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
