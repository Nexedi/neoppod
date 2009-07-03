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
from neo.protocol import MASTER_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, \
        DOWN_STATE
from neo.master.handler import MasterEventHandler
from neo.exception import ElectionFailure
from neo.protocol import INVALID_UUID
from neo.node import MasterNode

class ElectionEventHandler(MasterEventHandler):
    """This class deals with events for a primary master election."""

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        uuid = conn.getUUID()
        if uuid is None:
            raise protocol.UnexpectedPacketError
        app = self.app
        for node_type, ip_address, port, uuid, state in node_list:
            if node_type != MASTER_NODE_TYPE:
                # No interest.
                continue

            # Register new master nodes.
            addr = (ip_address, port)
            if app.server == addr:
                # This is self.
                continue
            else:
                node = app.nm.getNodeByServer(addr)
                if node is None:
                    node = MasterNode(server = addr)
                    app.nm.add(node)
                    app.unconnected_master_node_set.add(addr)

                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if node.getUUID() is None:
                        node.setUUID(uuid)

                if state in (node.getState(), RUNNING_STATE):
                    # No change. Don't care.
                    continue
                if state == RUNNING_STATE:
                    # No problem.
                    continue

                # Something wrong happened possibly. Cut the connection to
                # this node, if any, and notify the information to others.
                # XXX this can be very slow.
                for c in app.em.getConnectionList():
                    if c.getUUID() == uuid:
                        c.close()
                node.setState(state)

class ClientElectionEventHandler(MasterEventHandler):

    def packetReceived(self, conn, packet):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        if node.getState() != BROKEN_STATE:
            node.setState(RUNNING_STATE)
        MasterEventHandler.packetReceived(self, conn, packet)

    def connectionStarted(self, conn):
        app = self.app
        addr = conn.getAddress()
        app.unconnected_master_node_set.remove(addr)
        app.negotiating_master_node_set.add(addr)
        MasterEventHandler.connectionStarted(self, conn)

    def connectionCompleted(self, conn):
        conn.ask(protocol.askPrimaryMaster())
        MasterEventHandler.connectionCompleted(self, conn)

    def connectionClosed(self, conn):
        self.connectionFailed(conn)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        self.connectionFailed(conn)
        MasterEventHandler.timeoutExpired(self, conn)

    def connectionFailed(self, conn):
        app = self.app
        addr = conn.getAddress()
        app.negotiating_master_node_set.discard(addr)
        node = app.nm.getNodeByServer(addr)
        if node.getState() == RUNNING_STATE:
            app.unconnected_master_node_set.add(addr)
            node.setState(TEMPORARILY_DOWN_STATE)
        elif node.getState() == TEMPORARILY_DOWN_STATE:
            app.unconnected_master_node_set.add(addr)
        MasterEventHandler.connectionFailed(self, conn)

    def peerBroken(self, conn):
        app = self.app
        addr = conn.getAddress()
        node = app.nm.getNodeByServer(addr)
        if node is not None:
            node.setState(DOWN_STATE)
        app.negotiating_master_node_set.discard(addr)
        MasterEventHandler.peerBroken(self, conn)

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port, num_partitions,
                                       num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        if node_type != MASTER_NODE_TYPE:
            # The peer is not a master node!
            logging.error('%s:%d is not a master node', ip_address, port)
            app.nm.remove(node)
            app.negotiating_master_node_set.discard(node.getServer())
            conn.close()
            return
        if conn.getAddress() != (ip_address, port):
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d', 
                          conn.getAddress()[0], conn.getAddress()[1], ip_address, port)
            app.nm.remove(node)
            app.negotiating_master_node_set.discard(node.getServer())
            conn.close()
            return

        if your_uuid != app.uuid:
            # uuid conflict happened, accept the new one and restart election
            app.uuid = your_uuid
            raise ElectionFailure, 'new uuid supplied'

        conn.setUUID(uuid)
        node.setUUID(uuid)

        if app.uuid < uuid:
            # I lost.
            app.primary = False

        app.negotiating_master_node_set.discard(conn.getAddress())

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        app = self.app
        # Register new master nodes.
        for ip_address, port, uuid in known_master_list:
            addr = (ip_address, port)
            if app.server == addr:
                # This is self.
                continue
            else:
                n = app.nm.getNodeByServer(addr)
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)
                    app.unconnected_master_node_set.add(addr)

                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None or n.getUUID() != uuid:
                        n.setUUID(uuid)
                        
        if primary_uuid != INVALID_UUID:
            # The primary master is defined.
            if app.primary_master_node is not None \
                    and app.primary_master_node.getUUID() != primary_uuid:
                # There are multiple primary master nodes. This is
                # dangerous.
                raise ElectionFailure, 'multiple primary master nodes'
            primary_node = app.nm.getNodeByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('received an unknown primary node UUID')
            else:
                if primary_node.getUUID() == primary_uuid:
                    # Whatever the situation is, I trust this master.
                    app.primary = False
                    app.primary_master_node = primary_node

        # Request a node idenfitication.
        conn.ask(protocol.requestNodeIdentification(MASTER_NODE_TYPE,
                 app.uuid, app.server[0], app.server[1], app.name))


class ServerElectionEventHandler(MasterEventHandler):

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def peerBroken(self, conn):
        app = self.app
        addr = conn.getAddress()
        node = app.nm.getNodeByServer(addr)
        if node is not None and node.getUUID() is not None:
            node.setState(BROKEN_STATE)
        MasterEventHandler.peerBroken(self, conn)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        self.checkClusterName(name)
        app = self.app
        if node_type != MASTER_NODE_TYPE:
            logging.info('reject a connection from a non-master')
            raise protocol.NotReadyError
        addr = (ip_address, port)
        node = app.nm.getNodeByServer(addr)
        if node is None:
            node = MasterNode(server = addr, uuid = uuid)
            app.nm.add(node)
            app.unconnected_master_node_set.add(addr)
        else:
            # If this node is broken, reject it.
            if node.getUUID() == uuid:
                if node.getState() == BROKEN_STATE:
                    raise protocol.BrokenNodeDisallowedError

        # supplied another uuid in case of conflict
        while not app.isValidUUID(uuid, addr):
            uuid = app.getNewUUID(node_type)

        node.setUUID(uuid)
        conn.setUUID(uuid)

        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE, app.uuid, 
                app.server[0], app.server[1], app.pt.getPartitions(), 
                app.pt.getReplicas(), uuid)
        conn.answer(p, packet)

    def handleAnnouncePrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            raise protocol.UnexpectedPacketError
        app = self.app
        if app.primary:
            # I am also the primary... So restart the election.
            raise ElectionFailure, 'another primary arises'
        node = app.nm.getNodeByUUID(uuid)
        app.primary = False
        app.primary_master_node = node
        logging.info('%s is the primary', node)

