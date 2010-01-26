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

from neo import protocol
from neo.protocol import NodeTypes, NodeStates, Packets
from neo.master.handlers import MasterHandler
from neo.exception import ElectionFailure

class ElectionHandler(MasterHandler):
    """This class deals with events for a primary master election."""

    def notifyNodeInformation(self, conn, packet, node_list):
        uuid = conn.getUUID()
        if uuid is None:
            raise protocol.ProtocolError('Not identified')
        app = self.app
        for node_type, addr, uuid, state in node_list:
            if node_type != NodeTypes.MASTER:
                # No interest.
                continue

            # Register new master nodes.
            if app.server == addr:
                # This is self.
                continue
            else:
                node = app.nm.getByAddress(addr)
                # The master must be known
                assert node is not None

                if uuid is not None:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if node.getUUID() is None:
                        node.setUUID(uuid)

                if state in (node.getState(), NodeStates.RUNNING):
                    # No change. Don't care.
                    continue

                # Something wrong happened possibly. Cut the connection to
                # this node, if any, and notify the information to others.
                # XXX this can be very slow.
                for c in app.em.getConnectionList():
                    if c.getUUID() == uuid:
                        c.close()
                node.setState(state)

class ClientElectionHandler(ElectionHandler):

    def packetReceived(self, conn, packet):
        node = self.app.nm.getByAddress(conn.getAddress())
        if not node.isBroken():
            node.setRunning()
        MasterHandler.packetReceived(self, conn, packet)

    def connectionStarted(self, conn):
        app = self.app
        addr = conn.getAddress()
        app.unconnected_master_node_set.remove(addr)
        app.negotiating_master_node_set.add(addr)
        MasterHandler.connectionStarted(self, conn)

    def connectionCompleted(self, conn):
        conn.ask(Packets.AskPrimary())
        MasterHandler.connectionCompleted(self, conn)

    def connectionClosed(self, conn):
        self.connectionFailed(conn)
        MasterHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        self.connectionFailed(conn)
        MasterHandler.timeoutExpired(self, conn)

    def connectionFailed(self, conn):
        app = self.app
        addr = conn.getAddress()
        app.negotiating_master_node_set.discard(addr)
        node = app.nm.getByAddress(addr)
        if node.isRunning():
            app.unconnected_master_node_set.add(addr)
            node.setTemporarilyDown()
        elif node.isTemporarilyDown():
            app.unconnected_master_node_set.add(addr)
        MasterHandler.connectionFailed(self, conn)

    def peerBroken(self, conn):
        app = self.app
        addr = conn.getAddress()
        node = app.nm.getByAddress(addr)
        if node is not None:
            node.setDown()
        app.negotiating_master_node_set.discard(addr)
        MasterHandler.peerBroken(self, conn)

    def acceptIdentification(self, conn, packet, node_type,
                                       uuid, address, num_partitions,
                                       num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        if node_type != NodeTypes.MASTER:
            # The peer is not a master node!
            logging.error('%s:%d is not a master node', *address)
            app.nm.remove(node)
            app.negotiating_master_node_set.discard(node.getAddress())
            conn.close()
            return
        if conn.getAddress() != address:
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                          conn.getAddress()[0], conn.getAddress()[1], *address)
            app.nm.remove(node)
            app.negotiating_master_node_set.discard(node.getAddress())
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

    def answerPrimary(self, conn, packet, primary_uuid, known_master_list):
        if conn.getConnector() is None:
            # Connection can be closed by peer after he sent
            # AnswerPrimary if he finds the primary master before we
            # give him our UUID.
            # The connection gets closed before this message gets processed
            # because this message might have been queued, but connection
            # interruption takes effect as soon as received.
            return
        app = self.app
        # Register new master nodes.
        for address, uuid in known_master_list:
            if app.server == address:
                # This is self.
                continue
            else:
                n = app.nm.getByAddress(address)
                # master node must be known
                assert n is not None

                if uuid is not None:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None or n.getUUID() != uuid:
                        n.setUUID(uuid)

        if primary_uuid is not None:
            # The primary master is defined.
            if app.primary_master_node is not None \
                    and app.primary_master_node.getUUID() != primary_uuid:
                # There are multiple primary master nodes. This is
                # dangerous.
                raise ElectionFailure, 'multiple primary master nodes'
            primary_node = app.nm.getByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('received an unknown primary node UUID')
            elif primary_node.getUUID() == primary_uuid:
                # Whatever the situation is, I trust this master.
                app.primary = False
                app.primary_master_node = primary_node
                # Stop waiting for connections than primary master's to
                # complete to exit election phase ASAP.
                primary_server = primary_node.getAddress()
                app.unconnected_master_node_set.intersection_update(
                    [primary_server])
                app.negotiating_master_node_set.intersection_update(
                    [primary_server])

        # Request a node idenfitication.
        conn.ask(Packets.RequestIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.server,
            app.name
        ))


class ServerElectionHandler(ElectionHandler):

    def reelectPrimary(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def peerBroken(self, conn):
        app = self.app
        addr = conn.getAddress()
        node = app.nm.getByAddress(addr)
        if node is not None and node.getUUID() is not None:
            node.setBroken()
        MasterHandler.peerBroken(self, conn)

    def requestIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        if conn.getConnector() is None:
            # Connection can be closed by peer after he sent
            # RequestIdentification if he finds the primary master before
            # we answer him.
            # The connection gets closed before this message gets processed
            # because this message might have been queued, but connection
            # interruption takes effect as soon as received.
            return
        self.checkClusterName(name)
        app = self.app
        if node_type != NodeTypes.MASTER:
            logging.info('reject a connection from a non-master')
            raise protocol.NotReadyError
        node = app.nm.getByAddress(address)
        if node is None:
            logging.error('unknown master node: %s' % (address, ))
            raise protocol.ProtocolError('unknown master node')
        # If this node is broken, reject it.
        if node.getUUID() == uuid:
            if node.isBroken():
                raise protocol.BrokenNodeDisallowedError

        # supplied another uuid in case of conflict
        while not app.isValidUUID(uuid, address):
            uuid = app.getNewUUID(node_type)

        node.setUUID(uuid)
        conn.setUUID(uuid)

        p = Packets.AcceptIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.server,
            app.pt.getPartitions(),
            app.pt.getReplicas(),
            uuid
        )
        conn.answer(p, packet.getId())

    def announcePrimary(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            raise protocol.ProtocolError('Not identified')
        app = self.app
        if app.primary:
            # I am also the primary... So restart the election.
            raise ElectionFailure, 'another primary arises'
        node = app.nm.getByUUID(uuid)
        app.primary = False
        app.primary_master_node = node
        logging.info('%s is the primary', node)

