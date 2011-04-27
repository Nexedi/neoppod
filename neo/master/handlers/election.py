#
# Copyright (C) 2006-2010  Nexedi SA
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

import neo.lib

from neo.lib.protocol import NodeTypes, NodeStates, Packets
from neo.lib.protocol import NotReadyError, ProtocolError, \
                              UnexpectedPacketError
from neo.lib.protocol import BrokenNodeDisallowedError
from neo.master.handlers import MasterHandler
from neo.lib.exception import ElectionFailure
from neo.lib.util import dump

class ClientElectionHandler(MasterHandler):

    # FIXME: this packet is not allowed here, but handled in MasterHandler
    # a global handler review is required.
    def askPrimary(self, conn):
        raise UnexpectedPacketError, "askPrimary on server connection"

    def connectionStarted(self, conn):
        addr = conn.getAddress()
        # connection in progress
        self.app.unconnected_master_node_set.remove(addr)
        self.app.negotiating_master_node_set.add(addr)
        MasterHandler.connectionStarted(self, conn)

    def connectionFailed(self, conn):
        addr = conn.getAddress()
        node = self.app.nm.getByAddress(addr)
        assert node is not None, (dump(self.app.uuid), addr)
        assert node.isUnknown(), (dump(self.app.uuid), node.whoSetState(),
          node.getState())
        # connection never success, node is still in unknown state
        self.app.negotiating_master_node_set.discard(addr)
        self.app.unconnected_master_node_set.add(addr)
        MasterHandler.connectionFailed(self, conn)

    def connectionCompleted(self, conn):
        conn.ask(Packets.AskPrimary())
        MasterHandler.connectionCompleted(self, conn)

    def connectionLost(self, conn, new_state):
        addr = conn.getAddress()
        self.app.negotiating_master_node_set.discard(addr)

    def acceptIdentification(self, conn, node_type,
            uuid, num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        if node_type != NodeTypes.MASTER:
            # The peer is not a master node!
            neo.lib.logging.error('%r is not a master node', conn)
            app.nm.remove(node)
            conn.close()
            return

        if your_uuid != app.uuid:
            # uuid conflict happened, accept the new one and restart election
            app.uuid = your_uuid
            neo.lib.logging.info('UUID conflict, new UUID: %s',
                            dump(your_uuid))
            raise ElectionFailure, 'new uuid supplied'

        conn.setUUID(uuid)
        node.setUUID(uuid)

        if app.uuid < uuid:
            # I lost.
            app.primary = False

        app.negotiating_master_node_set.discard(conn.getAddress())

    def answerPrimary(self, conn, primary_uuid, known_master_list):
        app = self.app
        # Register new master nodes.
        for address, uuid in known_master_list:
            if app.server == address:
                # This is self.
                continue
            n = app.nm.getByAddress(address)
            # master node must be known
            assert n is not None, 'Unknown master node: %s' % (address, )
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
                neo.lib.logging.warning(
                                'received an unknown primary node UUID')
            else:
                # Whatever the situation is, I trust this master.
                app.primary = False
                app.primary_master_node = primary_node
                # Stop waiting for connections than primary master's to
                # complete to exit election phase ASAP.
                app.unconnected_master_node_set.clear()
                app.negotiating_master_node_set.clear()

        primary_node = app.primary_master_node
        if (primary_node is None or \
            conn.getAddress() == primary_node.getAddress()) and \
                not conn.isClosed():
            # Request a node identification.
            # There are 3 cases here:
            # - Peer doesn't know primary node
            #   We must ask its identification so we exchange our uuids, to
            #   know which of us is secondary.
            # - Peer knows primary node
            #   - He is the primary
            #     We must ask its identification, as part of the normal
            #     connection process
            #   - He is not the primary
            #     We don't need to ask its identification, as we will close
            #     this connection anyway (exiting election).
            # Also, connection can be closed by peer after he sent
            # AnswerPrimary if he finds the primary master before we
            # give him our UUID.
            # The connection gets closed before this message gets processed
            # because this message might have been queued, but connection
            # interruption takes effect as soon as received.
            conn.ask(Packets.RequestIdentification(
                NodeTypes.MASTER,
                app.uuid,
                app.server,
                app.name
            ))


class ServerElectionHandler(MasterHandler):

    def reelectPrimary(self, conn):
        raise ElectionFailure, 'reelection requested'

    def requestIdentification(self, conn, node_type,
                                        uuid, address, name):
        self.checkClusterName(name)
        app = self.app
        if node_type != NodeTypes.MASTER:
            neo.lib.logging.info('reject a connection from a non-master')
            raise NotReadyError
        node = app.nm.getByAddress(address)
        if node is None:
            neo.lib.logging.error('unknown master node: %s' % (address, ))
            raise ProtocolError('unknown master node')
        # If this node is broken, reject it.
        if node.getUUID() == uuid:
            if node.isBroken():
                raise BrokenNodeDisallowedError

        # supplied another uuid in case of conflict
        while not app.isValidUUID(uuid, address):
            uuid = app.getNewUUID(node_type)

        node.setUUID(uuid)
        conn.setUUID(uuid)

        p = Packets.AcceptIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.pt.getPartitions(),
            app.pt.getReplicas(),
            uuid
        )
        conn.answer(p)

    def announcePrimary(self, conn):
        uuid = conn.getUUID()
        if uuid is None:
            raise ProtocolError('Not identified')
        app = self.app
        if app.primary:
            # I am also the primary... So restart the election.
            raise ElectionFailure, 'another primary arises'
        node = app.nm.getByUUID(uuid)
        app.primary = False
        app.primary_master_node = node
        app.unconnected_master_node_set.clear()
        app.negotiating_master_node_set.clear()
        neo.lib.logging.info('%s is the primary', node)

