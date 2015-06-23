#
# Copyright (C) 2006-2015  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from neo.lib import logging
from neo.lib.protocol import uuid_str, NodeTypes, Packets
from neo.lib.protocol import NotReadyError
from neo.lib.exception import ElectionFailure
from neo.lib.handler import EventHandler
from . import MasterHandler

class BaseElectionHandler(EventHandler):

    def reelectPrimary(self, conn):
        raise ElectionFailure, 'reelection requested'

    def announcePrimary(self, conn):
        app = self.app
        if app.primary:
            # I am also the primary... So restart the election.
            raise ElectionFailure, 'another primary arises'
        try:
            address = app.master_address_dict[conn]
            assert conn.isServer()
        except KeyError:
            address = conn.getAddress()
            assert conn.isClient()
        app.primary = False
        app.primary_master_node = node = app.nm.getByAddress(address)
        app.negotiating_master_node_set.clear()
        logging.info('%s is the primary', node)

    def elect(self, conn, peer_address):
        app = self.app
        if app.server < peer_address:
            app.primary = False
            if conn is not None:
                app.master_address_dict[conn] = peer_address
        app.negotiating_master_node_set.discard(peer_address)


class ClientElectionHandler(BaseElectionHandler):

    def connectionFailed(self, conn):
        addr = conn.getAddress()
        node = self.app.nm.getByAddress(addr)
        assert node is not None, (uuid_str(self.app.uuid), addr)
        # node may still be in unknown state
        self.app.negotiating_master_node_set.discard(addr)
        super(ClientElectionHandler, self).connectionFailed(conn)

    def connectionCompleted(self, conn):
        app = self.app
        conn.ask(Packets.RequestIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.server,
            app.name,
        ))
        super(ClientElectionHandler, self).connectionCompleted(conn)

    def connectionLost(self, conn, new_state):
        # Retry connection. Either the node just died (and we will end up in
        # connectionFailed) or it just got elected (and we must not ignore
        # that node).
        addr = conn.getAddress()
        self.app.unconnected_master_node_set.add(addr)
        self.app.negotiating_master_node_set.discard(addr)

    def _acceptIdentification(self, node, peer_uuid, num_partitions,
            num_replicas, your_uuid, primary, known_master_list):
        app = self.app

        # Register new master nodes.
        for address, uuid in known_master_list:
            if app.server == address:
                # This is self.
                assert node.getAddress() != primary or uuid == your_uuid, (
                    uuid_str(uuid), uuid_str(your_uuid))
                continue
            n = app.nm.getByAddress(address)
            if n is None:
                n = app.nm.createMaster(address=address)

        if primary is not None:
            # The primary master is defined.
            if app.primary_master_node is not None \
                    and app.primary_master_node.getAddress() != primary:
                # There are multiple primary master nodes. This is
                # dangerous.
                raise ElectionFailure, 'multiple primary master nodes'
            primary_node = app.nm.getByAddress(primary)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('received an unknown primary node')
            else:
                # Whatever the situation is, I trust this master.
                app.primary = False
                app.primary_master_node = primary_node
                # Stop waiting for connections than primary master's to
                # complete to exit election phase ASAP.
                app.negotiating_master_node_set.clear()
                return

        self.elect(None, node.getAddress())


class ServerElectionHandler(BaseElectionHandler, MasterHandler):

    def _setupNode(self, conn, node_type, uuid, address, node):
        app = self.app
        if node_type != NodeTypes.MASTER:
            logging.info('reject a connection from a non-master')
            raise NotReadyError

        if node is None:
            node = app.nm.createMaster(address=address)

        self.elect(conn, address)
        return uuid

