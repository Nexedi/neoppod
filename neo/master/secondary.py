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
        DOWN_STATE, ADMIN_NODE_TYPE
from neo.master.handler import MasterEventHandler
from neo.connection import ClientConnection
from neo.exception import ElectionFailure, PrimaryFailure
from neo.protocol import Packet, UnexpectedPacketError, INVALID_UUID
from neo.node import MasterNode
from neo.handler import identification_required, restrict_node_types, \
        client_connection_required, server_connection_required

class SecondaryEventHandler(MasterEventHandler):
    """This class deals with events for a secondary master."""

    def connectionClosed(self, conn):
        if isinstance(conn, ClientConnection):
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is dead'
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        if isinstance(conn, ClientConnection):
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is down'
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        if isinstance(conn, ClientConnection):
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is crazy'
        MasterEventHandler.peerBroken(self, conn)

    def packetReceived(self, conn, packet):
        if isinstance(conn, ClientConnection):
            node = self.app.nm.getNodeByServer(conn.getAddress())
            if node.getState() != BROKEN_STATE:
                node.setState(RUNNING_STATE)
        MasterEventHandler.packetReceived(self, conn, packet)

    @server_connection_required
    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        app = self.app
        if name != app.name:
            logging.error('reject an alien cluster')
            conn.answer(protocol.protocolError('invalid cluster name'), packet)
            conn.abort()
            return

        # Add a node only if it is a master node and I do not know it yet.
        if node_type == MASTER_NODE_TYPE and uuid != INVALID_UUID:
            addr = (ip_address, port)
            node = app.nm.getNodeByServer(addr)
            if node is None:
                node = MasterNode(server = addr, uuid = uuid)
                app.nm.add(node)

            # Trust the UUID sent by the peer.
            node.setUUID(uuid)

        conn.setUUID(uuid)

        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE,
                                   app.uuid, app.server[0], app.server[1],
                                   app.num_partitions, app.num_replicas,
                                   uuid)
        # Next, the peer should ask a primary master node.
        conn.answer(p, packet)

    @identification_required
    @server_connection_required
    def handleAskPrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        app = self.app
        primary_uuid = app.primary_master_node.getUUID()

        known_master_list = []
        for n in app.nm.getMasterNodeList():
            if n.getState() == BROKEN_STATE:
                continue
            info = n.getServer() + (n.getUUID() or INVALID_UUID,)
            known_master_list.append(info)

        p = protocol.answerPrimaryMaster(primary_uuid, known_master_list)
        conn.answer(p, packet)

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
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
                n = app.nm.getNodeByServer(addr)
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)

                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)
