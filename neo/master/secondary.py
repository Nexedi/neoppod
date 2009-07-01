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
from neo.exception import ElectionFailure, PrimaryFailure
from neo.protocol import Packet, UnexpectedPacketError, INVALID_UUID
from neo.node import MasterNode

class SecondaryMasterEventHandler(MasterEventHandler):
    """ Handler used by primary to handle secondary masters"""

    def connectionCompleted(self, conn):
        pass

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        logging.error('/!\ NotifyNodeInformation packet from secondary master')


class PrimaryMasterEventHandler(MasterEventHandler):
    """ Handler used by secondaries to handle primary master"""

    def connectionClosed(self, conn):
        if not conn.isServerConnection():
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is dead'
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        if not conn.isServerConnection():
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is down'
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        if not conn.isServerConnection():
            self.app.primary_master_node.setState(DOWN_STATE)
            raise PrimaryFailure, 'primary master is crazy'
        MasterEventHandler.peerBroken(self, conn)

    def packetReceived(self, conn, packet):
        if not conn.isServerConnection():
            node = self.app.nm.getNodeByServer(conn.getAddress())
            if node.getState() != BROKEN_STATE:
                node.setState(RUNNING_STATE)
        MasterEventHandler.packetReceived(self, conn, packet)

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

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port, num_partitions,
                                       num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        assert node_type == MASTER_NODE_TYPE
        assert conn.getAddress() == (ip_address, port)

        if your_uuid != app.uuid:
            # uuid conflict happened, accept the new one
            app.uuid = your_uuid

        conn.setUUID(uuid)
        node.setUUID(uuid)

