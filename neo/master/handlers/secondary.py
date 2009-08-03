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

from neo import logging

from neo.protocol import MASTER_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, DOWN_STATE
from neo.master.handlers import MasterHandler
from neo.exception import ElectionFailure, PrimaryFailure
from neo.protocol import UnexpectedPacketError
from neo.node import MasterNode

class SecondaryMasterHandler(MasterHandler):
    """ Handler used by primary to handle secondary masters"""

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        assert node is not None
        node.setState(DOWN_STATE)
        self.app.broadcastNodeInformation(node)

    def connectionCompleted(self, conn):
        pass

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        logging.error('/!\ NotifyNodeInformation packet from secondary master')


class PrimaryMasterHandler(MasterHandler):
    """ Handler used by secondaries to handle primary master"""

    def packetReceived(self, conn, packet):
        if not conn.isServer():
            node = self.app.nm.getNodeByServer(conn.getAddress())
            if node.getState() != BROKEN_STATE:
                node.setState(RUNNING_STATE)
        MasterHandler.packetReceived(self, conn, packet)

    def connectionLost(self, conn, new_state):
        self.app.primary_master_node.setState(DOWN_STATE)
        raise PrimaryFailure, 'primary master is dead'

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        for node_type, addr, uuid, state in node_list:
            if node_type != MASTER_NODE_TYPE:
                # No interest.
                continue

            # Register new master nodes.
            if app.server == addr:
                # This is self.
                continue
            else:
                n = app.nm.getNodeByServer(addr)
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)

                if uuid is not None:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, address, num_partitions,
                                       num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        assert node_type == MASTER_NODE_TYPE
        assert conn.getAddress() == address

        if your_uuid != app.uuid:
            # uuid conflict happened, accept the new one
            app.uuid = your_uuid

        conn.setUUID(uuid)
        node.setUUID(uuid)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        pass

    def handleNotifyClusterInformation(self, conn, packet, state):
        pass
