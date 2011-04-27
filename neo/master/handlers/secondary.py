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

from neo.master.handlers import MasterHandler
from neo.lib.exception import ElectionFailure, PrimaryFailure
from neo.lib.protocol import NodeTypes, Packets

class SecondaryMasterHandler(MasterHandler):
    """ Handler used by primary to handle secondary masters"""

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node is not None
        node.setDown()
        self.app.broadcastNodesInformation([node])

    def announcePrimary(self, conn):
        raise ElectionFailure, 'another primary arises'

    def reelectPrimary(self, conn):
        raise ElectionFailure, 'reelection requested'

class PrimaryHandler(MasterHandler):
    """ Handler used by secondaries to handle primary master"""

    def packetReceived(self, conn, packet):
        if not conn.isServer():
            node = self.app.nm.getByAddress(conn.getAddress())
            if not node.isBroken():
                node.setRunning()
        MasterHandler.packetReceived(self, conn, packet)

    def connectionLost(self, conn, new_state):
        self.app.primary_master_node.setDown()
        raise PrimaryFailure, 'primary master is dead'

    def connectionFailed(self, conn):
        self.app.primary_master_node.setDown()
        raise PrimaryFailure, 'primary master is dead'

    def connectionCompleted(self, conn):
        addr = conn.getAddress()
        node = self.app.nm.getByAddress(addr)
        # connection successfull, set it as running
        node.setRunning()
        conn.ask(Packets.AskPrimary())
        MasterHandler.connectionCompleted(self, conn)

    def reelectPrimary(self, conn):
        raise ElectionFailure, 'reelection requested'

    def notifyNodeInformation(self, conn, node_list):
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
                n = app.nm.getByAddress(addr)
                # master node must be known
                assert n is not None

                if uuid is not None:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)

    def acceptIdentification(self, conn, node_type,
                                       uuid, num_partitions,
                                       num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        assert node_type == NodeTypes.MASTER

        if your_uuid != app.uuid:
            # uuid conflict happened, accept the new one
            app.uuid = your_uuid

        conn.setUUID(uuid)
        node.setUUID(uuid)

    def answerPrimary(self, conn, primary_uuid, known_master_list):
        app = self.app
        if primary_uuid != app.primary_master_node.getUUID():
            raise PrimaryFailure, 'unexpected primary uuid'
        conn.ask(Packets.RequestIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.server,
            app.name
        ))

    def notifyClusterInformation(self, conn, state):
        pass
