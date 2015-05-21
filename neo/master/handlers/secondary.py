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

import sys
from . import MasterHandler
from neo.lib.handler import EventHandler
from neo.lib.exception import ElectionFailure, PrimaryFailure
from neo.lib.protocol import NodeStates, NodeTypes, Packets, uuid_str
from neo.lib import logging

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

class PrimaryHandler(EventHandler):
    """ Handler used by secondaries to handle primary master"""

    def connectionLost(self, conn, new_state):
        self.app.primary_master_node.setDown()
        raise PrimaryFailure, 'primary master is dead'

    def connectionFailed(self, conn):
        self.app.primary_master_node.setDown()
        raise PrimaryFailure, 'primary master is dead'

    def connectionCompleted(self, conn):
        app = self.app
        addr = conn.getAddress()
        node = app.nm.getByAddress(addr)
        # connection successfull, set it as running
        node.setRunning()
        conn.ask(Packets.RequestIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.server,
            app.name,
        ))
        super(PrimaryHandler, self).connectionCompleted(conn)

    def reelectPrimary(self, conn):
        raise ElectionFailure, 'reelection requested'

    def notifyClusterInformation(self, conn, state):
        self.app.cluster_state = state

    def notifyNodeInformation(self, conn, node_list):
        app = self.app
        for node_type, addr, uuid, state in node_list:
            if node_type != NodeTypes.MASTER:
                # No interest.
                continue
            if uuid == app.uuid and state == NodeStates.UNKNOWN:
                sys.exit()
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

    def _acceptIdentification(self, node, uuid, num_partitions,
            num_replicas, your_uuid, primary, known_master_list):
        app = self.app
        if primary != app.primary_master_node.getAddress():
            raise PrimaryFailure('unexpected primary uuid')

        if your_uuid != app.uuid:
            app.uuid = your_uuid
            logging.info('My UUID: ' + uuid_str(your_uuid))

        node.setUUID(uuid)

