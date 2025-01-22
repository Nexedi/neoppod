#
# Copyright (C) 2006-2019  Nexedi SA
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
from neo.lib.exception import PrimaryElected, PrimaryFailure
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets


class SecondaryHandler(MasterHandler):
    """Handler used by primary to handle secondary masters"""

    def handlerSwitched(self, conn, new):
        pass

    def _connectionLost(self, conn):
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        node.setDown()
        app.broadcastNodesInformation([node])


class ElectionHandler(SecondaryHandler):
    """Handler used by primary to handle secondary masters during election"""

    def connectionCompleted(self, conn):
        super(ElectionHandler, self).connectionCompleted(conn)
        app = self.app
        conn.ask(Packets.RequestIdentification(NodeTypes.MASTER,
            app.uuid, app.server, app.name, app.election, {}))

    def connectionFailed(self, conn):
        super(ElectionHandler, self).connectionFailed(conn)
        self.connectionLost(conn)

    def _acceptIdentification(self, node):
        raise PrimaryElected(node)

    def _connectionLost(self, *args):
        if self.app.primary: # not switching to secondary role
            self.app._current_manager.try_secondary = True

    def notPrimaryMaster(self, *args):
        try:
            super(ElectionHandler, self).notPrimaryMaster(*args)
        except PrimaryElected as e:
            # We keep playing the primary role when the peer does not
            # know yet that we won election against the returned node.
            if not e.args[0].isIdentified():
                raise
        # There may be new master nodes. Connect to them.
        self.app._current_manager.try_secondary = True


class PrimaryHandler(ElectionHandler):
    """Handler used by secondaries to handle primary master"""

    def _acceptIdentification(self, node):
        assert self.app.primary_master is node, (self.app.primary_master, node)

    def _connectionLost(self, conn):
        node = self.app.primary_master
        # node is None when switching to primary role
        if node and not node.isConnected(True):
            raise PrimaryFailure('primary master is dead')

    def notPrimaryMaster(self, *args):
        try:
            super(ElectionHandler, self).notPrimaryMaster(*args)
        except PrimaryElected as e:
            if e.args[0] is not self.app.primary_master:
                raise

    def notifyClusterInformation(self, conn, state):
        if state == ClusterStates.STOPPING:
            sys.exit()

    def notifyNodeInformation(self, conn, timestamp, node_list):
        super(PrimaryHandler, self).notifyNodeInformation(
            conn, timestamp, node_list)
        for node_type, _, uuid, state, _ in node_list:
            assert node_type == NodeTypes.MASTER, node_type
            if uuid == self.app.uuid and state == NodeStates.DOWN:
                sys.exit()
