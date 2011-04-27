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

import neo

from neo.lib.handler import EventHandler
from neo.lib.protocol import NodeTypes, NodeStates, Packets
from neo.lib.util import dump

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def protocolError(self, conn, message):
        neo.lib.logging.error(
                        'Protocol error %s %s', message, conn.getAddress())

    def askPrimary(self, conn):
        app = self.app
        if app.primary:
            primary_uuid = app.uuid
        elif app.primary_master_node is not None:
            primary_uuid = app.primary_master_node.getUUID()
        else:
            primary_uuid = None

        known_master_list = [(app.server, app.uuid, )]
        for n in app.nm.getMasterList():
            if n.isBroken():
                continue
            known_master_list.append((n.getAddress(), n.getUUID(), ))
        conn.answer(Packets.AnswerPrimary(
            primary_uuid,
            known_master_list),
        )

    def askClusterState(self, conn):
        assert conn.getUUID() is not None
        state = self.app.getClusterState()
        conn.answer(Packets.AnswerClusterState(state))

    def askNodeInformation(self, conn):
        nm = self.app.nm
        node_list = []
        node_list.extend(n.asTuple() for n in nm.getMasterList())
        node_list.extend(n.asTuple() for n in nm.getClientList())
        node_list.extend(n.asTuple() for n in nm.getStorageList())
        conn.notify(Packets.NotifyNodeInformation(node_list))
        conn.answer(Packets.AnswerNodeInformation())

    def askPartitionTable(self, conn):
        ptid = self.app.pt.getID()
        row_list = self.app.pt.getRowList()
        conn.answer(Packets.AnswerPartitionTable(ptid, row_list))


DISCONNECTED_STATE_DICT = {
    NodeTypes.STORAGE: NodeStates.TEMPORARILY_DOWN,
}

class BaseServiceHandler(MasterHandler):
    """This class deals with events for a service phase."""

    def nodeLost(self, conn, node):
        # This method provides a hook point overridable by service classes.
        # It is triggered when a connection to a node gets lost.
        pass

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        if node is None:
            return # for example, when a storage is removed by an admin
        if new_state != NodeStates.BROKEN:
            new_state = DISCONNECTED_STATE_DICT.get(node.getType(),
                    NodeStates.DOWN)
        assert new_state in (NodeStates.TEMPORARILY_DOWN, NodeStates.DOWN,
            NodeStates.BROKEN), new_state
        assert node.getState() not in (NodeStates.TEMPORARILY_DOWN,
            NodeStates.DOWN, NodeStates.BROKEN), (dump(self.app.uuid),
            node.whoSetState(), new_state)
        was_pending = node.isPending()
        node.setState(new_state)
        if new_state != NodeStates.BROKEN and was_pending:
            # was in pending state, so drop it from the node manager to forget
            # it and do not set in running state when it comes back
            neo.lib.logging.info('drop a pending node from the node manager')
            self.app.nm.remove(node)
        self.app.broadcastNodesInformation([node])
        # clean node related data in specialized handlers
        self.nodeLost(conn, node)

    def notifyReady(self, conn):
        self.app.setStorageReady(conn.getUUID())

