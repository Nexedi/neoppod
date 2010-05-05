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

from neo import logging

from neo.handler import EventHandler
from neo.protocol import NodeTypes, NodeStates, Packets
from neo.util import dump

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def protocolError(self, conn, message):
        logging.error('Protocol error %s %s' % (message, conn.getAddress()))

    def askPrimary(self, conn):
        if conn.getConnector() is None:
            # Connection can be closed by peer after he sent AskPrimary
            # if he finds the primary master before we answer him.
            # The connection gets closed before this message gets processed
            # because this message might have been queued, but connection
            # interruption takes effect as soon as received.
            return
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
        self.app.sendNodesInformations(conn)
        conn.answer(Packets.AnswerNodeInformation())

    def askPartitionTable(self, conn, offset_list):
        assert len(offset_list) == 0
        app = self.app
        app.sendPartitionTable(conn)
        conn.answer(Packets.AnswerPartitionTable(app.pt.getID(), []))


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
        assert node is not None
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
            logging.info('drop a pending node from the node manager')
            self.app.nm.remove(node)
        self.app.broadcastNodesInformation([node])
        # clean node related data in specialized handlers
        self.nodeLost(conn, node)

