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
from neo.lib.handler import EventHandler
from neo.lib.protocol import (uuid_str, NodeTypes, NodeStates, Packets,
    BrokenNodeDisallowedError,
)

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def requestIdentification(self, conn, node_type, uuid, address, name):
        self.checkClusterName(name)
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node:
            assert node_type is not NodeTypes.MASTER or node.getAddress() in (
                address, None), (node, address)
            if node.isBroken():
                raise BrokenNodeDisallowedError
        else:
            node = app.nm.getByAddress(address)
        peer_uuid = self._setupNode(conn, node_type, uuid, address, node)
        if app.primary:
            primary_address = app.server
        elif app.primary_master_node is not None:
            primary_address = app.primary_master_node.getAddress()
        else:
            primary_address = None

        known_master_list = [(app.server, app.uuid)]
        for n in app.nm.getMasterList():
            if n.isBroken():
                continue
            known_master_list.append((n.getAddress(), n.getUUID()))
        conn.answer(Packets.AcceptIdentification(
            NodeTypes.MASTER,
            app.uuid,
            app.pt.getPartitions(),
            app.pt.getReplicas(),
            peer_uuid,
            primary_address,
            known_master_list),
        )

    def askClusterState(self, conn):
        state = self.app.getClusterState()
        conn.answer(Packets.AnswerClusterState(state))

    def askLastIDs(self, conn):
        app = self.app
        conn.answer(Packets.AnswerLastIDs(
            app.tm.getLastOID(),
            app.tm.getLastTID(),
            app.pt.getID(),
            app.backup_tid))

    def askLastTransaction(self, conn):
        conn.answer(Packets.AnswerLastTransaction(
            self.app.getLastTransaction()))

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
            NodeStates.DOWN, NodeStates.BROKEN), (uuid_str(self.app.uuid),
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

    def notifyReady(self, conn):
        self.app.setStorageReady(conn.getUUID())

