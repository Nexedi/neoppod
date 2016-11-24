#
# Copyright (C) 2006-2016  Nexedi SA
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
from neo.lib.exception import StoppedOperation
from neo.lib.handler import EventHandler
from neo.lib.protocol import (uuid_str, NodeTypes, NodeStates, Packets,
    BrokenNodeDisallowedError, ProtocolError,
)

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def connectionCompleted(self, conn, new=None):
        if new is None:
            super(MasterHandler, self).connectionCompleted(conn)
        elif new:
            self._notifyNodeInformation(conn)

    def requestIdentification(self, conn, node_type, uuid, address, name, _):
        self.checkClusterName(name)
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node:
            if node_type is NodeTypes.MASTER and not (
               None != address == node.getAddress()):
                raise ProtocolError
            if node.isBroken():
                raise BrokenNodeDisallowedError
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

    def askRecovery(self, conn):
        app = self.app
        conn.answer(Packets.AnswerRecovery(
            app.pt.getID(),
            app.backup_tid and app.pt.getBackupTid(),
            app.truncate_tid))

    def askLastIDs(self, conn):
        tm = self.app.tm
        conn.answer(Packets.AnswerLastIDs(tm.getLastOID(), tm.getLastTID()))

    def askLastTransaction(self, conn):
        conn.answer(Packets.AnswerLastTransaction(
            self.app.getLastTransaction()))

    def _notifyNodeInformation(self, conn):
        nm = self.app.nm
        node_list = []
        node_list.extend(n.asTuple() for n in nm.getMasterList())
        node_list.extend(n.asTuple() for n in nm.getClientList())
        node_list.extend(n.asTuple() for n in nm.getStorageList())
        conn.notify(Packets.NotifyNodeInformation(node_list))

    def askPartitionTable(self, conn):
        pt = self.app.pt
        conn.answer(Packets.AnswerPartitionTable(pt.getID(), pt.getRowList()))


DISCONNECTED_STATE_DICT = {
    NodeTypes.STORAGE: NodeStates.TEMPORARILY_DOWN,
}

class BaseServiceHandler(MasterHandler):
    """This class deals with events for a service phase."""

    def connectionCompleted(self, conn, new):
        self._notifyNodeInformation(conn)
        pt = self.app.pt
        conn.notify(Packets.SendPartitionTable(pt.getID(), pt.getRowList()))

    def connectionLost(self, conn, new_state):
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        if node is None:
            return # for example, when a storage is removed by an admin
        assert node.isStorage(), node
        logging.info('storage node lost')
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
            app.nm.remove(node)
        app.broadcastNodesInformation([node])
        if app.truncate_tid:
            raise StoppedOperation
        app.broadcastPartitionChanges(app.pt.outdate(node))
        if not app.pt.operational():
            raise StoppedOperation

    def notifyReady(self, conn):
        self.app.setStorageReady(conn.getUUID())

