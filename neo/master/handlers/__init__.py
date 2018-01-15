#
# Copyright (C) 2006-2017  Nexedi SA
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

from ..app import monotonic_time
from neo.lib import logging
from neo.lib.exception import StoppedOperation
from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def connectionCompleted(self, conn, new=None):
        if new is None:
            super(MasterHandler, self).connectionCompleted(conn)

    def connectionLost(self, conn, new_state=None):
        if self.app.listening_conn: # if running
            self._connectionLost(conn)

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
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        node_list = app.nm.getList()
        node_list.remove(node)
        node_list = ([node.asTuple()] # for id_timestamp
            + app.getNodeInformationDict(node_list)[node.getType()])
        conn.send(Packets.NotifyNodeInformation(monotonic_time(), node_list))

    def askPartitionTable(self, conn):
        pt = self.app.pt
        conn.answer(Packets.AnswerPartitionTable(pt.getID(), pt.getRowList()))


class BaseServiceHandler(MasterHandler):
    """This class deals with events for a service phase."""

    def connectionCompleted(self, conn, new):
        pt = self.app.pt
        conn.send(Packets.SendPartitionTable(pt.getID(), pt.getRowList()))

    def connectionLost(self, conn, new_state):
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        if node is None:
            return # for example, when a storage is removed by an admin
        assert node.isStorage(), node
        logging.info('storage node lost')
        if node.isPending():
            # was in pending state, so drop it from the node manager to forget
            # it and do not set in running state when it comes back
            logging.info('drop a pending node from the node manager')
            node.setUnknown()
        elif node.isDown():
            # Already put in DOWN state by AdministrationHandler.setNodeState
            return
        else:
            node.setDown()
        app.broadcastNodesInformation([node])
        if app.truncate_tid:
            raise StoppedOperation
        app.broadcastPartitionChanges(app.pt.outdate(node))
        if not app.pt.operational():
            raise StoppedOperation
