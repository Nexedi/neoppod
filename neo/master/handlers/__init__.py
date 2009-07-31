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

from neo import protocol
from neo.handler import EventHandler

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def handleProtocolError(self, conn, packet, message):
        logging.error('Protocol error %s %s' % (message, conn.getAddress()))

    def handleAskPrimaryMaster(self, conn, packet):
        if conn.getConnector() is None:
            # Connection can be closed by peer after he sent AskPrimaryMaster
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
        for n in app.nm.getMasterNodeList():
            if n.getState() == protocol.BROKEN_STATE:
                continue
            known_master_list.append((n.getServer(), n.getUUID(), ))
        conn.answer(protocol.answerPrimaryMaster(primary_uuid,
                                                 known_master_list),
                    packet.getId())

    def handleAskClusterState(self, conn, packet):
        assert conn.getUUID() is not None
        state = self.app.getClusterState()
        conn.answer(protocol.answerClusterState(state), packet.getId())

    def handleAskNodeInformation(self, conn, packet):
        self.app.sendNodesInformations(conn)
        conn.answer(protocol.answerNodeInformation([]), packet.getId())

    def handleAskPartitionTable(self, conn, packet, offset_list):
        assert len(offset_list) == 0
        app = self.app
        app.sendPartitionTable(conn)
        conn.answer(protocol.answerPartitionTable(app.pt.getID(), []),
                    packet.getId())


class BaseServiceHandler(MasterHandler):
    """This class deals with events for a service phase."""

    def handleNodeLost(self, conn, node):
        # This method provides a hook point overridable by service classes.
        # It is triggered when a connection to a node gets lost.
        pass

    def handleConnectionLost(self, conn, new_state):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        assert node is not None
        if node.getState() == new_state:
            return
        if new_state != protocol.BROKEN_STATE and node.getState() == protocol.PENDING_STATE:
            # was in pending state, so drop it from the node manager to forget
            # it and do not set in running state when it comes back
            logging.info('drop a pending node from the node manager')
            self.app.nm.remove(node)
        node.setState(new_state)
        self.app.broadcastNodeInformation(node)
        # clean node related data in specialized handlers
        self.handleNodeLost(conn, node)

    def handleAskLastIDs(self, conn, packet):
        app = self.app
        conn.answer(protocol.answerLastIDs(app.loid, app.ltid, app.pt.getID()), packet.getId())

    def handleAskUnfinishedTransactions(self, conn, packet):
        app = self.app
        p = protocol.answerUnfinishedTransactions(app.finishing_transaction_dict.keys())
        conn.answer(p, packet.getId())

