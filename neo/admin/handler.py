#
# Copyright (C) 2009  Nexedi SA
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

import logging

from neo.handler import EventHandler
from neo.node import StorageNode
from neo import protocol
from neo.exception import PrimaryFailure
from neo.util import dump

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # we only accept connection from command tool
        EventHandler.connectionAccepted(self, conn, s, addr)

    def handleAskPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        logging.info("ask partition list from %s to %s for %s" %(min_offset, max_offset, dump(uuid)))
        app = self.app
        # check we have one pt otherwise ask it to PMN
        if len(app.pt.getNodeList()) == 0:
            master_conn = self.app.master_conn
            p = protocol.askPartitionTable([])
            msg_id = master_conn.ask(p)
            app.dispatcher.register(msg_id, conn, {'min_offset' : min_offset,
                                                   'max_offset' : max_offset,
                                                   'uuid' : uuid,
                                                   'msg_id' : packet.getId()})
        else:
            app.sendPartitionTable(conn, min_offset, max_offset, uuid, packet.getId())


    def handleAskNodeList(self, conn, packet, node_type):
        logging.info("ask node list for %s" %(node_type))
        def node_filter(n):
            return n.getNodeType() is node_type
        node_list = self.app.nm.getNodeList(node_filter)
        node_information_list = []
        for node in node_list:
            try:
                ip, port = node.getServer()
            except TypeError:
                ip = "0.0.0.0"
                port = 0
            node_information_list.append((node.getNodeType(), (ip, port), node.getUUID(), node.getState()))
        p = protocol.answerNodeList(node_information_list)
        conn.answer(p, packet)

    def handleSetNodeState(self, conn, packet, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s" %(dump(uuid), state))
        node = self.app.nm.getNodeByUUID(uuid)
        if node is None:
            p = protocol.protocolError('invalid uuid')
            conn.notify(p)
            return
        if node.getState() == state and modify_partition_table is False:
            # no change
            p = protocol.answerNodeState(node.getUUID(), node.getState())
            conn.answer(p, packet)
            return
        # forward to primary master node
        master_conn = self.app.master_conn
        p = protocol.setNodeState(uuid, state, modify_partition_table)
        msg_id = master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def handleSetClusterState(self, conn, packet, name, state):
        self.checkClusterName(name)
        # forward to primary
        master_conn = self.app.master_conn
        p = protocol.setClusterState(name, state)
        msg_id = master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def handleAddPendingNodes(self, conn, packet, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        logging.info('Add nodes %s' % uuids)
        uuid = conn.getUUID()
        node = self.app.nm.getNodeByUUID(uuid)
        # forward the request to primary
        master_conn = self.app.master_conn
        msg_id = master_conn.ask(protocol.addPendingNodes(uuid_list))
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def handleAskClusterState(self, conn, packet):
        if self.app.cluster_state is None:
            # required it from PMN first
            msg_id = self.app.master_conn.ask(protocol.askClusterState())
            self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})
            return
        conn.answer(protocol.answerClusterState(self.app.cluster_state), packet)


class MasterEventHandler(EventHandler):
    """ This class is just used to dispacth message to right handler"""

    def _connectionLost(self, conn):
        raise PrimaryFailure

    def connectionFailed(self, conn):
        self._connectionLost(conn)
        EventHandler.connectionFailed(self, conn)

    def timeoutExpired(self, conn):
        self._connectionLost(conn)
        EventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        self._connectionLost(conn)
        EventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        self._connectionLost(conn)
        EventHandler.peerBroken(self, conn)

    def dispatch(self, conn, packet):
        if not packet.isResponse():
            # not an answer
            self.app.monitoring_handler.dispatch(conn, packet)
        elif self.app.dispatcher.registered(packet.getId()):
            # expected answer
            self.app.request_handler.dispatch(conn, packet)
        else:
            # unexpectexd answer, this should be answerNodeInformation or
            # answerPartitionTable from the master node during initialization.
            # This will no more exists when the initialization module will be
            # implemented for factorize code (as done for bootstrap)
            EventHandler.dispatch(self, conn, packet)

    def handleAnswerNodeInformation(self, conn, packet, node_list):
        logging.info("handleAnswerNodeInformation")

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        logging.info("handleAnswerPartitionTable")

    def handleNotifyClusterInformation(self, con, packet, cluster_state):
        logging.info("handleNotifyClusterInformation")


class MasterBaseEventHandler(EventHandler):
    """ This is the base class for connection to primary master node"""

    def handleNotifyClusterInformation(self, con, packet, cluster_state):
        self.app.cluster_state = cluster_state

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        self.app.nm.update(node_list)
        self.app.notified = True


class MasterRequestEventHandler(MasterBaseEventHandler):
    """ This class handle all answer from primary master node"""

    def handleAnswerClusterState(self, conn, packet, state):
        logging.info("handleAnswerClusterState for a conn")
        self.app.cluster_state = state
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        client_conn.notify(protocol.answerClusterState(state), kw['msg_id'])

    def handleAnswerNewNodes(self, conn, packet, uuid_list):
        logging.info("handleAnswerNewNodes for a conn")
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        client_conn.notify(protocol.answerNewNodes(uuid_list), kw['msg_id'])

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        logging.info("handleAnswerPartitionTable for a conn")
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        # sent client the partition table
        self.app.sendPartitionTable(client_conn, **kw)

    def handleAnswerNodeState(self, conn, packet, uuid, state):
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        p = protocol.answerNodeState(uuid, state)
        client_conn.notify(p, kw['msg_id'])

    def handleNoError(self, conn, packet, msg):
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        p = protocol.noError(msg)
        client_conn.notify(p, kw['msg_id'])

    def handleProtocolError(self, conn, packet, msg):
        client_conn, kw = self.app.dispatcher.retrieve(packet.getId())
        p = protocol.protocolError(msg)
        client_conn.notify(p, kw['msg_id'])


class MasterMonitoringEventHandler(MasterBaseEventHandler):
    """This class deals with events for monitoring cluster."""

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        app = self.app
        if ptid < app.ptid:
            # Ignore this packet.
            # XXX: is it safe ?
            return
        app.ptid = ptid
        app.pt.update(cell_list, app.nm)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        nm = app.nm
        pt = app.pt
        node = app.nm.getNodeByUUID(uuid)
        if app.ptid != ptid:
            app.ptid = ptid
            pt.clear()
        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    node.setState(protocol.TEMPORARILY_DOWN_STATE)
                    nm.add(node)
                pt.setCell(offset, node, state)
        pt.log()

