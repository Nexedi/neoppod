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

from neo import logging

from neo.handler import EventHandler
from neo.node import StorageNode
from neo import protocol
from neo.exception import PrimaryFailure
from neo.util import dump

def master_connection_required(handler):
    """ Check if the master connection is established """
    def decorator(self, *args, **kwargs):
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        handler(self, *args, **kwargs)
    return decorator

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    @master_connection_required
    def handleAskPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        logging.info("ask partition list from %s to %s for %s" %(min_offset, max_offset, dump(uuid)))
        app = self.app
        # check we have one pt otherwise ask it to PMN
        if app.pt is None:
            p = protocol.askPartitionTable([])
            msg_id = self.app.master_conn.ask(p)
            app.dispatcher.register(msg_id, conn,
                                    {'min_offset' : min_offset,
                                     'max_offset' : max_offset,
                                     'uuid' : uuid,
                                     'msg_id' : packet.getId()})
        else:
            app.sendPartitionTable(conn, min_offset, max_offset, uuid, packet.getId())


    def handleAskNodeList(self, conn, packet, node_type):
        logging.info("ask node list for %s" %(node_type))
        def node_filter(n):
            return n.getType() is node_type
        node_list = self.app.nm.getNodeList(node_filter)
        node_information_list = []
        for node in node_list:
            try:
                ip, port = node.getServer()
            except TypeError:
                ip = "0.0.0.0"
                port = 0
            node_information_list.append((node.getType(), (ip, port), node.getUUID(), node.getState()))
        p = protocol.answerNodeList(node_information_list)
        conn.answer(p, packet.getId())

    @master_connection_required
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
            conn.answer(p, packet.getId())
            return
        # forward to primary master node
        p = protocol.setNodeState(uuid, state, modify_partition_table)
        msg_id = self.app.master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    @master_connection_required
    def handleSetClusterState(self, conn, packet, state):
        # forward to primary
        p = protocol.setClusterState(state)
        msg_id = self.app.master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    @master_connection_required
    def handleAddPendingNodes(self, conn, packet, uuid_list):
        logging.info('Add nodes %s' % [dump(uuid) for uuid in uuid_list])
        # forward the request to primary
        msg_id = self.app.master_conn.ask(protocol.addPendingNodes(uuid_list))
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    @master_connection_required
    def handleAskClusterState(self, conn, packet):
        if self.app.cluster_state is None:
            # required it from PMN first
            msg_id = self.app.master_conn.ask(protocol.askClusterState())
            self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})
        else:
            conn.answer(protocol.answerClusterState(self.app.cluster_state), 
                packet.getId())

    @master_connection_required
    def handleAskPrimaryMaster(self, conn, packet):
        master_node = self.app.master_node
        conn.answer(protocol.answerPrimaryMaster(master_node.getUUID(), []),
            packet.getId())

class MasterEventHandler(EventHandler):
    """ This class is just used to dispacth message to right handler"""

    def _connectionLost(self, conn):
        app = self.app
        assert app.master_conn in (conn, None)
        app.master_conn = None
        app.master_node = None
        app.uuid = None
        raise PrimaryFailure

    def connectionFailed(self, conn):
        self._connectionLost(conn)

    def timeoutExpired(self, conn):
        self._connectionLost(conn)

    def connectionClosed(self, conn):
        self._connectionLost(conn)

    def peerBroken(self, conn):
        self._connectionLost(conn)

    def dispatch(self, conn, packet):
        if packet.isResponse() and \
           self.app.dispatcher.registered(packet.getId()):
            # expected answer
            self.app.request_handler.dispatch(conn, packet)
        else:
            # unexpectexd answers and notifications
            super(MasterEventHandler, self).dispatch(conn, packet)

    def handleAnswerNodeInformation(self, conn, packet, node_list):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("handleAnswerNodeInformation")

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("handleAnswerPartitionTable")

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        app = self.app
        if ptid < app.ptid:
            # Ignore this packet.
            return
        app.ptid = ptid
        app.pt.update(ptid, cell_list, app.nm)

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

    def handleNotifyClusterInformation(self, conn, packet, cluster_state):
        self.app.cluster_state = cluster_state

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        app.nm.update(node_list)
        if not app.pt.filled():
            # Re-ask partition table, in case node change filled it.
            # XXX: we should only ask it if received states indicates it is
            # possible (ignore TEMPORARILY_DOWN for example)
            conn.ask(protocol.askPartitionTable([]))

class MasterRequestEventHandler(EventHandler):
    """ This class handle all answer from primary master node"""

    def __answerNeoCTL(self, msg_id, packet):
        client_conn, kw = self.app.dispatcher.pop(msg_id)
        client_conn.answer(packet, kw['msg_id'])

    def handleAnswerClusterState(self, conn, packet, state):
        logging.info("handleAnswerClusterState for a conn")
        self.app.cluster_state = state
        self.__answerNeoCTL(packet.getId(),
                            protocol.answerClusterState(state))

    def handleAnswerNewNodes(self, conn, packet, uuid_list):
        logging.info("handleAnswerNewNodes for a conn")
        self.__answerNeoCTL(packet.getId(),
                            protocol.answerNewNodes(uuid_list))

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        logging.info("handleAnswerPartitionTable for a conn")
        client_conn, kw = self.app.dispatcher.pop(packet.getId())
        # sent client the partition table
        self.app.sendPartitionTable(client_conn, **kw)

    def handleAnswerNodeState(self, conn, packet, uuid, state):
        self.__answerNeoCTL(packet.getId(),
                            protocol.answerNodeState(uuid, state))

    def handleNoError(self, conn, packet, msg):
        self.__answerNeoCTL(packet.getId(), protocol.noError(msg))

    def handleProtocolError(self, conn, packet, msg):
        self.__answerNeoCTL(packet.getId(), protocol.protocolError(msg))

