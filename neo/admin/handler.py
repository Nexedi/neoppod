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
from neo.protocol import INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        ADMIN_NODE_TYPE, DISCARDED_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection
from neo import protocol
from neo.protocol import Packet, UnexpectedPacketError
from neo.pt import PartitionTable
from neo.exception import PrimaryFailure
from neo.util import dump
from neo import decorators, handler


class BaseEventHandler(EventHandler):
    """ Base handler for admin node """

    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

class AdminEventHandler(BaseEventHandler):
    """This class deals with events for administrating cluster."""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # we only accept connection from command tool
        BaseEventHandler.connectionAccepted(self, conn, s, addr)

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
            node_information_list.append((node.getNodeType(), ip, port, node.getUUID(), node.getState()))
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

class MasterEventHandler(BaseEventHandler):
    """ This class is just used to dispacth message to right handler"""

    def dispatch(self, conn, packet):
        if self.app.dispatcher.registered(packet.getId()):
            # answer to a request
            self.app.request_handler.dispatch(conn, packet)
        else:
            # monitoring phase
            self.app.monitoring_handler.dispatch(conn, packet)


class MasterBaseEventHandler(BaseEventHandler):
    """ This is the base class for connection to primary master node"""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        raise UnexpectedPacketError

    def connectionCompleted(self, conn):
        app = self.app
        if app.trying_master_node is None:
            # Should not happen.
            raise RuntimeError('connection completed while not trying to connect')

        # Ask a primary master.
        conn.ask(protocol.askPrimaryMaster())
        EventHandler.connectionCompleted(self, conn)

    def connectionFailed(self, conn):
        app = self.app

        if app.primary_master_node and conn.getUUID() == app.primary_master_node.getUUID():
            raise PrimaryFailure

        if app.trying_master_node is None:
            # Should not happen.
            raise RuntimeError('connection failed while not trying to connect')

        if app.trying_master_node is app.primary_master_node:
            # Tried to connect to a primary master node and failed.
            # So this would effectively mean that it is dead.
            app.primary_master_node = None

        app.trying_master_node = None

        EventHandler.connectionFailed(self, conn)

    def timeoutExpired(self, conn):
        app = self.app

        if app.primary_master_node and conn.getUUID() == app.primary_master_node.getUUID():
            raise PrimaryFailure

        if app.trying_master_node is app.primary_master_node:
            # If a primary master node timeouts, I should not rely on it.
            app.primary_master_node = None

        app.trying_master_node = None

        EventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        app = self.app

        if app.primary_master_node and conn.getUUID() == app.primary_master_node.getUUID():
            raise PrimaryFailure

        if app.trying_master_node is app.primary_master_node:
            # If a primary master node closes, I should not rely on it.
            app.primary_master_node = None

        app.trying_master_node = None

        EventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        app = self.app

        if app.primary_master_node and conn.getUUID() == app.primary_master_node.getUUID():
            raise PrimaryFailure

        if app.trying_master_node is app.primary_master_node:
            # If a primary master node gets broken, I should not rely
            # on it.
            app.primary_master_node = None

        app.trying_master_node = None

        EventHandler.peerBroken(self, conn)

    @decorators.identification_required
    def handleNotifyClusterInformation(self, con, packet, cluster_state):
        self.app.cluster_state = cluster_state

    @decorators.identification_required
    def handleNotifyNodeInformation(self, conn, packet, node_list):
        uuid = conn.getUUID()
        app = self.app
        nm = app.nm
        node = nm.getNodeByUUID(uuid)
        # This must be sent only by a primary master node.
        # Note that this may be sent before I know that it is
        # a primary master node.
        if node.getNodeType() != MASTER_NODE_TYPE:
            logging.warn('ignoring notify node information from %s',
                         dump(uuid))
            return
        for node_type, ip_address, port, uuid, state in node_list:
            # Register/update  nodes.
            addr = (ip_address, port)
            # Try to retrieve it from nm
            n = None
            if uuid != INVALID_UUID:
                n = nm.getNodeByUUID(uuid)
            if n is None:
                n = nm.getNodeByServer(addr)
                if n is not None and uuid != INVALID_UUID:
                    # node only exists by address, remove it
                    nm.remove(n)
                    n = None
            elif n.getServer() != addr:
                # same uuid but different address, remove it
                nm.remove(n)
                n = None

            if node_type == MASTER_NODE_TYPE:
                if n is None:
                    n = MasterNode(server = addr)
                    nm.add(n)
                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)
                else:
                    n.setUUID(INVALID_UUID)
            elif node_type in (STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                if uuid == INVALID_UUID:
                    # No interest.
                    continue
                if n is None:
                    if node_type == STORAGE_NODE_TYPE:
                        n = StorageNode(server = addr, uuid = uuid)
                    elif node_type == CLIENT_NODE_TYPE:
                        n = ClientNode(server = addr, uuid = uuid)
                    elif node_type == ADMIN_NODE_TYPE:
                        n = AdminNode(server = addr, uuid = uuid)
                    nm.add(n)
            else:
                logging.warning("unknown node type %s" %(node_type))
                continue

            n.setState(state)

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

    @decorators.identification_required
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


class MasterBootstrapEventHandler(MasterBaseEventHandler):
    """This class manage the bootstrap part to the primary master node"""

    def handleNotReady(self, conn, packet, message):
        app = self.app
        if app.trying_master_node is not None:
            app.trying_master_node = None

        conn.close()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        if node_type != MASTER_NODE_TYPE:
            # The peer is not a master node!
            logging.error('%s:%d is not a master node', ip_address, port)
            app.nm.remove(node)
            conn.close()
            return
        if conn.getAddress() != (ip_address, port):
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                          conn.getAddress()[0], conn.getAddress()[1],
                          ip_address, port)
            app.nm.remove(node)
            conn.close()
            return

        if app.num_partitions is None:
            app.num_partitions = num_partitions
            app.num_replicas = num_replicas
            app.pt = PartitionTable(num_partitions, num_replicas)
        elif app.num_partitions != num_partitions:
            raise RuntimeError('the number of partitions is inconsistent')
        elif app.num_replicas != num_replicas:
            raise RuntimeError('the number of replicas is inconsistent')

        conn.setUUID(uuid)
        node.setUUID(uuid)

        if your_uuid != INVALID_UUID:
            # got an uuid from the primary master
            app.uuid = your_uuid

        conn.ask(protocol.askNodeInformation())
        conn.ask(protocol.askPartitionTable([]))
        logging.info("changing handler for master conn")
        conn.setHandler(MasterEventHandler(self.app))

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        app = self.app
        # Register new master nodes.
        for ip_address, port, uuid in known_master_list:
            addr = (ip_address, port)
            n = app.nm.getNodeByServer(addr)
            if n is None:
                n = MasterNode(server = addr)
                app.nm.add(n)

            if uuid != INVALID_UUID:
                # If I don't know the UUID yet, believe what the peer
                # told me at the moment.
                if n.getUUID() is None or n.getUUID() != uuid:
                    n.setUUID(uuid)
            else:
                n.setUUID(INVALID_UUID)

        if primary_uuid != INVALID_UUID:
            primary_node = app.nm.getNodeByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                pass
            else:
                app.primary_master_node = primary_node
                if app.trying_master_node is primary_node:
                    # I am connected to the right one.
                    logging.info('connected to a primary master node')
                    # This is a workaround to prevent handling of
                    # packets for the verification phase.
                else:
                    app.trying_master_node = None
                    conn.close()
        else:
            if app.primary_master_node is not None:
                # The primary master node is not a primary master node
                # any longer.
                app.primary_master_node = None

            app.trying_master_node = None
            conn.close()

        p = protocol.requestNodeIdentification(ADMIN_NODE_TYPE,
                app.uuid, app.server[0], app.server[1], app.name)
        conn.ask(p)


class MasterMonitoringEventHandler(MasterBaseEventHandler):
    """This class deals with events for monitoring cluster."""

    @decorators.identification_required
    def handleAnswerNodeInformation(self, conn, packet, node_list):
        logging.info("handleAnswerNodeInformation")

    @decorators.identification_required
    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        logging.info("handleAnswerPartitionTable")

    @decorators.identification_required
    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        app = self.app
        nm = app.nm
        pt = app.pt
        uuid = conn.getUUID()
        node = app.nm.getNodeByUUID(uuid)
        # This must be sent only by primary master node
        if node.getNodeType() != MASTER_NODE_TYPE \
               or app.primary_master_node is None \
               or app.primary_master_node.getUUID() != uuid:
            return

        if app.ptid >= ptid:
            # Ignore this packet.
            return

        app.ptid = ptid
        for offset, uuid, state in cell_list:
            node = nm.getNodeByUUID(uuid)
            if node is None:
                node = StorageNode(uuid = uuid)
                if uuid != app.uuid:
                    node.setState(TEMPORARILY_DOWN_STATE)
                nm.add(node)
            pt.setCell(offset, node, state)
        pt.log()

    @decorators.identification_required
    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        nm = app.nm
        pt = app.pt
        node = app.nm.getNodeByUUID(uuid)
        # This must be sent only by primary master node
        if node.getNodeType() != MASTER_NODE_TYPE:
            return

        if app.ptid != ptid:
            app.ptid = ptid
            pt.clear()
        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    node.setState(TEMPORARILY_DOWN_STATE)
                    nm.add(node)
                pt.setCell(offset, node, state)

        pt.log()
