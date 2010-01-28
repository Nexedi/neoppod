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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging

from neo.handler import EventHandler
from neo import protocol
from neo.protocol import Packets
from neo.exception import PrimaryFailure
from neo.util import dump

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    def askPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        logging.info("ask partition list from %s to %s for %s" %
                (min_offset, max_offset, dump(uuid)))
        app = self.app
        # check we have one pt otherwise ask it to PMN
        if app.pt is None:
            if self.app.master_conn is None:
                raise protocol.NotReadyError('Not connected to a primary ' \
                        'master.')
            p = Packets.AskPartitionTable([])
            msg_id = self.app.master_conn.ask(p)
            app.dispatcher.register(msg_id, conn,
                                    {'min_offset' : min_offset,
                                     'max_offset' : max_offset,
                                     'uuid' : uuid,
                                     'msg_id' : packet.getId()})
        else:
            app.sendPartitionTable(conn, min_offset, max_offset, uuid,
                    packet.getId())


    def askNodeList(self, conn, packet, node_type):
        logging.info("ask node list for %s" %(node_type))
        def node_filter(n):
            return n.getType() is node_type
        node_list = self.app.nm.getList(node_filter)
        node_information_list = [node.asTuple() for node in node_list ]
        p = Packets.AnswerNodeList(node_information_list)
        conn.answer(p, packet.getId())

    def setNodeState(self, conn, packet, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s" %(dump(uuid), state))
        node = self.app.nm.getByUUID(uuid)
        if node is None:
            raise protocol.ProtocolError('invalid uuid')
        if node.getState() == state and modify_partition_table is False:
            # no change
            p = protocol.ack('no change')
            conn.answer(p, packet.getId())
            return
        # forward to primary master node
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        p = Packets.SetNodeState(uuid, state, modify_partition_table)
        msg_id = self.app.master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def setClusterState(self, conn, packet, state):
        # forward to primary
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        p = Packets.SetClusterState(state)
        msg_id = self.app.master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def addPendingNodes(self, conn, packet, uuid_list):
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        logging.info('Add nodes %s' % [dump(uuid) for uuid in uuid_list])
        # forward the request to primary
        msg_id = self.app.master_conn.ask(Packets.AddPendingNodes(uuid_list))
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : packet.getId()})

    def askClusterState(self, conn, packet):
        if self.app.cluster_state is None:
            if self.app.master_conn is None:
                raise protocol.NotReadyError('Not connected to a primary ' \
                        'master.')
            # required it from PMN first
            msg_id = self.app.master_conn.ask(Packets.AskClusterState())
            self.app.dispatcher.register(msg_id, conn,
                    {'msg_id' : packet.getId()})
        else:
            conn.answer(Packets.AnswerClusterState(self.app.cluster_state),
                packet.getId())

    def askPrimary(self, conn, packet):
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        master_node = self.app.master_node
        conn.answer(Packets.AnswerPrimary(master_node.getUUID(), []),
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

    def answerNodeInformation(self, conn, packet):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("answerNodeInformation")

    def answerPartitionTable(self, conn, packet, ptid, row_list):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("answerPartitionTable")

    def notifyPartitionChanges(self, conn, packet, ptid, cell_list):
        app = self.app
        if ptid < app.ptid:
            # Ignore this packet.
            return
        app.ptid = ptid
        app.pt.update(ptid, cell_list, app.nm)

    def sendPartitionTable(self, conn, packet, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        nm = app.nm
        pt = app.pt
        node = app.nm.getByUUID(uuid)
        if app.ptid != ptid:
            app.ptid = ptid
            pt.clear()
        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getByUUID(uuid)
                if node is None:
                    nm.createStorage(uuid=uuid)
                pt.setCell(offset, node, state)
        pt.log()

    def notifyClusterInformation(self, conn, packet, cluster_state):
        self.app.cluster_state = cluster_state

    def notifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        app.nm.update(node_list)
        if not app.pt.filled():
            # Re-ask partition table, in case node change filled it.
            # XXX: we should only ask it if received states indicates it is
            # possible (ignore TEMPORARILY_DOWN for example)
            conn.ask(Packets.AskPartitionTable([]))

class MasterRequestEventHandler(EventHandler):
    """ This class handle all answer from primary master node"""

    def __answerNeoCTL(self, msg_id, packet):
        client_conn, kw = self.app.dispatcher.pop(msg_id)
        client_conn.answer(packet, kw['msg_id'])

    def answerClusterState(self, conn, packet, state):
        logging.info("answerClusterState for a conn")
        self.app.cluster_state = state
        self.__answerNeoCTL(packet.getId(),
                            Packets.AnswerClusterState(state))

    def answerNewNodes(self, conn, packet, uuid_list):
        logging.info("answerNewNodes for a conn")
        self.__answerNeoCTL(packet.getId(),
                            Packets.AnswerNewNodes(uuid_list))

    def answerPartitionTable(self, conn, packet, ptid, row_list):
        logging.info("answerPartitionTable for a conn")
        client_conn, kw = self.app.dispatcher.pop(packet.getId())
        # sent client the partition table
        self.app.sendPartitionTable(client_conn, **kw)

    def answerNodeState(self, conn, packet, uuid, state):
        self.__answerNeoCTL(packet.getId(),
                            Packets.AnswerNodeState(uuid, state))

    def ack(self, conn, packet, msg):
        self.__answerNeoCTL(packet.getId(), protocol.ack(msg))

    def protocolError(self, conn, packet, msg):
        self.__answerNeoCTL(packet.getId(), protocol.protocolError(msg))

