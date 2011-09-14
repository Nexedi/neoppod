#
# Copyright (C) 2009-2010  Nexedi SA
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
from neo.lib import protocol
from neo.lib.protocol import Packets, Errors
from neo.lib.exception import PrimaryFailure
from neo.lib.util import dump

def forward_ask(klass):
    def wrapper(self, conn, *args, **kw):
        app = self.app
        if app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        msg_id = app.master_conn.ask(klass(*args, **kw))
        app.dispatcher.register(msg_id, conn, {'msg_id': conn.getPeerId()})
    return wrapper

def forward_answer(klass):
    def wrapper(self, conn, *args, **kw):
        packet = klass(*args, **kw)
        self._answerNeoCTL(conn, packet)
    return wrapper

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    def askPartitionList(self, conn, min_offset, max_offset, uuid):
        neo.lib.logging.info("ask partition list from %s to %s for %s" %
                (min_offset, max_offset, dump(uuid)))
        app = self.app
        # check we have one pt otherwise ask it to PMN
        if app.pt is None:
            if self.app.master_conn is None:
                raise protocol.NotReadyError('Not connected to a primary ' \
                        'master.')
            msg_id = self.app.master_conn.ask(Packets.AskPartitionTable())
            app.dispatcher.register(msg_id, conn,
                                    {'min_offset' : min_offset,
                                     'max_offset' : max_offset,
                                     'uuid' : uuid,
                                     'msg_id' : conn.getPeerId()})
        else:
            app.sendPartitionTable(conn, min_offset, max_offset, uuid)


    def askNodeList(self, conn, node_type):
        if node_type is None:
            node_type = 'all'
            node_filter = None
        else:
            node_filter = lambda n: n.getType() is node_type
        neo.lib.logging.info("ask list of %s nodes", node_type)
        node_list = self.app.nm.getList(node_filter)
        node_information_list = [node.asTuple() for node in node_list ]
        p = Packets.AnswerNodeList(node_information_list)
        conn.answer(p)

    def setNodeState(self, conn, uuid, state, modify_partition_table):
        neo.lib.logging.info("set node state for %s-%s" %(dump(uuid), state))
        node = self.app.nm.getByUUID(uuid)
        if node is None:
            raise protocol.ProtocolError('invalid uuid')
        if node.getState() == state and modify_partition_table is False:
            # no change
            p = Errors.Ack('no change')
            conn.answer(p)
            return
        # forward to primary master node
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        p = Packets.SetNodeState(uuid, state, modify_partition_table)
        msg_id = self.app.master_conn.ask(p)
        self.app.dispatcher.register(msg_id, conn, {'msg_id' : conn.getPeerId()})

    def askClusterState(self, conn):
        if self.app.cluster_state is None:
            if self.app.master_conn is None:
                raise protocol.NotReadyError('Not connected to a primary ' \
                        'master.')
            # required it from PMN first
            msg_id = self.app.master_conn.ask(Packets.AskClusterState())
            self.app.dispatcher.register(msg_id, conn,
                    {'msg_id' : conn.getPeerId()})
        else:
            conn.answer(Packets.AnswerClusterState(self.app.cluster_state))

    def askPrimary(self, conn):
        if self.app.master_conn is None:
            raise protocol.NotReadyError('Not connected to a primary master.')
        master_node = self.app.master_node
        conn.answer(Packets.AnswerPrimary(master_node.getUUID(), []))

    addPendingNodes = forward_ask(Packets.AddPendingNodes)
    setClusterState = forward_ask(Packets.SetClusterState)


class MasterEventHandler(EventHandler):
    """ This class is just used to dispacth message to right handler"""

    def _connectionLost(self, conn):
        app = self.app
        if app.listening_conn: # if running
            assert app.master_conn in (conn, None)
            app.dispatcher.clear()
            app.reset()
            app.uuid = None
            raise PrimaryFailure

    def connectionFailed(self, conn):
        self._connectionLost(conn)

    def connectionClosed(self, conn):
        self._connectionLost(conn)

    def dispatch(self, conn, packet):
        if packet.isResponse() and \
           self.app.dispatcher.registered(packet.getId()):
            # expected answer
            self.app.request_handler.dispatch(conn, packet)
        else:
            # unexpectexd answers and notifications
            super(MasterEventHandler, self).dispatch(conn, packet)

    def answerNodeInformation(self, conn):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        neo.lib.logging.debug("answerNodeInformation")

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        self.app.pt.update(ptid, cell_list, self.app.nm)

    def answerPartitionTable(self, conn, ptid, row_list):
        self.app.pt.load(ptid, row_list, self.app.nm)
        self.app.bootstrapped = True

    def sendPartitionTable(self, conn, ptid, row_list):
        if self.app.bootstrapped:
            self.app.pt.load(ptid, row_list, self.app.nm)

    def notifyClusterInformation(self, conn, cluster_state):
        self.app.cluster_state = cluster_state

    def notifyNodeInformation(self, conn, node_list):
        app = self.app
        app.nm.update(node_list)

class MasterRequestEventHandler(EventHandler):
    """ This class handle all answer from primary master node"""

    def _answerNeoCTL(self, conn, packet):
        msg_id = conn.getPeerId()
        client_conn, kw = self.app.dispatcher.pop(msg_id)
        client_conn.answer(packet)

    def answerClusterState(self, conn, state):
        neo.lib.logging.info("answerClusterState for a conn")
        self.app.cluster_state = state
        self._answerNeoCTL(conn, Packets.AnswerClusterState(state))

    def answerPartitionTable(self, conn, ptid, row_list):
        neo.lib.logging.info("answerPartitionTable for a conn")
        client_conn, kw = self.app.dispatcher.pop(conn.getPeerId())
        # sent client the partition table
        self.app.sendPartitionTable(client_conn)

    ack = forward_answer(Errors.Ack)
    protocolError = forward_answer(Errors.ProtocolError)
