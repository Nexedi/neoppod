#
# Copyright (C) 2009-2015  Nexedi SA
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

from neo.lib import logging, protocol
from neo.lib.handler import EventHandler
from neo.lib.protocol import uuid_str, Packets
from neo.lib.exception import PrimaryFailure

def check_primary_master(func):
    def wrapper(self, *args, **kw):
        if self.app.bootstrapped:
            return func(self, *args, **kw)
        raise protocol.NotReadyError('Not connected to a primary master.')
    return wrapper

def forward_ask(klass):
    return check_primary_master(lambda self, conn, *args, **kw:
        self.app.master_conn.ask(klass(*args, **kw),
                                 conn=conn, msg_id=conn.getPeerId()))

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    @check_primary_master
    def askPartitionList(self, conn, min_offset, max_offset, uuid):
        logging.info("ask partition list from %s to %s for %s",
                     min_offset, max_offset, uuid_str(uuid))
        self.app.sendPartitionTable(conn, min_offset, max_offset, uuid)

    @check_primary_master
    def askNodeList(self, conn, node_type):
        if node_type is None:
            node_type = 'all'
            node_filter = None
        else:
            node_filter = lambda n: n.getType() is node_type
        logging.info("ask list of %s nodes", node_type)
        node_list = self.app.nm.getList(node_filter)
        node_information_list = [node.asTuple() for node in node_list ]
        p = Packets.AnswerNodeList(node_information_list)
        conn.answer(p)

    @check_primary_master
    def askClusterState(self, conn):
        conn.answer(Packets.AnswerClusterState(self.app.cluster_state))

    @check_primary_master
    def askPrimary(self, conn):
        master_node = self.app.master_node
        conn.answer(Packets.AnswerPrimary(master_node.getUUID()))

    askLastIDs = forward_ask(Packets.AskLastIDs)
    askLastTransaction = forward_ask(Packets.AskLastTransaction)
    addPendingNodes = forward_ask(Packets.AddPendingNodes)
    tweakPartitionTable = forward_ask(Packets.TweakPartitionTable)
    setClusterState = forward_ask(Packets.SetClusterState)
    setNodeState = forward_ask(Packets.SetNodeState)
    checkReplicas = forward_ask(Packets.CheckReplicas)


class MasterEventHandler(EventHandler):
    """ This class is just used to dispacth message to right handler"""

    def _connectionLost(self, conn):
        app = self.app
        if app.listening_conn: # if running
            assert app.master_conn in (conn, None)
            conn.cancelRequests("connection to master lost")
            app.reset()
            app.uuid = None
            raise PrimaryFailure

    def connectionFailed(self, conn):
        self._connectionLost(conn)

    def connectionClosed(self, conn):
        self._connectionLost(conn)

    def dispatch(self, conn, packet, kw={}):
        if 'conn' in kw:
            # expected answer
            if packet.isResponse():
                packet.setId(kw['msg_id'])
                kw['conn'].answer(packet)
            else:
                self.app.request_handler.dispatch(conn, packet, kw)
        else:
            # unexpected answers and notifications
            super(MasterEventHandler, self).dispatch(conn, packet, kw)

    def answerClusterState(self, conn, state):
        self.app.cluster_state = state

    def answerNodeInformation(self, conn):
        # XXX: This will no more exists when the initialization module will be
        # implemented for factorize code (as done for bootstrap)
        logging.debug("answerNodeInformation")

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
        self.app.nm.update(node_list)

class MasterRequestEventHandler(EventHandler):
    """ This class handle all answer from primary master node"""
    # XXX: to be deleted ?
