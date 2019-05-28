#
# Copyright (C) 2009-2019  Nexedi SA
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
from neo.lib.pt import PartitionTable
from neo.lib.exception import PrimaryFailure

def AdminEventHandlerType(name, bases, d):
    def check_primary_master(func):
        def wrapper(self, *args, **kw):
            if self.app.master_conn is not None:
                return func(self, *args, **kw)
            raise protocol.NotReadyError('Not connected to a primary master.')
        return wrapper

    def forward_ask(klass):
        return lambda self, conn, *args: self.app.master_conn.ask(
            klass(*args), conn=conn, msg_id=conn.getPeerId())

    del d['__metaclass__']
    for x in (
            Packets.AddPendingNodes,
            Packets.AskLastIDs,
            Packets.AskLastTransaction,
            Packets.AskRecovery,
            Packets.CheckReplicas,
            Packets.Repair,
            Packets.SetClusterState,
            Packets.SetNodeState,
            Packets.SetNumReplicas,
            Packets.Truncate,
            Packets.TweakPartitionTable,
        ):
        d[x.handler_method_name] = forward_ask(x)
    return type(name, bases, {k: v if k[0] == '_' else check_primary_master(v)
                              for k, v in d.iteritems()})

class AdminEventHandler(EventHandler):
    """This class deals with events for administrating cluster."""

    __metaclass__ = AdminEventHandlerType

    def askPartitionList(self, conn, min_offset, max_offset, uuid):
        logging.info("ask partition list from %s to %s for %s",
                     min_offset, max_offset, uuid_str(uuid))
        self.app.sendPartitionTable(conn, min_offset, max_offset, uuid)

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

    def askClusterState(self, conn):
        conn.answer(Packets.AnswerClusterState(self.app.cluster_state))

    def askPrimary(self, conn):
        master_node = self.app.master_node
        conn.answer(Packets.AnswerPrimary(master_node.getUUID()))

    def flushLog(self, conn):
        self.app.master_conn.send(Packets.FlushLog())
        super(AdminEventHandler, self).flushLog(conn)


class MasterEventHandler(EventHandler):
    """ This class is just used to dispatch message to right handler"""

    def connectionClosed(self, conn):
        app = self.app
        if app.listening_conn: # if running
            assert app.master_conn in (conn, None)
            conn.cancelRequests("connection to master lost")
            app.reset()
            app.uuid = None
            raise PrimaryFailure

    def dispatch(self, conn, packet, kw={}):
        forward = kw.get('conn')
        if forward is None:
            super(MasterEventHandler, self).dispatch(conn, packet, kw)
        else:
            packet.setId(kw['msg_id'])
            forward.answer(packet)

    def answerClusterState(self, conn, state):
        self.app.cluster_state = state

    notifyClusterInformation = answerClusterState

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        pt = self.app.pt = object.__new__(PartitionTable)
        pt.load(ptid, num_replicas, row_list, self.app.nm)

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        self.app.pt.update(ptid, num_replicas, cell_list, self.app.nm)
