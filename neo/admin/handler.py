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

from neo import *
from neo.lib import logging
from neo.lib.exception import NotReadyError, PrimaryFailure, ProtocolError
from neo.lib.handler import EventHandler
from neo.lib.protocol import uuid_str, NodeTypes, Packets
from neo.lib.pt import PartitionTable

NOT_CONNECTED_MESSAGE = 'Not connected to a primary master.'

def AdminEventHandlerType(name, bases, d):
    def check_connection(func):
        return lambda self, conn, *args, **kw: \
            self._checkConnection(conn) and func(self, conn, *args, **kw)

    def forward_ask(klass):
        return lambda self, conn, *args: self.app.master_conn.ask(
            klass(*args), conn=conn, msg_id=conn.getPeerId())

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
    return type(name, bases, {k: v if k[0] == '_' else check_connection(v)
                              for k, v in six.iteritems(d)})
AdminEventHandlerType.__prepare__ = lambda *_: {} # PY3: only for six

class AdminEventHandler(six.with_metaclass(AdminEventHandlerType, EventHandler)):
    """This class deals with events for administrating cluster."""

    def _checkConnection(self, conn):
        if self.app.master_conn is None:
            raise NotReadyError(NOT_CONNECTED_MESSAGE)
        return True

    def requestIdentification(self, conn, node_type, uuid, address, name, *_):
        if node_type != NodeTypes.ADMIN:
            raise ProtocolError("reject non-admin node")
        app = self.app
        try:
            backup = app.backup_dict[name]
        except KeyError:
            raise ProtocolError("unknown backup cluster %r" % name)
        if backup.conn is not None:
            raise ProtocolError("already connected")
        backup.conn = conn
        conn.setHandler(app.backup_handler)
        conn.answer(Packets.AcceptIdentification(
            NodeTypes.ADMIN, None, None))

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

    def askMonitorInformation(self, conn):
        self.app.askMonitorInformation(conn)


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
            forward.send(packet, kw['msg_id'])

    def answerClusterState(self, conn, state):
        self.app.updateMonitorInformation(None, cluster_state=state)

    notifyClusterInformation = answerClusterState

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        app = self.app
        app.pt = object.__new__(PartitionTable)
        app.pt.load(ptid, num_replicas, row_list, app.nm)
        app.partitionTableUpdated()

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        app = self.app
        app.pt.update(ptid, num_replicas, cell_list, app.nm)
        app.partitionTableUpdated()

    def notifyNodeInformation(self, *args):
        super(MasterEventHandler, self).notifyNodeInformation(*args)
        self.app.partitionTableUpdated()

    def notifyUpstreamAdmin(self, conn, addr):
        app = self.app
        node = app.upstream_admin
        if node is None:
            node = app.upstream_admin = app.nm.createAdmin()
        elif node.getAddress() == addr:
            return
        node.setAddress(addr)
        if app.upstream_admin_conn:
            app.upstream_admin_conn.close()
        else:
            app.connectToUpstreamAdmin()

    def answerLastTransaction(self, conn, ltid):
        app = self.app
        app.ltid = ltid
        app.maybeNotify(None)

    def answerRecovery(self, name, ptid, backup_tid, truncate_tid):
        self.app.backup_tid = backup_tid

def monitor(func):
    def wrapper(self, conn, *args, **kw):
        for name, backup in six.iteritems(self.app.backup_dict):
            if backup.conn is conn:
                return func(self, name, *args, **kw)
        raise AssertionError
    return wrapper

class BackupHandler(EventHandler):

    @monitor
    def connectionClosed(self, name):
        app = self.app
        old = app.backup_dict[name]
        new = app.backup_dict[name] = old.__class__()
        new.max_lag = old.max_lag
        app.maybeNotify(name)

    @monitor
    def notifyMonitorInformation(self, name, info):
        self.app.updateMonitorInformation(name, **info)

    @monitor
    def answerRecovery(self, name, ptid, backup_tid, truncate_tid):
        self.app.backup_dict[name].backup_tid = backup_tid

    @monitor
    def answerLastTransaction(self, name, ltid):
        app = self.app
        app.backup_dict[name].ltid = ltid
        app.maybeNotify(name)


class UpstreamAdminHandler(AdminEventHandler):

    def _checkConnection(self, conn):
        assert conn is self.app.upstream_admin_conn
        return super(UpstreamAdminHandler, self)._checkConnection(conn)

    def connectionClosed(self, conn):
        app = self.app
        if conn is app.upstream_admin_conn:
            app.connectToUpstreamAdmin()

    connectionFailed = connectionClosed

    def connectionCompleted(self, conn):
        super(UpstreamAdminHandler, self).connectionCompleted(conn)
        conn.ask(Packets.RequestIdentification(NodeTypes.ADMIN,
            None, None, self.app.name, None, {}))

    def _acceptIdentification(self, node):
        node.send(Packets.NotifyMonitorInformation({
            'cluster_state': self.app.cluster_state,
            'down': self.app.down,
            'pt_summary': self.app.pt_summary,
            }))
