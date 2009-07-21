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

from neo.handler import EventHandler
from neo.protocol import UnexpectedPacketError
from neo.exception import OperationFailure
from neo.util import dump

class CommandEventHandler(EventHandler):
    """ Base handler for command """

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        raise UnexpectedPacketError

    def connectionCompleted(self, conn):
        # connected to admin node
        self.app.trying_admin_node = False
        EventHandler.connectionCompleted(self, conn)

    def connectionFailed(self, conn):
        EventHandler.connectionFailed(self, conn)
        raise OperationFailure, "impossible to connect to admin node %s:%d" % conn.getAddress()

    def timeoutExpired(self, conn):
        EventHandler.timeoutExpired(self, conn)
        raise OperationFailure, "connection to admin node %s:%d timeout" % conn.getAddress()

    def connectionClosed(self, conn):
        if self.app.trying_admin_node:
            raise OperationFailure, "cannot connect to admin node %s:%d" % conn.getAddress()
        EventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        EventHandler.peerBroken(self, conn)
        raise OperationFailure, "connect to admin node %s:%d broken" % conn.getAddress()

    def handleAnswerPartitionList(self, conn, packet, ptid, row_list):
        data = ""
        if len(row_list) == 0:
            data = "No partition"
        else:
            for offset, cell_list in row_list:
                data += "\n%s | " %offset
                for uuid, state in cell_list:
                    data += "%s - %s |" %(dump(uuid), state)
        self.app.result = data

    def handleAnswerNodeList(self, conn, packet, node_list):
        data = ""
        if len(node_list) == 0:
            data = "No Node"
        else:
            for node_type, address, uuid, state in node_list:
                if address is None:
                    address = (None, None)
                ip, port = address
                data += "\n%s - %s - %s:%s - %s" % (node_type, dump(uuid), ip, port, state)
        self.app.result = data

    def handleAnswerNodeState(self, conn, packet, uuid, state):
        self.app.result = "Node %s set to state %s" %(dump(uuid), state)

    def handleAnswerClusterState(self, conn, packet, state):
        self.app.result = "Cluster state : %s" % state

    def handleAnswerNewNodes(self, conn, packet, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        self.app.result = 'New storage nodes : %s' % uuids

    def handleNoError(self, conn, packet, msg):
        self.app.result = msg
