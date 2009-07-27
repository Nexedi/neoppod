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

from neo.connector import getConnectorHandler
from neo.connection import ClientConnection
from neo.event import EventManager
from neo.neoctl.handler import CommandEventHandler
from neo import protocol

class NeoCTL(object):

    connection = None
    connected = False

    def __init__(self, ip, port, handler):
        self.connector_handler = getConnectorHandler(handler)
        self.server = (ip, port)
        self.em = EventManager()
        self.handler = CommandEventHandler(self)
        self.response_queue = []

    def __getConnection(self):
        while not self.connected:
            self.connection = ClientConnection(
                self.em, self.handler, addr=self.server,
                connector_handler=self.connector_handler)
            while not self.connected and self.connection is not None:
                self.em.poll(0)
        return self.connection

    def __ask(self, packet):
        # TODO: make thread-safe
        connection = self.__getConnection()
        connection.ask(packet)
        response_queue = self.response_queue
        assert len(response_queue) == 0
        while len(response_queue) == 0:
            self.em.poll(0)
            if not self.connected:
                raise Exception, 'Connection closed'
        return response_queue.pop()

    def enableStorageList(self, node_list):
        """
          Put all given storage nodes in "running" state.
        """
        packet = protocol.addPendingNodes(node_list)
        response = self.__ask(packet)
        assert response[0] == protocol.ERROR
        assert response[1] == protocol.NO_ERROR_CODE

    def setClusterState(self, state):
        """
          Set cluster state.
        """
        packet = protocol.setClusterState(state)
        response = self.__ask(packet)
        #assert response[0] == protocol.ANSWER_CLUSTER_STATE
        #assert state == response[1]

    def setNodeState(self, node, state, update_partition_table=False):
        """
          Set node state, and allow (or not) updating partition table.
        """
        if update_partition_table:
            update_partition_table = 1
        else:
            update_partition_table = 0
        packet = protocol.setNodeState(node, state, update_partition_table)
        response = self.__ask(packet)
        assert response[0] == protocol.ANSWER_NODE_STATE
        assert node == response[1]
        assert state == response[2]

    def getClusterState(self):
        """
          Get cluster state.
        """
        packet = protocol.askClusterState()
        response = self.__ask(packet)
        assert response[0] == protocol.ANSWER_CLUSTER_STATE
        return response[1]

    def getNodeList(self, node_type=None):
        """
          Get a list of nodes, filtering with given type.
        """
        packet = protocol.askNodeList(node_type)
        response = self.__ask(packet)
        assert response[0] == protocol.ANSWER_NODE_LIST
        return response[1]

    def getPartitionRowList(self, min_offset=0, max_offset=0, node=None):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
        """
        packet = protocol.askPartitionList(min_offset, max_offset, node)
        response = self.__ask(packet)
        assert response[0] == protocol.ANSWER_PARTITION_LIST
        return (response[1], response[2])

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        self.setClusterState(protocol.VERIFYING)

    def dropNode(self, node):
        """
          Set node into "down" state and remove it from partition table.
        """
        self.setNodeState(node, protocol.DOWN_STATE, update_partition_table=1)

