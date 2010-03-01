#
# Copyright (C) 2006-2010  Nexedi SA
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

from neo.connector import getConnectorHandler
from neo.connection import ClientConnection
from neo.event import EventManager
from neo.neoctl.handler import CommandEventHandler
from neo.protocol import ClusterStates, NodeStates, ErrorCodes, Packets

class NotReadyException(Exception):
    pass

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
        if not self.connected:
            self.connection = ClientConnection(self.em, self.handler, 
                    addr=self.server, connector=self.connector_handler())
            while not self.connected and self.connection is not None:
                self.em.poll(0)
            if self.connection is None:
                raise NotReadyException
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
                raise NotReadyException, 'Connection closed'
        response = response_queue.pop()
        if response[0] == Packets.Error and \
           response[1] == ErrorCodes.NOT_READY:
            raise NotReadyException(response[2])
        return response

    def enableStorageList(self, uuid_list):
        """
          Put all given storage nodes in "running" state.
        """
        packet = Packets.AddPendingNodes(uuid_list)
        response = self.__ask(packet)
        assert response[0] == Packets.Error
        assert response[1] == ErrorCodes.ACK

    def setClusterState(self, state):
        """
          Set cluster state.
        """
        packet = Packets.SetClusterState(state)
        response = self.__ask(packet)
        assert response[0] == Packets.Error
        assert response[1] == ErrorCodes.ACK
        return response[1]

    def setNodeState(self, node, state, update_partition_table=False):
        """
          Set node state, and allow (or not) updating partition table.
        """
        if update_partition_table:
            update_partition_table = 1
        else:
            update_partition_table = 0
        packet = Packets.SetNodeState(node, state, update_partition_table)
        response = self.__ask(packet)
        assert response[0] == Packets.Error
        assert response[1] == ErrorCodes.ACK
        return response[1]

    def getClusterState(self):
        """
          Get cluster state.
        """
        packet = Packets.AskClusterState()
        response = self.__ask(packet)
        assert response[0] == Packets.AnswerClusterState
        return response[1]

    def getNodeList(self, node_type=None):
        """
          Get a list of nodes, filtering with given type.
        """
        packet = Packets.AskNodeList(node_type)
        response = self.__ask(packet)
        assert response[0] == Packets.AnswerNodeList
        return response[1]

    def getPartitionRowList(self, min_offset=0, max_offset=0, node=None):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
        """
        packet = Packets.AskPartitionList(min_offset, max_offset, node)
        response = self.__ask(packet)
        assert response[0] == Packets.AnswerPartitionList
        return (response[1], response[2])

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        return self.setClusterState(ClusterStates.VERIFYING)

    def dropNode(self, node):
        """
          Set node into "down" state and remove it from partition table.
        """
        return self.setNodeState(node, NodeStates.DOWN, 
                update_partition_table=1)

    def getPrimary(self):
        """
          Return the primary master UUID.
        """
        packet = Packets.AskPrimary()
        response = self.__ask(packet)
        assert response[0] == Packets.AnswerPrimary
        return response[1]

