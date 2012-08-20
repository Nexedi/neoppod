#
# Copyright (C) 2006-2012  Nexedi SA
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

from neo.lib.connector import getConnectorHandler
from neo.lib.connection import ClientConnection
from neo.lib.event import EventManager
from neo.lib.protocol import ClusterStates, NodeStates, ErrorCodes, Packets
from neo.lib.util import getConnectorFromAddress
from neo.lib.node import NodeManager
from .handler import CommandEventHandler

class NotReadyException(Exception):
    pass

class NeoCTL(object):

    connection = None
    connected = False

    def __init__(self, address):
        connector_name = getConnectorFromAddress(address)
        self.connector_handler = getConnectorHandler(connector_name)
        self.nm = nm = NodeManager()
        self.server = nm.createAdmin(address=address)
        self.em = EventManager()
        self.handler = CommandEventHandler(self)
        self.response_queue = []

    def close(self):
        self.em.close()
        self.nm.close()
        del self.__dict__

    def __getConnection(self):
        if not self.connected:
            self.connection = ClientConnection(self.em, self.handler,
                    node=self.server, connector=self.connector_handler())
            while self.connection is not None:
                if self.connected:
                    break
                self.em.poll(1)
            else:
                raise NotReadyException('not connected')
        return self.connection

    def __ask(self, packet):
        # TODO: make thread-safe
        connection = self.__getConnection()
        connection.ask(packet)
        response_queue = self.response_queue
        assert len(response_queue) == 0
        while self.connected:
            self.em.poll(1)
            if response_queue:
                break
        else:
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
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def tweakPartitionTable(self, uuid_list=()):
        response = self.__ask(Packets.TweakPartitionTable(uuid_list))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def setClusterState(self, state):
        """
          Set cluster state.
        """
        packet = Packets.SetClusterState(state)
        response = self.__ask(packet)
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def _setNodeState(self, node, state):
        """
          Kill node, or remove it permanently
        """
        response = self.__ask(Packets.SetNodeState(node, state))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def getClusterState(self):
        """
          Get cluster state.
        """
        packet = Packets.AskClusterState()
        response = self.__ask(packet)
        if response[0] != Packets.AnswerClusterState:
            raise RuntimeError(response)
        return response[1]

    def getLastIds(self):
        response = self.__ask(Packets.AskLastIDs())
        if response[0] != Packets.AnswerLastIDs:
            raise RuntimeError(response)
        return response[1:]

    def getLastTransaction(self):
        response = self.__ask(Packets.AskLastTransaction())
        if response[0] != Packets.AnswerLastTransaction:
            raise RuntimeError(response)
        return response[1]

    def getNodeList(self, node_type=None):
        """
          Get a list of nodes, filtering with given type.
        """
        packet = Packets.AskNodeList(node_type)
        response = self.__ask(packet)
        if response[0] != Packets.AnswerNodeList:
            raise RuntimeError(response)
        return response[1] # node_list

    def getPartitionRowList(self, min_offset=0, max_offset=0, node=None):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
        """
        packet = Packets.AskPartitionList(min_offset, max_offset, node)
        response = self.__ask(packet)
        if response[0] != Packets.AnswerPartitionList:
            raise RuntimeError(response)
        return response[1:3] # ptid, row_list

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        return self.setClusterState(ClusterStates.VERIFYING)

    def killNode(self, node):
        return self._setNodeState(node, NodeStates.UNKNOWN)

    def dropNode(self, node):
        return self._setNodeState(node, NodeStates.DOWN)

    def getPrimary(self):
        """
          Return the primary master UUID.
        """
        packet = Packets.AskPrimary()
        response = self.__ask(packet)
        if response[0] != Packets.AnswerPrimary:
            raise RuntimeError(response)
        return response[1]

    def checkReplicas(self, *args):
        response = self.__ask(Packets.CheckReplicas(*args))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]
