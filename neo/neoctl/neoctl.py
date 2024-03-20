#
# Copyright (C) 2006-2019  Nexedi SA
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

import argparse
from neo.lib import util
from neo.lib.app import BaseApplication, buildOptionParser
from neo.lib.connection import ClientConnection, ConnectionClosed
from neo.lib.protocol import ClusterStates, NodeStates, ErrorCodes, Packets
from .handler import CommandEventHandler

class NotReadyException(Exception):
    pass

@buildOptionParser
class NeoCTL(BaseApplication):

    connection = None
    connected = False

    @classmethod
    def _buildOptionParser(cls):
        # XXX: Use argparse sub-commands.
        parser = cls.option_parser
        parser.description = "NEO Control node"
        parser('a', 'address', default='127.0.0.1:9999',
            parse=lambda x: util.parseNodeAddress(x, 9999),
            help="address of an admin node")
        parser.argument('cmd', nargs=argparse.REMAINDER,
            help="command to execute; if not supplied,"
                 " the list of available commands is displayed")

    def __init__(self, address, **kw):
        super(NeoCTL, self).__init__(**kw)
        self.server = self.nm.createAdmin(address=address)
        self.handler = CommandEventHandler(self)
        self.response_queue = []

    def __getConnection(self):
        if not self.connected:
            self.connection = ClientConnection(self, self.handler, self.server)
            # Never delay reconnection to master. This speeds up unit tests
            # and it should not change anything for normal use.
            try:
                self.connection.setReconnectionNoDelay()
            except ConnectionClosed:
                self.connection = None
            while not self.connected:
                if self.connection is None:
                    raise NotReadyException('not connected')
                self.em.poll(1)
        return self.connection

    def __ask(self, packet):
        # TODO: make thread-safe
        with self.em.wakeup_fd():
            connection = self.__getConnection()
            connection.ask(packet)
            response_queue = self.response_queue
            assert not response_queue
            while self.connected:
                self.em.poll(1)
                if response_queue:
                    break
            else:
                raise NotReadyException('Connection closed')
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

    def tweakPartitionTable(self, uuid_list=(), dry_run=False):
        response = self.__ask(Packets.TweakPartitionTable(dry_run, uuid_list))
        if response[0] != Packets.AnswerTweakPartitionTable:
            raise RuntimeError(response)
        return response[1:]

    def setNumReplicas(self, nr):
        response = self.__ask(Packets.SetNumReplicas(nr))
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

    def getRecovery(self):
        response = self.__ask(Packets.AskRecovery())
        if response[0] != Packets.AnswerRecovery:
            raise RuntimeError(response)
        return response[1:]

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
        return response[1:]

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        return self.setClusterState(ClusterStates.VERIFYING)

    def killNode(self, node):
        return self._setNodeState(node, NodeStates.DOWN)

    def dropNode(self, node):
        return self._setNodeState(node, NodeStates.UNKNOWN)

    def getPrimary(self):
        """
          Return the primary master UUID.
        """
        packet = Packets.AskPrimary()
        response = self.__ask(packet)
        if response[0] != Packets.AnswerPrimary:
            raise RuntimeError(response)
        return response[1]

    def repair(self, *args):
        response = self.__ask(Packets.Repair(*args))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def truncate(self, tid):
        response = self.__ask(Packets.Truncate(tid))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def checkReplicas(self, *args):
        response = self.__ask(Packets.CheckReplicas(*args))
        if response[0] != Packets.Error or response[1] != ErrorCodes.ACK:
            raise RuntimeError(response)
        return response[2]

    def flushLog(self):
        with self.em.wakeup_fd():
            conn = self.__getConnection()
            conn.send(Packets.FlushLog())
            while conn.pending():
                self.em.poll(1)

    def getMonitorInformation(self):
        response = self.__ask(Packets.AskMonitorInformation())
        if response[0] != Packets.AnswerMonitorInformation:
            raise RuntimeError(response)
        return response[1:]
