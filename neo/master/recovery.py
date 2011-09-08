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

from struct import pack

import neo
from neo.lib.util import dump
from neo.lib.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from neo.lib.protocol import NotReadyError, ZERO_OID, ZERO_TID
from neo.master.handlers import MasterHandler


class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        super(RecoveryManager, self).__init__(app)
        # The target node's uuid to request next.
        self.target_ptid = None

    def getHandler(self):
        return self

    def identifyStorageNode(self, uuid, node):
        """
            Returns the handler for storage nodes
        """
        return uuid, NodeStates.PENDING, self

    def run(self):
        """
        Recover the status about the cluster. Obtain the last OID, the last
        TID, and the last Partition Table ID from storage nodes, then get
        back the latest partition table or make a new table from scratch,
        if this is the first time.
        """
        neo.lib.logging.info('begin the recovery of the status')

        self.app.changeClusterState(ClusterStates.RECOVERING)
        em = self.app.em

        self.app.tm.setLastOID(None)
        self.app.pt.setID(None)

        # collect the last partition table available
        while 1:
            em.poll(1)
            if self.app._startup_allowed:
                allowed_node_set = set()
                for node in self.app.nm.getStorageList():
                    if node.isPending():
                        break # waiting for an answer
                    if node.isRunning():
                        allowed_node_set.add(node)
                else:
                    if allowed_node_set:
                        break # no ready storage node

        neo.lib.logging.info('startup allowed')

        if self.app.pt.getID() is None:
            neo.lib.logging.info('creating a new partition table')
            # reset IDs generators & build new partition with running nodes
            self.app.tm.setLastOID(ZERO_OID)
            self.app.pt.make(allowed_node_set)
            self._broadcastPartitionTable(self.app.pt.getID(),
                                          self.app.pt.getRowList())

        # collect node that are connected but not in the selected partition
        # table and set them in pending state
        refused_node_set = allowed_node_set.difference(
            self.app.pt.getNodeList())
        if refused_node_set:
            for node in refused_node_set:
                node.setPending()
            self.app.broadcastNodesInformation(refused_node_set)

        self.app.setLastTransaction(self.app.tm.getLastTID())
        neo.lib.logging.debug(
                        'cluster starts with loid=%s and this partition ' \
                        'table :', dump(self.app.tm.getLastOID()))
        self.app.pt.log()

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node is not None
        if node.getState() == new_state:
            return
        node.setState(new_state)
        # broadcast to all so that admin nodes gets informed
        self.app.broadcastNodesInformation([node])

    def connectionCompleted(self, conn):
        # ask the last IDs to perform the recovery
        conn.ask(Packets.AskLastIDs())

    def answerLastIDs(self, conn, loid, ltid, lptid):
        # Get max values.
        if loid is not None:
            self.app.tm.setLastOID(max(loid, self.app.tm.getLastOID()))
        if ltid is not None:
            self.app.tm.setLastTID(ltid)
        if lptid > self.target_ptid:
            # something newer
            self.target_ptid = lptid
            conn.ask(Packets.AskPartitionTable())
        else:
            node = self.app.nm.getByUUID(conn.getUUID())
            assert node.isPending()
            node.setRunning()
            self.app.broadcastNodesInformation([node])

    def answerPartitionTable(self, conn, ptid, row_list):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node.isPending()
        node.setRunning()
        if ptid != self.target_ptid:
            # If this is not from a target node, ignore it.
            neo.lib.logging.warn('Got %s while waiting %s', dump(ptid),
                    dump(self.target_ptid))
        else:
            self._broadcastPartitionTable(ptid, row_list)
        self.app.broadcastNodesInformation([node])

    def _broadcastPartitionTable(self, ptid, row_list):
        try:
            new_nodes = self.app.pt.load(ptid, row_list, self.app.nm)
        except IndexError:
            raise ProtocolError('Invalid offset')
        else:
            notification = Packets.NotifyNodeInformation(new_nodes)
            ptid = self.app.pt.getID()
            row_list = self.app.pt.getRowList()
            partition_table = Packets.SendPartitionTable(ptid, row_list)
            # notify the admin nodes
            for node in self.app.nm.getAdminList(only_identified=True):
                node.notify(notification)
                node.notify(partition_table)
