#
# Copyright (C) 2006-2015  Nexedi SA
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

from neo.lib import logging
from neo.lib.util import dump
from neo.lib.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from neo.lib.protocol import ZERO_OID
from .handlers import MasterHandler


class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        # The target node's uuid to request next.
        self.target_ptid = None
        self.backup_tid_dict = {}

    def getHandler(self):
        return self

    def identifyStorageNode(self, _):
        """
            Returns the handler for storage nodes
        """
        return NodeStates.PENDING, self

    def run(self):
        """
        Recover the status about the cluster. Obtain the last OID, the last
        TID, and the last Partition Table ID from storage nodes, then get
        back the latest partition table or make a new table from scratch,
        if this is the first time.
        """
        logging.info('begin the recovery of the status')
        app = self.app
        pt = app.pt
        app.changeClusterState(ClusterStates.RECOVERING)
        pt.setID(None)

        # collect the last partition table available
        poll = app.em.poll
        while 1:
            poll(1)
            if pt.filled():
                # A partition table exists, we are starting an existing
                # cluster.
                node_list = pt.getReadableCellNodeSet()
                if app._startup_allowed:
                    node_list = [node for node in node_list if node.isPending()]
                elif not all(node.isPending() for node in node_list):
                    continue
            elif app._startup_allowed or app.autostart:
                # No partition table and admin allowed startup, we are
                # creating a new cluster out of all pending nodes.
                node_list = app.nm.getStorageList(only_identified=True)
                if not app._startup_allowed and len(node_list) < app.autostart:
                    continue
            else:
                continue
            if node_list and not any(node.getConnection().isPending()
                                     for node in node_list):
                if pt.filled():
                    node_list = pt.getConnectedNodeList()
                break

        logging.info('startup allowed')

        for node in node_list:
            assert node.isPending(), node
            node.setRunning()
        app.broadcastNodesInformation(node_list)

        if pt.getID() is None:
            logging.info('creating a new partition table')
            # reset IDs generators & build new partition with running nodes
            app.tm.setLastOID(ZERO_OID)
            pt.make(node_list)
            self._broadcastPartitionTable(pt.getID(), pt.getRowList())
        elif app.backup_tid:
            pt.setBackupTidDict(self.backup_tid_dict)
            app.backup_tid = pt.getBackupTid()

        app.setLastTransaction(app.tm.getLastTID())
        logging.debug('cluster starts with loid=%s and this partition table :',
                      dump(app.tm.getLastOID()))
        pt.log()

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

    def answerLastIDs(self, conn, loid, ltid, lptid, backup_tid):
        # Get max values.
        if loid is not None:
            self.app.tm.setLastOID(loid)
        if ltid is not None:
            self.app.tm.setLastTID(ltid)
        if lptid > self.target_ptid:
            # something newer
            self.target_ptid = lptid
            conn.ask(Packets.AskPartitionTable())
        self.backup_tid_dict[conn.getUUID()] = backup_tid

    def answerPartitionTable(self, conn, ptid, row_list):
        if ptid != self.target_ptid:
            # If this is not from a target node, ignore it.
            logging.warn('Got %s while waiting %s', dump(ptid),
                    dump(self.target_ptid))
        else:
            self._broadcastPartitionTable(ptid, row_list)
            self.app.backup_tid = self.backup_tid_dict[conn.getUUID()]

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
