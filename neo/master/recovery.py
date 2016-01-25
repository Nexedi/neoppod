#
# Copyright (C) 2006-2016  Nexedi SA
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
from neo.lib.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from .handlers import MasterHandler


class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        # The target node's uuid to request next.
        self.target_ptid = None
        self.ask_pt = []
        self.backup_tid_dict = {}
        self.truncate_dict = {}

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
        pt.clear()

        # collect the last partition table available
        poll = app.em.poll
        while 1:
            poll(1)
            if pt.filled():
                # A partition table exists, we are starting an existing
                # cluster.
                node_list = pt.getOperationalNodeSet()
                if app._startup_allowed:
                    node_list = [node for node in node_list if node.isPending()]
                elif node_list:
                    # we want all nodes to be there if we're going to truncate
                    if app.truncate_tid:
                        node_list = pt.getNodeSet()
                    if not all(node.isPending() for node in node_list):
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
                    if app.truncate_tid:
                        node_list = app.nm.getIdentifiedList(pool_set={uuid
                            for uuid, tid in self.truncate_dict.iteritems()
                            if not tid or app.truncate_tid < tid})
                        if node_list:
                            truncate = Packets.Truncate(app.truncate_tid)
                            for node in node_list:
                                conn = node.getConnection()
                                conn.notify(truncate)
                                self.connectionCompleted(conn, False)
                            continue
                    node_list = pt.getConnectedNodeList()
                break

        logging.info('startup allowed')

        for node in node_list:
            assert node.isPending(), node
            node.setRunning()
        app.broadcastNodesInformation(node_list)

        if pt.getID() is None:
            logging.info('creating a new partition table')
            pt.make(node_list)
            self._notifyAdmins(Packets.SendPartitionTable(
                pt.getID(), pt.getRowList()))
        else:
            cell_list = pt.outdate()
            if cell_list:
                self._notifyAdmins(Packets.NotifyPartitionChanges(
                    pt.setNextID(), cell_list))
            if app.backup_tid:
                pt.setBackupTidDict(self.backup_tid_dict)
                app.backup_tid = pt.getBackupTid()

        logging.debug('cluster starts this partition table:')
        pt.log()

    def connectionLost(self, conn, new_state):
        uuid = conn.getUUID()
        self.backup_tid_dict.pop(uuid, None)
        self.truncate_dict.pop(uuid, None)
        node = self.app.nm.getByUUID(uuid)
        try:
            i = self.ask_pt.index(uuid)
        except ValueError:
            pass
        else:
            del self.ask_pt[i]
            if not i:
                if self.ask_pt:
                    self.app.nm.getByUUID(self.ask_pt[0]) \
                        .ask(Packets.AskPartitionTable())
                else:
                    logging.warning("Waiting for %r to come back."
                        " No other node has version %s of the partition table.",
                        node, self.target_ptid)
        if node.getState() == new_state:
            return
        node.setState(new_state)
        # broadcast to all so that admin nodes gets informed
        self.app.broadcastNodesInformation([node])

    def connectionCompleted(self, conn, new):
        # ask the last IDs to perform the recovery
        conn.ask(Packets.AskRecovery())

    def answerRecovery(self, conn, ptid, backup_tid, truncate_tid):
        uuid = conn.getUUID()
        if self.target_ptid <= ptid:
            # Maybe a newer partition table.
            if self.target_ptid == ptid and self.ask_pt:
                # Another node is already asked.
                self.ask_pt.append(uuid)
            elif self.target_ptid < ptid or self.ask_pt is not ():
                # No node asked yet for the newest partition table.
                self.target_ptid = ptid
                self.ask_pt = [uuid]
                conn.ask(Packets.AskPartitionTable())
        self.backup_tid_dict[uuid] = backup_tid
        self.truncate_dict[uuid] = truncate_tid

    def answerPartitionTable(self, conn, ptid, row_list):
        # If this is not from a target node, ignore it.
        if ptid == self.target_ptid:
            app = self.app
            try:
                new_nodes = app.pt.load(ptid, row_list, app.nm)
            except IndexError:
                raise ProtocolError('Invalid offset')
            self._notifyAdmins(Packets.NotifyNodeInformation(new_nodes),
                               Packets.SendPartitionTable(ptid, row_list))
            self.ask_pt = ()
            uuid = conn.getUUID()
            app.backup_tid = self.backup_tid_dict[uuid]
            app.truncate_tid = self.truncate_dict[uuid]

    def _notifyAdmins(self, *packets):
        for node in self.app.nm.getAdminList(only_identified=True):
            for packet in packets:
                node.notify(packet)
