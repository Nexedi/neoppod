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

from neo.lib import logging
from neo.lib.connection import ClientConnection
from neo.lib.protocol import Packets, ProtocolError, ClusterStates, NodeStates
from .app import monotonic_time
from .handlers import MasterHandler


class RecoveryManager(MasterHandler):
    """
      Manage the cluster recovery
    """

    def __init__(self, app):
        # The target node's uuid to request next.
        self.target_ptid = 0
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
        A new primary master may also arise during this phase.
        """
        logging.info('begin the recovery of the status')
        app = self.app
        pt = app.pt = app.newPartitionTable()
        app.changeClusterState(ClusterStates.RECOVERING)

        self.try_secondary = True

        # collect the last partition table available
        poll = app.em.poll
        while 1:
            if self.try_secondary:
                # Keep trying to connect to all other known masters,
                # to make sure there is a challege between each pair
                # of masters in the cluster. If we win, all connections
                # opened here will be closed.
                self.try_secondary = False
                node_list = []
                for node in app.nm.getMasterList():
                    if not (node is app._node or node.isConnected(True)):
                        # During recovery, master nodes are not put back in
                        # DOWN state by handlers. This is done
                        # entirely in this method (here and after this poll
                        # loop), to minimize the notification packets.
                        if not node.isDown():
                            node.setDown()
                            node_list.append(node)
                        ClientConnection(app, app.election_handler, node)
                if node_list:
                    app.broadcastNodesInformation(node_list)
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
                                conn.send(truncate)
                                self.handlerSwitched(conn, False)
                            continue
                    node_list = pt.getConnectedNodeList()
                break

        logging.info('startup allowed')

        for node in node_list:
            assert node.isPending(), node
            node.setRunning()

        for node in app.nm.getMasterList():
            if not (node is app._node or node.isIdentified()):
                if node.isConnected(True):
                    node.getConnection().close()
                    assert node.isDown(), node
                elif not node.isDown():
                    assert self.try_secondary, node
                    node.setDown()
                    node_list.append(node)

        app.broadcastNodesInformation(node_list)

        if pt.getID() is None:
            logging.info('creating a new partition table')
            pt.make(node_list)
            self._notifyAdmins(Packets.SendPartitionTable(
                pt.getID(), pt.getReplicas(), pt.getRowList()))
        else:
            cell_list = pt.outdate()
            if cell_list:
                self._notifyAdmins(Packets.NotifyPartitionChanges(
                    pt.setNextID(), pt.getReplicas(), cell_list))
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
        if node is None or node.getState() == new_state:
            return
        node.setState(new_state)
        self.app.broadcastNodesInformation([node])

    def handlerSwitched(self, conn, new):
        # ask the last IDs to perform the recovery
        conn.ask(Packets.AskRecovery())

    def answerRecovery(self, conn, ptid, backup_tid, truncate_tid):
        uuid = conn.getUUID()
        # ptid is None if the node has an empty partition table.
        if ptid and self.target_ptid <= ptid:
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

    def answerPartitionTable(self, conn, ptid, num_replicas, row_list):
        # If this is not from a target node, ignore it.
        if ptid == self.target_ptid:
            app = self.app
            new_nodes = app.pt.load(ptid, num_replicas, row_list, app.nm)
            self._notifyAdmins(
                Packets.NotifyNodeInformation(monotonic_time(), new_nodes),
                Packets.SendPartitionTable(ptid, num_replicas, row_list))
            self.ask_pt = ()
            uuid = conn.getUUID()
            app.backup_tid = self.backup_tid_dict[uuid]
            app.truncate_tid = self.truncate_dict[uuid]

    def _notifyAdmins(self, *packets):
        for node in self.app.nm.getAdminList(only_identified=True):
            for packet in packets:
                node.send(packet)
