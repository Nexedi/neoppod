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

import neo

from neo.master.handlers import MasterHandler
from neo.lib.protocol import ClusterStates, NodeStates, Packets, ProtocolError
from neo.lib.protocol import Errors
from neo.lib.util import dump

CLUSTER_STATE_WORKFLOW = {
    # destination: sources
    ClusterStates.VERIFYING: set([ClusterStates.RECOVERING]),
    ClusterStates.STOPPING: set([ClusterStates.RECOVERING,
            ClusterStates.VERIFYING, ClusterStates.RUNNING]),
}

class AdministrationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        self.app.nm.remove(node)

    def askPrimary(self, conn):
        app = self.app
        # I'm the primary
        conn.answer(Packets.AnswerPrimary(app.uuid, []))

    def setClusterState(self, conn, state):
        # check request
        if not state in CLUSTER_STATE_WORKFLOW.keys():
            raise ProtocolError('Invalid state requested')
        valid_current_states = CLUSTER_STATE_WORKFLOW[state]
        if self.app.cluster_state not in valid_current_states:
            raise ProtocolError('Cannot switch to this state')

        # change state
        if state == ClusterStates.VERIFYING:
            # XXX: /!\ this allow leave the first phase of recovery
            self.app._startup_allowed = True
        else:
            self.app.changeClusterState(state)

        # answer
        conn.answer(Errors.Ack('Cluster state changed'))
        if state == ClusterStates.STOPPING:
            self.app.cluster_state = state
            self.app.shutdown()

    def setNodeState(self, conn, uuid, state, modify_partition_table):
        neo.lib.logging.info("set node state for %s-%s : %s" %
                (dump(uuid), state, modify_partition_table))
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node is None:
            raise ProtocolError('unknown node')

        if uuid == app.uuid:
            node.setState(state)
            # get message for self
            if state != NodeStates.RUNNING:
                p = Errors.Ack('node state changed')
                conn.answer(p)
                app.shutdown()

        if node.getState() == state:
            # no change, just notify admin node
            p = Errors.Ack('node already in %s state' % state)
            conn.answer(p)
            return

        if state == NodeStates.RUNNING:
            # first make sure to have a connection to the node
            if not node.isConnected():
                raise ProtocolError('no connection to the node')
            node.setState(state)

        elif state == NodeStates.DOWN and node.isStorage():
            # update it's state
            node.setState(state)
            if node.isConnected():
                # notify itself so it can shutdown
                node.notify(Packets.NotifyNodeInformation([node.asTuple()]))
                # close to avoid handle the closure as a connection lost
                node.getConnection().abort()
            # modify the partition table if required
            cell_list = []
            if modify_partition_table:
                # remove from pt
                cell_list = app.pt.dropNode(node)
                app.nm.remove(node)
            else:
                # outdate node in partition table
                cell_list = app.pt.outdate()
            app.broadcastPartitionChanges(cell_list)

        else:
            node.setState(state)

        # /!\ send the node information *after* the partition table change
        p = Errors.Ack('state changed')
        conn.answer(p)
        app.broadcastNodesInformation([node])

    def addPendingNodes(self, conn, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        neo.lib.logging.debug('Add nodes %s' % uuids)
        app = self.app
        nm = app.nm
        em = app.em
        pt = app.pt
        cell_list = []
        uuid_set = set()
        if app.getClusterState() == ClusterStates.RUNNING:
            # take all pending nodes
            for node in nm.getStorageList():
                if node.isPending():
                    uuid_set.add(node.getUUID())
            # keep only selected nodes
            if uuid_list:
                uuid_set = uuid_set.intersection(set(uuid_list))
        # nothing to do
        if not uuid_set:
            neo.lib.logging.warning('No nodes added')
            conn.answer(Errors.Ack('No nodes added'))
            return
        uuids = ', '.join([dump(uuid) for uuid in uuid_set])
        neo.lib.logging.info('Adding nodes %s' % uuids)
        # switch nodes to running state
        node_list = [nm.getByUUID(uuid) for uuid in uuid_set]
        for node in node_list:
            new_cells = pt.addNode(node)
            cell_list.extend(new_cells)
            node.setRunning()
            node.getConnection().notify(Packets.StartOperation())
        app.broadcastNodesInformation(node_list)
        # broadcast the new partition table
        app.broadcastPartitionChanges(cell_list)
        conn.answer(Errors.Ack('Nodes added: %s' % (uuids, )))
