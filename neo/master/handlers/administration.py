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

from neo import logging

from neo import protocol
from neo.master.handlers import MasterHandler
from neo.protocol import ClusterStates, NodeStates, Packets
from neo.util import dump

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
        # XXX: dedicate a packet to start the cluster
        if state == ClusterStates.VERIFYING:
            self.app._startup_allowed = True
        else:
            self.app.changeClusterState(state)
        p = protocol.ack('cluster state changed')
        conn.answer(p)
        if state == ClusterStates.STOPPING:
            self.app.cluster_state = state
            self.app.shutdown()

    def setNodeState(self, conn, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s : %s" %
                (dump(uuid), state, modify_partition_table))
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node is None:
            raise protocol.ProtocolError('unknown node')

        if uuid == app.uuid:
            node.setState(state)
            # get message for self
            if state != NodeStates.RUNNING:
                p = protocol.ack('node state changed')
                conn.answer(p)
                app.shutdown()

        if node.getState() == state:
            # no change, just notify admin node
            p = protocol.ack('node state changed')
            conn.answer(p)
            return

        if state == NodeStates.RUNNING:
            # first make sure to have a connection to the node
            node_conn = None
            for node_conn in app.em.getConnectionList():
                if node_conn.getUUID() == node.getUUID():
                    break
            else:
                # no connection to the node
                raise protocol.ProtocolError('no connection to the node')
            node.setState(state)

        elif state == NodeStates.DOWN and node.isStorage():
            # update it's state
            node.setState(state)
            for storage_conn in app.em.getConnectionListByUUID(uuid):
                # notify itself so it can shutdown
                node_list = [node.asTuple()]
                storage_conn.notify(Packets.NotifyNodeInformation(node_list))
                # close to avoid handle the closure as a connection lost
                storage_conn.close()
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
        p = protocol.ack('state changed')
        conn.answer(p)
        app.broadcastNodesInformation([node])

    def addPendingNodes(self, conn, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        logging.debug('Add nodes %s' % uuids)
        app, nm, em, pt = self.app, self.app.nm, self.app.em, self.app.pt
        cell_list = []
        uuid_set = set()
        # take all pending nodes
        for node in nm.getStorageList():
            if node.isPending():
                uuid_set.add(node.getUUID())
        # keep only selected nodes
        if uuid_list:
            uuid_set = uuid_set.intersection(set(uuid_list))
        # nothing to do
        if not uuid_set:
            logging.warning('No nodes added')
            p = protocol.ack('no nodes added')
            conn.answer(p)
            return
        uuids = ', '.join([dump(uuid) for uuid in uuid_set])
        logging.info('Adding nodes %s' % uuids)
        # switch nodes to running state
        node_list = [nm.getByUUID(uuid) for uuid in uuid_set]
        for node in node_list:
            new_cells = pt.addNode(node)
            cell_list.extend(new_cells)
            node.setRunning()
        app.broadcastNodesInformation(node_list)
        # start nodes
        for s_conn in em.getConnectionList():
            if s_conn.getUUID() in uuid_set:
                s_conn.notify(Packets.NotifyLastOID(app.loid))
                s_conn.notify(Packets.StartOperation())
        # broadcast the new partition table
        app.broadcastPartitionChanges(cell_list)
        p = protocol.ack('node added')
        conn.answer(p)
