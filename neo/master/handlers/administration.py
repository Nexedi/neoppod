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

import logging

from neo import protocol
from neo.master.handlers import MasterHandler
from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        HIDDEN_STATE, PENDING_STATE, RUNNING
from neo.util import dump

class AdministrationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def _nodeLost(self, conn, node):
        self.app.nm.remove(node)

    def handleAskPrimaryMaster(self, conn, packet):
        app = self.app
        # I'm the primary
        conn.answer(protocol.answerPrimaryMaster(app.uuid, []), packet)

    def handleSetClusterState(self, conn, packet, state):
        self.app.changeClusterState(state)
        p = protocol.noError('cluster state changed')
        conn.answer(p, packet)
        if state == protocol.STOPPING:
            self.app.cluster_state = state
            self.app.shutdown()

    def handleSetNodeState(self, conn, packet, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s : %s" % (dump(uuid), state, modify_partition_table))
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            p = protocol.protocolError('invalid uuid')
            conn.answer(p, packet)
            return

        if uuid == app.uuid:
            # get message for self
            if state == RUNNING_STATE:
                # yes I know
                p = protocol.noError('node state changed')
                conn.answer(p, packet)
                return
            else:
                # I was asked to shutdown
                node.setState(state)
                p = protocol.noError('node state changed')
                conn.answer(p, packet)
                app.shutdown()

        if node.getState() == state:
            # no change, just notify admin node
            p = protocol.noError('node state changed')
            conn.answer(p, packet)
        else:
            # first make sure to have a connection to the node
            node_conn = None
            conn_found = False
            for node_conn in app.em.getConnectionList():
                if node_conn.getUUID() == node.getUUID():
                    conn_found = True
                    break
            if conn_found is False:
                # no connection to the node
                p = protocol.protocolError('no connection to the node')
                conn.notify(p)
                return

            node.setState(state)
            p = protocol.noError('state changed')
            conn.answer(p, packet)
            app.broadcastNodeInformation(node)
            # If this is a storage node, ask it to start.
            if node.isStorage() and state == RUNNING_STATE  \
                   and self.app.cluster_state == RUNNING:
                logging.info("asking sn to start operation")
                node_conn.notify(protocol.startOperation())

        # modify the partition table if required
        if modify_partition_table and node.isStorage():
            if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, HIDDEN_STATE):
                # remove from pt
                cell_list = app.pt.dropNode(node)
            else:
                # add to pt
                cell_list = app.pt.addNode(node)
            if len(cell_list) != 0:
                ptid = app.pt.setNextID()
                app.broadcastPartitionChanges(ptid, cell_list)
        else:
            # outdate node in partition table
            cell_list = app.pt.outdate()
            if len(cell_list) != 0:
                ptid = app.pt.setNextID()
                app.broadcastPartitionChanges(ptid, cell_list)

    def handleAddPendingNodes(self, conn, packet, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        logging.debug('Add nodes %s' % uuids)
        app, nm, em, pt = self.app, self.app.nm, self.app.em, self.app.pt
        cell_list = []
        uuid_set = set()
        # take all pending nodes
        for node in nm.getStorageNodeList():
            if node.getState() == PENDING_STATE:
                uuid_set.add(node.getUUID())
        # keep only selected nodes
        if uuid_list:
            uuid_set = uuid_set.intersection(set(uuid_list))
        # nothing to do
        if not uuid_set:
            logging.warning('No nodes added')
            p = protocol.noError('no nodes added')
            conn.answer(p, packet)
            return
        uuids = ', '.join([dump(uuid) for uuid in uuid_set])
        logging.info('Adding nodes %s' % uuids)
        # switch nodes to running state
        for uuid in uuid_set:
            node = nm.getNodeByUUID(uuid)
            new_cells = pt.addNode(node)
            cell_list.extend(new_cells)
            node.setState(RUNNING_STATE)
            app.broadcastNodeInformation(node)
        # start nodes
        for s_conn in em.getConnectionList():
            if s_conn.getUUID() in uuid_set:
                s_conn.notify(protocol.startOperation())
        # broadcast the new partition table
        app.broadcastPartitionChanges(app.pt.setNextID(), cell_list)
        p = protocol.noError('node added')
        conn.answer(p, packet)
