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
from neo.master.handler import MasterEventHandler
from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        STORAGE_NODE_TYPE, HIDDEN_STATE, PENDING_STATE
from neo.util import dump

class AdministrationEventHandler(MasterEventHandler):
    """This class deals with messages from the admin node only"""

    def _discardNode(self, conn):
        uuid = conn.getUUID()
        node = self.app.nm.getNodeByUUID(uuid)
        if node is not None:
            self.app.nm.remove(node)

    def handleAskPrimaryMaster(self, conn, packet):
        app = self.app
        # I'm the primary
        conn.answer(protocol.answerPrimaryMaster(app.uuid, []), packet)
        # TODO: Admin will ask itself for these data
        app.sendNodesInformations(conn)
        app.sendPartitionTable(conn)

    def connectionClosed(self, conn):
        self._discardNode(conn)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        self._discardNode(conn)
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        self._discardNode(conn)
        MasterEventHandler.peerBroken(self, conn)

    def handleSetClusterState(self, conn, packet, name, state):
        self.checkClusterName(name)
        if state == protocol.RUNNING:
            self.app.cluster_state = state
        conn.answer(protocol.answerClusterState(self.app.cluster_state), packet)

    def handleSetNodeState(self, conn, packet, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s : %s" % (dump(uuid), state, modify_partition_table))
        app = self.app
        if uuid == app.uuid:
            # get message for self
            if state == RUNNING_STATE:
                # yes I know
                p = protocol.answerNodeState(app.uuid, state)
                conn.answer(p, packet)
                return
            else:
                # I was asked to shutdown
                node.setState(state)
                ip, port = node.getServer()
                node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
                conn.answer(protocol.notifyNodeInformation(node_list), packet)
                app.shutdown()

        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            p = protocol.protocolError('invalid uuid')
            conn.notify(p)
            return
        if node.getState() == state:
            # no change, just notify admin node
            node.setState(state)
            ip, port = node.getServer()
            node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
            conn.answer(protocol.notifyNodeInformation(node_list), packet)

        # forward information to all nodes
        if node.getState() != state:
            node.setState(state)
            ip, port = node.getServer()
            node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
            conn.answer(protocol.notifyNodeInformation(node_list), packet)
            app.broadcastNodeInformation(node)
            # If this is a storage node, ask it to start.
            if node.getNodeType() == STORAGE_NODE_TYPE and state == RUNNING_STATE:
                for sn_conn in app.em.getConnectionList():
                    if sn_conn.getUUID() == node.getUUID():
                        logging.info("asking sn to start operation")
                        sn_conn.notify(protocol.startOperation())

        # modify the partition table if required
        if modify_partition_table and node.getNodeType() == STORAGE_NODE_TYPE: 
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
            conn.answer(protocol.answerNewNodes(()), packet)
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
        conn.answer(protocol.answerNewNodes(list(uuid_set)), packet)
