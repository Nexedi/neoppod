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

import random

from . import MasterHandler
from ..app import StateChangedException
from neo.lib import logging
from neo.lib.protocol import ClusterStates, NodeStates, Packets, ProtocolError
from neo.lib.protocol import Errors
from neo.lib.util import dump

CLUSTER_STATE_WORKFLOW = {
    # destination: sources
    ClusterStates.VERIFYING: (ClusterStates.RECOVERING,),
    ClusterStates.STARTING_BACKUP: (ClusterStates.RUNNING,
                                    ClusterStates.STOPPING_BACKUP),
    ClusterStates.STOPPING_BACKUP: (ClusterStates.BACKINGUP,
                                    ClusterStates.STARTING_BACKUP),
}

class AdministrationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        self.app.nm.remove(node)

    def setClusterState(self, conn, state):
        app = self.app
        # check request
        try:
            if app.cluster_state not in CLUSTER_STATE_WORKFLOW[state]:
                raise ProtocolError('Can not switch to this state')
        except KeyError:
            if state != ClusterStates.STOPPING:
                raise ProtocolError('Invalid state requested')

        # change state
        if state == ClusterStates.VERIFYING:
            storage_list = app.nm.getStorageList(only_identified=True)
            if not storage_list:
                raise ProtocolError('Cannot exit recovery without any '
                    'storage node')
            for node in storage_list:
                assert node.isPending(), node
                if node.getConnection().isPending():
                    raise ProtocolError('Cannot exit recovery now: node %r is '
                        'entering cluster' % (node, ))
            app._startup_allowed = True
            state = app.cluster_state
        elif state == ClusterStates.STARTING_BACKUP:
            if app.tm.hasPending() or app.nm.getClientList(True):
                raise ProtocolError("Can not switch to %s state with pending"
                    " transactions or connected clients" % state)

        conn.answer(Errors.Ack('Cluster state changed'))
        if state != app.cluster_state:
            raise StateChangedException(state)

    def setNodeState(self, conn, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s : %s",
                     dump(uuid), state, modify_partition_table)
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
        uuids = ', '.join(map(dump, uuid_list))
        logging.debug('Add nodes %s', uuids)
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
            logging.warning('No nodes added')
            conn.answer(Errors.Ack('No nodes added'))
            return
        uuids = ', '.join(map(dump, uuid_set))
        logging.info('Adding nodes %s', uuids)
        # switch nodes to running state
        node_list = map(nm.getByUUID, uuid_set)
        for node in node_list:
            new_cells = pt.addNode(node)
            cell_list.extend(new_cells)
            node.setRunning()
            node.getConnection().notify(Packets.StartOperation())
        app.broadcastNodesInformation(node_list)
        # broadcast the new partition table
        app.broadcastPartitionChanges(cell_list)
        conn.answer(Errors.Ack('Nodes added: %s' % (uuids, )))

    def checkReplicas(self, conn, partition_dict, min_tid, max_tid):
        app = self.app
        pt = app.pt
        backingup = app.cluster_state == ClusterStates.BACKINGUP
        if not max_tid:
            max_tid = pt.getCheckTid(partition_dict) if backingup else \
                app.getLastTransaction()
        if min_tid > max_tid:
            logging.warning("nothing to check: min_tid=%s > max_tid=%s",
                            dump(min_tid), dump(max_tid))
        else:
            getByUUID = app.nm.getByUUID
            node_set = set()
            for offset, source in partition_dict.iteritems():
                # XXX: For the moment, code checking replicas is unable to fix
                #      corrupted partitions (when a good cell is known)
                #      so only check readable ones.
                #      (see also Checker._nextPartition of storage)
                cell_list = pt.getCellList(offset, True)
                #cell_list = [cell for cell in pt.getCellList(offset)
                #                  if not cell.isOutOfDate()]
                if len(cell_list) + (backingup and not source) <= 1:
                    continue
                for cell in cell_list:
                    node = cell.getNode()
                    if node in node_set:
                        break
                else:
                    node_set.add(node)
                if source:
                    source = '', getByUUID(source).getAddress()
                else:
                    readable = [cell for cell in cell_list if cell.isReadable()]
                    if 1 == len(readable) < len(cell_list):
                        source = '', readable[0].getAddress()
                    elif backingup:
                        source = app.backup_app.name, random.choice(
                            app.backup_app.pt.getCellList(offset, readable=True)
                            ).getAddress()
                    else:
                        source = '', None
                node.getConnection().notify(Packets.CheckPartition(
                    offset, source, min_tid, max_tid))
        conn.answer(Errors.Ack(''))
