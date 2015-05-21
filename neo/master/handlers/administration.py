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

import random

from . import MasterHandler
from ..app import StateChangedException
from neo.lib import logging
from neo.lib.pt import PartitionTableException
from neo.lib.protocol import ClusterStates, Errors, \
    NodeStates, NodeTypes, Packets, ProtocolError, uuid_str
from neo.lib.util import dump

CLUSTER_STATE_WORKFLOW = {
    # destination: sources
    ClusterStates.VERIFYING: (ClusterStates.RECOVERING,),
    ClusterStates.STARTING_BACKUP: (ClusterStates.RUNNING,
                                    ClusterStates.STOPPING_BACKUP),
    ClusterStates.STOPPING_BACKUP: (ClusterStates.BACKINGUP,
                                    ClusterStates.STARTING_BACKUP),
}
NODE_STATE_WORKFLOW = {
    NodeTypes.MASTER: (NodeStates.UNKNOWN,),
    NodeTypes.STORAGE: (NodeStates.UNKNOWN, NodeStates.DOWN),
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

    def setNodeState(self, conn, uuid, state):
        logging.info("set node state for %s: %s", uuid_str(uuid), state)
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node is None:
            raise ProtocolError('unknown node')
        if state not in NODE_STATE_WORKFLOW.get(node.getType(), ()):
            raise ProtocolError('can not switch node to this state')
        if uuid == app.uuid:
            raise ProtocolError('can not kill primary master node')

        state_changed = state != node.getState()
        message = ('state changed' if state_changed else
                   'node already in %s state' % state)
        if node.isStorage():
            keep = state == NodeStates.UNKNOWN
            try:
                cell_list = app.pt.dropNodeList([node], keep)
            except PartitionTableException, e:
                raise ProtocolError(str(e))
            node.setState(state)
            if node.isConnected():
                # notify itself so it can shutdown
                node.notify(Packets.NotifyNodeInformation([node.asTuple()]))
                # close to avoid handle the closure as a connection lost
                node.getConnection().abort()
            if keep:
                cell_list = app.pt.outdate()
            elif cell_list:
                message = 'node permanently removed'
            app.broadcastPartitionChanges(cell_list)
        else:
            node.setState(state)

        # /!\ send the node information *after* the partition table change
        conn.answer(Errors.Ack(message))
        if state_changed:
            # notify node explicitly because broadcastNodesInformation()
            # ignores non-running nodes
            assert not node.isRunning()
            if node.isConnected():
                node.notify(Packets.NotifyNodeInformation([node.asTuple()]))
            app.broadcastNodesInformation([node])

    def addPendingNodes(self, conn, uuid_list):
        uuids = ', '.join(map(uuid_str, uuid_list))
        logging.debug('Add nodes %s', uuids)
        app = self.app
        state = app.getClusterState()
        # XXX: Would it be safe to allow more states ?
        if state not in (ClusterStates.RUNNING,
                         ClusterStates.STARTING_BACKUP,
                         ClusterStates.BACKINGUP):
            raise ProtocolError('Can not add nodes in %s state' % state)
        # take all pending nodes
        node_list = list(app.pt.addNodeList(node
            for node in app.nm.getStorageList()
            if node.isPending() and node.getUUID() in uuid_list))
        if node_list:
            p = Packets.StartOperation(bool(app.backup_tid))
            for node in node_list:
                node.setRunning()
                node.notify(p)
            app.broadcastNodesInformation(node_list)
            conn.answer(Errors.Ack('Nodes added: %s' %
                ', '.join(uuid_str(x.getUUID()) for x in node_list)))
        else:
            logging.warning('No node added')
            conn.answer(Errors.Ack('No node added'))

    def tweakPartitionTable(self, conn, uuid_list):
        app = self.app
        state = app.getClusterState()
        # XXX: Would it be safe to allow more states ?
        if state not in (ClusterStates.RUNNING,
                         ClusterStates.STARTING_BACKUP,
                         ClusterStates.BACKINGUP):
            raise ProtocolError('Can not tweak partition table in %s state'
                                % state)
        app.broadcastPartitionChanges(app.pt.tweak(
            map(app.nm.getByUUID, uuid_list)))
        conn.answer(Errors.Ack(''))

    def checkReplicas(self, conn, partition_dict, min_tid, max_tid):
        app = self.app
        pt = app.pt
        backingup = bool(app.backup_tid)
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
