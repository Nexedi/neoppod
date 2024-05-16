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

import random
from functools import wraps

from . import MasterHandler
from ..app import monotonic_time, StateChangedException
from neo.lib import logging
from neo.lib.exception import StoppedOperation
from neo.lib.handler import AnswerDenied
from neo.lib.pt import PartitionTableException
from neo.lib.protocol import ClusterStates, Errors, \
    NodeStates, NodeTypes, Packets, uuid_str
from neo.lib.util import add64, dump

CLUSTER_STATE_WORKFLOW = {
    # destination: sources
    ClusterStates.VERIFYING: (ClusterStates.RECOVERING,),
    ClusterStates.STARTING_BACKUP: (ClusterStates.RUNNING,
                                    ClusterStates.STOPPING_BACKUP),
    ClusterStates.STOPPING_BACKUP: (ClusterStates.BACKINGUP,
                                    ClusterStates.STARTING_BACKUP),
}
NODE_STATE_WORKFLOW = {
    NodeTypes.MASTER: (NodeStates.DOWN,),
    NodeTypes.STORAGE: (NodeStates.DOWN, NodeStates.UNKNOWN),
}

def check_state(*states):
    def decorator(wrapped):
        def wrapper(self, *args):
            state = self.app.getClusterState()
            if state not in states:
                raise AnswerDenied('%s RPC can not be used in %s state'
                                   % (wrapped.__name__, state))
            wrapped(self, *args)
        return wraps(wrapped)(wrapper)
    return decorator


class AdministrationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def handlerSwitched(self, conn, new):
        assert new
        super(AdministrationHandler, self).handlerSwitched(conn, new)
        app = self.app.backup_app
        if app is not None:
            for node in app.nm.getAdminList():
                if node.isRunning():
                    app.notifyUpstreamAdmin(node.getAddress())
                    break

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        if node is not None:
            self.app.nm.remove(node)

    def flushLog(self, conn):
        p = Packets.FlushLog()
        for node in self.app.nm.getConnectedList():
            c = node.getConnection()
            c is conn or c.send(p)
        super(AdministrationHandler, self).flushLog(conn)

    def setClusterState(self, conn, state):
        app = self.app
        # check request
        try:
            if app.cluster_state not in CLUSTER_STATE_WORKFLOW[state]:
                raise AnswerDenied('Can not switch to this state')
        except KeyError:
            if state != ClusterStates.STOPPING:
                raise AnswerDenied('Invalid state requested')

        # change state
        if state == ClusterStates.VERIFYING:
            storage_list = app.nm.getStorageList(only_identified=True)
            if not storage_list:
                raise AnswerDenied(
                    'Cannot exit recovery without any storage node')
            for node in storage_list:
                assert node.isPending(), node
                if node.getConnection().isPending():
                    raise AnswerDenied(
                        'Cannot exit recovery now: node %r is entering cluster'
                        % node,)
            app._startup_allowed = True
            state = app.cluster_state
        elif state == ClusterStates.STARTING_BACKUP:
            if app.tm.hasPending() or app.nm.getClientList(True):
                raise AnswerDenied("Can not switch to %s state with pending"
                    " transactions or connected clients" % state)
            if app.backup_app is None:
                raise AnswerDenied(app.no_upstream_msg)

        conn.answer(Errors.Ack('Cluster state changed'))
        if state != app.cluster_state:
            raise StateChangedException(state)

    def setNodeState(self, conn, uuid, state):
        logging.info("set node state for %s: %s", uuid_str(uuid), state)
        app = self.app
        node = app.nm.getByUUID(uuid)
        if node is None:
            raise AnswerDenied('unknown node')
        if state not in NODE_STATE_WORKFLOW.get(node.getType(), ()):
            raise AnswerDenied('can not switch node to %s state' % state)
        if uuid == app.uuid:
            raise AnswerDenied('can not kill primary master node')

        state_changed = state != node.getState()
        message = ('state changed' if state_changed else
                   'node already in %s state' % state)
        if node.isStorage():
            keep = state == NodeStates.DOWN
            if node.isRunning() and not keep:
                raise AnswerDenied(
                    "a running node must be stopped before removal")
            try:
                cell_list = app.pt.dropNodeList([node], keep)
            except PartitionTableException, e:
                raise AnswerDenied(str(e))
            node.setState(state)
            if node.isConnected():
                # notify itself so it can shutdown
                node.send(Packets.NotifyNodeInformation(
                    monotonic_time(), [node.asTuple()]))
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
                node.send(Packets.NotifyNodeInformation(
                    monotonic_time(), [node.asTuple()]))
            app.broadcastNodesInformation([node])

    # XXX: Would it be safe to allow more states ?
    __change_pt_rpc = check_state(
        ClusterStates.RUNNING,
        ClusterStates.STARTING_BACKUP,
        ClusterStates.BACKINGUP)

    @__change_pt_rpc
    def addPendingNodes(self, conn, uuid_list):
        uuids = ', '.join(map(uuid_str, uuid_list))
        logging.debug('Add nodes %s', uuids)
        app = self.app
        # take all pending nodes
        node_list = list(app.pt.addNodeList(node
            for node in app.nm.getStorageList()
            if node.isPending() and node.getUUID() in uuid_list))
        if node_list:
            for node in node_list:
                node.setRunning()
                app.startStorage(node)
            app.broadcastNodesInformation(node_list)
            conn.answer(Errors.Ack('Nodes added: %s' %
                ', '.join(uuid_str(x.getUUID()) for x in node_list)))
        else:
            logging.warning('No node added')
            conn.answer(Errors.Ack('No node added'))

    def repair(self, conn, uuid_list, *args):
        getByUUID = self.app.nm.getByUUID
        node_list = []
        for uuid in uuid_list:
            node = getByUUID(uuid)
            if node is None or not (node.isStorage() and node.isIdentified()):
                raise AnswerDenied("invalid storage node %s" % uuid_str(uuid))
            node_list.append(node)
        repair = Packets.NotifyRepair(*args)
        for node in node_list:
            node.send(repair)
        conn.answer(Errors.Ack(''))

    @__change_pt_rpc
    def setNumReplicas(self, conn, num_replicas):
        self.app.broadcastPartitionChanges((), num_replicas)
        conn.answer(Errors.Ack(''))

    @__change_pt_rpc
    def tweakPartitionTable(self, conn, dry_run, uuid_list):
        app = self.app
        drop_list = []
        for node in app.nm.getStorageList():
            if node.getUUID() in uuid_list or node.isPending():
                drop_list.append(node)
            elif not node.isRunning():
                raise AnswerDenied(
                    'tweak: down nodes must be listed explicitly')
        if dry_run:
            pt = object.__new__(app.pt.__class__)
            new_nodes = pt.load(app.pt.getID(), app.pt.getReplicas(),
                                app.pt.getRowList(), app.nm)
            assert not new_nodes
            pt.addNodeList(node
                for node, count in app.pt.count_dict.iteritems()
                if not count)
        else:
            pt = app.pt
        try:
            changed_list = pt.tweak(drop_list)
        except PartitionTableException, e:
            raise AnswerDenied(str(e))
        if not dry_run:
            app.broadcastPartitionChanges(changed_list)
        conn.answer(Packets.AnswerTweakPartitionTable(
            bool(changed_list), pt.getRowList()))

    @check_state(ClusterStates.RUNNING)
    def truncate(self, conn, tid):
        app = self.app
        if app.getLastTransaction() <= tid:
            raise AnswerDenied("Truncating after last transaction does nothing")
        first_tid = app.tm.getFirstTID()
        if first_tid is None or first_tid > tid:
            raise AnswerDenied("Truncating before first transaction is "
                               "probably not what you intended to do")
        if app.pm.getApprovedRejected(add64(tid, 1))[0]:
            # TODO: The protocol must be extended to support safe cases
            #       (e.g. no started pack whose id is after truncation tid).
            #       The user may also accept having a truncated DB with missing
            #       records (i.e. have an option to force that).
            raise AnswerDenied("Can not truncate before an approved pack")
        conn.answer(Errors.Ack(''))
        raise StoppedOperation(tid)

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
                node.send(Packets.CheckPartition(
                    offset, source, min_tid, max_tid))
        conn.answer(Errors.Ack(''))

    del __change_pt_rpc
