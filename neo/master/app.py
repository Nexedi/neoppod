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

import sys
from collections import defaultdict
from time import time

from neo.lib import logging, util
from neo.lib.app import BaseApplication, buildOptionParser
from neo.lib.debug import register as registerLiveDebugger
from neo.lib.protocol import UUID_NAMESPACES, ZERO_TID
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.lib.handler import EventHandler
from neo.lib.connection import ListeningConnection, ClientConnection
from neo.lib.exception import PrimaryElected, PrimaryFailure, StoppedOperation

class StateChangedException(Exception): pass

_previous_time = 0
def monotonic_time():
    global _previous_time
    now = time()
    if _previous_time < now:
        _previous_time = now
    else:
        _previous_time = now = _previous_time + 1e-3
    return now

from .backup_app import BackupApplication
from .handlers import identification, administration, client, master, storage
from .pt import PartitionTable
from .recovery import RecoveryManager
from .transactions import TransactionManager
from .verification import VerificationManager


@buildOptionParser
class Application(BaseApplication):
    """The master node application."""
    packing = None
    storage_readiness = 0
    # Latest completely committed TID
    last_transaction = ZERO_TID
    backup_tid = None
    backup_app = None
    truncate_tid = None

    def setUUID(self, uuid):
        node = self.nm.getByUUID(uuid)
        if node is not self._node:
            if node:
                node.setUUID(None)
                if node.isConnected(True):
                    node.getConnection().close()
            self._node.setUUID(uuid)
        logging.node(self.name, uuid)
    uuid = property(lambda self: self._node.getUUID(), setUUID)

    @property
    def election(self):
        if self.primary and self.cluster_state == ClusterStates.RECOVERING:
            return self.primary

    @classmethod
    def _buildOptionParser(cls):
        _ = cls.option_parser
        _.description = "NEO Master node"
        cls.addCommonServerOptions('master', '127.0.0.1:10000', '')

        _ = _.group('master')
        _.int('r', 'replicas', default=0, help="replicas number")
        _.int('p', 'partitions', default=100, help="partitions number")
        _.int('A', 'autostart',
            help="minimum number of pending storage nodes to automatically"
                 " start new cluster (to avoid unwanted recreation of the"
                 " cluster, this should be the total number of storage nodes)")
        _('C', 'upstream-cluster',
            help='the name of cluster to backup')
        _('M', 'upstream-masters', parse=util.parseMasterList,
            help='list of master nodes in the cluster to backup')
        _.int('i', 'nid',
            help="specify an NID to use for this process (testing purpose)")

    def __init__(self, config):
        super(Application, self).__init__(
            config.get('ssl'), config.get('dynamic_master_list'))
        self.tm = TransactionManager(self.onTransactionCommitted)

        self.name = config['cluster']
        self.server = config['bind']
        self.autostart = config.get('autostart')

        self.storage_ready_dict = {}
        self.storage_starting_set = set()
        for master_address in config['masters']:
            self.nm.createMaster(address=master_address)
        self._node = self.nm.createMaster(address=self.server,
                                          uuid=config.get('nid'))
        logging.node(self.name, self.uuid)

        logging.debug('IP address is %s, port is %d', *self.server)

        # Partition table
        replicas = config['replicas']
        partitions = config['partitions']
        if replicas < 0:
            raise RuntimeError, 'replicas must be a positive integer'
        if partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        self.pt = PartitionTable(partitions, replicas)
        logging.info('Configuration:')
        logging.info('Partitions: %d', partitions)
        logging.info('Replicas  : %d', replicas)
        logging.info('Name      : %s', self.name)

        self.listening_conn = None
        self.cluster_state = None
        self._current_manager = None

        # backup
        upstream_cluster = config.get('upstream_cluster')
        if upstream_cluster:
            if upstream_cluster == self.name:
                raise ValueError("upstream cluster name must be"
                                 " different from cluster name")
            self.backup_app = BackupApplication(self, upstream_cluster,
                                                config['upstream_masters'])

        self.administration_handler = administration.AdministrationHandler(
            self)
        self.election_handler = master.ElectionHandler(self)
        self.secondary_handler = master.SecondaryHandler(self)
        self.client_service_handler = client.ClientServiceHandler(self)
        self.client_ro_service_handler = client.ClientReadOnlyServiceHandler(self)
        self.storage_service_handler = storage.StorageServiceHandler(self)

        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        if self.backup_app is not None:
            self.backup_app.close()
        super(Application, self).close()

    def log(self):
        self.em.log()
        if self.backup_app is not None:
            self.backup_app.log()
        self.nm.log()
        self.tm.log()
        if self.pt is not None:
            self.pt.log()

    def run(self):
        try:
            self._run()
        except Exception:
            logging.exception('Pre-mortem data:')
            self.log()
            logging.flush()
            raise

    def _run(self):
        self.listening_conn = ListeningConnection(self, None, self.server)
        while True:
            self.playPrimaryRole()
            self.playSecondaryRole()

    def getNodeInformationDict(self, node_list):
        node_dict = defaultdict(list)
        # group modified nodes by destination node type
        for node in node_list:
            node_info = node.asTuple()
            if node.isAdmin():
                continue
            node_dict[NodeTypes.ADMIN].append(node_info)
            node_dict[NodeTypes.STORAGE].append(node_info)
            if node.isClient():
                continue
            node_dict[NodeTypes.CLIENT].append(node_info)
            if node.isStorage():
                continue
            node_dict[NodeTypes.MASTER].append(node_info)
        return node_dict

    def broadcastNodesInformation(self, node_list, exclude=None):
        """
          Broadcast changes for a set a nodes
          Send only one packet per connection to reduce bandwidth
        """
        node_dict = self.getNodeInformationDict(node_list)
        now = monotonic_time()
        # send at most one non-empty notification packet per node
        for node in self.nm.getIdentifiedList():
            node_list = node_dict.get(node.getType())
            # We don't skip pending storage nodes because we don't send them
            # the full list of nodes when they're added, and it's also quite
            # useful to notify them about new masters.
            if node_list and node is not exclude:
                node.send(Packets.NotifyNodeInformation(now, node_list))

    def broadcastPartitionChanges(self, cell_list):
        """Broadcast a Notify Partition Changes packet."""
        if cell_list:
            ptid = self.pt.setNextID()
            self.pt.logUpdated()
            packet = Packets.NotifyPartitionChanges(ptid, cell_list)
            for node in self.nm.getIdentifiedList():
                # As for broadcastNodesInformation, we don't send the full PT
                # when pending storage nodes are added, so keep them notified.
                if not node.isMaster():
                    node.send(packet)

    def provideService(self):
        """
        This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophe happens or the user commits
        a shutdown.
        """
        logging.info('provide service')
        poll = self.em.poll
        self.changeClusterState(ClusterStates.RUNNING)

        # Now everything is passive.
        try:
            while True:
                poll(1)
        except StateChangedException, e:
            if e.args[0] != ClusterStates.STARTING_BACKUP:
                raise
            self.backup_tid = tid = self.getLastTransaction()
            packet = Packets.StartOperation(True)
            tid_dict = {}
            for node in self.nm.getStorageList(only_identified=True):
                tid_dict[node.getUUID()] = tid
                if node.isRunning():
                    node.send(packet)
            self.pt.setBackupTidDict(tid_dict)

    def playPrimaryRole(self):
        logging.info('play the primary role with %r', self.listening_conn)
        self.primary_master = None
        for conn in self.em.getConnectionList():
            if conn.isListening():
                conn.setHandler(identification.IdentificationHandler(self))
            else:
                conn.close()

        # If I know any storage node, make sure that they are not in the
        # running state, because they are not connected at this stage.
        for node in self.nm.getStorageList():
            assert node.isDown(), node

        if self.uuid is None:
            self.uuid = self.getNewUUID(None, self.server, NodeTypes.MASTER)
        self._node.setRunning()
        self._node.id_timestamp = None
        self.primary = monotonic_time()

        # Do not restart automatically if an election happens, in order
        # to avoid a split of the database. For example, with 2 machines with
        # a master and a storage on each one and replicas=1, the secondary
        # master becomes primary in case of network failure between the 2
        # machines but must not start automatically: otherwise, each storage
        # node would diverge.
        self._startup_allowed = False
        try:
            while True:
                self.runManager(RecoveryManager)
                try:
                    self.runManager(VerificationManager)
                    if not self.backup_tid:
                        self.provideService()
                        # self.provideService only returns without raising
                        # when switching to backup mode.
                    if self.backup_app is None:
                        raise RuntimeError("No upstream cluster to backup"
                                           " defined in configuration")
                    truncate = Packets.Truncate(
                        self.backup_app.provideService())
                except StoppedOperation, e:
                    logging.critical('No longer operational')
                    truncate = Packets.Truncate(*e.args) if e.args else None
                    # Automatic restart except if we truncate or retry to.
                    self._startup_allowed = not (self.truncate_tid or truncate)
                self.storage_readiness = 0
                self.storage_ready_dict.clear()
                self.storage_starting_set.clear()
                node_list = []
                for node in self.nm.getIdentifiedList():
                    if node.isStorage() or node.isClient():
                        conn = node.getConnection()
                        conn.send(Packets.StopOperation())
                        if node.isClient():
                            conn.abort()
                            continue
                        if truncate:
                            conn.send(truncate)
                        if node.isRunning():
                            node.setPending()
                            node_list.append(node)
                self.broadcastNodesInformation(node_list)
        except StateChangedException, e:
            assert e.args[0] == ClusterStates.STOPPING
            self.shutdown()
        except PrimaryElected, e:
            self.primary_master, = e.args

    def playSecondaryRole(self):
        """
        A master play the secondary role when it is unlikely to win the
        election (it lost against against another master during identification
        or it was notified that another is the primary master).
        Its only task is to try again to become the primary master when the
        later fail. When connected to the cluster, the only communication is
        with the primary master, to stay informed about removed/added master
        nodes, and exit if requested.
        """
        logging.info('play the secondary role with %r', self.listening_conn)
        self.primary = None
        handler = master.PrimaryHandler(self)
        # The connection to the probably-primary master can be in any state
        # depending on how we were informed. The only case in which it can not
        # be reused in when we have pending requests.
        if self.primary_master.isConnected(True):
            master_conn = self.primary_master.getConnection()
            # When we find the primary during identification, we don't attach
            # the connection (a server one) to any node, and it will be closed
            # in the below 'for' loop.
            assert master_conn.isClient(), master_conn
            try:
                # We want the handler to be effective immediately.
                # If it's not possible, let's just reconnect.
                if not master_conn.setHandler(handler):
                    master_conn.close()
                    assert False
            except PrimaryFailure:
                master_conn = None
        else:
            master_conn = None
        for conn in self.em.getConnectionList():
            if conn.isListening():
                conn.setHandler(
                    identification.SecondaryIdentificationHandler(self))
            elif conn is not master_conn:
                conn.close()

        failed = {self.server}
        poll = self.em.poll
        while True:
            try:
                if master_conn is None:
                    for node in self.nm.getMasterList():
                        node.setDown()
                    node = self.primary_master
                    failed.add(node.getAddress())
                    if not node.isConnected(True):
                        # On immediate connection failure,
                        # PrimaryFailure is raised.
                        ClientConnection(self, handler, node)
                else:
                    master_conn = None
                while True:
                    poll(1)
            except PrimaryFailure:
                if self.primary_master.isRunning():
                    # XXX: What's the best to do here ? Another option is to
                    #      choose the RUNNING master node with the lowest
                    #      election key (i.e. (id_timestamp, address) as
                    #      defined in IdentificationHandler), and return if we
                    #      have the lowest one.
                    failed = {self.server}
                else:
                    # Since the last primary failure (or since we play the
                    # secondary role), do not try any node more than once.
                    for self.primary_master in self.nm.getMasterList():
                        if self.primary_master.getAddress() not in failed:
                            break
                    else:
                        # All known master nodes are either down or secondary.
                        # Let's play the primary role again.
                        break
            except PrimaryElected, e:
                node = self.primary_master
                self.primary_master, = e.args
                assert node is not self.primary_master, node
                try:
                    node.getConnection().close()
                except PrimaryFailure:
                    pass

    def runManager(self, manager_klass):
        self._current_manager = manager_klass(self)
        try:
            self._current_manager.run()
        finally:
            self._current_manager = None

    def changeClusterState(self, state):
        """
        Change the cluster state and apply right handler on each connections
        """
        if self.cluster_state == state:
            return

        # select the storage handler
        if state in (ClusterStates.RUNNING, ClusterStates.STARTING_BACKUP,
                     ClusterStates.BACKINGUP, ClusterStates.STOPPING_BACKUP):
            storage_handler = self.storage_service_handler
        elif self._current_manager is not None:
            storage_handler = self._current_manager.getHandler()
        elif state == ClusterStates.STOPPING:
            storage_handler = None
        else:
            raise RuntimeError('Unexpected cluster state')

        # change handlers
        notification_packet = Packets.NotifyClusterInformation(state)
        for node in self.nm.getList():
            if not node.isConnected(True):
                continue
            conn = node.getConnection()
            if node.isIdentified():
                conn.send(notification_packet)
            elif conn.isServer():
                continue
            if node.isClient():
                if state == ClusterStates.RUNNING:
                    handler = self.client_service_handler
                elif state == ClusterStates.BACKINGUP:
                    handler = self.client_ro_service_handler
                else:
                    if state != ClusterStates.STOPPING:
                        conn.abort()
                    continue
            elif node.isMaster():
                if state == ClusterStates.RECOVERING:
                    handler = self.election_handler
                else:
                    handler = self.secondary_handler
            elif node.isStorage() and storage_handler:
                handler = storage_handler
            else:
                continue # keep handler
            if type(handler) is not type(conn.getLastHandler()):
                conn.setHandler(handler)
                handler.connectionCompleted(conn, new=False)
        self.cluster_state = state

    def getNewUUID(self, uuid, address, node_type):
        getByUUID = self.nm.getByUUID
        if None != uuid != self.uuid:
            node = getByUUID(uuid)
            if node is None or node.getAddress() == address:
                return uuid
        hob = UUID_NAMESPACES[node_type]
        for uuid in xrange((hob << 24) + 1, hob + 0x10 << 24):
            node = getByUUID(uuid)
            if node is None or None is not address == node.getAddress():
                assert uuid != self.uuid
                return uuid
        raise RuntimeError

    def getClusterState(self):
        return self.cluster_state

    def shutdown(self):
        """Close all connections and exit"""
        self.changeClusterState(ClusterStates.STOPPING)
        # Marking a fictional storage node as starting operation blocks any
        # request to start a new transaction. Do this way has 2 advantages:
        # - It's simpler than changing the handler of all clients,
        #   which is anyway not supported by EventQueue.
        # - Returning an error code would cause activity on client side for
        #   nothing.
        # What's important is to not abort during the second phase of commits
        # and for this, clients must even be able to reconnect, in case of
        # failure during tpc_finish.
        # We're rarely involved in vote, so we have to trust clients that they
        # abort any transaction that is still in the first phase.
        self.storage_starting_set.add(None)
        try:
            # wait for all transaction to be finished
            while self.tm.hasPending():
                self.em.poll(1)
        except StoppedOperation:
            logging.critical('No longer operational')

        logging.info("asking remaining nodes to shutdown")
        self.listening_conn.close()
        handler = EventHandler(self)
        for node in self.nm.getList():
            if not node.isConnected(True):
                continue
            conn = node.getConnection()
            conn.setHandler(handler)
            if not conn.connecting:
                if node.isStorage():
                    conn.send(Packets.NotifyNodeInformation(monotonic_time(), ((
                        node.getType(), node.getAddress(), node.getUUID(),
                        NodeStates.DOWN, None),)))
                if conn.pending():
                    conn.abort()
                    continue
            conn.close()

        while self.em.connection_dict:
            self.em.poll(1)

        # then shutdown
        sys.exit()

    def identifyStorageNode(self, known):
        if known:
            state = NodeStates.RUNNING
        else:
            # same as for verification
            state = NodeStates.PENDING
        return state, self.storage_service_handler

    def onTransactionCommitted(self, txn):
        # I have received all the lock answers now:
        # - send a Notify Transaction Finished to the initiated client node
        # - Invalidate Objects to the other client nodes
        ttid = txn.getTTID()
        tid = txn.getTID()
        transaction_node = txn.getNode()
        invalidate_objects = Packets.InvalidateObjects(tid, txn.getOIDList())
        for client_node in self.nm.getClientList(only_identified=True):
            if client_node is transaction_node:
                client_node.send(Packets.AnswerTransactionFinished(ttid, tid),
                                 msg_id=txn.getMessageId())
            else:
                client_node.send(invalidate_objects)

        # Unlock Information to relevant storage nodes.
        notify_unlock = Packets.NotifyUnlockInformation(ttid)
        getByUUID = self.nm.getByUUID
        for storage_uuid in txn.getUUIDList():
            getByUUID(storage_uuid).send(notify_unlock)

        # Notify storage that have replications blocked by this transaction,
        # and clients that try to recover from a failure during tpc_finish.
        notify_finished = Packets.NotifyTransactionFinished(ttid, tid)
        for uuid in txn.getNotificationUUIDList():
            node = getByUUID(uuid)
            if node.isClient():
                # There should be only 1 client interested.
                node.answer(Packets.AnswerFinalTID(tid))
            else:
                node.send(notify_finished)

        assert self.last_transaction < tid, (self.last_transaction, tid)
        self.setLastTransaction(tid)

    def getLastTransaction(self):
        return self.last_transaction

    def setLastTransaction(self, tid):
        self.last_transaction = tid

    def setStorageNotReady(self, uuid):
        self.storage_starting_set.discard(uuid)
        self.storage_ready_dict.pop(uuid, None)
        self.tm.executeQueuedEvents()

    def startStorage(self, node):
        node.send(Packets.StartOperation(self.backup_tid))
        uuid = node.getUUID()
        assert uuid not in self.storage_starting_set
        if uuid not in self.storage_ready_dict:
            self.storage_starting_set.add(uuid)

    def setStorageReady(self, uuid):
        self.storage_starting_set.remove(uuid)
        assert uuid not in self.storage_ready_dict, self.storage_ready_dict
        self.storage_readiness = self.storage_ready_dict[uuid] = \
            self.storage_readiness + 1
        self.tm.executeQueuedEvents()

    def isStorageReady(self, uuid):
        return uuid in self.storage_ready_dict

    def getStorageReadySet(self, readiness=float('inf')):
        return {k for k, v in self.storage_ready_dict.iteritems()
                  if v <= readiness}

    def notifyTransactionAborted(self, ttid, uuids):
        uuid_set = self.getStorageReadySet()
        uuid_set.intersection_update(uuids)
        if uuid_set:
            p = Packets.AbortTransaction(ttid, ())
            getByUUID = self.nm.getByUUID
            for uuid in uuid_set:
                getByUUID(uuid).send(p)
