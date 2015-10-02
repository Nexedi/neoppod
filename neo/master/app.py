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

import sys, weakref
from time import time

from neo.lib import logging
from neo.lib.app import BaseApplication
from neo.lib.debug import register as registerLiveDebugger
from neo.lib.protocol import uuid_str, UUID_NAMESPACES, ZERO_TID
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.lib.handler import EventHandler
from neo.lib.connection import ListeningConnection, ClientConnection
from neo.lib.exception import ElectionFailure, PrimaryFailure, OperationFailure

class StateChangedException(Exception): pass

from .backup_app import BackupApplication
from .handlers import election, identification, secondary
from .handlers import administration, client, storage
from .pt import PartitionTable
from .recovery import RecoveryManager
from .transactions import TransactionManager
from .verification import VerificationManager


class Application(BaseApplication):
    """The master node application."""
    packing = None
    # Latest completely commited TID
    last_transaction = ZERO_TID
    backup_tid = None
    backup_app = None
    uuid = None

    def __init__(self, config):
        super(Application, self).__init__(
            config.getSSL(), config.getDynamicMasterList())
        self.tm = TransactionManager(self.onTransactionCommitted)

        self.name = config.getCluster()
        self.server = config.getBind()
        self.autostart = config.getAutostart()

        self.storage_readiness = set()
        for master_address in config.getMasters():
            self.nm.createMaster(address=master_address)

        logging.debug('IP address is %s, port is %d', *self.server)

        # Partition table
        replicas, partitions = config.getReplicas(), config.getPartitions()
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
        self.primary = None
        self.primary_master_node = None
        self.cluster_state = None
        self._startup_allowed = False

        uuid = config.getUUID()
        if uuid:
            self.uuid = uuid

        # election related data
        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()
        self.master_address_dict = weakref.WeakKeyDictionary()

        self._current_manager = None

        # backup
        upstream_cluster = config.getUpstreamCluster()
        if upstream_cluster:
            if upstream_cluster == self.name:
                raise ValueError("upstream cluster name must be"
                                 " different from cluster name")
            self.backup_app = BackupApplication(self, upstream_cluster,
                                                config.getUpstreamMasters())

        self.administration_handler = administration.AdministrationHandler(
            self)
        self.secondary_master_handler = secondary.SecondaryMasterHandler(self)
        self.client_service_handler = client.ClientServiceHandler(self)
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
        """Make sure that the status is sane and start a loop."""
        # Make a listening port.
        self.listening_conn = ListeningConnection(self, None, self.server)

        # Start a normal operation.
        while self.cluster_state != ClusterStates.STOPPING:
            # (Re)elect a new primary master.
            self.primary = not self.nm.getMasterList()
            if not self.primary:
                self.electPrimary()
            try:
                if self.primary:
                    self.playPrimaryRole()
                else:
                    self.playSecondaryRole()
                raise RuntimeError, 'should not reach here'
            except (ElectionFailure, PrimaryFailure):
                # Forget all connections.
                for conn in self.em.getClientList():
                    conn.close()


    def electPrimary(self):
        """Elect a primary master node.

        The difficulty is that a master node must accept connections from
        others while attempting to connect to other master nodes at the
        same time. Note that storage nodes and client nodes may connect
        to self as well as master nodes."""
        logging.info('begin the election of a primary master')

        client_handler = election.ClientElectionHandler(self)
        self.unconnected_master_node_set.clear()
        self.negotiating_master_node_set.clear()
        self.master_address_dict.clear()
        self.listening_conn.setHandler(election.ServerElectionHandler(self))
        getByAddress = self.nm.getByAddress

        while True:

            # handle new connected masters
            for node in self.nm.getMasterList():
                node.setUnknown()
                self.unconnected_master_node_set.add(node.getAddress())

            # start the election process
            self.primary = None
            self.primary_master_node = None
            try:
                while (self.unconnected_master_node_set or
                        self.negotiating_master_node_set):
                    for addr in self.unconnected_master_node_set:
                        self.negotiating_master_node_set.add(addr)
                        ClientConnection(self, client_handler,
                            # XXX: Ugly, but the whole election code will be
                            # replaced soon
                            getByAddress(addr))
                    self.unconnected_master_node_set.clear()
                    self.em.poll(1)
            except ElectionFailure, m:
                # something goes wrong, clean then restart
                logging.error('election failed: %s', m)

                # Ask all connected nodes to reelect a single primary master.
                for conn in self.em.getClientList():
                    conn.notify(Packets.ReelectPrimary())
                    conn.abort()

                # Wait until the connections are closed.
                self.primary = None
                self.primary_master_node = None
                t = time() + 10
                while self.em.getClientList() and time() < t:
                    try:
                        self.em.poll(1)
                    except ElectionFailure:
                        pass

                # Close all connections.
                for conn in self.em.getClientList() + self.em.getServerList():
                    conn.close()
            else:
                # election succeed, stop the process
                self.primary = self.primary is None
                break

    def broadcastNodesInformation(self, node_list):
        """
          Broadcast changes for a set a nodes
          Send only one packet per connection to reduce bandwidth
        """
        node_dict = {}
        # group modified nodes by destination node type
        for node in node_list:
            node_info = node.asTuple()
            def assign_for_notification(node_type):
                # helper function
                node_dict.setdefault(node_type, []).append(node_info)
            if node.isMaster() or node.isStorage():
                # client get notifications for master and storage only
                assign_for_notification(NodeTypes.CLIENT)
            if node.isMaster() or node.isStorage() or node.isClient():
                assign_for_notification(NodeTypes.STORAGE)
                assign_for_notification(NodeTypes.ADMIN)

        # send at most one non-empty notification packet per node
        for node in self.nm.getIdentifiedList():
            node_list = node_dict.get(node.getType())
            if node_list and node.isRunning():
                node.notify(Packets.NotifyNodeInformation(node_list))

    def broadcastPartitionChanges(self, cell_list):
        """Broadcast a Notify Partition Changes packet."""
        logging.debug('broadcastPartitionChanges')
        if cell_list:
            self.pt.log()
            ptid = self.pt.setNextID()
            packet = Packets.NotifyPartitionChanges(ptid, cell_list)
            for node in self.nm.getIdentifiedList():
                # TODO: notify masters
                if node.isRunning() and not node.isMaster():
                    node.notify(packet)

    def provideService(self):
        """
        This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophy happens or the user commits
        a shutdown.
        """
        logging.info('provide service')
        poll = self.em.poll
        self.tm.reset()

        self.changeClusterState(ClusterStates.RUNNING)

        # Now everything is passive.
        try:
            while True:
                poll(1)
        except StateChangedException, e:
            if e.args[0] != ClusterStates.STARTING_BACKUP:
                raise
            self.backup_tid = tid = self.getLastTransaction()
            self.pt.setBackupTidDict({node.getUUID(): tid
                for node in self.nm.getStorageList(only_identified=True)})

    def playPrimaryRole(self):
        logging.info('play the primary role with %r', self.listening_conn)
        self.master_address_dict.clear()
        em = self.em
        packet = Packets.AnnouncePrimary()
        for conn in em.getConnectionList():
            if conn.isListening():
                conn.setHandler(identification.IdentificationHandler(self))
            else:
                conn.notify(packet)
                # Primary master should rather establish connections to all
                # secondaries, rather than the other way around. This requires
                # a bit more work when a new master joins a cluster but makes
                # it easier to resolve UUID conflicts with minimal cluster
                # impact, and ensure primary master unicity (primary masters
                # become noisy, in that they actively try to maintain
                # connections to all other master nodes, so duplicate
                # primaries will eventually get in touch with each other and
                # resolve the situation with a duel).
                # TODO: only abort client connections, don't close server
                # connections as we want to have them in the end. Secondary
                # masters will reconnect nevertheless, but it's dirty.
                # Currently, it's not trivial to preserve connected nodes,
                # because of poor node status tracking during election.
                conn.abort()

        # If I know any storage node, make sure that they are not in the
        # running state, because they are not connected at this stage.
        for node in self.nm.getStorageList():
            if node.isRunning():
                node.setTemporarilyDown()

        if self.uuid is None:
            self.uuid = self.getNewUUID(None, self.server, NodeTypes.MASTER)
            logging.info('My UUID: ' + uuid_str(self.uuid))
        else:
            in_conflict = self.nm.getByUUID(self.uuid)
            if in_conflict is not None:
                logging.warning('UUID conflict at election exit with %r',
                    in_conflict)
                in_conflict.setUUID(None)

        # recover the cluster status at startup
        try:
            self.runManager(RecoveryManager)
            while True:
                self.runManager(VerificationManager)
                try:
                    if self.backup_tid:
                        if self.backup_app is None:
                            raise RuntimeError("No upstream cluster to backup"
                                               " defined in configuration")
                        self.backup_app.provideService()
                        # Reset connection with storages (and go through a
                        # recovery phase) when leaving backup mode in order
                        # to get correct last oid/tid.
                        self.runManager(RecoveryManager)
                        continue
                    self.provideService()
                except OperationFailure:
                    logging.critical('No longer operational')
                for node in self.nm.getIdentifiedList():
                    if node.isStorage() or node.isClient():
                        node.notify(Packets.StopOperation())
                        if node.isClient():
                            node.getConnection().abort()
        except StateChangedException, e:
            assert e.args[0] == ClusterStates.STOPPING
            self.shutdown()

    def playSecondaryRole(self):
        """
        I play a secondary role, thus only wait for a primary master to fail.
        """
        logging.info('play the secondary role with %r', self.listening_conn)

        # Wait for an announcement. If this is too long, probably
        # the primary master is down.
        t = time() + 10
        while self.primary_master_node is None:
            self.em.poll(1)
            if t < time():
                # election timeout
                raise ElectionFailure("Election timeout")
        self.master_address_dict.clear()

        # Restart completely. Non-optimized
        # but lower level code needs to be stabilized first.
        for conn in self.em.getConnectionList():
            if not conn.isListening():
                conn.close()

        # Reconnect to primary master node.
        primary_handler = secondary.PrimaryHandler(self)
        ClientConnection(self, primary_handler, self.primary_master_node)

        # and another for the future incoming connections
        self.listening_conn.setHandler(
            identification.SecondaryIdentificationHandler(self))

        while True:
            self.em.poll(1)

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
        client_handler = self.client_service_handler
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
        for node in self.nm.getIdentifiedList():
            conn = node.getConnection()
            conn.notify(notification_packet)
            if node.isClient():
                if state != ClusterStates.RUNNING:
                    conn.abort()
                    continue
                handler = client_handler
            elif node.isStorage() and storage_handler:
                handler = storage_handler
            else:
                continue # keep handler
            if type(handler) is not type(conn.getLastHandler()):
                conn.setHandler(handler)
                handler.connectionCompleted(conn)
        self.cluster_state = state

    def getNewUUID(self, uuid, address, node_type):
        getByUUID = self.nm.getByUUID
        if None != uuid != self.uuid:
            node = getByUUID(uuid)
            if node is None or node.getAddress() == address:
                return uuid
        hob = UUID_NAMESPACES[node_type]
        for uuid in xrange((hob << 24) + 1, hob + 0x10 << 24):
            if uuid != self.uuid and getByUUID(uuid) is None:
                return uuid
        raise RuntimeError

    def getClusterState(self):
        return self.cluster_state

    def shutdown(self):
        """Close all connections and exit"""
        self.changeClusterState(ClusterStates.STOPPING)
        self.listening_conn.close()
        for conn in self.em.getConnectionList():
            node = self.nm.getByUUID(conn.getUUID())
            if node is None or not node.isIdentified():
                conn.close()
        # No need to change handlers in order to reject RequestIdentification
        # & AskBeginTransaction packets because they won't be any:
        # the only remaining connected peers are identified non-clients
        # and we don't accept new connections anymore.
        try:
            # wait for all transaction to be finished
            while self.tm.hasPending():
                self.em.poll(1)
        except OperationFailure:
            logging.critical('No longer operational')

        logging.info("asking remaining nodes to shutdown")
        handler = EventHandler(self)
        for node in self.nm.getConnectedList():
            conn = node.getConnection()
            if node.isStorage():
                conn.setHandler(handler)
                conn.notify(Packets.NotifyNodeInformation(((
                  node.getType(), node.getAddress(), node.getUUID(),
                  NodeStates.TEMPORARILY_DOWN),)))
                conn.abort()
            elif conn.pending():
                conn.abort()
            else:
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
        transaction_finished = Packets.AnswerTransactionFinished(ttid, tid)
        for client_node in self.nm.getClientList(only_identified=True):
            c = client_node.getConnection()
            if client_node is transaction_node:
                c.answer(transaction_finished, msg_id=txn.getMessageId())
            else:
                c.notify(invalidate_objects)

        # Unlock Information to relevant storage nodes.
        notify_unlock = Packets.NotifyUnlockInformation(ttid)
        getByUUID = self.nm.getByUUID
        for storage_uuid in txn.getUUIDList():
            getByUUID(storage_uuid).getConnection().notify(notify_unlock)

        # Notify storage that have replications blocked by this transaction
        notify_finished = Packets.NotifyTransactionFinished(ttid, tid)
        for storage_uuid in txn.getNotificationUUIDList():
            node = getByUUID(storage_uuid)
            if node is not None and node.isConnected():
                node.getConnection().notify(notify_finished)

        # remove transaction from manager
        self.tm.remove(transaction_node.getUUID(), ttid)
        assert self.last_transaction < tid, (self.last_transaction, tid)
        self.setLastTransaction(tid)

    def getLastTransaction(self):
        return self.last_transaction

    def setLastTransaction(self, tid):
        self.last_transaction = tid

    def setStorageNotReady(self, uuid):
        self.storage_readiness.discard(uuid)

    def setStorageReady(self, uuid):
        self.storage_readiness.add(uuid)

    def isStorageReady(self, uuid):
        return uuid in self.storage_readiness

