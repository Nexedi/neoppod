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

import os, sys
from time import time

from neo.lib import logging
from neo.lib.connector import getConnectorHandler
from neo.lib.debug import register as registerLiveDebugger
from neo.lib.protocol import UUID_NAMESPACES, ZERO_TID, NotReadyError
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.lib.node import NodeManager
from neo.lib.event import EventManager
from neo.lib.connection import ListeningConnection, ClientConnection
from neo.lib.exception import ElectionFailure, PrimaryFailure, OperationFailure
from neo.lib.util import dump

class StateChangedException(Exception): pass

from .backup_app import BackupApplication
from .handlers import election, identification, secondary
from .handlers import administration, client, storage, shutdown
from .pt import PartitionTable
from .recovery import RecoveryManager
from .transactions import TransactionManager
from .verification import VerificationManager


class Application(object):
    """The master node application."""
    packing = None
    # Latest completely commited TID
    last_transaction = ZERO_TID
    backup_tid = None
    backup_app = None

    def __init__(self, config):
        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager(config.getDynamicMasterList())
        self.tm = TransactionManager(self.onTransactionCommitted)

        self.name = config.getCluster()
        self.server = config.getBind()

        self.storage_readiness = set()
        master_addresses, connector_name = config.getMasters()
        self.connector_handler = getConnectorHandler(connector_name)
        for master_address in master_addresses:
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

        # Generate an UUID for self
        uuid = config.getUUID()
        if uuid is None or uuid == '':
            uuid = self.getNewUUID(NodeTypes.MASTER)
        self.uuid = uuid
        logging.info('UUID      : %s', dump(uuid))

        # election related data
        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()

        self._current_manager = None

        # backup
        upstream_cluster = config.getUpstreamCluster()
        if upstream_cluster:
            if upstream_cluster == self.name:
                raise ValueError("upstream cluster name must be"
                                 " different from cluster name")
            self.backup_app = BackupApplication(self, upstream_cluster,
                                                *config.getUpstreamMasters())

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
        self.nm.close()
        self.em.close()
        del self.__dict__

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
        except:
            logging.exception('Pre-mortem data:')
            self.log()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        # Make a listening port.
        self.listening_conn = ListeningConnection(self.em, None,
            addr=self.server, connector=self.connector_handler())

        # Start a normal operation.
        while True:
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
                        ClientConnection(self.em, client_handler,
                            # XXX: Ugly, but the whole election code will be
                            # replaced soon
                            node=getByAddress(addr),
                            connector=self.connector_handler())
                        self.negotiating_master_node_set.add(addr)
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
            node_list = node_dict.get(node.getType(), [])
            if node_list and node.isRunning():
                node.notify(Packets.NotifyNodeInformation(node_list))

    def broadcastPartitionChanges(self, cell_list, selector=None):
        """Broadcast a Notify Partition Changes packet."""
        logging.debug('broadcastPartitionChanges')
        if not cell_list:
            return
        if not selector:
            selector = lambda n: n.isClient() or n.isStorage() or n.isAdmin()
        self.pt.log()
        ptid = self.pt.setNextID()
        packet = Packets.NotifyPartitionChanges(ptid, cell_list)
        for node in self.nm.getIdentifiedList():
            if not node.isRunning():
                continue
            if selector(node):
                node.notify(packet)

    def broadcastLastOID(self):
        oid = self.tm.getLastOID()
        logging.debug('Broadcast last OID to storages : %s', dump(oid))
        packet = Packets.NotifyLastOID(oid)
        for node in self.nm.getStorageList(only_identified=True):
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
        except OperationFailure:
            # If not operational, send Stop Operation packets to storage
            # nodes and client nodes. Abort connections to client nodes.
            logging.critical('No longer operational')
        except StateChangedException, e:
            assert e.args[0] == ClusterStates.STARTING_BACKUP
            self.backup_tid = tid = self.getLastTransaction()
            self.pt.setBackupTidDict(dict((node.getUUID(), tid)
                for node in self.nm.getStorageList(only_identified=True)))

    def playPrimaryRole(self):
        logging.info('play the primary role with %r', self.listening_conn)
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

        # recover the cluster status at startup
        self.runManager(RecoveryManager)
        while True:
            self.runManager(VerificationManager)
            if self.backup_tid:
                if self.backup_app is None:
                    raise RuntimeError("No upstream cluster to backup"
                                       " defined in configuration")
                self.backup_app.provideService()
            else:
                self.provideService()
            for node in self.nm.getIdentifiedList():
                if node.isStorage() or node.isClient():
                    node.notify(Packets.StopOperation())
                    if node.isClient():
                        node.getConnection().abort()

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

        # Restart completely. Non-optimized
        # but lower level code needs to be stabilized first.
        for conn in self.em.getConnectionList():
            if not conn.isListening():
                conn.close()

        # Reconnect to primary master node.
        primary_handler = secondary.PrimaryHandler(self)
        ClientConnection(self.em, primary_handler,
            node=self.primary_master_node,
            connector=self.connector_handler())

        # and another for the future incoming connections
        self.listening_conn.setHandler(
            identification.SecondaryIdentificationHandler(self))

        while True:
            self.em.poll(1)

    def runManager(self, manager_klass):
        self._current_manager = manager_klass(self)
        self._current_manager.run()
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
        else:
            raise RuntimeError('Unexpected cluster state')

        # change handlers
        notification_packet = Packets.NotifyClusterInformation(state)
        for node in self.nm.getIdentifiedList():
            if node.isMaster():
                continue
            conn = node.getConnection()
            node.notify(notification_packet)
            if node.isClient():
                if state != ClusterStates.RUNNING:
                    conn.abort()
                    continue
                handler = client_handler
            elif node.isStorage():
                handler = storage_handler
            else:
                continue # keep handler
            if type(handler) is not type(conn.getLastHandler()):
                conn.setHandler(handler)
                handler.connectionCompleted(conn)
        self.cluster_state = state

    def getNewUUID(self, node_type):
        try:
            return UUID_NAMESPACES[node_type] + os.urandom(15)
        except KeyError:
            raise RuntimeError, 'No UUID namespace found for this node type'

    def isValidUUID(self, uuid, addr):
        if uuid == self.uuid or uuid is None:
            return False
        node = self.nm.getByUUID(uuid)
        return node is None or node.getAddress() in (None, addr)

    def getClusterState(self):
        return self.cluster_state

    def shutdown(self):
        """Close all connections and exit"""
        # XXX: This behaviour is probably broken, as it applies the same
        #   handler to all connection types. It must be carefuly reviewed and
        #   corrected.
        # change handler
        handler = shutdown.ShutdownHandler(self)
        for node in self.nm.getIdentifiedList():
            node.getConnection().setHandler(handler)

        # wait for all transaction to be finished
        while self.tm.hasPending():
            self.em.poll(1)

        if self.cluster_state != ClusterStates.RUNNING:
            logging.info("asking all nodes to shutdown")
            # This code sends packets but never polls, so they never reach
            # network.
            for node in self.nm.getIdentifiedList():
                notification = Packets.NotifyNodeInformation([node.asTuple()])
                if node.isClient():
                    node.notify(notification)
                elif node.isStorage() or node.isMaster():
                    node.notify(notification)

        # then shutdown
        sys.exit()

    def identifyStorageNode(self, known):
        if self.cluster_state == ClusterStates.STOPPING:
            raise NotReadyError
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
        self.setLastTransaction(tid)

    def getLastTransaction(self):
        return self.last_transaction

    def setLastTransaction(self, tid):
        ltid = self.last_transaction
        assert tid >= ltid, (tid, ltid)
        self.last_transaction = tid

    def setStorageNotReady(self, uuid):
        self.storage_readiness.discard(uuid)

    def setStorageReady(self, uuid):
        self.storage_readiness.add(uuid)

    def isStorageReady(self, uuid):
        return uuid in self.storage_readiness

