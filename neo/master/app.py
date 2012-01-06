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
import os, sys
from time import time

from neo.lib import protocol
from neo.lib.protocol import UUID_NAMESPACES, ZERO_TID
from neo.lib.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.lib.node import NodeManager
from neo.lib.event import EventManager
from neo.lib.connection import ListeningConnection, ClientConnection
from neo.lib.exception import ElectionFailure, PrimaryFailure, OperationFailure
from neo.master.handlers import election, identification, secondary
from neo.master.handlers import storage, client, shutdown
from neo.master.handlers import administration
from neo.master.pt import PartitionTable
from neo.master.transactions import TransactionManager
from neo.master.verification import VerificationManager
from neo.master.recovery import RecoveryManager
from neo.lib.util import dump
from neo.lib.connector import getConnectorHandler

from neo.lib.debug import register as registerLiveDebugger

class Application(object):
    """The master node application."""
    packing = None
    # Latest completely commited TID
    last_transaction = ZERO_TID

    def __init__(self, config):
        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.tm = TransactionManager(self.onTransactionCommitted)

        self.name = config.getCluster()
        self.server = config.getBind()

        self.storage_readiness = set()
        master_addresses, connector_name = config.getMasters()
        self.connector_handler = getConnectorHandler(connector_name)
        for master_address in master_addresses :
            self.nm.createMaster(address=master_address)

        neo.lib.logging.debug('IP address is %s, port is %d', *(self.server))

        # Partition table
        replicas, partitions = config.getReplicas(), config.getPartitions()
        if replicas < 0:
            raise RuntimeError, 'replicas must be a positive integer'
        if partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        self.pt = PartitionTable(partitions, replicas)
        neo.lib.logging.info('Configuration:')
        neo.lib.logging.info('Partitions: %d', partitions)
        neo.lib.logging.info('Replicas  : %d', replicas)
        neo.lib.logging.info('Name      : %s', self.name)

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
        neo.lib.logging.info('UUID      : %s', dump(uuid))

        # election related data
        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()

        self._current_manager = None

        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        self.nm.close()
        self.em.close()
        del self.__dict__

    def log(self):
        self.em.log()
        self.nm.log()
        self.tm.log()
        if self.pt is not None:
            self.pt.log()

    def run(self):
        try:
            self._run()
        except:
            neo.lib.logging.info('\nPre-mortem informations:')
            self.log()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        bootstrap = True

        # Make a listening port.
        self.listening_conn = ListeningConnection(self.em, None,
            addr=self.server, connector=self.connector_handler())

        # Start a normal operation.
        while True:
            # (Re)elect a new primary master.
            self.primary = not self.nm.getMasterList()
            if not self.primary:
                self.electPrimary(bootstrap=bootstrap)
                bootstrap = False
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


    def electPrimary(self, bootstrap = True):
        """Elect a primary master node.

        The difficulty is that a master node must accept connections from
        others while attempting to connect to other master nodes at the
        same time. Note that storage nodes and client nodes may connect
        to self as well as master nodes."""
        neo.lib.logging.info('begin the election of a primary master')

        self.unconnected_master_node_set.clear()
        self.negotiating_master_node_set.clear()
        self.listening_conn.setHandler(election.ServerElectionHandler(self))

        while True:

            # handle new connected masters
            for node in self.nm.getMasterList():
                node.setUnknown()
                self.unconnected_master_node_set.add(node.getAddress())

            # start the election process
            self.primary = None
            self.primary_master_node = None
            try:
                self._doElection(bootstrap)
            except ElectionFailure, m:
                # something goes wrong, clean then restart
                self._electionFailed(m)
                bootstrap = False
            else:
                # election succeed, stop the process
                self.primary = self.primary is None
                break


    def _doElection(self, bootstrap):
        """
            Start the election process:
                - Try to connect to any known master node
                - Wait at most for the timeout defined by bootstrap parameter
            When done, the current process is defined either as primary or
            secondary master node
        """
        # Wait at most 20 seconds at bootstrap. Otherwise, wait at most
        # 10 seconds to avoid stopping the whole cluster for a long time.
        # Note that even if not all master are up in the first 20 seconds
        # this is not an issue because the first up will timeout and take
        # the primary role.
        if bootstrap:
            expiration = 20
        else:
            expiration = 10
        client_handler = election.ClientElectionHandler(self)
        t = 0
        while True:
            current_time = time()
            if current_time >= t:
                t = current_time + 1
                for node in self.nm.getMasterList():
                    if not node.isRunning() and node.getLastStateChange() + \
                            expiration < current_time:
                        neo.lib.logging.info('%s is down' % (node, ))
                        node.setDown()
                        self.unconnected_master_node_set.discard(
                                node.getAddress())

                # Try to connect to master nodes.
                for addr in self.unconnected_master_node_set.difference(
                              x.getAddress() for x in self.em.getClientList()):
                    ClientConnection(self.em, client_handler, addr=addr,
                                     connector=self.connector_handler())
            self.em.poll(1)

            if not (self.unconnected_master_node_set or
                    self.negotiating_master_node_set):
                break

    def _announcePrimary(self):
        """
            Broadcast the announce that I'm the primary
        """
        # I am the primary.
        neo.lib.logging.debug('I am the primary, sending an announcement')
        for conn in self.em.getClientList():
            conn.notify(Packets.AnnouncePrimary())
            conn.abort()
        t = time()
        while self.em.getClientList():
            self.em.poll(1)
            if t + 10 < time():
                for conn in self.em.getClientList():
                    conn.close()
                break


    def _electionFailed(self, m):
        """
            Ask other masters to reelect a primary after an election failure.
        """
        neo.lib.logging.error('election failed: %s', (m, ))

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
        neo.lib.logging.debug('broadcastPartitionChanges')
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

    def outdateAndBroadcastPartition(self):
        " Outdate cell of non-working nodes and broadcast changes """
        self.broadcastPartitionChanges(self.pt.outdate())

    def broadcastLastOID(self):
        oid = self.tm.getLastOID()
        neo.lib.logging.debug(
                        'Broadcast last OID to storages : %s' % dump(oid))
        packet = Packets.NotifyLastOID(oid)
        for node in self.nm.getStorageList(only_identified=True):
            node.notify(packet)

    def provideService(self):
        """
        This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophy happens or the user commits
        a shutdown.
        """
        neo.lib.logging.info('provide service')
        em = self.em
        self.tm.reset()

        self.changeClusterState(ClusterStates.RUNNING)

        # Now everything is passive.
        while True:
            try:
                em.poll(1)
            except OperationFailure:
                # If not operational, send Stop Operation packets to storage
                # nodes and client nodes. Abort connections to client nodes.
                neo.lib.logging.critical('No longer operational')
                for node in self.nm.getIdentifiedList():
                    if node.isStorage() or node.isClient():
                        node.notify(Packets.StopOperation())
                        if node.isClient():
                            node.getConnection().abort()

                # Then, go back, and restart.
                return

    def playPrimaryRole(self):
        neo.lib.logging.info(
                        'play the primary role with %r', self.listening_conn)

        # i'm the primary, send the announcement
        self._announcePrimary()
        # all incoming connections identify through this handler
        self.listening_conn.setHandler(
                identification.IdentificationHandler(self))

        em = self.em
        nm = self.nm

        # Close all remaining connections to other masters,
        # for the same reason as in playSecondaryRole.
        for conn in em.getConnectionList():
            conn_uuid = conn.getUUID()
            if conn_uuid is not None:
                node = nm.getByUUID(conn_uuid)
                assert node is not None
                assert node.isMaster() and not conn.isClient()
                assert node._connection is None and node.isUnknown()
                # this may trigger 'unexpected answer' warnings on remote side
                conn.close()

        # If I know any storage node, make sure that they are not in the
        # running state, because they are not connected at this stage.
        for node in nm.getStorageList():
            if node.isRunning():
                node.setTemporarilyDown()

        # recover the cluster status at startup
        self.runManager(RecoveryManager)
        while True:
            self.runManager(VerificationManager)
            self.provideService()

    def playSecondaryRole(self):
        """
        I play a secondary role, thus only wait for a primary master to fail.
        """
        neo.lib.logging.info('play the secondary role with %r',
            self.listening_conn)

        # Wait for an announcement. If this is too long, probably
        # the primary master is down.
        t = time()
        while self.primary_master_node is None:
            self.em.poll(1)
            if t + 10 < time():
                # election timeout
                raise ElectionFailure("Election timeout")

        # Restart completely. Non-optimized
        # but lower level code needs to be stabilized first.
        addr = self.primary_master_node.getAddress()
        for conn in self.em.getConnectionList():
            conn.close()

        # Reconnect to primary master node.
        primary_handler = secondary.PrimaryHandler(self)
        ClientConnection(self.em, primary_handler, addr=addr,
            connector=self.connector_handler())

        # and another for the future incoming connections
        handler = identification.IdentificationHandler(self)
        self.listening_conn.setHandler(handler)

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
        client_handler = client.ClientServiceHandler(self)
        if state == ClusterStates.RUNNING:
            storage_handler = storage.StorageServiceHandler(self)
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
            if node.isClient() and conn.isAborted():
                continue
            node.notify(notification_packet)
            if node.isClient():
                if state != ClusterStates.RUNNING:
                    conn.close()
                    continue
                handler = client_handler
            elif node.isStorage():
                handler = storage_handler
            else:
                continue # keep handler
            conn.setHandler(handler)
            handler.connectionCompleted(conn)
        self.cluster_state = state

    def getNewUUID(self, node_type):
        # build an UUID
        uuid = os.urandom(15)
        while uuid == protocol.INVALID_UUID[1:]:
            uuid = os.urandom(15)
        # look for the prefix
        prefix = UUID_NAMESPACES.get(node_type, None)
        if prefix is None:
            raise RuntimeError, 'No UUID namespace found for this node type'
        return prefix + uuid

    def isValidUUID(self, uuid, addr):
        node = self.nm.getByUUID(uuid)
        if node is not None and node.getAddress() is not None \
                and node.getAddress() != addr:
            return False
        return uuid != self.uuid and uuid is not None

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
            neo.lib.logging.info("asking all nodes to shutdown")
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

    def identifyStorageNode(self, uuid, node):
        state = NodeStates.RUNNING
        handler = None
        if self.cluster_state == ClusterStates.RUNNING:
            if uuid is None or node is None:
                # same as for verification
                state = NodeStates.PENDING
            handler = storage.StorageServiceHandler(self)
        elif self.cluster_state == ClusterStates.STOPPING:
            raise protocol.NotReadyError
        else:
            raise RuntimeError('unhandled cluster state: %s' %
                    (self.cluster_state, ))
        return (uuid, state, handler)

    def identifyNode(self, node_type, uuid, node):

        state = NodeStates.RUNNING

        if node_type == NodeTypes.ADMIN:
            # always accept admin nodes
            node_ctor = self.nm.createAdmin
            handler = administration.AdministrationHandler(self)
            neo.lib.logging.info('Accept an admin %s' % (dump(uuid), ))
        elif node_type == NodeTypes.MASTER:
            if node is None:
                # unknown master, rejected
                raise protocol.ProtocolError('Reject an unknown master node')
            # always put other master in waiting state
            node_ctor = self.nm.createMaster
            handler = secondary.SecondaryMasterHandler(self)
            neo.lib.logging.info('Accept a master %s' % (dump(uuid), ))
        elif node_type == NodeTypes.CLIENT:
            # refuse any client before running
            if self.cluster_state != ClusterStates.RUNNING:
                neo.lib.logging.info('Reject a connection from a client')
                raise protocol.NotReadyError
            node_ctor = self.nm.createClient
            handler = client.ClientServiceHandler(self)
            neo.lib.logging.info('Accept a client %s' % (dump(uuid), ))
        elif node_type == NodeTypes.STORAGE:
            node_ctor = self.nm.createStorage
            manager = self._current_manager
            if manager is None:
                manager = self
            (uuid, state, handler) = manager.identifyStorageNode(uuid, node)
            neo.lib.logging.info('Accept a storage %s (%s)' %
                            (dump(uuid), state))
        else:
            handler = identification.IdentificationHandler(self)
        return (uuid, node, state, handler, node_ctor)

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

