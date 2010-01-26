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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging
import os, sys
from time import time
from struct import pack, unpack

from neo import protocol
from neo.protocol import UUID_NAMESPACES
from neo.protocol import ClusterStates, NodeStates, NodeTypes, Packets
from neo.node import NodeManager
from neo.event import EventManager
from neo.connection import ListeningConnection, ClientConnection
from neo.exception import ElectionFailure, PrimaryFailure, \
        VerificationFailure, OperationFailure
from neo.master.handlers import election, identification, secondary, recovery
from neo.master.handlers import verification, storage, client, shutdown
from neo.master.handlers import administration
from neo.master.pt import PartitionTable
from neo.master.transactions import TransactionManager
from neo.util import dump
from neo.connector import getConnectorHandler

REQUIRED_NODE_NUMBER = 1

class Application(object):
    """The master node application."""

    def __init__(self, config):

        # always use default connector for now
        self.connector_handler = getConnectorHandler()

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.tm = TransactionManager()

        self.name = config.getCluster()
        self.server = config.getBind()

        for address in config.getMasters():
            self.nm.createMaster(address=address)

        logging.debug('IP address is %s, port is %d', *(self.server))

        # Partition table
        replicas, partitions = config.getReplicas(), config.getPartitions()
        if replicas < 0:
            raise RuntimeError, 'replicas must be a positive integer'
        if partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        self.pt = PartitionTable(partitions, replicas)
        logging.debug('the number of replicas is %d, the number of ' \
                'partitions is %d, the name is %s',
                replicas, partitions, self.name)

        self.listening_conn = None
        self.primary = None
        self.primary_master_node = None
        self.cluster_state = None

        # Generate an UUID for self
        uuid = config.getUUID()
        if uuid is None or uuid == '':
            uuid = self.getNewUUID(NodeTypes.MASTER)
        self.uuid = uuid

        # The last OID.
        self.loid = None
        # The target node's uuid to request next.
        self.target_uuid = None

        # election related data
        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()

        # verification related data
        self.unfinished_oid_set = set()
        self.unfinished_tid_set = set()
        self.asking_uuid_dict = {}
        self.object_present = False


    def run(self):
        """Make sure that the status is sane and start a loop."""

        # Make a listening port.
        self.listening_conn = ListeningConnection(self.em, None,
            addr = self.server, connector_handler = self.connector_handler)

        self.cluster_state = ClusterStates.BOOTING
        # Start the election of a primary master node.
        self.electPrimary()

        # Start a normal operation.
        while True:
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
                # Reelect a new primary master.
                self.electPrimary(bootstrap = False)


    def electPrimary(self, bootstrap = True):
        """Elect a primary master node.

        The difficulty is that a master node must accept connections from
        others while attempting to connect to other master nodes at the
        same time. Note that storage nodes and client nodes may connect
        to self as well as master nodes."""
        logging.info('begin the election of a primary master')

        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()
        self.listening_conn.setHandler(election.ServerElectionHandler(self))

        for node in self.nm.getMasterList():
            # For now, believe that every node should be available,
            # since down or broken nodes may be already repaired.
            node.setRunning()

        while True:

            # handle new connected masters
            for node in self.nm.getMasterList():
                if node.isRunning():
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
                - Wait a most for the timeout defined by bootstrap parameter
            When done, the current process si defined either as primary or
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
            if current_time >= t + 1:
                t = current_time
                for node in self.nm.getMasterList():
                    if node.isTemporarilyDown() \
                            and node.getLastStateChange() + \
                            expiration < current_time:
                        logging.info('%s is down' % (node, ))
                        node.setDown()
                        self.unconnected_master_node_set.discard(
                                node.getAddress())

                # Try to connect to master nodes.
                if self.unconnected_master_node_set:
                    for addr in list(self.unconnected_master_node_set):
                        ClientConnection(self.em, client_handler, addr=addr,
                             connector_handler=self.connector_handler)
            self.em.poll(1)
            if len(self.unconnected_master_node_set |
                    self.negotiating_master_node_set) == 0:
                break


    def _announcePrimary(self):
        """
            Broadcast the announce that I'm the primary
        """
        # I am the primary.
        logging.debug('I am the primary, sending an announcement')
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


    def _waitForPrimaryAnnouncement(self):
        """
            For for the primary announcement as i'm not the primary.
            If this is too long, raise ElectionFailure to restart the whole
            election process.
        """
        # Wait for an announcement. If this is too long, probably
        # the primary master is down.
        t = time()
        while self.primary_master_node is None:
            self.em.poll(1)
            if t + 10 < time():
                # election timeout
                raise ElectionFailure("Election timeout")

        # Now I need only a connection to the primary master node.
        primary = self.primary_master_node
        addr = primary.getAddress()
        for conn in self.em.getServerList():
            conn.close()
        for conn in self.em.getClientList():
            if conn.getAddress() != addr:
                conn.close()

        # But if there is no such connection, something wrong
        # happened.
        for conn in self.em.getClientList():
            if conn.getAddress() == addr:
                # primary master elected and connected
                break
        else:
            raise ElectionFailure('No connection remains to the primary')


    def _electionFailed(self, m):
        """
            Ask other masters to reelect a primary after an election failure.
        """
        logging.error('election failed: %s', (m, ))

        # Ask all connected nodes to reelect a single primary master.
        for conn in self.em.getClientList():
            conn.notify(Packets.ReelectPrimary())
            conn.abort()

        # Wait until the connections are closed.
        self.primary = None
        self.primary_master_node = None
        t = time()
        while self.em.getClientList():
            try:
                self.em.poll(1)
            except ElectionFailure:
                pass
            if time() > t + 10:
                # If too long, do not wait.
                break

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
            address = node.getAddress()
            uuid = node.getUUID()
            state = node.getState()
            node_info = (node.getType(), address, uuid, state)
            def assign_for_notification(node_type):
                # helper function
                node_dict.setdefault(node_type, []).append(node_info)
            if node.isMaster() or node.isStorage():
                # client get notifications for master and storage only
                assign_for_notification(NodeTypes.CLIENT)
            if node.isMaster() or node.isStorage() or node.isClient():
                assign_for_notification(NodeTypes.MASTER)
                assign_for_notification(NodeTypes.STORAGE)
                assign_for_notification(NodeTypes.ADMIN)

        # send at most one non-empty notification packet per node
        for conn in self.em.getConnectionList():
            if conn.getUUID() is None:
                continue
            node = self.nm.getByUUID(conn.getUUID())
            node_list = node_dict.get(node.getType(), [])
            if node_list:
                conn.notify(Packets.NotifyNodeInformation(node_list))

    def broadcastPartitionChanges(self, cell_list):
        """Broadcast a Notify Partition Changes packet."""
        logging.debug('broadcastPartitionChanges')
        if not cell_list:
           return
        ptid = self.pt.setNextID()
        self.pt.log()
        for c in self.em.getConnectionList():
            n = self.nm.getByUUID(c.getUUID())
            if n is None:
                continue
            if n.isClient() or n.isStorage() or n.isAdmin():
                # Split the packet if too big.
                size = len(cell_list)
                start = 0
                while size:
                    amt = min(10000, size)
                    cell_list = cell_list[start:start+amt]
                    p = Packets.NotifyPartitionChanges(ptid, cell_list)
                    c.notify(p)
                    size -= amt
                    start += amt

    def outdateAndBroadcastPartition(self):
        " Outdate cell of non-working nodes and broadcast changes """
        self.broadcastPartitionChanges(self.pt.outdate())

    def sendPartitionTable(self, conn):
        """ Send the partition table through the given connection """
        row_list = []
        for offset in xrange(self.pt.getPartitions()):
            row_list.append((offset, self.pt.getRow(offset)))
            # Split the packet if too huge.
            if len(row_list) == 1000:
                conn.notify(Packets.SendPartitionTable(self.pt.getID(),
                    row_list))
                del row_list[:]
        if row_list:
            conn.notify(Packets.SendPartitionTable(self.pt.getID(), row_list))

    def sendNodesInformations(self, conn):
        """ Send informations on all nodes through the given connection """
        node_list = []
        for n in self.nm.getList():
            if not n.isAdmin():
                node_list.append(n.asTuple())
                # Split the packet if too huge.
                if len(node_list) == 10000:
                    conn.notify(Packets.NotifyNodeInformation(node_list))
                    del node_list[:]
        if node_list:
            conn.notify(Packets.NotifyNodeInformation(node_list))

    def broadcastLastOID(self, oid):
        logging.debug('Broadcast last OID to storages : %s' % dump(oid))
        packet = Packets.NotifyLastOID(oid)
        for conn in self.em.getConnectionList():
            node = self.nm.getByUUID(conn.getUUID())
            if node is not None and node.isStorage():
                conn.notify(packet)

    def buildFromScratch(self):
        nm, em, pt = self.nm, self.em, self.pt
        logging.debug('creating a new partition table, wait for a storage node')
        # wait for some empty storage nodes, their are accepted
        while len(nm.getStorageList()) < REQUIRED_NODE_NUMBER:
            em.poll(1)
        # take the first node available
        node_list = nm.getStorageList()[:REQUIRED_NODE_NUMBER]
        for node in node_list:
            node.setRunning()
        self.broadcastNodesInformation(node_list)
        # resert IDs generators
        self.loid = '\0' * 8
        # build the partition with this node
        pt.setID(pack('!Q', 1))
        pt.make(node_list)

    def recoverStatus(self):
        """
        Recover the status about the cluster. Obtain the last OID, the last
        TID, and the last Partition Table ID from storage nodes, then get
        back the latest partition table or make a new table from scratch,
        if this is the first time.
        """
        logging.info('begin the recovery of the status')

        self.changeClusterState(ClusterStates.RECOVERING)
        em = self.em

        self.loid = None
        self.pt.setID(None)
        self.target_uuid = None

        # collect the last partition table available
        while self.cluster_state == ClusterStates.RECOVERING:
            em.poll(1)

        logging.info('startup allowed')

        # build a new partition table
        if self.pt.getID() is None:
            self.buildFromScratch()

        # collect node that are connected but not in the selected partition
        # table and set them in pending state
        allowed_node_set = set(self.pt.getNodeList())
        refused_node_set = set(self.nm.getStorageList()) - allowed_node_set
        for node in refused_node_set:
            node.setPending()
        self.broadcastNodesInformation(refused_node_set)

        logging.debug('cluster starts with loid=%s and this partition table :',
                dump(self.loid))
        self.pt.log()

    def verifyTransaction(self, tid):
        em = self.em
        uuid_set = set()

        # Determine to which nodes I should ask.
        partition = self.pt.getPartition(tid)
        transaction_uuid_list = [cell.getUUID() for cell \
                in self.pt.getCellList(partition, readable=True)]
        if len(transaction_uuid_list) == 0:
            raise VerificationFailure
        uuid_set.update(transaction_uuid_list)

        # Gather OIDs.
        self.asking_uuid_dict = {}
        self.unfinished_oid_set = set()
        for conn in em.getConnectionList():
            uuid = conn.getUUID()
            if uuid in transaction_uuid_list:
                self.asking_uuid_dict[uuid] = False
                conn.ask(Packets.AskTransactionInformation(tid))
        if len(self.asking_uuid_dict) == 0:
            raise VerificationFailure

        while True:
            em.poll(1)
            if not self.pt.operational():
                raise VerificationFailure
            if False not in self.asking_uuid_dict.values():
                break

        if self.unfinished_oid_set is None or len(self.unfinished_oid_set) == 0:
            # Not commitable.
            return None
        else:
            # Verify that all objects are present.
            for oid in self.unfinished_oid_set:
                self.asking_uuid_dict.clear()
                partition = self.pt.getPartition(oid)
                object_uuid_list = [cell.getUUID() for cell \
                            in self.pt.getCellList(partition, readable=True)]
                if len(object_uuid_list) == 0:
                    raise VerificationFailure
                uuid_set.update(object_uuid_list)

                self.object_present = True
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid in object_uuid_list:
                        self.asking_uuid_dict[uuid] = False
                        conn.ask(Packets.AskObjectPresent(oid, tid))

                while True:
                    em.poll(1)
                    if not self.pt.operational():
                        raise VerificationFailure
                    if False not in self.asking_uuid_dict.values():
                        break

                if not self.object_present:
                    # Not commitable.
                    return None

        return uuid_set

    def verifyData(self):
        """Verify the data in storage nodes and clean them up, if necessary."""

        em, nm = self.em, self.nm
        self.changeClusterState(ClusterStates.VERIFYING)

        # wait for any missing node
        logging.debug('waiting for the cluster to be operational')
        while not self.pt.operational():
            em.poll(1)

        logging.info('start to verify data')

        # Gather all unfinished transactions.
        self.asking_uuid_dict = {}
        self.unfinished_tid_set = set()
        for conn in em.getConnectionList():
            uuid = conn.getUUID()
            if uuid is not None:
                node = nm.getByUUID(uuid)
                if node.isStorage():
                    self.asking_uuid_dict[uuid] = False
                    conn.ask(Packets.AskUnfinishedTransactions())

        while True:
            em.poll(1)
            if not self.pt.operational():
                raise VerificationFailure
            if False not in self.asking_uuid_dict.values():
                break

        # Gather OIDs for each unfinished TID, and verify whether the transaction
        # can be finished or must be aborted. This could be in parallel in theory,
        # but not so easy. Thus do it one-by-one at the moment.
        for tid in self.unfinished_tid_set:
            uuid_set = self.verifyTransaction(tid)
            if uuid_set is None:
                # Make sure that no node has this transaction.
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid is not None:
                        node = nm.getByUUID(uuid)
                        if node.isStorage():
                            conn.notify(Packets.DeleteTransaction(tid))
            else:
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid in uuid_set:
                        conn.ask(Packets.CommitTransaction(tid))

            # If possible, send the packets now.
            em.poll(0)

        # At this stage, all non-working nodes are out-of-date.
        cell_list = self.pt.outdate()

        # Tweak the partition table, if the distribution of storage nodes
        # is not uniform.
        cell_list.extend(self.pt.tweak())

        # If anything changed, send the changes.
        self.broadcastPartitionChanges(cell_list)

    def provideService(self):
        """
        This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophy happens or the user commits
        a shutdown.
        """
        logging.info('provide service')
        em = self.em
        nm = self.nm
        self.tm.reset()

        self.changeClusterState(ClusterStates.RUNNING)

        # Now everything is passive.
        while True:
            try:
                em.poll(1)
            except OperationFailure:
                # If not operational, send Stop Operation packets to storage
                # nodes and client nodes. Abort connections to client nodes.
                logging.critical('No longer operational, stopping the service')
                for conn in em.getConnectionList():
                    node = nm.getByUUID(conn.getUUID())
                    if node is not None and (node.isStorage()
                            or node.isClient()):
                        conn.notify(Packets.StopOperation())
                        if node.isClient():
                            conn.abort()

                # Then, go back, and restart.
                return

    def playPrimaryRole(self):
        logging.info('play the primary role with %s (%s:%d)',
                dump(self.uuid), *(self.server))

        # i'm the primary, send the announcement
        self._announcePrimary()
        # all incoming connections identify through this handler
        self.listening_conn.setHandler(
                identification.IdentificationHandler(self))

        handler = secondary.SecondaryMasterHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the secondary event handler.
        for conn in em.getConnectionList():
            conn_uuid = conn.getUUID()
            if conn_uuid is not None:
                node = nm.getByUUID(conn_uuid)
                assert node is not None
                assert node.isMaster()
                conn.setHandler(handler)


        # If I know any storage node, make sure that they are not in the
        # running state, because they are not connected at this stage.
        for node in nm.getStorageList():
            if node.isRunning():
                node.setTemporarilyDown()

        # recover the cluster status at startup
        self.recoverStatus()

        while True:
            try:
                self.verifyData()
            except VerificationFailure:
                continue
            self.provideService()

    def playSecondaryRole(self):
        """
        I play a secondary role, thus only wait for a primary master to fail.
        """
        logging.info('play the secondary role with %s (%s:%d)',
                dump(self.uuid), *(self.server))

        # otherwise, wait for the primary announcement
        self._waitForPrimaryAnnouncement()

        # apply the new handler to the primary connection
        client_list = [x for x in self.em.getClientList() if x.getUUID() ==
                self.primary_master_node.getUUID()]
        assert len(client_list) == 1
        client_list[0].setHandler(secondary.PrimaryHandler(self))

        # and another for the future incoming connections
        handler = identification.IdentificationHandler(self)
        self.listening_conn.setHandler(handler)

        while True:
            self.em.poll(1)

    def changeClusterState(self, state):
        """
        Change the cluster state and apply right handler on each connections
        """
        if self.cluster_state == state:
            return
        nm, em = self.nm, self.em

        # select the storage handler
        if state == ClusterStates.BOOTING:
            storage_handler = recovery.RecoveryHandler
        elif state == ClusterStates.RECOVERING:
            storage_handler = recovery.RecoveryHandler
        elif state == ClusterStates.VERIFYING:
            storage_handler = verification.VerificationHandler
        elif state == ClusterStates.RUNNING:
            storage_handler = storage.StorageServiceHandler
        else:
            RuntimeError('Unexpected node type')

        # change handlers
        notification_packet = Packets.NotifyClusterInformation(state)
        for conn in em.getConnectionList():
            node = nm.getByUUID(conn.getUUID())
            if conn.isListening() or node is None:
                # not identified or listening, keep the identification handler
                continue
            conn.notify(notification_packet)
            if node.isAdmin() or node.isMaster():
                # those node types keep their own handler
                continue
            if node.isClient():
                if state != ClusterStates.RUNNING:
                    conn.close()
                handler = client.ClientServiceHandler
            elif node.isStorage():
                handler = storage_handler
            handler = handler(self)
            conn.setHandler(handler)
            handler.connectionCompleted(conn)
        self.cluster_state = state

    def getNewOIDList(self, num_oids):
        if self.loid is None:
            raise RuntimeError, 'I do not know the last OID'
        oid = unpack('!Q', self.loid)[0] + 1
        oid_list = [pack('!Q', oid + i) for i in xrange(num_oids)]
        self.loid = oid_list[-1]
        self.broadcastLastOID(self.loid)
        return oid_list

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
        # change handler
        handler = shutdown.ShutdownHandler(self)
        for c in self.em.getConnectionList():
            c.setHandler(handler)

        # wait for all transaction to be finished
        while True:
            self.em.poll(1)
            if not self.tm.hasPending():
                if self.cluster_state == ClusterStates.RUNNING:
                    sys.exit("Application has been asked to shut down")
                else:
                    # no more transaction, ask clients to shutdown
                    logging.info("asking all clients to shutdown")
                    for c in self.em.getConnectionList():
                        node = self.nm.getByUUID(c.getUUID())
                        if node.isClient():
                            node_list = [(node.getType(), node.getAddress(),
                                node.getUUID(), NodeStates.DOWN)]
                            c.notify(Packets.NotifyNodeInformation(node_list))
                    # then ask storages and master nodes to shutdown
                    logging.info("asking all remaining nodes to shutdown")
                    for c in self.em.getConnectionList():
                        node = self.nm.getByUUID(c.getUUID())
                        if node.isStorage() or node.isMaster():
                            node_list = [(node.getType(), node.getAddress(),
                                node.getUUID(), NodeStates.DOWN)]
                            c.notify(Packets.NotifyNodeInformation(node_list))
                    # then shutdown
                    sys.exit("Cluster has been asked to shut down")

    def identifyStorageNode(self, uuid, node):
        state = NodeStates.RUNNING
        handler = None
        if self.cluster_state == ClusterStates.RECOVERING:
            if uuid is None:
                logging.info('reject empty storage node')
                raise protocol.NotReadyError
            handler = recovery.RecoveryHandler
        elif self.cluster_state == ClusterStates.VERIFYING:
            if uuid is None or node is None:
                # if node is unknown, it has been forget when the current
                # partition was validated by the admin
                # Here the uuid is not cleared to allow lookup pending nodes by
                # uuid from the test framework. It's safe since nodes with a
                # conflicting UUID are rejected in the identification handler.
                state = NodeStates.PENDING
            handler = verification.VerificationHandler
        elif self.cluster_state == ClusterStates.RUNNING:
            if uuid is None or node is None:
                # same as for verification
                state = NodeStates.PENDING
            handler = storage.StorageServiceHandler
        elif self.cluster_state == ClusterStates.STOPPING:
            raise protocol.NotReadyError
        else:
            raise RuntimeError('unhandled cluster state')
        return (uuid, state, handler)

    def identifyNode(self, node_type, uuid, node):

        state = NodeStates.RUNNING
        handler = identification.IdentificationHandler

        if node_type == NodeTypes.ADMIN:
            # always accept admin nodes
            node_ctor = self.nm.createAdmin
            handler = administration.AdministrationHandler
            logging.info('Accept an admin %s' % (dump(uuid), ))
        elif node_type == NodeTypes.MASTER:
            if node is None:
                # unknown master, rejected
                raise protocol.ProtocolError('Reject an unknown master node')
            # always put other master in waiting state
            node_ctor = self.nm.createMaster
            handler = secondary.SecondaryMasterHandler
            logging.info('Accept a master %s' % (dump(uuid), ))
        elif node_type == NodeTypes.CLIENT:
            # refuse any client before running
            if self.cluster_state != ClusterStates.RUNNING:
                logging.info('Reject a connection from a client')
                raise protocol.NotReadyError
            node_ctor = self.nm.createClient
            handler = client.ClientServiceHandler
            logging.info('Accept a client %s' % (dump(uuid), ))
        elif node_type == NodeTypes.STORAGE:
            node_ctor = self.nm.createStorage
            (uuid, state, handler) = self.identifyStorageNode(uuid, node)
            logging.info('Accept a storage %s (%s)' % (dump(uuid), state))
        return (uuid, node, state, handler, node_ctor)

