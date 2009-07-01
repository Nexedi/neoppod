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
import os, sys
from time import time, gmtime
from struct import pack, unpack

from neo.config import ConfigurationManager
from neo import protocol
from neo.protocol import Packet, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        PENDING_STATE, INVALID_UUID, INVALID_OID, INVALID_TID, INVALID_PTID, \
        CLIENT_NODE_TYPE, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, \
        UUID_NAMESPACES, ADMIN_NODE_TYPE, BOOTING
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode, AdminNode
from neo.event import EventManager
from neo.connection import ListeningConnection, ClientConnection, ServerConnection
from neo.exception import ElectionFailure, PrimaryFailure, VerificationFailure, \
        OperationFailure
from neo.master.identification import IdentificationEventHandler
from neo.master.administration import AdministrationEventHandler
from neo.master.election import ClientElectionEventHandler, ServerElectionEventHandler
from neo.master.recovery import RecoveryEventHandler
from neo.master.verification import VerificationEventHandler
from neo.master.service import ClientServiceEventHandler, StorageServiceEventHandler
from neo.master.secondary import PrimaryMasterEventHandler, SecondaryMasterEventHandler
from neo.master.pt import PartitionTable
from neo.util import dump
from neo.connector import getConnectorHandler
from neo.master import ENABLE_PENDING_NODES

class Application(object):
    """The master node application."""

    def __init__(self, file, section):

        config = ConfigurationManager(file, section)
        self.connector_handler = getConnectorHandler(config.getConnector())

        self.name = config.getName()
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        self.server = config.getServer()
        logging.debug('IP address is %s, port is %d', *(self.server))

        # Exclude itself from the list.
        self.master_node_list = [n for n in config.getMasterNodeList() if n != self.server]
        logging.debug('master nodes are %s', self.master_node_list)

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()

        # Partition table
        replicas, partitions = config.getReplicas(), config.getPartitions()
        if replicas < 0:
            raise RuntimeError, 'replicas must be a positive integer'
        if partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        self.pt = PartitionTable(partitions, replicas)
        logging.debug('the number of replicas is %d, the number of partitions is %d, the name is %s',
                      replicas, partitions, self.name)

        self.primary = None
        self.primary_master_node = None

        # Generate an UUID for self
        self.uuid = self.getNewUUID(MASTER_NODE_TYPE)

        # The last OID.
        self.loid = INVALID_OID
        # The last TID.
        self.ltid = INVALID_TID
        # The target node's uuid to request next.
        self.target_uuid = None


    def run(self):
        """Make sure that the status is sane and start a loop."""
        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Make a listening port.
        self.listening_conn = ListeningConnection(self.em, None, 
            addr = self.server, connector_handler = self.connector_handler)

        self.cluster_state = BOOTING

        # Start the election of a primary master node.
        self.electPrimary()

        # Start a normal operation.
        while 1:
            try:
                if self.primary:
                    self.playPrimaryRole()
                else:
                    self.playSecondaryRole()
                raise RuntimeError, 'should not reach here'
            except (ElectionFailure, PrimaryFailure):
                # Forget all connections.
                for conn in self.em.getConnectionList():
                    if not conn.isListeningConnection():
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
        self.listening_conn.setHandler(ServerElectionEventHandler(self))
        client_handler = ClientElectionEventHandler(self)
        em = self.em
        nm = self.nm

        while 1:
            t = 0
            self.primary = None
            self.primary_master_node = None

            for node in nm.getMasterNodeList():
                self.unconnected_master_node_set.add(node.getServer())
                # For now, believe that every node should be available,
                # since down or broken nodes may be already repaired.
                node.setState(RUNNING_STATE)
            self.negotiating_master_node_set.clear()

            try:
                while 1:
                    em.poll(1)
                    current_time = time()
                    if current_time >= t + 1:
                        t = current_time
                        # Expire temporarily down nodes. For now, assume that a node
                        # which is down for 60 seconds is really down, if this is a
                        # bootstrap. 60 seconds may sound too long, but this is reasonable
                        # when rebooting many cluster machines. Otherwise, wait for only
                        # 10 seconds, because stopping the whole cluster for a long time
                        # is a bad idea.
                        if bootstrap:
                            expiration = 60
                        else:
                            expiration = 10
                        for node in nm.getMasterNodeList():
                            if node.getState() == TEMPORARILY_DOWN_STATE \
                                    and node.getLastStateChange() + expiration < current_time:
                                logging.info('%s is down' % (node, ))
                                node.setState(DOWN_STATE)
                                self.unconnected_master_node_set.discard(node.getServer())

                        # Try to connect to master nodes.
                        if self.unconnected_master_node_set:
                            for addr in list(self.unconnected_master_node_set):
                                ClientConnection(em, client_handler, addr = addr,
                                                 connector_handler = self.connector_handler)
                    if (len(self.unconnected_master_node_set) == 0 \
                            and len(self.negotiating_master_node_set) == 0) \
                       or self.primary is not None:
                        break

                # Now there are three situations:
                #   - I am the primary master
                #   - I am secondary but don't know who is primary
                #   - I am secondary and know who is primary
                if self.primary is None:
                    # I am the primary.
                    self.primary = True
                    logging.info('I am the primary, so sending an announcement')
                    for conn in em.getConnectionList():
                        if isinstance(conn, ClientConnection):
                            conn.notify(protocol.announcePrimaryMaster())
                            conn.abort()
                    closed = False
                    t = time()
                    while not closed:
                        em.poll(1)
                        closed = True
                        for conn in em.getConnectionList():
                            if isinstance(conn, ClientConnection):
                                closed = False
                                break
                        if t + 10 < time():
                            for conn in em.getConnectionList():
                                if isinstance(conn, ClientConnection):
                                    conn.close()
                            closed = True
                else:
                    # Wait for an announcement. If this is too long, probably
                    # the primary master is down.
                    t = time()
                    while self.primary_master_node is None:
                        em.poll(1)
                        if t + 10 < time():
                            raise ElectionFailure, 'no primary master elected'

                    # Now I need only a connection to the primary master node.
                    primary = self.primary_master_node
                    addr = primary.getServer()
                    for conn in em.getConnectionList():
                        if isinstance(conn, ServerConnection) \
                                or isinstance(conn, ClientConnection) \
                                and addr != conn.getAddress():
                            conn.close()

                    # But if there is no such connection, something wrong happened.
                    for conn in em.getConnectionList():
                        if isinstance(conn, ClientConnection) \
                                and addr == conn.getAddress():
                            break
                    else:
                        raise ElectionFailure, 'no connection remains to the primary'

                return
            except ElectionFailure, m:
                logging.error('election failed; %s' % m)

                # Ask all connected nodes to reelect a single primary master.
                for conn in em.getConnectionList():
                    if isinstance(conn, ClientConnection):
                        conn.notify(protocol.reelectPrimaryMaster())
                        conn.abort()

                # Wait until the connections are closed.
                self.primary = None
                self.primary_master_node = None
                closed = False
                t = time()
                while not closed:
                    try:
                        em.poll(1)
                    except ElectionFailure:
                        pass

                    closed = True
                    for conn in em.getConnectionList():
                        if isinstance(conn, ClientConnection):
                            # Still not closed.
                            closed = False
                            break

                    if time() > t + 10:
                        # If too long, do not wait.
                        break

                # Close all connections.
                for conn in em.getConnectionList():
                    if not conn.isListeningConnection():
                        conn.close()
                bootstrap = False

    def broadcastNodeInformation(self, node):
        """Broadcast a Notify Node Information packet."""
        logging.debug('broadcasting node information')
        node_type = node.getNodeType()
        state = node.getState()
        uuid = node.getUUID()

        # The server address may be None.
        addr = node.getServer()
        if addr is None:
            ip_address, port = None, None
        else:
            ip_address, port = addr

        if ip_address is None:
            ip_address = '0.0.0.0'
        if port is None:
            port = 0

        if node_type == CLIENT_NODE_TYPE:
            # Only to master nodes and storage nodes.
            for c in self.em.getConnectionList():
                if c.getUUID() is not None:
                    n = self.nm.getNodeByUUID(c.getUUID())
                    if n.getNodeType() in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
                        node_list = [(node_type, ip_address, port, uuid, state)]
                        c.notify(protocol.notifyNodeInformation(node_list))
        elif node.getNodeType() in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE):
            for c in self.em.getConnectionList():
                if c.getUUID() is not None:
                    node_list = [(node_type, ip_address, port, uuid, state)]
                    c.notify(protocol.notifyNodeInformation(node_list))
        elif node.getNodeType() != ADMIN_NODE_TYPE:
            raise RuntimeError('unknown node type')

    def broadcastPartitionChanges(self, ptid, cell_list):
        """Broadcast a Notify Partition Changes packet."""
        logging.info('broadcastPartitionChanges')
        self.pt.log()
        for c in self.em.getConnectionList():
            if c.getUUID() is not None:
                n = self.nm.getNodeByUUID(c.getUUID())
                if n.getNodeType() in (CLIENT_NODE_TYPE, STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
                    # Split the packet if too big.
                    size = len(cell_list)
                    start = 0
                    while size:
                        amt = min(10000, size)
                        p = protocol.notifyPartitionChanges(ptid,
                                                 cell_list[start:start+amt])
                        c.notify(p)
                        size -= amt
                        start += amt

    def sendPartitionTable(self, conn):
        """ Send the partition table through the given connection """
        row_list = []
        for offset in xrange(self.pt.getPartitions()):
            row_list.append((offset, self.pt.getRow(offset)))
            # Split the packet if too huge.
            if len(row_list) == 1000:
                conn.notify(protocol.sendPartitionTable( self.pt.getID(), row_list))
                del row_list[:]
        if row_list:
            conn.notify(protocol.sendPartitionTable(self.pt.getID(), row_list))

    def sendNodesInformations(self, conn):
        """ Send informations on all nodes through the given connection """
        node_list = []
        for n in self.nm.getNodeList():
            if n.getNodeType() != ADMIN_NODE_TYPE:
                try:
                    ip_address, port = n.getServer()
                except TypeError:
                    ip_address, port = '0.0.0.0', 0
                node_list.append((n.getNodeType(), ip_address, port, 
                                  n.getUUID() or INVALID_UUID, n.getState()))
                # Split the packet if too huge.
                if len(node_list) == 10000:
                    conn.notify(protocol.notifyNodeInformation(node_list))
                    del node_list[:]
        if node_list:
            conn.notify(protocol.notifyNodeInformation(node_list))

    def recoverStatus(self):
        """Recover the status about the cluster. Obtain the last OID, the last TID,
        and the last Partition Table ID from storage nodes, then get back the latest
        partition table or make a new table from scratch, if this is the first time."""
        logging.info('begin the recovery of the status')

        self.changeClusterState(protocol.RECOVERING)

        em = self.em
        nm = self.nm
    
        self.loid = INVALID_OID
        self.ltid = INVALID_TID
        self.pt.setID(INVALID_PTID)
        while 1:
            self.target_uuid = None
            self.pt.clear()

            if self.pt.getID() != INVALID_PTID:
                # I need to retrieve last ids again.
                logging.info('resending Ask Last IDs')
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid is not None:
                        node = nm.getNodeByUUID(uuid)
                        if node.getNodeType() == STORAGE_NODE_TYPE \
                                and node.getState() == RUNNING_STATE:
                            conn.ask(protocol.askLastIDs())

            # Wait for at least one storage node to appear.
            while self.target_uuid is None:
                em.poll(1)

            # Wait a bit, 1 second is too short for the ZODB test running on a
            # dedibox
            t = time()
            while time() < t + 5:
                em.poll(1)

            # Now I have at least one to ask.
            prev_lptid = self.pt.getID()
            node = nm.getNodeByUUID(self.target_uuid)
            if node is None or node.getState() != RUNNING_STATE:
                # Weird. It's dead.
                logging.info('the target storage node is dead')
                continue

            for conn in em.getConnectionList():
                if conn.getUUID() == self.target_uuid:
                    break
            else:
                # Why?
                logging.info('no connection to the target storage node')
                continue

            if self.pt.getID() == INVALID_PTID:
                # This looks like the first time. So make a fresh table.
                logging.debug('creating a new partition table')
                self.pt.setID(pack('!Q', 1)) # ptid != INVALID_PTID
                self.pt.make(nm.getStorageNodeList())
            else:
                # Obtain a partition table. It is necessary to split this
                # message, because the packet size can be huge.
                logging.debug('asking a partition table to %s', node)
                start = 0
                size = self.pt.getPartitions()
                while size:
                    amt = min(1000, size)
                    conn.ask(protocol.askPartitionTable(range(start, start + amt)))
                    size -= amt
                    start += amt

                t = time()
                while 1:
                    em.poll(1)
                    if node.getState() != RUNNING_STATE:
                        # Dead.
                        break
                    if self.pt.filled() or t + 30 < time():
                        break

                if self.pt.getID() != prev_lptid or not self.pt.filled():
                    # I got something newer or the target is dead.
                    logging.debug('lptid = %s, prev_lptid = %s',
                              dump(self.pt.getID()), dump(prev_lptid))
                    self.pt.log()
                    continue

                # Wait until the cluster gets operational or the Partition
                # Table ID turns out to be not the latest.
                logging.info('waiting for the cluster to be operational')
                self.pt.log()
                while 1:
                    em.poll(1)
                    if self.pt.operational():
                        break
                    if self.pt.getID() != prev_lptid:
                        break

                if self.pt.getID() != prev_lptid:
                    # I got something newer.
                    continue
            break

    def verifyTransaction(self, tid):
        em = self.em
        uuid_set = set()

        # Determine to which nodes I should ask.
        partition = self.getPartition(tid)
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
                conn.ask(protocol.askTransactionInformation(tid))
        if len(self.asking_uuid_dict) == 0:
            raise VerificationFailure

        while 1:
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
                partition = self.getPartition(oid)
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
                        conn.ask(protocol.askObjectPresent(oid, tid))

                while 1:
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
        logging.info('start to verify data')

        em = self.em
        nm = self.nm

        # Wait ask/request primary master exchange with the last storage node
        # because it have to be in the verification state
        t = time()
        while time() < t + 1:
            em.poll(1)

        self.changeClusterState(protocol.VERIFYING)

        # FIXME this part has a potential problem that the write buffers can
        # be very huge. Thus it would be better to flush the buffers from time
        # to time _without_ reading packets.

        # Send the current partition table to storage and admin nodes, so that
        # all nodes share the same view.
        for conn in em.getConnectionList():
            uuid = conn.getUUID()
            if uuid is not None:
                node = nm.getNodeByUUID(uuid)
                if node.getNodeType() in (STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
                    self.sendPartitionTable(conn)

        # Gather all unfinished transactions.
        #
        # FIXME this part requires more brainstorming. Currently, this deals with
        # only unfinished transactions. But how about finished transactions?
        # Suppose that A and B have an unfinished transaction. First, A and B are
        # asked to commit the transaction. Then, A succeeds. B gets down. Now,
        # A believes that the transaction has been committed, while B still believes
        # that the transaction is unfinished. Next, if B goes back and A is working,
        # no problem; because B's unfinished transaction will be committed correctly.
        # However, when B goes back, if A is down, what happens? If the state is
        # not very good, B may be asked to abort the transaction!
        #
        # This situation won't happen frequently, and B shouldn't be asked to drop
        # the transaction, if the cluster is not ready. However, there might be
        # some corner cases where this may happen. That's why more brainstorming
        # is required.
        self.asking_uuid_dict = {}
        self.unfinished_tid_set = set()
        for conn in em.getConnectionList():
            uuid = conn.getUUID()
            if uuid is not None:
                node = nm.getNodeByUUID(uuid)
                if node.getNodeType() == STORAGE_NODE_TYPE:
                    self.asking_uuid_dict[uuid] = False
                    conn.ask(protocol.askUnfinishedTransactions())

        while 1:
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
                        node = nm.getNodeByUUID(uuid)
                        if node.getNodeType() == STORAGE_NODE_TYPE:
                            conn.notify(protocol.deleteTransaction(tid))
            else:
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid in uuid_set:
                        conn.ask(protocol.commitTransaction(tid))

            # If possible, send the packets now.
            em.poll(0)

        # At this stage, all non-working nodes are out-of-date.
        cell_list = self.pt.outdate()

        # Tweak the partition table, if the distribution of storage nodes
        # is not uniform.
        cell_list.extend(self.pt.tweak())

        # If anything changed, send the changes.
        if cell_list:
            self.broadcastPartitionChanges(self.pt.setNextID(), cell_list)

    def provideService(self):
        """This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophy happens or the user commits
        a shutdown."""
        logging.info('provide service')
        em = self.em
        nm = self.nm

        self.changeClusterState(protocol.RUNNING)

        # This dictionary is used to hold information on transactions being finished.
        self.finishing_transaction_dict = {}

        # Now everything is passive.
        expiration = 10
        while 1:
            t = 0
            try:
                em.poll(1)
                # implement an expiration of temporary down nodes.
                # If a temporary down storage node is expired, it moves to
                # down state, and the partition table must drop the node,
                # thus repartitioning must be performed.
                current_time = time()
                if current_time >= t + 1:
                    t = current_time
                    for node in nm.getStorageNodeList():
                        if node.getState() == TEMPORARILY_DOWN_STATE \
                               and node.getLastStateChange() + expiration < current_time:
                            logging.warning('%s is down, have to notify the admin' % (node, ))
                            # XXX: here we should notify the administrator that
                            # a node seems dead and should be dropped frop the
                            # partition table. This should not be done
                            # automaticaly to avoid data lost.
                            node.setState(DOWN_STATE)
                            #self.broadcastNodeInformation(node)
                            #cell_list = self.pt.dropNode(node)
                            #self.broadcastPartitionChanges(self.pt.setNextID(), cell_list)
                            if not self.pt.operational():
                                # Catastrophic.
                                raise OperationFailure, 'cannot continue operation'

                        
            except OperationFailure:
                # If not operational, send Stop Operation packets to storage nodes
                # and client nodes. Abort connections to client nodes.
                logging.critical('No longer operational, so stopping the service')
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid is not None:
                        node = nm.getNodeByUUID(uuid)
                        if node.getNodeType() in (STORAGE_NODE_TYPE, CLIENT_NODE_TYPE):
                            conn.notify(protocol.stopOperation())
                            if node.getNodeType() == CLIENT_NODE_TYPE:
                                conn.abort()

                # Then, go back, and restart.
                return

    def playPrimaryRole(self):
        logging.info('play the primary role with %s (%s:%d)', 
                dump(self.uuid), *(self.server))

        # all incoming connections identify through this handler
        self.listening_conn.setHandler(IdentificationEventHandler(self))

        # If I know any storage node, make sure that they are not in the running state,
        # because they are not connected at this stage.
        for node in self.nm.getStorageNodeList():
            if node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)

        while 1:
            recovering = True
            while recovering:
                self.recoverStatus()
                recovering = False
                try:
                    self.verifyData()
                except VerificationFailure:
                    recovering = True
            self.provideService()

    def playSecondaryRole(self):
        """I play a secondary role, thus only wait for a primary master to fail."""
        logging.info('play the secondary role with %s (%s:%d)', 
                dump(self.uuid), *(self.server))

        handler = PrimaryMasterEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the secondary event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        while 1:
            em.poll(1)

    def changeClusterState(self, state):
        """ Change the cluster state and apply right handler on each connections """
        if self.cluster_state == state:
            return
        nm, em = self.nm, self.em

        # select the storage handler
        if state == protocol.BOOTING:
            storage_handler = RecoveryEventHandler
        elif state == protocol.RECOVERING:
            storage_handler = RecoveryEventHandler
        elif state == protocol.VERIFYING:
            storage_handler = VerificationEventHandler
        elif state == protocol.RUNNING:
            storage_handler = StorageServiceEventHandler
        else:
            RuntimeError('Unexpected node type')

        # change handlers
        notification_packet = protocol.notifyClusterInformation(state)
        for conn in em.getConnectionList():
            node = nm.getNodeByUUID(conn.getUUID())
            if conn.isListeningConnection() or node is None:
                # not identified or listening, keep the identification handler
                continue
            node_type = node.getNodeType()
            conn.notify(notification_packet)
            if node_type in (ADMIN_NODE_TYPE, MASTER_NODE_TYPE):
                # those node types keep their own handler
                continue
            if node_type == CLIENT_NODE_TYPE:
                if state != protocol.RUNNING:
                    conn.close()
                handler = ClientServiceEventHandler
            elif node_type == STORAGE_NODE_TYPE:
                handler = storage_handler
            handler = handler(self)
            conn.setHandler(handler)
            handler.connectionCompleted(conn)
        self.cluster_state = state


    def getNextOID(self):
        if self.loid is None:
            raise RuntimeError, 'I do not know the last OID'

        oid = unpack('!Q', self.loid)[0]
        self.loid = pack('!Q', oid + 1)
        return self.loid

    def getNextTID(self):
        tm = time()
        gmt = gmtime(tm)
        upper = ((((gmt.tm_year - 1900) * 12 + gmt.tm_mon - 1) * 31 \
                  + gmt.tm_mday - 1) * 24 + gmt.tm_hour) * 60 + gmt.tm_min
        lower = int((gmt.tm_sec % 60 + (tm - int(tm))) / (60.0 / 65536.0 / 65536.0))
        tid = pack('!LL', upper, lower)
        if tid <= self.ltid:
            upper, lower = unpack('!LL', self.ltid)
            if lower == 0xffffffff:
                # This should not happen usually.
                from datetime import timedelta, datetime
                d = datetime(gmt.tm_year, gmt.tm_mon, gmt.tm_mday, 
                             gmt.tm_hour, gmt.tm_min) \
                        + timedelta(0, 60)
                upper = ((((d.year - 1900) * 12 + d.month - 1) * 31 \
                          + d.day - 1) * 24 + d.hour) * 60 + d.minute
                lower = 0
            else:
                lower += 1
            tid = pack('!LL', upper, lower)
        self.ltid = tid
        return tid

    def getPartition(self, oid_or_tid):
        return unpack('!Q', oid_or_tid)[0] % self.pt.getPartitions()

    def getNewOIDList(self, num_oids):
        return [self.getNextOID() for i in xrange(num_oids)]

    def getNewUUID(self, node_type):
        # build an UUID
        uuid = os.urandom(15)
        while uuid == INVALID_UUID[1:]:
            uuid = os.urandom(15)
        # look for the prefix
        prefix = UUID_NAMESPACES.get(node_type, None)
        if prefix is None:
            raise RuntimeError, 'No UUID namespace found for this node type'
        return prefix + uuid

    def isValidUUID(self, uuid, addr):
        node = self.nm.getNodeByUUID(uuid)
        if node is not None and node.getServer() is not None and node.getServer() != addr:
            return False
        return uuid != self.uuid and uuid != INVALID_UUID

    def getClusterState(self):
        return self.cluster_state

    def shutdown(self):
        """Close all connections and exit"""
        # change handler
        handler = ShutdownEventHandler(self)
        for c in self.em.getConnectionList():
            c.setHandler(handler)

        # wait for all transaction to be finished
        while 1:
            self.em.poll(1)
            if len(self.finishing_transaction_dict) == 0:
                if self.cluster_state == RUNNING:
                    sys.exit("Application has been asked to shut down")
                else:
                    # no more transaction, ask clients to shutdown
                    logging.info("asking all clients to shutdown")
                    for c in self.em.getConnectionList():
                        node = self.nm.getNodeByUUID(c.getUUID())
                        if node.getType() == CLIENT_NODE_TYPE:
                            ip_address, port = node.getServer()
                            node_list = [(node.getType(), ip_address, port, node.getUUID(), DOWN_STATE)]
                            c.notify(protocol.notifyNodeInformation(node_list))
                    # then ask storages and master nodes to shutdown
                    logging.info("asking all remaining nodes to shutdown")
                    for c in self.em.getConnectionList():
                        node = self.nm.getNodeByUUID(c.getUUID())
                        if node.getType() in (STORAGE_NODE_TYPE, MASTER_NODE_TYPE):
                            ip_address, port = node.getServer()
                            node_list = [(node.getType(), ip_address, port, node.getUUID(), DOWN_STATE)]
                            c.notify(protocol.notifyNodeInformation(node_list))
                    # then shutdown
                    sys.exit("Cluster has been asked to shut down")

    def identifyStorageNode(self, uuid, node):
        # TODO: check all cases here, when server address change...
        # in verification and running states, if the node is unknown but the
        # uuid != INVALID_UUID, we have to give it a new uuid, but in recovery
        # the node must keep it's UUID
        state = protocol.RUNNING_STATE
        handler = None
        if self.cluster_state == protocol.RECOVERING:
            # TODO: Enable empty node rejection when manual startup is ok :
            if False and uuid == protocol.INVALID_UUID:
                logging.info('reject empty storage node')
                raise protocol.NotReadyError
            handler = RecoveryEventHandler
        elif self.cluster_state == protocol.VERIFYING:
            if uuid == INVALID_UUID or node is None:
                # if node is unknown, it has been forget when the current
                # partition was validated by the admin
                uuid = INVALID_UUID
                state = protocol.PENDING_STATE
            handler = VerificationEventHandler
        elif self.cluster_state == protocol.RUNNING:
            if uuid == INVALID_UUID or node is None:
                # same as for verification
                uuid = INVALID_UUID
                state = protocol.PENDING_STATE
            handler = StorageServiceEventHandler
        elif self.cluster_state == protocol.STOPPING:
            # FIXME: raise a ShutdowningError ?
            raise protocol.NotReadyError
        else:
            raise RuntimeError('unhandled cluster state')
        return (uuid, state, handler)


    def identifyNode(self, node_type, uuid, node):

        state = protocol.RUNNING_STATE
        handler = IdentificationEventHandler

        if node_type == protocol.ADMIN_NODE_TYPE:
            # always accept admin nodes
            klass = AdminNode
            handler = AdministrationEventHandler
            logging.info('Accept an admin %s' % dump(uuid))
        elif node_type == protocol.MASTER_NODE_TYPE:
            # always put other master in waiting state
            klass = MasterNode
            handler = SecondaryMasterEventHandler
            logging.info('Accept a master %s' % dump(uuid))
        elif node_type == protocol.CLIENT_NODE_TYPE:
            # refuse any client before running
            if self.cluster_state != protocol.RUNNING:
                logging.info('reject a connection from a client')
                raise protocol.NotReadyError
            klass = ClientNode
            # FIXME: Apply an handler dedicated to client nodes
            handler = ClientServiceEventHandler
            logging.info('Accept a client %s' % dump(uuid))
        elif node_type == protocol.STORAGE_NODE_TYPE:
            klass = StorageNode
            (uuid, state, handler) = self.identifyStorageNode(uuid, node)
        return (uuid, node, state, handler, klass)


