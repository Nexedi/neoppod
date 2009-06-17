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
        INVALID_UUID, INVALID_OID, INVALID_TID, INVALID_PTID, \
        CLIENT_NODE_TYPE, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, \
        UUID_NAMESPACES, ADMIN_NODE_TYPE
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode
from neo.event import EventManager
from neo.connection import ListeningConnection, ClientConnection, ServerConnection
from neo.exception import ElectionFailure, PrimaryFailure, VerificationFailure, \
        OperationFailure
from neo.master.election import ElectionEventHandler
from neo.master.recovery import RecoveryEventHandler
from neo.master.verification import VerificationEventHandler
from neo.master.service import ServiceEventHandler
from neo.master.secondary import SecondaryEventHandler
from neo.master.pt import PartitionTable
from neo.util import dump
from neo.connector import getConnectorHandler

class Application(object):
    """The master node application."""

    def __init__(self, file, section):
        config = ConfigurationManager(file, section)

        self.num_replicas = config.getReplicas()
        self.num_partitions = config.getPartitions()
        self.name = config.getName()
        self.connector_handler = getConnectorHandler(config.getConnector())
        logging.debug('the number of replicas is %d, the number of partitions is %d, the name is %s',
                      self.num_replicas, self.num_partitions, self.name)

        self.server = config.getServer()
        logging.debug('IP address is %s, port is %d', *(self.server))

        # Exclude itself from the list.
        self.master_node_list = [n for n in config.getMasterNodeList() if n != self.server]
        logging.debug('master nodes are %s', self.master_node_list)

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.pt = PartitionTable(self.num_partitions, self.num_replicas)

        self.primary = None
        self.primary_master_node = None

        # Generate an UUID for self
        self.uuid = self.getNewUUID(MASTER_NODE_TYPE)

        # The last OID.
        self.loid = INVALID_OID
        # The last TID.
        self.ltid = INVALID_TID
        # The last Partition Table ID.
        self.lptid = INVALID_PTID
        # The target node's uuid to request next.
        self.target_uuid = None

    def run(self):
        """Make sure that the status is sane and start a loop."""
        if self.num_replicas < 0:
            raise RuntimeError, 'replicas must be a positive integer'
        if self.num_partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Make a listening port.
        ListeningConnection(self.em, None, addr = self.server,
                            connector_handler = self.connector_handler)

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
                    if not isinstance(conn, ListeningConnection):
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
        handler = ElectionEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the election event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

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
                                ClientConnection(em, handler, addr = addr,
                                                 connector_handler = self.connector_handler)
                    if len(self.unconnected_master_node_set) == 0 \
                            and len(self.negotiating_master_node_set) == 0:
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
                    if not isinstance(conn, ListeningConnection):
                        conn.close()
                bootstrap = False

    def broadcastNodeInformation(self, node):
        """Broadcast a Notify Node Information packet."""
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
        for offset in xrange(self.num_partitions):
            row_list.append((offset, self.pt.getRow(offset)))
            # Split the packet if too huge.
            if len(row_list) == 1000:
                conn.notify(protocol.sendPartitionTable( self.lptid, row_list))
                del row_list[:]
        if row_list:
            conn.notify(protocol.sendPartitionTable(self.lptid, row_list))

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

        handler = RecoveryEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the status recovery event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        self.loid = INVALID_OID
        self.ltid = INVALID_TID
        self.lptid = INVALID_PTID
        while 1:
            self.target_uuid = None
            self.pt.clear()

            if self.lptid != INVALID_PTID:
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
            prev_lptid = self.lptid
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

            if self.lptid == INVALID_PTID:
                # This looks like the first time. So make a fresh table.
                logging.debug('creating a new partition table')
                self.lptid = pack('!Q', 1) # ptid != INVALID_PTID
                self.pt.make(nm.getStorageNodeList())
            else:
                # Obtain a partition table. It is necessary to split this
                # message, because the packet size can be huge.
                logging.debug('asking a partition table to %s', node)
                start = 0
                size = self.num_partitions
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

                if self.lptid != prev_lptid or not self.pt.filled():
                    # I got something newer or the target is dead.
                    logging.debug('self.lptid = %s, prev_lptid = %s',
                                  dump(self.lptid), dump(prev_lptid))
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
                    if self.lptid != prev_lptid:
                        break

                if self.lptid != prev_lptid:
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

        handler = VerificationEventHandler(self)
        em = self.em
        nm = self.nm

        # Wait ask/request primary master exchange with the last storage node
        # because it have to be in the verification state
        t = time()
        while time() < t + 1:
            em.poll(1)

        # Make sure that every connection has the data verification event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

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

        # And, add unused nodes.
        node_list = self.pt.getNodeList()
        for node in nm.getStorageNodeList():
            if node.getState() == RUNNING_STATE and node not in node_list:
                cell_list.extend(self.pt.addNode(node))

        # If anything changed, send the changes.
        if cell_list:
            self.broadcastPartitionChanges(self.getNextPartitionTableID(),
                                           cell_list)

    def provideService(self):
        """This is the normal mode for a primary master node. Handle transactions
        and stop the service only if a catastrophy happens or the user commits
        a shutdown."""
        logging.info('provide service')

        handler = ServiceEventHandler(self)
        em = self.em
        nm = self.nm

        # This dictionary is used to hold information on transactions being finished.
        self.finishing_transaction_dict = {}

        # Make sure that every connection has the service event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        # Now storage nodes should know that the cluster is operational.
        for conn in em.getConnectionList():
            uuid = conn.getUUID()
            if uuid is not None:
                node = nm.getNodeByUUID(uuid)
                if node.getNodeType() == STORAGE_NODE_TYPE:
                    conn.notify(protocol.startOperation())

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
                            logging.info('%s is down' % (node, ))
                            node.setState(DOWN_STATE)
                            self.broadcastNodeInformation(node)
                            cell_list = self.pt.dropNode(node)
                            ptid = self.getNextPartitionTableID()
                            self.broadcastPartitionChanges(ptid, cell_list)
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

        handler = SecondaryEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the secondary event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        while 1:
            em.poll(1)

    def getNextPartitionTableID(self):
        if self.lptid == INVALID_PTID:
            raise RuntimeError, 'I do not know the last Partition Table ID'

        ptid = unpack('!Q', self.lptid)[0]
        self.lptid = pack('!Q', ptid + 1)
        return self.lptid

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
        return unpack('!Q', oid_or_tid)[0] % self.num_partitions

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
        for node in self.nm.getNodeList():
            if node.getUUID() == uuid and node.getServer() is not None \
                    and addr != node.getServer():
                return False
        return uuid != self.uuid and uuid != INVALID_UUID


    def shutdown(self):
        """Close all connections and exit"""
        self.em.poll(1)
        for c in self.em.getConnectionList():
            if not c.isListeningConnection():
                c.close()
        sys.exit("Application has been asked to shut down")

