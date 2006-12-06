import logging
import MySQLdb
import os
from socket import inet_aton
from time import time

from neo.config import ConfigurationManager
from neo.protocol import Packet, ProtocolError, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        INVALID_UUID, INVALID_OID, INVALID_TID, INVALID_PTID
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode
from neo.event import EventManager
from neo.util import dump
from neo.connection import ListeningConnection, ClientConnection, ServerConnection
from neo.exception import ElectionFailure, PrimaryFailure, VerificationFailure
from neo.master.election import ElectionEventHandler
from neo.master.recovery import RecoveryEventHandler
from neo.pt import PartitionTable

class Application(object):
    """The master node application."""

    def __init__(self, file, section):
        config = ConfigurationManager(file, section)

        self.num_replicas = config.getReplicas()
        self.num_partitions = config.getPartitions()
        self.name = config.getName()
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

        # XXX Generate an UUID for self. For now, just use a random string.
        # Avoid an invalid UUID.
        while 1:
            uuid = os.urandom(16)
            if uuid != INVALID_UUID:
                break
        self.uuid = uuid

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
        if self.num_replicas <= 0:
            raise RuntimeError, 'replicas must be more than zero'
        if self.num_partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Make a listening port.
        ListeningConnection(self.em, None, addr = self.server)

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
                                logging.info('%s:%d is down' % node.getServer())
                                node.setState(DOWN_STATE)
                                self.unconnected_master_node_set.discard(node.getServer())

                        # Try to connect to master nodes.
                        if self.unconnected_master_node_set:
                            for addr in list(self.unconnected_master_node_set):
                                ClientConnection(em, handler, addr = addr)

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
                            p = Packet().announcePrimaryMaster(conn.getNextId())
                            conn.addPacket(p)
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
                        conn.addPacket(Packet().reelectPrimaryMaster(conn.getNextId()))
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
                            closed = Falsed
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
        ip_address, port = node.getServer()
        if ip_address is None:
            ip_address = '0.0.0.0'
        if port is None:
            port = 0

        if node_type == CLIENT_NODE_TYPE:
            # Only to master nodes and storage nodes.
            for c in self.em.getConnectionList():
                if c.getUUID() is not None:
                    n = self.nm.getNodeByUUID(c.getUUID())
                    if isinstance(n, (MasterNode, StorageNode)):
                        p = Packet()
                        node_list = [(node_type, ip_address, port, uuid, state)]
                        p.notifyNodeInformation(c.getNextId(), node_list)
                        c.addPacket(p)
        elif isinstance(node, (MasterNode, StorageNode)):
            for c in self.em.getConnectionList():
                if c.getUUID() is not None:
                    p = Packet()
                    node_list = [(node_type, ip_address, port, uuid, state)]
                    p.notifyNodeInformation(c.getNextId(), node_list)
                    c.addPacket(p)
        else:
            raise Runtime, 'unknown node type'

    def broadcastPartitionChanges(self, ptid, cell_list):
        """Broadcast a Notify Partition Changes packet."""
        for c in em.getConnectionList():
            if c.getUUID() is not None:
                n = self.nm.getNodeByUUID(c.getUUID())
                if isinstance(n, (ClientNode, StorageNode)):
                    # Split the packet if too big.
                    size = len(cell_list)
                    start = 0
                    while size:
                        amt = min(10000, size)
                        p = Packet()
                        p.notifyPartitionChanges(ptid, cell_list[start:start+amt])
                        c.addPacket(p)
                        size -= amt
                        start += amt

    def recoverStatus(self):
        logging.info('begin the recovery of the status')

        handler = RecoveryEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the status recovery event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        prev_lptid = None
        self.loid = INVALID_OID
        self.ltid = INVALID_TID
        self.lptid = None
        while 1:
            self.target_uuid = None
            self.pt.clear()

            if self.lptid is not None:
                # I need to retrieve last ids again.
                logging.debug('resending Ask Last IDs')
                for conn in em.getConnectionList():
                    uuid = conn.getUUID()
                    if uuid is not None:
                        node = nm.getNodeByUUID(uuid)
                        if isinstance(node, StorageNode) \
                                and node.getState() == RUNNING_STATE:
                            p = Packet()
                            msg_id = conn.getNextId()
                            p.askLastIDs(msg_id)
                            conn.addPacket(p)
                            conn.expectMessage(msg_id)

            # Wait for at least one storage node to appear.
            while self.target_uuid is None:
                em.poll(1)

            # Wait a bit.
            t = time()
            while time() < t + 5:
                em.poll(1)

            # Now I have at least one to ask.
            prev_lptid = self.lptid
            node = nm.getNodeByUUID(uuid)
            if node.getState() != RUNNING_STATE:
                # Weird. It's dead.
                logging.info('the target storage node is dead')
                continue

            for conn in em.getConnectionList():
                if conn.getUUID() == self.lptid:
                    break
            else:
                # Why?
                logging.info('no connection to the target storage node')
                continue

            if self.lptid == INVALID_PTID:
                # This looks like the first time. So make a fresh table.
                logging.debug('creating a new partition table')
                self.pt.make(nm.getStorageNodeList())
            else:
                # Obtain a partition table. It is necessary to split this message
                # because the packet size can be huge.
                logging.debug('asking a partition table to %s:%d', *(node.getServer()))
                start = 0
                size = self.num_partitions
                while size:
                    amt = min(1000, size)
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askPartitionTable(msg_id, range(start, start + amt))
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
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
                    continue

                # Wait until the cluster gets operational or the Partition Table ID
                # turns out to be not the latest.
                logging.debug('waiting for the cluster to be operational')
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

    def playPrimaryRole(self):
        logging.info('play the primary role')
        recovering = True
        while recovering:
            self.recoverStatus()
            recovering = False
            try:
                self.verifyData()
            except VerificationFailure:
                recovering = True
        raise NotImplementedError

    def playSecondaryRole(self):
        logging.info('play the secondary role')
        raise NotImplementedError

    def getNextPartitionTableID(self):
        if self.lptid is None:
            raise RuntimeError, 'I do not know the last Partition Table ID'

        l = []
        append = l.append
        for c in self.lptid:
            append(c)
        for i in xrange(7, -1, -1):
            d = ord(l[i])
            if d == 255:
                l[i] = chr(0)
            else:
                l[i] = chr(d + 1)
                break
        else:
            raise RuntimeError, 'Partition Table ID overflowed'

        self.lptid = ''.join(l)
        return self.lptid
