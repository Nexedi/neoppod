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
from neo.exception import ElectionFailure, PrimaryFailure
from neo.master.election import ElectionEventHandler

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
                    while 1:
                        self.startRecovery()
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

    def broadcastNodeStateChange(self, node):
        state = node.getState()
        uuid = node.getUUID()
        ip_address, port = node.getServer()
        if ip_address is None:
            ip_address = '0.0.0.0'
        if port is None:
            port = 0

        if isinstance(node, ClientNode):
            # Notify secondary master nodes and storage nodes of
            # the removal of the client node.

            for c in em.getConnectionList():
                if c.getUUID() is not None:
                    n = nm.getNodeByUUID(uuid)
                    if isinstance(n, (MasterNode, StorageNode)):
                        p = Packet()
                        p.notifyNodeStateChange(c.getNextId(), 
                                                CLIENT_NODE_TYPE,
                                                ip_address, port,
                                                uuid, state)
                        c.addPacket(p)
        elif isinstance(node, MasterNode):
            for c in em.getConnectionList():
                if c.getUUID() is not None:
                    p = Packet()
                    p.notifyNodeStateChange(c.getNextId(),
                                            MASTER_NODE_TYPE,
                                            ip_address, port,
                                            uuid, state)
                    c.addPacket(p)
        elif isinstance(node, StorageNode):
            for c in em.getConnectionList():
                if c.getUUID() is not None:
                    p = Packet()
                    p.notifyNodeStateChange(c.getNextId(),
                                            STORAGE_NODE_TYPE,
                                            ip_address, port,
                                            uuid, state)
                    c.addPacket(p)
        else:
            raise Runtime, 'unknown node type'

    def playPrimaryRoleServerIterator(self):
        """Handle events for a server connection."""
        em = self.em
        nm = self.nm
        while 1:
            method, conn = self.event[0], self.event[1]
            logging.debug('method is %r, conn is %s:%d', 
                          method, conn.ip_address, conn.port)
            if method is self.CONNECTION_ACCEPTED:
                pass
            elif method in (self.CONNECTION_CLOSED, self.TIMEOUT_EXPIRED):
                uuid = conn.getUUID()
                if uuid is not None:
                    # If the peer is identified, mark it as temporarily down or down.
                    node = nm.getNodeByUUID(uuid)
                    if isinstance(node, ClientNode):
                        node.setState(DOWN_STATE)
                        self.broadcastNodeStateChange(node)
                        # For now, down client nodes simply get forgotten.
                        nm.remove(node)
                    elif isinstance(node, MasterNode):
                        if node.getState() not in (BROKEN_STATE, DOWN_STATE):
                            node.setState(TEMPORARILY_DOWN_STATE)
                            self.broadcastNodeStateChange(node)
                    elif isinstance(node, StorageNode):
                        if node.getState() not in (BROKEN_STATE, DOWN_STATE):
                            node.setState(TEMPORARILY_DOWN_STATE)
                            self.broadcastNodeStateChange(node)
                            # FIXME check the partition table.
                            self.pt.setTemporarilyDown(node.getUUID())
                            if self.ready and self.pt.fatal():
                                logging.critical('the storage nodes are not enough')
                                self.ready = False
                                self.broadcast

                            # FIXME update the database.
                    else:
                        raise RuntimeError, 'unknown node type'
                return
            elif method is self.PEER_BROKEN:
                uuid = conn.getUUID()
                if uuid is not None:
                    # If the peer is identified, mark it as broken.
                    node = nm.getNodeByUUID(uuid)
                    node.setState(BROKEN_STATE)
                return
            elif method is self.PACKET_RECEIVED:
                if node is not None and node.getState() != BROKEN_STATE:
                    node.setState(RUNNING_STATE)

                packet = self.event[2]
                t = packet.getType()
                try:
                    if t == ERROR:
                        code, msg = packet.decode()
                        if code in (PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, 
                                    BROKEN_NODE_DISALLOWED_CODE):
                            # In those cases, it is better to assume that I am unusable.
                            logging.critical(msg)
                            raise RuntimeError, msg
                        else:
                            # Otherwise, the peer has an error.
                            logging.error('an error happened at the peer %s:%d', 
                                          conn.ip_address, conn.port)
                            if node is not None:
                                node.setState(BROKEN_STATE)
                            conn.close()
                            return
                    elif t == PING:
                        logging.info('got a keep-alive message from %s:%d; overloaded?',
                                     conn.ip_address, conn.port)
                        conn.addPacket(Packet().pong(packet.getId()))
                    elif t == PONG:
                        pass
                    elif t == REQUEST_NODE_IDENTIFICATION:
                        node_type, uuid, ip_address, port, name = packet.decode()
                        if node_type != MASTER_NODE_TYPE:
                            logging.info('reject a connection from a non-master')
                            conn.addPacket(Packet().notReady(packet.getId(), 
                                                             'retry later'))
                            conn.abort()
                            continue
                        if name != self.name:
                            logging.info('reject an alien cluster')
                            conn.addPacket(Packet().protocolError(packet.getId(), 
                                                                  'invalid cluster name'))
                            conn.abort()
                            continue
                        node = self.nm.getNodeByServer(ip_address, port)
                        if node is None:
                            node = MasterNode(ip_address, port, uuid)
                            self.nm.add(node)
                            self.unconnected_master_node_set.add((ip_address, port))
                        else:
                            # Trust the UUID sent by the peer.
                            if node.getUUID() != uuid:
                                node.setUUID(uuid)
                        conn.setUUID(uuid)
                        p = Packet()
                        p.acceptNodeIdentification(packet.getId(), MASTER_NODE_TYPE,
                                                   self.uuid, self.ip_address, self.port)
                        conn.addPacket(p)
                        conn.expectMessage()
                    elif t == ASK_PRIMARY_MASTER:
                        if node is None:
                            raise ProtocolError(packet, 'not identified')

                        ltid, loid = packet.decode()

                        p = Packet()
                        if self.primary:
                            uuid = self.uuid
                        elif self.primary_master_node is not None:
                            uuid = self.primary_master_node.getUUID()
                        else:
                            uuid = INVALID_UUID

                        known_master_list = []
                        for n in self.nm.getMasterNodeList():
                            info = n.getServer() + (n.getUUID() or INVALID_UUID,)
                            known_master_list.append(info)
                        p.answerPrimaryMaster(packet.getId(), self.ltid, self.loid,
                                              uuid, known_master_list)
                        conn.addPacket(p)

                        if self.primary and (self.ltid < ltid or self.loid < loid):
                            # I am not really primary... So restart the election.
                            raise ElectionFailure, 'not a primary master any longer'
                    elif t == ANNOUNCE_PRIMARY_MASTER:
                        if node is None:
                            raise ProtocolError(packet, 'not identified')

                        if self.primary:
                            # I am also the primary... So restart the election.
                            raise ElectionFailure, 'another primary arises'

                        self.primary = False
                        self.primary_master_node = node
                        logging.info('%s:%d is the primary' % node.getServer())
                    elif t == REELECT_PRIMARY_MASTER:
                        raise ElectionFailure, 'reelection requested'
                    else:
                        raise ProtocolError(packet, 'unexpected packet 0x%x' % t)
                except ProtocolError, m:
                    logging.debug('protocol problem %s', m[1])
                    conn.addPacket(Packet().protocolError(m[0].getId(), m[1]))
                    conn.abort()
            else:
                raise RuntimeError, 'unexpected event %r' % (method,)
        
    def playPrimaryRole(self):
        logging.info('play the primary role')
        self.ready = False
        raise NotImplementedError

    def playSecondaryRole(self):
        logging.info('play the secondary role')
        raise NotImplementedError
