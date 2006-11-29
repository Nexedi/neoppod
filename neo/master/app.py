import logging
import MySQLdb
import os
from socket import inet_aton
from time import time

from neo.config import ConfigurationManager
from neo.protocol import Packet, ProtocolError, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        INVALID_UUID, INVALID_OID, INVALID_TID
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode
from neo.handler import EventHandler
from neo.event import EventManager
from neo.util import dump
from neo.connection import ListeningConnection

class NeoException(Exception): pass
class ElectionFailure(NeoException): pass
class PrimaryFailure(NeoException): pass

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

        self.loid = INVALID_OID
        self.ltid = INVALID_TID

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
                    conn.close()
                # Reelect a new primary master.
                self.electPrimary(bootstrap = False)

    def electPrimaryClientIterator(self):
        """Handle events for a client connection."""
        # The first event. This must be a connection failure or connection completion.
        # Keep the Connection object and the server address only at this time,
        # because they never change in this context.
        method, conn = self.event[0], self.event[1]
        logging.debug('method is %r, conn is %s:%d', method, conn.ip_address, conn.port)
        serv = (conn.ip_address, conn.port)
        node = self.nm.getNodeByServer(*serv)
        if node is None:
            raise RuntimeError, 'attempted to connect to an unknown node'
        if not isinstance(node, MasterNode):
            raise RuntimeError, 'should not happen'

        if method is self.CONNECTION_FAILED:
            self.negotiating_master_node_set.discard(serv)
            self.unconnected_master_node_set.add(serv)
            if node.getState() not in (DOWN_STATE, BROKEN_STATE):
                node.setState(TEMPORARILY_DOWN_STATE)
            return
        elif method is self.CONNECTION_COMPLETED:
            self.negotiating_master_node_set.add(serv)

            # Request a node idenfitication.
            p = Packet()
            msg_id = conn.getNextId()
            p.requestNodeIdentification(msg_id, MASTER_NODE_TYPE, self.uuid,
                                        self.ip_address, self.port, self.name)
            conn.addPacket(p) 
            conn.expectMessage(msg_id)
        else:
            raise RuntimeError, 'unexpected event %r' % (method,)

        while 1:
            # Wait for next event.
            yield None

            method = self.event[0]
            logging.debug('method is %r, conn is %s:%d', method, conn.ip_address, conn.port)
            if method in (self.CONNECTION_CLOSED, self.TIMEOUT_EXPIRED):
                self.negotiating_master_node_set.discard(serv)
                self.unconnected_master_node_set.add(serv)

                if node.getState() not in (DOWN_STATE, BROKEN_STATE):
                    node.setState(TEMPORARILY_DOWN_STATE)
                return
            elif method is self.PEER_BROKEN:
                self.negotiating_master_node_set.discard(serv)

                # For now, do not use BROKEN_STATE, because the effect is unknown
                # when a node was buggy and restarted immediately.
                node.setState(DOWN_STATE)
                return
            elif method is self.PACKET_RECEIVED:
                if node.getState() != BROKEN_STATE:
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
                            node.setState(DOWN_STATE)
                            self.negotiating_master_node_set.discard(serv)
                            conn.close()
                            return
                    elif t == PING:
                        logging.info('got a keep-alive message from %s:%d; overloaded?',
                                     conn.ip_address, conn.port)
                        conn.addPacket(Packet().pong(packet.getId()))
                    elif t == PONG:
                        pass
                    elif t == ACCEPT_NODE_IDENTIFICATION:
                        node_type, uuid, ip_address, port = packet.decode()
                        if node_type != MASTER_NODE_TYPE:
                            # Why? Isn't this a master node?
                            self.nm.remove(node)
                            self.negotiating_master_node_set.discard(serv)
                            conn.close()
                            return

                        conn.setUUID(uuid)
                        node.setUUID(uuid)

                        # Ask a primary master.
                        msg_id = conn.getNextId()
                        p = Packet()
                        conn.addPacket(p.askPrimaryMaster(msg_id, self.ltid, self.loid))
                        conn.expectMessage(msg_id)
                    elif t == ANSWER_PRIMARY_MASTER:
                        ltid, loid, primary_uuid, known_master_list = packet.decode()

                        # Register new master nodes.
                        for ip_address, port, uuid in known_master_list:
                            if self.ip_address == ip_address and self.port == port:
                                # This is self.
                                continue
                            else:
                                n = self.nm.getNodeByServer(ip_address, port)
                                if n is None:
                                    n = MasterNode(ip_address, port)
                                    self.nm.add(n)
                                    self.unconnected_master_node_set.add((ip_address, port))
                                    if uuid != INVALID_UUID:
                                        n.setUUID(uuid)
                                elif uuid != INVALID_UUID:
                                    # If I don't know the UUID yet, believe what the peer
                                    # told me at the moment.
                                    if n.getUUID() is None:
                                        n.setUUID(uuid)

                        if primary_uuid != INVALID_UUID:
                            # The primary master is defined.
                            if self.primary_master_node is not None \
                                    and self.primary_master_node.getUUID() != primary_uuid:
                                # There are multiple primary master nodes. This is
                                # dangerous.
                                raise ElectionFailure, 'multiple primary master nodes'
                            primary_node = self.nm.getNodeByUUID(primary_uuid)
                            if primary_node is None:
                                # I don't know such a node. Probably this information
                                # is old. So ignore it.
                                pass
                            else:
                                if node.getUUID() == primary_uuid:
                                    if self.ltid <= ltid and self.loid <= loid:
                                        # This one is good.
                                        self.primary = False
                                        self.primary_master_node = node
                                    else:
                                        # Not nice. I am newer. If the primary master is
                                        # already serving, the situation is catastrophic.
                                        # In this case, it will shutdown the cluster.
                                        # Otherwise, I can be a new primary master, so
                                        # continue this job.
                                        pass
                                else:
                                    # I will continue this, until I find the primary
                                    # master.
                                    pass
                        else:
                            if self.ltid < ltid or self.loid < loid \
                                    or inet_aton(self.ip_address) > inet_aton(ip_address) \
                                    or self.port > port:
                                # I lost.
                                self.primary = False
                            else:
                                # I won.
                                pass

                        self.negotiating_master_node_set.discard(serv)
                    else:
                        raise ProtocolError(packet, 'unexpected packet 0x%x' % t)
                except ProtocolError, m:
                    logging.debug('protocol problem %s', m[1])
                    conn.addPacket(Packet().protocolError(m[0].getId(), m[1]))
                    conn.abort()
                    self.negotiating_master_node_set.discard(serv)
                    self.unconnected_master_node_set.add(serv)
            else:
                raise RuntimeError, 'unexpected event %r' % (method,)

    def electPrimaryServerIterator(self):
        """Handle events for a server connection."""
        # The first event. This must be a connection acception.
        method, conn = self.event[0], self.event[1]
        logging.debug('method is %r, conn is %s:%d', method, conn.ip_address, conn.port)
        serv = (conn.ip_address, conn.port)
        node = None
        if method is self.CONNECTION_ACCEPTED:
            # Nothing to do at the moment. The timeout handling is done in
            # the connection itself.
            pass
        else:
            raise RuntimeError, 'unexpected event %r' % (method,)

        while 1:
            # Wait for next event.
            yield None

            method = self.event[0]
            logging.debug('method is %r, conn is %s:%d', method, conn.ip_address, conn.port)
            if method in (self.CONNECTION_CLOSED, self.TIMEOUT_EXPIRED):
                return
            elif method is self.PEER_BROKEN:
                if node is not None:
                    if isinstance(node, MasterNode):
                        node.setState(DOWN_STATE)
                    elif isinstance(node, (ClientNode, StorageNode)):
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

    def electPrimary(self, bootstrap = True):
        """Elect a primary master node.
        
        The difficulty is that a master node must accept connections from
        others while attempting to connect to other master nodes at the
        same time. Note that storage nodes and client nodes may connect
        to self as well as master nodes."""
        logging.info('begin the election of a primary master')

        self.server_thread_method = self.electPrimaryServerIterator

        self.unconnected_master_node_set = set()
        self.negotiating_master_node_set = set()

        em = self.em
        nm = self.nm
        while 1:
            t = 0
            self.primary = None
            self.primary_master_node = None

            for node in nm.getMasterNodeList():
                if node.getState() in (RUNNING_STATE, TEMPORARILY_DOWN_STATE):
                    self.unconnected_master_node_set.add(node.getServer())
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
                            for node in list(self.unconnected_master_node_set):
                                self.unconnected_master_node_set.remove(node)
                                self.negotiating_master_node_set.add(node)
                                client = self.electPrimaryClientIterator()
                                self.thread_dict[node] = client
                                cm.connect(*node)

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
                    for conn in cm.getConnectionList():
                        if conn.from_self:
                            p = Packet().announcePrimaryMaster(conn.getNextId())
                            conn.addPacket(p)
                            conn.abort()
                    closed = False
                    t = time()
                    while not closed:
                        cm.poll(1)
                        closed = True
                        for conn in cm.getConnectionList():
                            if conn.from_self:
                                closed = False
                                break
                        if t + 10 < time():
                            for conn in cm.getConnectionList():
                                if conn.from_self:
                                    del self.thread_dict[(conn.ip_address, conn.port)]
                                    conn.close()
                            closed = True
                else:
                    # Wait for an announcement. If this is too long, probably
                    # the primary master is down.
                    t = time()
                    while self.primary_master_node is None:
                        cm.poll(1)
                        if t + 10 < time():
                            raise ElectionFailure, 'no primary master elected'

                    # Now I need only a connection to the primary master node.
                    primary = self.primary_master_node
                    for conn in cm.getConnectionList():
                        if not conn.from_self \
                                or primary.getServer() != (conn.ip_address, conn.port):
                            del self.thread_dict[(conn.ip_address, conn.port)]
                            conn.close()
                    # But if there is no such connection, something wrong happened.
                    for conn in cm.getConnectionList():
                        if conn.from_self \
                                and primary.getServer() == (conn.ip_address, conn.port):
                            break
                    else:
                        raise ElectionFailure, 'no connection remains to the primary'

                return
            except ElectionFailure, m:
                logging.info('election failed; %s' % m)

                # Ask all connected nodes to reelect a single primary master.
                for conn in cm.getConnectionList():
                    if conn.from_self:
                        conn.addPacket(Packet().reelectPrimaryMaster(conn.getNextId()))
                        conn.abort()

                # Wait until the connections are closed.
                self.primary = None
                self.primary_master_node = None
                closed = False
                t = time()
                while not closed:
                    try:
                        cm.poll(1)
                    except ElectionFailure:
                        pass

                    closed = True
                    for conn in cm.getConnectionList():
                        if conn.from_self:
                            # Still not closed.
                            closed = Falsed
                            break
                    
                    if time() > t + 10:
                        # If too long, do not wait.
                        break
                # Close all connections.
                for conn in cm.getConnectionList():
                    conn.close()
                self.thread_dict.clear()
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

            for c in cm.getConnectionList():
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
            for c in cm.getConnectionList():
                if c.getUUID() is not None:
                    p = Packet()
                    p.notifyNodeStateChange(c.getNextId(),
                                            MASTER_NODE_TYPE,
                                            ip_address, port,
                                            uuid, state)
                    c.addPacket(p)
        elif isinstance(node, StorageNode):
            for c in cm.getConnectionList():
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
        cm = self.cm
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
