import logging
import MySQLdb
import os
from socket import inet_aton
from time import time

from connection import ConnectionManager
from connection import Connection as BaseConnection
from database import DatabaseManager
from config import ConfigurationManager
from protocol import Packet, ProtocolError, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        INVALID_UUID, INVALID_TID, INVALID_OID, \
        PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE, \
        INTERNAL_ERROR_CODE, \
        ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
        PING, PONG, ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, \
        ANNOUNCE_PRIMARY_MASTER, REELECT_PRIMARY_MASTER
from node import NodeManager, MasterNode, StorageNode, ClientNode, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE

from util import dump

class ElectionFailure(Exception): pass
class PrimaryFailure(Exception): pass

class Connection(BaseConnection):
    """This class provides a master-specific connection."""

    _uuid = None

    def setUUID(self, uuid):
        self._uuid = uuid

    def getUUID(self):
        return self._uuid

    # Feed callbacks to the master node.
    def connectionFailed(self):
        self.cm.app.connectionFailed(self)
        BaseConnection.connectionFailed(self)

    def connectionCompleted(self):
        self.cm.app.connectionCompleted(self)
        BaseConnection.connectionCompleted(self)

    def connectionAccepted(self):
        self.cm.app.connectionAccepted(self)
        BaseConnection.connectionAccepted(self)

    def connectionClosed(self):
        self.cm.app.connectionClosed(self)
        BaseConnection.connectionClosed(self)

    def packetReceived(self, packet):
        self.cm.app.packetReceived(self, packet)
        BaseConnection.packetReceived(self, packet)

    def timeoutExpired(self):
        self.cm.app.timeoutExpired(self)
        BaseConnection.timeoutExpired(self)

    def peerBroken(self):
        self.cm.app.peerBroken(self)
        BaseConnection.peerBroken(self)
    
class Application(object):
    """The master node application."""

    def __init__(self, file, section):
        config = ConfigurationManager(file, section)

        self.database = config.getDatabase()
        self.user = config.getUser()
        self.password = config.getPassword()
        logging.debug('database is %s, user is %s, password is %s', 
                      self.database, self.user, self.password)

        self.num_replicas = config.getReplicas()
        self.num_partitions = config.getPartitions()
        self.name = config.getName()
        logging.debug('the number of replicas is %d, the number of partitions is %d, the name is %s',
                      self.num_replicas, self.num_partitions, self.name)

        self.ip_address, self.port = config.getServer()
        logging.debug('IP address is %s, port is %d', self.ip_address, self.port)

        # Exclude itself from the list.
        self.master_node_list = [n for n in config.getMasterNodeList()
                                    if n != (self.ip_address, self.port)]
        logging.debug('master nodes are %s', self.master_node_list)

        # Internal attributes.
        self.dm = DatabaseManager(self.database, self.user, self.password)
        self.cm = ConnectionManager(app = self, connection_klass = Connection)
        self.nm = NodeManager()

        self.primary = None
        self.primary_master_node = None

        # Co-operative threads. Simulated by generators.
        self.thread_dict = {}
        self.server_thread_method = None
        self.event = None

    def initializeDatabase(self):
        """Initialize a database by recreating all the tables.
        
        In master nodes, the database is used only to make
        some data persistent. All operations are executed on memory.
        Thus it is not necessary to make indices on the tables."""
        q = self.dm.query
        e = MySQLdb.escape_string

        q("""DROP TABLE IF EXISTS loid, ltid, self, stn, part""")

        q("""CREATE TABLE loid (
                 oid BINARY(8) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'Last Object ID'""")
        q("""INSERT loid VALUES ('%s')""" % e(INVALID_OID))

        q("""CREATE TABLE ltid (
                 tid BINARY(8) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'Last Transaction ID'""")
        q("""INSERT ltid VALUES ('%s')""" % e(INVALID_TID))

        q("""CREATE TABLE self (
                 uuid BINARY(16) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'UUID'""")

        # XXX Generate an UUID for self. For now, just use a random string.
        # Avoid an invalid UUID.
        while 1:
            uuid = os.urandom(16)
            if uuid != INVALID_UUID:
                break

        q("""INSERT self VALUES ('%s')""" % e(uuid))

        q("""CREATE TABLE stn (
                 nid INT UNSIGNED NOT NULL UNIQUE,
                 uuid BINARY(16) NOT NULL UNIQUE,
                 state CHAR(1) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'Storage Nodes'""")

        q("""CREATE TABLE part (
                 pid INT UNSIGNED NOT NULL,
                 nid INT UNSIGNED NOT NULL,
                 state CHAR(1) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'Partition Table'""")

    def loadData(self):
        """Load persistent data from a database."""
        logging.info('loading data from MySQL')
        q = self.dm.query
        result = q("""SELECT oid FROM loid""")
        if len(result) != 1:
            raise RuntimeError, 'the table loid has %d rows' % len(result)
        self.loid = result[0][0]
        logging.info('the last OID is %r' % dump(self.loid))

        result = q("""SELECT tid FROM ltid""")
        if len(result) != 1:
            raise RuntimeError, 'the table ltid has %d rows' % len(result)
        self.ltid = result[0][0]
        logging.info('the last TID is %r' % dump(self.ltid))

        result = q("""SELECT uuid FROM self""")
        if len(result) != 1:
            raise RuntimeError, 'the table self has %d rows' % len(result)
        self.uuid = result[0][0]
        logging.info('the UUID is %r' % dump(self.uuid))

        # FIXME load storage and partition information here.


    def run(self):
        """Make sure that the status is sane and start a loop."""
        # Sanity checks.
        logging.info('checking the database')
        result = self.dm.query("""SHOW TABLES""")
        table_set = set([r[0] for r in result])
        existing_table_list = [t for t in ('loid', 'ltid', 'self', 'stn', 'part')
                                   if t in table_set]
        if len(existing_table_list) == 0:
            # Possibly this is the first time to launch...
            self.initializeDatabase()
        elif len(existing_table_list) != 5:
            raise RuntimeError, 'database inconsistent'

        # XXX More tests are necessary (e.g. check the table structures,
        # check the number of partitions, etc.).

        # Now ready to load persistent data from the database.
        self.loadData()

        for ip_address, port in self.master_node_list:
            self.nm.add(MasterNode(ip_address = ip_address, port = port))

        # Make a listening port.
        self.cm.listen(self.ip_address, self.port)

        # Start the election of a primary master node.
        self.electPrimary()
        
        # Start a normal operation.
        while 1:
            try:
                if self.primary:
                    self.playPrimaryRole()
                else:
                    self.playSecondaryRole()
            except (ElectionFailure, PrimaryFailure):
                self.electPrimary(bootstrap = False)

    CONNECTION_FAILED = 'connection failed'
    def connectionFailed(self, conn):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.CONNECTION_FAILED, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    CONNECTION_COMPLETED = 'connection completed'
    def connectionCompleted(self, conn):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.CONNECTION_COMPLETED, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    CONNECTION_CLOSED = 'connection closed'
    def connectionClosed(self, conn):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.CONNECTION_CLOSED, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    CONNECTION_ACCEPTED = 'connection accepted'
    def connectionAccepted(self, conn):
        addr = (conn.ip_address, conn.port)
        logging.debug('making a server thread for %s:%d', conn.ip_address, conn.port)
        t = self.server_thread_method()
        self.thread_dict[addr] = t
        self.event = (self.CONNECTION_ACCEPTED, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    TIMEOUT_EXPIRED = 'timeout expired'
    def timeoutExpired(self, conn):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.TIMEOUT_EXPIRED, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    PEER_BROKEN = 'peer broken'
    def peerBroken(self, conn):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.PEER_BROKEN, conn)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]

    PACKET_RECEIVED = 'packet received'
    def packetReceived(self, conn, packet):
        addr = (conn.ip_address, conn.port)
        t = self.thread_dict[addr]
        self.event = (self.PACKET_RECEIVED, conn, packet)
        try:
            t.next()
        except StopIteration:
            del self.thread_dict[addr]
        
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

        cm = self.cm
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
                    cm.poll(1)
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
            except ElectionFailure:
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

    def playPrimaryRole(self):
        logging.info('play the primary role')
        raise NotImplementedError

    def playSecondaryRole(self):
        logging.info('play the secondary role')
        raise NotImplementedError
