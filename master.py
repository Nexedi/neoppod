import logging
import MySQLdb
import os

from connection import ConnectionManager
from connection import Connection as BaseConnection
from database import DatabaseManager
from config import ConfigurationManager
from protocol import Packet, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE

class Connection(BaseConnection):
    """This class provides a master-specific connection."""

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
    
class Application:
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
        q("""INSERT loid VALUES ('%s')""" % e('\0\0\0\0\0\0\0\0'))

        q("""CREATE TABLE ltid (
                 tid BINARY(8) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'Last Transaction ID'""")
        q("""INSERT ltid VALUES ('%s')""" % e('\0\0\0\0\0\0\0\0'))

        q("""CREATE TABLE self (
                 uuid BINARY(16) NOT NULL
             ) ENGINE = InnoDB COMMENT = 'UUID'""")
        # XXX Generate an UUID for self. For now, just use a random string.
        q("""INSERT self VALUES ('%s')""" % e(os.urandom(16)))

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
        logging.info('the last OID is %r' % self.loid)

        result = q("""SELECT tid FROM ltid""")
        if len(result) != 1:
            raise RuntimeError, 'the table ltid has %d rows' % len(result)
        self.ltid = result[0][0]
        logging.info('the last TID is %r' % self.ltid)

        result = q("""SELECT uuid FROM self""")
        if len(result) != 1:
            raise RuntimeError, 'the table self has %d rows' % len(result)
        self.uuid = result[0][0]
        logging.info('the UUID is %r' % self.uuid)

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

        # Make a listening port.
        self.cm.listen(self.ip_address, self.port)

        # Start the election of a primary master node.
        self.electPrimary()

    def connectionFailed(self, conn):
        # The connection failed, so I must attempt to retry.
        self.unconnected_master_node_list.append((conn.ip_address, conn.port))

    def connectionCompleted(self, conn):
        self.connecting_master_node_list.remove((conn.ip_address, conn.port))
        p = Packet()
        msg_id = conn.getNextId()
        p.requestNodeIdentification(msg_id, MASTER_NODE_TYPE, self.uuid,
                                    self.ip_address, self.port, self.name)
        conn.addPacket(p) 
        conn.expectMessage(msg_id)

    def connectionClosed(self, conn):
        pass

    def connectionAccepted(self, conn):
        pass

    def timeoutExpired(self, conn):
        pass

    def peerBroken(self, conn):
        pass

    def packetReceived(self, conn, packet):
        try:
            # If the packet is an error, deal with it as much as possible here.
            t = packet.getType()
            if t == ERROR:
                code, msg = packet.decode()
                if code in (PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE):
                    # In those cases, it is better to assume that I am unusable.
                    logging.critical(msg)
                    raise RuntimeError, msg
            elif t == PING:
                self.addPacket(Packet().pong(packet.getId()))
                return
            elif t == PONG:
                return

            if self.ready:
                self.cm.app.packetReceived(self, packet)
            else:
                if self.from_self:
                    if t == ACCEPT_NODE_IDENTIFICATION:
                        node_type, uuid, ip_address, port = packet.decode()
                        self.node_type = node_type
                        self.uuid = node_type
                        self.ip_address = ip_address
                        self.port = port
                        self.ready = True
                    else:
                        raise ProtocolError(packet, 'unexpected packet 0x%x' % t)
                else:
                    if t == REQUEST_NODE_IDENTIFICATION:
                        node_type, uuid, ip_address, port, name = packet.decode()
                        self.node_type = node_type
                        self.uuid = uuid
                        self.ip_address = ip_address
                        self.port = port
                        self.name = name
                        if self.cm.app.verifyNodeIdentification(self):
                            self.cm.app.acceptNodeIdentification(self)
                            self.ready = True
                        else:
                            self.addPacket(Packet().protocolError(packet.getId(),
                                                                  'invalid identification'))
                            self.abort()
                    else:
                        raise ProtocolError(packet, 'unexpected packet 0x%x' % t)

                if self.ready:
                    self.connectionReady()
        except ProtocolError, m:
            self.packetMalformed(*m)

    def electPrimary(self):
        """Elect a primary master node.
        
        The difficulty is that a master node must accept connections from
        others while attempting to connect to other master nodes at the
        same time. Note that storage nodes and client nodes may connect
        to self as well as master nodes."""
        logging.info('begin the election of a primary master')

        self.unconnected_master_node_list = list(self.master_node_list)
        self.connecting_master_node_list = []
        cm = self.cm
        while 1:
            for node in list(self.unconnected_master_node_list):
                self.unconnected_master_node_list.remove(node)
                self.connecting_master_node_list.append(node)
                cm.connect(*node)
            cm.poll(1)


