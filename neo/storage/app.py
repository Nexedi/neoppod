import logging
import MySQLdb
import os
from time import time
from struct import pack, unpack

from neo.config import ConfigurationManager
from neo.protocol import Packet, ProtocolError, \
        RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        INVALID_UUID, INVALID_OID, INVALID_TID, INVALID_PTID
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode
from neo.event import EventManager
from neo.database import DatabaseManager
from neo.util import dump
from neo.connection import ListeningConnection, ClientConnection, ServerConnection
from neo.exception import OperationFailure, PrimaryFailure
from neo.pt import PartitionTable
from neo.storage.bootstrap import BoostrapEventHandler
from neo.storage.verification import VerificationEventHandler
from neo.storage.operation import OperationEventHandler

class Application(object):
    """The storage node application."""

    def __init__(self, file, section, reset = False):
        config = ConfigurationManager(file, section)

        self.num_partitions = config.getPartitions()
        self.name = config.getName()
        logging.debug('the number of replicas is %d, the number of partitions is %d, the name is %s',
                      self.num_replicas, self.num_partitions, self.name)

        self.server = config.getServer()
        logging.debug('IP address is %s, port is %d', *(self.server))

        self.master_node_list = config.getMasterNodeList()
        logging.debug('master nodes are %s', self.master_node_list)

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.dm = DatabaseManager(config.getDatabase(), config.getUser(), 
                                  config.getPassword())
        self.pt = PartitionTable(self.num_partitions, 0)

        self.primary_master_node = None

        if reset:
            self.dropTables()

        self.createTables()
        self.loadConfiguration()
        self.loadPartitionTable()

    def dropTables(self):
        """Drop all the tables, if any."""
        q = self.dm.query
        q("""DROP TABLE IF EXISTS config, pt, trans, obj, ttrans, tobj""")

    def createTables(self):
        """Create all the tables, if not present."""
        q = self.dm.query

        # The table "config" stores configuration parameters which affect the
        # persistent data.
        q("""CREATE TABLE IF NOT EXISTS config (
                 name VARBINARY(16) NOT NULL PRIMARY KEY,
                 value VARBINARY(255) NOT NULL
             ) ENGINE = InnoDB""")

        # The table "pt" stores a partition table.
        q("""CREATE TABLE IF NOT EXISTS pt (
                 rid INT UNSIGNED NOT NULL,
                 uuid BINARY(16) NOT NULL,
                 state TINYINT UNSIGNED NOT NULL,
                 PRIMARY KEY (rid, uuid)
             ) ENGINE = InnoDB""")

        # The table "trans" stores information on committed transactions.
        q("""CREATE TABLE IF NOT EXISTS trans (
                 tid BINARY(8) NOT NULL PRIMARY KEY,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 desc BLOB NOT NULL,
                 ext BLOB NOT NULL
             ) ENGINE = InnoDB""")

        # The table "obj" stores committed object data.
        q("""CREATE TABLE IF NOT EXISTS obj (
                 oid BINARY(8) NOT NULL,
                 serial BINARY(8) NOT NULL,
                 checksum INT UNSIGNED NOT NULL,
                 compression TINYINT UNSIGNED NOT NULL,
                 value MEDIUMBLOB NOT NULL,
                 PRIMARY KEY (oid, serial)
             ) ENGINE = InnoDB""")

        # The table "ttrans" stores information on uncommitted transactions.
        q("""CREATE TABLE IF NOT EXISTS ttrans (
                 tid BINARY(8) NOT NULL,
                 oids MEDIUMBLOB NOT NULL,
                 user BLOB NOT NULL,
                 desc BLOB NOT NULL,
                 ext BLOB NOT NULL
             ) ENGINE = InnoDB""")

        # The table "tobj" stores uncommitted object data.
        q("""CREATE TABLE IF NOT EXISTS tobj (
                 oid BINARY(8) NOT NULL,
                 serial BINARY(8) NOT NULL,
                 checksum INT UNSIGNED NOT NULL,
                 compression TINYINT UNSIGNED NOT NULL,
                 value MEDIUMBLOB NOT NULL
             ) ENGINE = InnoDB""")

    def loadConfiguration(self):
        """Load persistent configuration data from the database.
        If data is not present, generate it."""
        q = self.dm.query
        e = self.dm.escape

        r = q("""SELECT value FROM config WHERE name = 'uuid'""")
        try:
            self.uuid = r[0][0]
        except IndexError:
            # XXX Generate an UUID for self. For now, just use a random string.
            # Avoid an invalid UUID.
            while 1:
                uuid = os.urandom(16)
                if uuid != INVALID_UUID:
                    break
            self.uuid = uuid
            q("""INSERT config VALUES ('uuid', '%s')""" % e(uuid))

        r = q("""SELECT value FROM config WHERE name = 'partitions'""")
        try:
            if self.num_partitions != int(r[0][0]):
                raise RuntimeError('partitions do not match with the database')
        except IndexError:
            q("""INSERT config VALUES ('partitions', '%s')""" \
                    % e(str(self.num_replicas)))

        r = q("""SELECT value FROM config WHERE name = 'name'""")
        try:
            if self.name != r[0][0]:
                raise RuntimeError('name does not match with the database')
        except IndexError:
            q("""INSERT config VALUES ('name', '%s')""" % e(self.name))

        r = q("""SELECT value FROM config WHERE name = 'ptid'""")
        try:
            self.ptid = r[0][0]
        except IndexError:
            self.ptid = INVALID_PTID
            q("""INSERT config VALUES ('ptid', '%s')""" % e(INVALID_PTID))

    def loadPartitionTable(self):
        """Load a partition table from the database."""
        nm = self.nm
        pt = self.pt
        r = q("""SELECT rid, uuid, state FROM pt""")
        for offset, uuid, state in r:
            node = nm.getNodeByUUID(uuid)
            if node is None:
                node = StorageNode(uuid = uuid)
                if uuid != self.uiid:
                    # If this node is not self, assume that it is temporarily
                    # down at the moment. This state will change once every
                    # node starts to connect to a primary master node.
                    node.setState(TEMPORARILY_DOWN_STATE)
                nm.add(node)

            pt.setCell(offset, node, state)

    def run(self):
        """Make sure that the status is sane and start a loop."""
        if self.num_partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Make a listening port.
        ListeningConnection(self.em, None, addr = self.server)

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permentnly,
        # until the user explicitly requests a shutdown.
        while 1:
            self.connectToPrimaryMaster()
            try:
                while 1:
                    try:
                        self.verifyData()
                        self.doOperation()
                    except OperationFailure:
                        logging.error('operation stopped')
            except PrimaryFailure:
                logging.error('primary master is down')

    def connectToPrimaryMaster(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.
        
        Note that I do not accept any connection from non-master nodes
        at this stage."""
        logging.info('connecting to a primary master node')

        handler = BootstrapEventHandler(self)
        em = self.em
        nm = self.nm

        # First of all, make sure that I have no connection.
        for conn in em.getConnectionList():
            if not isinstance(conn, ListeningConnection):
                conn.close()

        # Make sure that every connection has the boostrap event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        index = 0
        self.trying_master_node = None
        t = 0
        while 1:
            em.poll(1)
            if self.primary_master_node is not None:
                # If I know which is a primary master node, check if
                # I have a connection to it already.
                for conn in em.getConnectionList():
                    if isinstance(conn, ClientConnection):
                        uuid = conn.getUUID()
                        if uuid is not None:
                            node = nm.getNodeByUUID(uuid)
                            if node is self.primary_master_node:
                                # Yes, I have.
                                return

                self.trying_master_node = self.primary_master_node

            if t + 1 < time():
                # Choose a master node to connect to.
                if self.primary_master_node is not None:
                    # If I know a primary master node, pinpoint it.
                    self.trying_master_node = self.primary_master_node
                else:
                    # Otherwise, check one by one.
                    master_list = nm.getMasterNodeList()
                    try:
                        self.trying_master_node = master_list[index]
                    except IndexError:
                        index = 0
                        self.trying_master_node = master_list[0]
                    index += 1

                ClientConnection(em, handler, \
                        addr = self.trying_master_node.getServer())
                t = time()

    def verifyData(self):
        """Verify data under the control by a primary master node.
        Connections from client nodes may not be accepted at this stage."""
        logging.info('verifying data')

        handler = VerificationEventHandler(self)
        em = self.em

        # Make sure that every connection has the verfication event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        self.operational = False
        while not self.operational:
            em.poll(1)

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        handler = OperationEventHandler(self)
        em = self.em

        # Make sure that every connection has the verfication event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        while 1:
            em.poll(1)

    def getPartition(self, oid_or_tid):
        return unpack('!Q', oid_or_tid)[0] % self.num_partitions
