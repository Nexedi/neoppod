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

from neo import logging
import sys
from collections import deque

from neo.config import ConfigurationManager
from neo import protocol
from neo.protocol import TEMPORARILY_DOWN_STATE, \
        partition_cell_states, HIDDEN_STATE
from neo.node import NodeManager, MasterNode, StorageNode
from neo.event import EventManager
from neo.storage.mysqldb import MySQLDatabaseManager
from neo.connection import ListeningConnection
from neo.exception import OperationFailure, PrimaryFailure
from neo.storage.handlers import identification, verification, initialization
from neo.storage.handlers import master, hidden
from neo.storage.replicator import Replicator
from neo.connector import getConnectorHandler
from neo.pt import PartitionTable
from neo.util import dump
from neo.bootstrap import BootstrapManager

class Application(object):
    """The storage node application."""

    def __init__(self, filename, section, reset=False):
        config = ConfigurationManager(filename, section)

        self.uuid = None
        self.name = config.getName()
        logging.debug('the name is %s', self.name)
        self.connector_handler = getConnectorHandler(config.getConnector())

        self.server = config.getServer()
        logging.debug('IP address is %s, port is %d', *(self.server))

        self.master_node_list = config.getMasterNodeList()
        logging.debug('master nodes are %s', self.master_node_list)

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.dm = MySQLDatabaseManager(database = config.getDatabase(),
                                       user = config.getUser(),
                                       password = config.getPassword())
        self.loid = None
        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        # XXX: shoud use self.pt.getID() instead
        self.ptid = None

        self.replicator = None
        self.listening_conn = None
        self.master_conn = None
        self.master_node = None

        # operation related data
        self.transaction_dict = {}
        self.store_lock_dict = {}
        self.load_lock_dict = {}
        self.event_queue = None
        self.operational = False

        # ready is True when operational and got all informations
        self.ready = False
        self.has_node_information = False
        self.has_partition_table = False

        self.dm.setup(reset)
        self.loadConfiguration()

    def loadConfiguration(self):
        """Load persistent configuration data from the database.
        If data is not present, generate it."""
        dm = self.dm

        self.uuid = dm.getUUID()
        num_partitions = dm.getNumPartitions()
        num_replicas = dm.getNumReplicas()

        if num_partitions is not None and num_replicas is not None:
            if num_partitions <= 0:
                raise RuntimeError, 'partitions must be more than zero'
            # create a partition table
            self.pt = PartitionTable(num_partitions, num_replicas)

        name = dm.getName()
        if name is None:
            dm.setName(self.name)
        elif name != self.name:
            raise RuntimeError('name does not match with the database')
        self.ptid = dm.getPTID()
        logging.info("loaded configuration from db : uuid = %s, ptid = %s, name = %s, np = %s, nr = %s" \
                     %(dump(self.uuid), dump(self.ptid), name, num_partitions, num_replicas))

    def loadPartitionTable(self):
        """Load a partition table from the database."""
        nm = self.nm
        pt = self.pt
        pt.clear()
        for offset, uuid, state in self.dm.getPartitionTable():
            node = nm.getNodeByUUID(uuid)
            if node is None:
                node = StorageNode(uuid = uuid)
                if uuid != self.uuid:
                    # If this node is not self, assume that it is temporarily
                    # down at the moment. This state will change once every
                    # node starts to connect to a primary master node.
                    node.setState(TEMPORARILY_DOWN_STATE)
                nm.add(node)
            state = partition_cell_states.get(state)

            pt.setCell(offset, node, state)

    def run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Make a listening port
        handler = identification.IdentificationHandler(self)
        self.listening_conn = ListeningConnection(self.em, handler, 
            addr=self.server, connector_handler=self.connector_handler)

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permentnly,
        # until the user explicitly requests a shutdown.
        while 1:
            # look for the primary master
            self.connectToPrimaryMaster()
            self.operational = False
            try:
                while 1:
                    try:
                        # check my state
                        node = self.nm.getNodeByUUID(self.uuid)
                        if node is not None and node.getState() == HIDDEN_STATE:
                            self.wait()
                        self.verifyData()
                        self.initialize()
                        self.doOperation()
                    except OperationFailure:
                        logging.error('operation stopped')
                        # XXX still we can receive answer packet here
                        # this must be handle in order not to fail
                        self.operational = False

            except PrimaryFailure, msg:
                logging.error('primary master is down : %s' % msg)

    def connectToPrimaryMaster(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""
        pt = self.pt

        # First of all, make sure that I have no connection.
        for conn in self.em.getConnectionList():
            if not isinstance(conn, ListeningConnection):
                conn.close()

        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name,
                protocol.STORAGE_NODE_TYPE, self.uuid, self.server)
        data = bootstrap.getPrimaryConnection(self.connector_handler)
        (node, conn, uuid, num_partitions, num_replicas) = data
        self.master_node = node
        self.master_conn = conn
        self.uuid = uuid
        self.dm.setUUID(uuid)

        # Reload a partition table from the database. This is necessary
        # when a previous primary master died while sending a partition
        # table, because the table might be incomplete.
        if pt is not None:
            self.loadPartitionTable()
            self.ptid = self.dm.getPTID()
            if num_partitions != pt.getPartitions():
                raise RuntimeError('the number of partitions is inconsistent')

        if pt is None or pt.getReplicas() != num_replicas:
            # changing number of replicas is not an issue
            self.dm.setNumPartitions(num_partitions)
            self.dm.setNumReplicas(num_replicas)
            self.pt = PartitionTable(num_partitions, num_replicas)
            self.loadPartitionTable()
            self.ptid = self.dm.getPTID()

    def verifyData(self):
        """Verify data under the control by a primary master node.
        Connections from client nodes may not be accepted at this stage."""
        logging.info('verifying data')

        handler = verification.VerificationHandler(self)
        self.master_conn.setHandler(handler)
        em = self.em

        while not self.operational:
            em.poll(1)

    def initialize(self):
        """ Retreive partition table and node informations from the primary """
        logging.debug('initializing...')
        handler = initialization.InitializationHandler(self)
        self.master_conn.setHandler(handler)

        # ask node list and partition table
        self.has_node_information = False
        self.has_partition_table = False
        self.pt.clear()
        self.master_conn.ask(protocol.askNodeInformation())        
        self.master_conn.ask(protocol.askPartitionTable(()))
        while not self.has_node_information or not self.has_partition_table:
            self.em.poll(1)
        self.ready = True
        # TODO: notify the master that I switch to operation state, so other
        # nodes can now connect to me

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        em = self.em

        handler = master.MasterOperationHandler(self)
        self.master_conn.setHandler(handler)

        # Forget all unfinished data.
        self.dm.dropUnfinishedData()

        # This is a mapping between transaction IDs and information on
        # UUIDs of client nodes which issued transactions and objects
        # which were stored.
        self.transaction_dict = {}

        # This is a mapping between object IDs and transaction IDs. Used
        # for locking objects against store operations.
        self.store_lock_dict = {}

        # This is a mapping between object IDs and transactions IDs. Used
        # for locking objects against load operations.
        self.load_lock_dict = {}

        # This is a queue of events used to delay operations due to locks.
        self.event_queue = deque()

        # The replicator.
        self.replicator = Replicator(self)

        while 1:
            em.poll(1)
            if self.replicator.pending():
                self.replicator.act()

    def wait(self):
        # change handler
        logging.info("waiting in hidden state")
        handler = hidden.HiddenHandler(self)
        for conn in self.em.getConnectionList():
            conn.setHandler(handler)

        node = self.nm.getNodeByUUID(self.uuid)
        while 1:
            self.em.poll(1)
            if node.getState() != HIDDEN_STATE:
                break

    def queueEvent(self, some_callable, *args, **kwargs):
        self.event_queue.append((some_callable, args, kwargs))

    def executeQueuedEvents(self):
        l = len(self.event_queue)
        p = self.event_queue.popleft
        for i in xrange(l):
            some_callable, args, kwargs = p()
            some_callable(*args, **kwargs)

    def shutdown(self):
        """Close all connections and exit"""
        for c in self.em.getConnectionList():
            if not c.isListeningConnection():
                c.close()
        sys.exit("Application has been asked to shut down")
