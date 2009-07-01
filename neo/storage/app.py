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
from time import time
from struct import unpack, pack
from collections import deque

from neo.config import ConfigurationManager
from neo import protocol
from neo.protocol import TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        INVALID_UUID, INVALID_PTID, partition_cell_states, HIDDEN_STATE
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode
from neo.event import EventManager
from neo.storage.mysqldb import MySQLDatabaseManager
from neo.connection import ListeningConnection, ClientConnection
from neo.exception import OperationFailure, PrimaryFailure
from neo.storage.bootstrap import BootstrapEventHandler
from neo.storage.verification import VerificationEventHandler
from neo.storage.operation import OperationEventHandler
from neo.storage.hidden import HiddenEventHandler
from neo.storage.replicator import Replicator
from neo.connector import getConnectorHandler
from neo.pt import PartitionTable
from neo.util import dump

class Application(object):
    """The storage node application."""

    def __init__(self, file, section, reset = False):
        config = ConfigurationManager(file, section)

        self.num_partitions = None
        self.num_replicas = None
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
        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.primary_master_node = None
        self.replicator = None
        self.listening_conn = None
        self.master_conn = None

        self.has_node_information = False
        self.has_partition_table = False

        self.dm.setup(reset)
        self.loadConfiguration()

    def loadConfiguration(self):
        """Load persistent configuration data from the database.
        If data is not present, generate it."""
        dm = self.dm

        self.uuid = dm.getUUID()
        if self.uuid is None:
            self.uuid = INVALID_UUID

        self.num_partitions = dm.getNumPartitions()
        self.num_replicas = dm.getNumReplicas()

        if self.num_partitions is not None and self.num_replicas is not None:
            # create a partition table
            self.pt = PartitionTable(self.num_partitions, self.num_replicas)

        name = dm.getName()
        if name is None:
            dm.setName(self.name)
        elif name != self.name:
            raise RuntimeError('name does not match with the database')
        self.ptid = dm.getPTID() # return ptid or INVALID_PTID
        if self.ptid == INVALID_PTID:
            dm.setPTID(self.ptid)
        logging.info("loaded configuration from db : uuid = %s, ptid = %s, name = %s, np = %s, nr = %s" \
                     %(dump(self.uuid), dump(self.ptid), name, self.num_partitions, self.num_replicas))

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
        if self.num_partitions is not None and self.num_partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permentnly,
        # until the user explicitly requests a shutdown.
        while 1:
            self.operational = False
            # refuse any incoming connections for now
            if self.listening_conn is not None:
                self.listening_conn.close()
                self.listening_conn = None
            # look for the primary master
            self.connectToPrimaryMaster()
            assert self.master_conn is not None
            if self.uuid == INVALID_UUID:
                raise RuntimeError, 'No UUID supplied from the primary master'
            # Make a listening port when connected to the primary
            self.listening_conn = ListeningConnection(self.em, None, 
                    addr=self.server, connector_handler=self.connector_handler)
            try:
                while 1:
                    try:
                        # check my state
                        node = self.nm.getNodeByUUID(self.uuid)
                        if node is not None and node.getState() == HIDDEN_STATE:
                            self.wait()
                        self.verifyData()
                        self.doOperation()
                    except OperationFailure:
                        logging.error('operation stopped')
                        # XXX still we can receive answer packet here
                        # this must be handle in order not to fail
                        self.operational = False

            except PrimaryFailure:
                logging.error('primary master is down')

    def connectToPrimaryMaster(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""
        logging.info('connecting to a primary master node')

        # Reload a partition table from the database. This is necessary
        # when a previous primary master died while sending a partition
        # table, because the table might be incomplete.
        if self.pt is not None:
            self.loadPartitionTable()
            self.ptid = self.dm.getPTID()

        # bootstrap handler, only for outgoing connections
        handler = BootstrapEventHandler(self)
        em = self.em
        nm = self.nm

        # First of all, make sure that I have no connection.
        for conn in em.getConnectionList():
            if not isinstance(conn, ListeningConnection):
                conn.close()

        index = 0
        self.trying_master_node = None
        t = 0
        while 1:
            em.poll(1)
            if self.trying_master_node is None:
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
                                     addr = self.trying_master_node.getServer(),
                                     connector_handler = self.connector_handler)
                    t = time()
            elif self.primary_master_node is self.trying_master_node:
                # If I know which is a primary master node, check if
                # I have a connection to it already.
                for conn in em.getConnectionList():
                    if isinstance(conn, ClientConnection):
                        uuid = conn.getUUID()
                        if uuid is not None:
                            node = nm.getNodeByUUID(uuid)
                            if node is self.primary_master_node:
                                # Yes, I have.
                                conn.setHandler(VerificationEventHandler(self))
                                self.master_conn = conn
                                return

    def verifyData(self):
        """Verify data under the control by a primary master node.
        Connections from client nodes may not be accepted at this stage."""
        logging.info('verifying data')

        handler = VerificationEventHandler(self)
        em = self.em

        # Make sure that every connection has the verfication event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

        while not self.operational:
            em.poll(1)

        # ask node list
        self.master_conn.ask(protocol.askNodeInformation())        
        self.master_conn.ask(protocol.askPartitionTable(()))
        while not self.has_node_information or not self.has_partition_table:
            em.poll(1)

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        handler = OperationEventHandler(self)
        em = self.em
        nm = self.nm

        # Make sure that every connection has the verfication event handler.
        for conn in em.getConnectionList():
            conn.setHandler(handler)

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
        handler = HiddenEventHandler(self)
        for conn in self.em.getConnectionList():
            conn.setHandler(handler)

        node = self.nm.getNodeByUUID(self.uuid)
        while 1:
            self.em.poll(1)
            if node.getState() != HIDDEN_STATE:
                break

    def queueEvent(self, callable, *args, **kwargs):
        self.event_queue.append((callable, args, kwargs))

    def executeQueuedEvents(self):
        l = len(self.event_queue)
        p = self.event_queue.popleft
        for i in xrange(l):
            callable, args, kwargs = p()
            callable(*args, **kwargs)

    def getPartition(self, oid_or_tid):
        return unpack('!Q', oid_or_tid)[0] % self.num_partitions


    def shutdown(self):
        """Close all connections and exit"""
        for c in self.em.getConnectionList():
            if not c.isListeningConnection():
                c.close()
        sys.exit("Application has been asked to shut down")
