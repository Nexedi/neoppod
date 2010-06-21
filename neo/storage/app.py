#
# Copyright (C) 2006-2010  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging
import sys
from collections import deque

from neo.protocol import NodeTypes, CellStates, Packets
from neo.node import NodeManager
from neo.event import EventManager
from neo.connection import ListeningConnection
from neo.exception import OperationFailure, PrimaryFailure
from neo.storage.handlers import identification, verification, initialization
from neo.storage.handlers import master, hidden
from neo.storage.replicator import Replicator
from neo.storage.database import buildDatabaseManager
from neo.storage.transactions import TransactionManager
from neo.connector import getConnectorHandler
from neo.pt import PartitionTable
from neo.util import dump
from neo.bootstrap import BootstrapManager

from neo.live_debug import register as registerLiveDebugger

class Application(object):
    """The storage node application."""

    def __init__(self, config):
        # always use default connector for now
        self.connector_handler = getConnectorHandler()

        # set the cluster name
        self.name = config.getCluster()

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.tm = TransactionManager(self)
        self.dm = buildDatabaseManager(config.getAdapter(), config.getDatabase())

        # load master nodes
        for address in config.getMasters():
            self.nm.createMaster(address=address)

        # set the bind address
        self.server = config.getBind()
        logging.debug('IP address is %s, port is %d', *(self.server))

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.replicator = None
        self.listening_conn = None
        self.master_conn = None
        self.master_node = None

        # operation related data
        self.event_queue = None
        self.operational = False

        # ready is True when operational and got all informations
        self.ready = False
        self.has_node_information = False
        self.has_partition_table = False

        self.dm.setup(reset=config.getReset())
        self.loadConfiguration()

        # force node uuid from command line argument, for testing purpose only
        if config.getUUID() is not None:
            self.uuid = config.getUUID()

        registerLiveDebugger(on_log=self.log)

    def log(self):
        self.em.log()
        self.logQueuedEvents()
        self.nm.log()
        self.tm.log()
        if self.pt is not None:
            self.pt.log()

    def loadConfiguration(self):
        """Load persistent configuration data from the database.
        If data is not present, generate it."""

        def NoneOnKeyError(getter):
            try:
                return getter()
            except KeyError:
                return None
        dm = self.dm

        # check cluster name
        try:
            if dm.getName() != self.name:
                raise RuntimeError('name does not match with the database')
        except KeyError:
            dm.setName(self.name)

        # load configuration
        self.uuid = NoneOnKeyError(dm.getUUID)
        num_partitions = NoneOnKeyError(dm.getNumPartitions)
        num_replicas = NoneOnKeyError(dm.getNumReplicas)
        ptid = NoneOnKeyError(dm.getPTID)

        # check partition table configuration
        if num_partitions is not None and num_replicas is not None:
            if num_partitions <= 0:
                raise RuntimeError, 'partitions must be more than zero'
            # create a partition table
            self.pt = PartitionTable(num_partitions, num_replicas)

        logging.info('Configuration loaded:')
        logging.info('UUID      : %s', dump(self.uuid))
        logging.info('PTID      : %s', dump(ptid))
        logging.info('Name      : %s', self.name)
        logging.info('Partitions: %s', num_partitions)
        logging.info('Replicas  : %s', num_replicas)

    def loadPartitionTable(self):
        """Load a partition table from the database."""
        try:
            ptid = self.dm.getPTID()
        except KeyError:
            ptid = None
        cell_list = self.dm.getPartitionTable()
        new_cell_list = []
        for offset, uuid, state in cell_list:
            # convert from int to Enum
            state = CellStates[state]
            # register unknown nodes
            if self.nm.getByUUID(uuid) is None:
                self.nm.createStorage(uuid=uuid)
            new_cell_list.append((offset, uuid, state))
        # load the partition table in manager
        self.pt.clear()
        self.pt.update(ptid, new_cell_list, self.nm)

    def run(self):
        try:
            self._run()
        except:
            logging.info('\nPre-mortem informations:')
            self.log()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port
        handler = identification.IdentificationHandler(self)
        self.listening_conn = ListeningConnection(self.em, handler,
            addr=self.server, connector=self.connector_handler())

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permanently,
        # until the user explicitly requests a shutdown.
        while True:
            self.ready = False
            self.operational = False
            if self.master_node is None:
                # look for the primary master
                self.connectToPrimary()
            # check my state
            node = self.nm.getByUUID(self.uuid)
            if node is not None and node.isHidden():
                self.wait()
            # drop any client node and clear event queue
            for node in self.nm.getClientList(only_identified=True):
                node.getConnection().close()
            self.event_queue = deque()
            try:
                self.verifyData()
                self.initialize()
                self.doOperation()
                raise RuntimeError, 'should not reach here'
            except OperationFailure, msg:
                logging.error('operation stopped: %s', msg)
            except PrimaryFailure, msg:
                logging.error('primary master is down: %s', msg)
                self.master_node = None

    def connectToPrimary(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""
        pt = self.pt

        # First of all, make sure that I have no connection.
        for conn in self.em.getConnectionList():
            if not conn.isListening():
                conn.close()

        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name,
                NodeTypes.STORAGE, self.uuid, self.server)
        data = bootstrap.getPrimaryConnection(self.connector_handler)
        (node, conn, uuid, num_partitions, num_replicas) = data
        self.master_node = node
        self.master_conn = conn
        logging.info('I am %s', dump(uuid))
        self.uuid = uuid
        self.dm.setUUID(uuid)

        # Reload a partition table from the database. This is necessary
        # when a previous primary master died while sending a partition
        # table, because the table might be incomplete.
        if pt is not None:
            self.loadPartitionTable()
            if num_partitions != pt.getPartitions():
                raise RuntimeError('the number of partitions is inconsistent')

        if pt is None or pt.getReplicas() != num_replicas:
            # changing number of replicas is not an issue
            self.dm.setNumPartitions(num_partitions)
            self.dm.setNumReplicas(num_replicas)
            self.pt = PartitionTable(num_partitions, num_replicas)
            self.loadPartitionTable()

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
        self.has_last_ids = False
        self.pt.clear()
        self.master_conn.ask(Packets.AskLastIDs())
        self.master_conn.ask(Packets.AskNodeInformation())
        self.master_conn.ask(Packets.AskPartitionTable())
        while not self.has_node_information or not self.has_partition_table \
                or not self.has_last_ids:
            self.em.poll(1)
        self.ready = True

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        em = self.em

        handler = master.MasterOperationHandler(self)
        self.master_conn.setHandler(handler)

        # Forget all unfinished data.
        self.dm.dropUnfinishedData()
        self.tm.reset()

        # The replicator.
        self.replicator = Replicator(self)

        while True:
            em.poll(1)
            if self.replicator.pending():
                self.replicator.act()

    def wait(self):
        # change handler
        logging.info("waiting in hidden state")
        handler = hidden.HiddenHandler(self)
        for conn in self.em.getConnectionList():
            conn.setHandler(handler)

        node = self.nm.getByUUID(self.uuid)
        while True:
            self.em.poll(1)
            if not node.isHidden():
                break

    def queueEvent(self, some_callable, conn, *args, **kwargs):
        msg_id = conn.getPeerId()
        self.event_queue.append((some_callable, msg_id, conn, args, kwargs))

    def executeQueuedEvents(self):
        l = len(self.event_queue)
        p = self.event_queue.popleft
        for _ in xrange(l):
            some_callable, msg_id, conn, args, kwargs = p()
            if conn.isAborted() or conn.isClosed():
                continue
            conn.setPeerId(msg_id)
            some_callable(conn, *args, **kwargs)

    def logQueuedEvents(self):
        if self.event_queue is None:
            return
        logging.info("Pending events:")
        for event, _msg_id, _conn, args, _kwargs in self.event_queue:
            oid, serial, _compression, _checksum, data, tid, time = args
            logging.info('  %r: %r:%r by %r -> %r (%r)', event.__name__, dump(oid),
                    dump(serial), dump(tid), data, time)

    def shutdown(self, erase=False):
        """Close all connections and exit"""
        for c in self.em.getConnectionList():
            if not c.isListening():
                c.close()
        # clear database to avoid polluting the cluster at restart
        self.dm.setup(reset=True)
        sys.exit("Application has been asked to shut down")
