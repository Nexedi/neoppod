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

import neo
import sys
from collections import deque

from neo.lib.protocol import NodeTypes, CellStates, Packets
from neo.lib.node import NodeManager
from neo.lib.event import EventManager
from neo.lib.connection import ListeningConnection
from neo.lib.exception import OperationFailure, PrimaryFailure
from neo.storage.handlers import identification, verification, initialization
from neo.storage.handlers import master, hidden
from neo.storage.replicator import Replicator
from neo.storage.database import buildDatabaseManager
from neo.storage.transactions import TransactionManager
from neo.storage.exception import AlreadyPendingError
from neo.lib.connector import getConnectorHandler
from neo.lib.pt import PartitionTable
from neo.lib.util import dump
from neo.lib.bootstrap import BootstrapManager

from neo.lib.debug import register as registerLiveDebugger

class Application(object):
    """The storage node application."""

    def __init__(self, config):
        # set the cluster name
        self.name = config.getCluster()

        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()
        self.tm = TransactionManager(self)
        self.dm = buildDatabaseManager(config.getAdapter(), config.getDatabase())

        # load master nodes
        master_addresses, connector_name = config.getMasters()
        self.connector_handler = getConnectorHandler(connector_name)
        for master_address in master_addresses :
            self.nm.createMaster(address=master_address)

        # set the bind address
        self.server = config.getBind()
        neo.lib.logging.debug('IP address is %s, port is %d', *(self.server))

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.replicator = Replicator(self)
        self.listening_conn = None
        self.master_conn = None
        self.master_node = None

        # operation related data
        self.event_queue = None
        self.event_queue_dict = None
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

    def close(self):
        self.listening_conn = None
        self.nm.close()
        self.em.close()
        try:
            self.dm.close()
        except AttributeError:
            pass
        del self.__dict__

    def _poll(self):
        self.em.poll(1)

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

        neo.lib.logging.info('Configuration loaded:')
        neo.lib.logging.info('UUID      : %s', dump(self.uuid))
        neo.lib.logging.info('PTID      : %s', dump(ptid))
        neo.lib.logging.info('Name      : %s', self.name)
        neo.lib.logging.info('Partitions: %s', num_partitions)
        neo.lib.logging.info('Replicas  : %s', num_replicas)

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
            neo.lib.logging.info('\nPre-mortem informations:')
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
        self.server = self.listening_conn.getAddress()

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
            # drop any client node
            for conn in self.em.getConnectionList():
                if conn not in (self.listening_conn, self.master_conn):
                    conn.close()
            # create/clear event queue
            self.event_queue = deque()
            self.event_queue_dict = dict()
            try:
                self.verifyData()
                self.initialize()
                self.doOperation()
                raise RuntimeError, 'should not reach here'
            except OperationFailure, msg:
                neo.lib.logging.error('operation stopped: %s', msg)
            except PrimaryFailure, msg:
                self.replicator.masterLost()
                neo.lib.logging.error('primary master is down: %s', msg)
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
        neo.lib.logging.info('I am %s', dump(uuid))
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
        neo.lib.logging.info('verifying data')

        handler = verification.VerificationHandler(self)
        self.master_conn.setHandler(handler)
        _poll = self._poll

        while not self.operational:
            _poll()

    def initialize(self):
        """ Retreive partition table and node informations from the primary """
        neo.lib.logging.debug('initializing...')
        _poll = self._poll
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
            _poll()
        self.ready = True
        self.replicator.populate()
        self.master_conn.notify(Packets.NotifyReady())

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        neo.lib.logging.info('doing operation')

        _poll = self._poll

        handler = master.MasterOperationHandler(self)
        self.master_conn.setHandler(handler)

        # Forget all unfinished data.
        self.dm.dropUnfinishedData()
        self.tm.reset()

        while True:
            _poll()
            if self.replicator.pending():
                # Call processDelayedTasks before act, so tasks added in the
                # act call are executed after one poll call, so that sent
                # packets are already on the network and delayed task
                # processing happens in parallel with the same task on the
                # other storage node.
                self.replicator.processDelayedTasks()
                self.replicator.act()

    def wait(self):
        # change handler
        neo.lib.logging.info("waiting in hidden state")
        _poll = self._poll
        handler = hidden.HiddenHandler(self)
        for conn in self.em.getConnectionList():
            conn.setHandler(handler)

        node = self.nm.getByUUID(self.uuid)
        while True:
            _poll()
            if not node.isHidden():
                break

    def queueEvent(self, some_callable, conn, args, key=None,
            raise_on_duplicate=True):
        msg_id = conn.getPeerId()
        event_queue_dict = self.event_queue_dict
        if raise_on_duplicate and key in event_queue_dict:
            raise AlreadyPendingError()
        else:
            self.event_queue.append((key, some_callable, msg_id, conn, args))
            if key is not None:
                try:
                    event_queue_dict[key] += 1
                except KeyError:
                    event_queue_dict[key] = 1

    def executeQueuedEvents(self):
        l = len(self.event_queue)
        p = self.event_queue.popleft
        event_queue_dict = self.event_queue_dict
        for _ in xrange(l):
            key, some_callable, msg_id, conn, args = p()
            if key is not None:
                event_queue_dict[key] -= 1
                if event_queue_dict[key] == 0:
                    del event_queue_dict[key]
            if conn.isAborted() or conn.isClosed():
                continue
            orig_msg_id = conn.getPeerId()
            conn.setPeerId(msg_id)
            some_callable(conn, *args)
            conn.setPeerId(orig_msg_id)

    def logQueuedEvents(self):
        if self.event_queue is None:
            return
        neo.lib.logging.info("Pending events:")
        for key, event, _msg_id, _conn, args in self.event_queue:
            neo.lib.logging.info('  %r:%r: %r:%r %r %r', key, event.__name__,
                _msg_id, _conn, args)

    def shutdown(self, erase=False):
        """Close all connections and exit"""
        for c in self.em.getConnectionList():
            try:
                c.close()
            except PrimaryFailure:
                pass
        # clear database to avoid polluting the cluster at restart
        self.dm.setup(reset=erase)
        neo.lib.logging.info("Application has been asked to shut down")
        sys.exit()
