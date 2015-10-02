#
# Copyright (C) 2006-2015  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
from collections import deque

from neo.lib import logging
from neo.lib.app import BaseApplication
from neo.lib.protocol import uuid_str, \
    CellStates, ClusterStates, NodeTypes, Packets
from neo.lib.node import NodeManager
from neo.lib.connection import ListeningConnection
from neo.lib.exception import OperationFailure, PrimaryFailure
from neo.lib.pt import PartitionTable
from neo.lib.util import dump
from neo.lib.bootstrap import BootstrapManager
from .checker import Checker
from .database import buildDatabaseManager
from .exception import AlreadyPendingError
from .handlers import identification, verification, initialization
from .handlers import master, hidden
from .replicator import Replicator
from .transactions import TransactionManager

from neo.lib.debug import register as registerLiveDebugger

class Application(BaseApplication):
    """The storage node application."""

    def __init__(self, config):
        super(Application, self).__init__(
            config.getSSL(), config.getDynamicMasterList())
        # set the cluster name
        self.name = config.getCluster()

        self.tm = TransactionManager(self)
        self.dm = buildDatabaseManager(config.getAdapter(),
            (config.getDatabase(), config.getEngine(), config.getWait()),
        )

        # load master nodes
        for master_address in config.getMasters():
            self.nm.createMaster(address=master_address)

        # set the bind address
        self.server = config.getBind()
        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.checker = Checker(self)
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

        self.dm.setup(reset=config.getReset())
        self.loadConfiguration()

        # force node uuid from command line argument, for testing purpose only
        if config.getUUID() is not None:
            self.uuid = config.getUUID()

        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        self.dm.close()
        super(Application, self).close()

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

        dm = self.dm

        # check cluster name
        name = dm.getName()
        if name is None:
            dm.setName(self.name)
        elif name != self.name:
            raise RuntimeError('name %r does not match with the database: %r'
                               % (self.name, name))

        # load configuration
        self.uuid = dm.getUUID()
        num_partitions = dm.getNumPartitions()
        num_replicas = dm.getNumReplicas()
        ptid = dm.getPTID()

        # check partition table configuration
        if num_partitions is not None and num_replicas is not None:
            if num_partitions <= 0:
                raise RuntimeError, 'partitions must be more than zero'
            # create a partition table
            self.pt = PartitionTable(num_partitions, num_replicas)

        logging.info('Configuration loaded:')
        logging.info('UUID      : %s', uuid_str(self.uuid))
        logging.info('PTID      : %s', dump(ptid))
        logging.info('Name      : %s', self.name)
        logging.info('Partitions: %s', num_partitions)
        logging.info('Replicas  : %s', num_replicas)

    def loadPartitionTable(self):
        """Load a partition table from the database."""
        ptid = self.dm.getPTID()
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
        except Exception:
            logging.exception('Pre-mortem data:')
            self.log()
            logging.flush()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port
        handler = identification.IdentificationHandler(self)
        self.listening_conn = ListeningConnection(self, handler, self.server)
        self.server = self.listening_conn.getAddress()

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permanently,
        # until the user explicitly requests a shutdown.
        while True:
            self.cluster_state = None
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
            self.event_queue_dict = {}
            try:
                self.verifyData()
                self.initialize()
                self.doOperation()
                raise RuntimeError, 'should not reach here'
            except OperationFailure, msg:
                logging.error('operation stopped: %s', msg)
                if self.cluster_state == ClusterStates.STOPPING_BACKUP:
                    self.dm.setBackupTID(None)
            except PrimaryFailure, msg:
                logging.error('primary master is down: %s', msg)
            finally:
                self.checker = Checker(self)

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
        data = bootstrap.getPrimaryConnection()
        (node, conn, uuid, num_partitions, num_replicas) = data
        self.master_node = node
        self.master_conn = conn
        logging.info('I am %s', uuid_str(uuid))
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
        _poll = self._poll

        while not self.operational:
            _poll()

    def initialize(self):
        """ Retreive partition table and node informations from the primary """
        logging.debug('initializing...')
        _poll = self._poll
        handler = initialization.InitializationHandler(self)
        self.master_conn.setHandler(handler)

        # ask node list and partition table
        self.pt.clear()
        self.master_conn.ask(Packets.AskNodeInformation())
        self.master_conn.ask(Packets.AskPartitionTable())
        while self.master_conn.isPending():
            _poll()
        self.ready = True
        self.replicator.populate()
        self.master_conn.notify(Packets.NotifyReady())

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        poll = self._poll
        _poll = self.em._poll
        isIdle = self.em.isIdle

        handler = master.MasterOperationHandler(self)
        self.master_conn.setHandler(handler)

        # Forget all unfinished data.
        self.dm.dropUnfinishedData()
        self.tm.reset()

        self.task_queue = task_queue = deque()
        try:
            self.dm.doOperation(self)
            while True:
                while task_queue:
                    try:
                        while isIdle():
                            if task_queue[-1].next():
                                _poll(0)
                            task_queue.rotate()
                        break
                    except StopIteration:
                        task_queue.pop()
                poll()
        finally:
            del self.task_queue
            # XXX: Although no handled exception should happen between
            #      replicator.populate() and the beginning of this 'try'
            #      clause, the replicator should be reset in a safer place.
            self.replicator = Replicator(self)
            # Abort any replication, whether we are feeding or out-of-date.
            for node in self.nm.getStorageList(only_identified=True):
                node.getConnection().close()

    def changeClusterState(self, state):
        self.cluster_state = state
        if state == ClusterStates.STOPPING_BACKUP:
            self.replicator.stop()

    def wait(self):
        # change handler
        logging.info("waiting in hidden state")
        _poll = self._poll
        handler = hidden.HiddenHandler(self)
        for conn in self.em.getConnectionList():
            conn.setHandler(handler)

        node = self.nm.getByUUID(self.uuid)
        while True:
            _poll()
            if not node.isHidden():
                break

    def queueEvent(self, some_callable, conn=None, args=(), key=None,
            raise_on_duplicate=True):
        event_queue_dict = self.event_queue_dict
        n = event_queue_dict.get(key)
        if n and raise_on_duplicate:
            raise AlreadyPendingError()
        msg_id = None if conn is None else conn.getPeerId()
        self.event_queue.append((key, some_callable, msg_id, conn, args))
        if key is not None:
            event_queue_dict[key] = n + 1 if n else 1

    def executeQueuedEvents(self):
        p = self.event_queue.popleft
        event_queue_dict = self.event_queue_dict
        for _ in xrange(len(self.event_queue)):
            key, some_callable, msg_id, conn, args = p()
            if key is not None:
                n = event_queue_dict[key] - 1
                if n:
                    event_queue_dict[key] = n
                else:
                    del event_queue_dict[key]
            if conn is None:
                some_callable(*args)
            elif not conn.isClosed():
                orig_msg_id = conn.getPeerId()
                try:
                    conn.setPeerId(msg_id)
                    some_callable(conn, *args)
                finally:
                    conn.setPeerId(orig_msg_id)

    def logQueuedEvents(self):
        if self.event_queue is None:
            return
        logging.info("Pending events:")
        for key, event, _msg_id, _conn, args in self.event_queue:
            logging.info('  %r:%r: %r:%r %r %r', key, event.__name__,
                _msg_id, _conn, args)

    def newTask(self, iterator):
        try:
            iterator.next()
        except StopIteration:
            return
        self.task_queue.appendleft(iterator)

    def closeClient(self, connection):
        if connection is not self.replicator.getCurrentConnection() and \
           connection not in self.checker.conn_dict:
            connection.closeClient()

    def shutdown(self, erase=False):
        """Close all connections and exit"""
        for c in self.em.getConnectionList():
            try:
                c.close()
            except PrimaryFailure:
                pass
        # clear database to avoid polluting the cluster at restart
        if erase:
            self.dm.erase()
        logging.info("Application has been asked to shut down")
        sys.exit()
