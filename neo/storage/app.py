#
# Copyright (C) 2006-2017  Nexedi SA
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
from neo.lib.connection import ListeningConnection
from neo.lib.exception import StoppedOperation, PrimaryFailure
from neo.lib.pt import PartitionTable
from neo.lib.util import dump
from neo.lib.bootstrap import BootstrapManager
from .checker import Checker
from .database import buildDatabaseManager
from .handlers import identification, initialization, master
from .replicator import Replicator
from .transactions import TransactionManager

from neo.lib.debug import register as registerLiveDebugger

class Application(BaseApplication):
    """The storage node application."""

    checker = replicator = tm = None

    def __init__(self, config):
        super(Application, self).__init__(
            config.getSSL(), config.getDynamicMasterList())
        # set the cluster name
        self.name = config.getCluster()

        self.dm = buildDatabaseManager(config.getAdapter(),
            (config.getDatabase(), config.getEngine(), config.getWait()),
        )
        self.disable_drop_partitions = config.getDisableDropPartitions()

        # load master nodes
        for master_address in config.getMasters():
            self.nm.createMaster(address=master_address)

        # set the bind address
        self.server = config.getBind()
        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.listening_conn = None
        self.master_conn = None
        self.master_node = None

        # operation related data
        self.operational = False

        self.dm.setup(reset=config.getReset(), dedup=config.getDedup())
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
        self.nm.log()
        if self.tm:
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
        self.pt.clear()
        ptid = self.dm.getPTID()
        if ptid is None:
            return
        cell_list = []
        for offset, uuid, state in self.dm.getPartitionTable():
            # register unknown nodes
            if self.nm.getByUUID(uuid) is None:
                self.nm.createStorage(uuid=uuid)
            cell_list.append((offset, uuid, CellStates[state]))
        self.pt.update(ptid, cell_list, self.nm)

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
        self.operational = False
        while True:
            self.cluster_state = None
            if self.master_node is None:
                # look for the primary master
                self.connectToPrimary()
            self.checker = Checker(self)
            self.replicator = Replicator(self)
            self.tm = TransactionManager(self)
            try:
                self.initialize()
                self.doOperation()
                raise RuntimeError, 'should not reach here'
            except StoppedOperation, msg:
                logging.error('operation stopped: %s', msg)
            except PrimaryFailure, msg:
                logging.error('primary master is down: %s', msg)
            finally:
                self.operational = False
            # When not ready, we reject any incoming connection so for
            # consistency, we also close any connection except that to the
            # master. This includes connections to other storage nodes and any
            # replication is aborted, whether we are feeding or out-of-date.
            for conn in self.em.getConnectionList():
                if conn not in (self.listening_conn, self.master_conn):
                    conn.close()
            del self.checker, self.replicator, self.tm

    def connectToPrimary(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""
        pt = self.pt

        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, NodeTypes.STORAGE, self.server)
        self.master_node, self.master_conn, num_partitions, num_replicas = \
            bootstrap.getPrimaryConnection()
        uuid = self.uuid
        logging.info('I am %s', uuid_str(uuid))
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

    def initialize(self):
        logging.debug('initializing...')
        _poll = self._poll
        self.master_conn.setHandler(initialization.InitializationHandler(self))
        while not self.operational:
            _poll()
        #self.master_conn.send(Packets.NotifyReady())
        #self.master_conn.answer(Packets.NotifyReady())  # NOTE to keep same conn as was used when M send StartOperation
        # XXX ^^^ answer check pkt.isResponse()
        self.master_conn.send(Packets.NotifyReady(), msg_id=self.master_conn.peer_id)

    def doOperation(self):
        """Handle everything, including replications and transactions."""
        logging.info('doing operation')

        poll = self._poll
        _poll = self.em._poll
        isIdle = self.em.isIdle

        self.master_conn.setHandler(master.MasterOperationHandler(self))
        self.replicator.populate()

        # Forget all unfinished data.
        self.dm.dropUnfinishedData()

        self.task_queue = task_queue = deque()
        try:
            self.dm.doOperation(self)
            while True:
                while task_queue:
                    try:
                        # NOTE backup/importer processed under isIdle
                        while isIdle():
                            next(task_queue[-1]) or task_queue.rotate()
                            _poll(0)
                        break
                    except StopIteration:
                        task_queue.pop()
                poll()
        finally:
            del self.task_queue

    def changeClusterState(self, state):
        self.cluster_state = state
        if state == ClusterStates.STOPPING_BACKUP:
            self.replicator.stop()

    def newTask(self, iterator):
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
