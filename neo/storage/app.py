#
# Copyright (C) 2006-2019  Nexedi SA
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
from neo.lib.app import BaseApplication, buildOptionParser
from neo.lib.protocol import CellStates, ClusterStates, NodeTypes, Packets, \
    ZERO_TID
from neo.lib.connection import ListeningConnection
from neo.lib.exception import StoppedOperation, PrimaryFailure
from neo.lib.pt import PartitionTable
from neo.lib.util import add64, dump
from neo.lib.bootstrap import BootstrapManager
from .checker import Checker
from .database import buildDatabaseManager, DATABASE_MANAGERS
from .handlers import identification, initialization, master
from .replicator import Replicator
from .transactions import TransactionManager

from neo.lib.debug import register as registerLiveDebugger

option_defaults = {
  'adapter': 'MySQL',
  'wait': 0,
}
assert option_defaults['adapter'] in DATABASE_MANAGERS

@buildOptionParser
class Application(BaseApplication):
    """The storage node application."""

    checker = replicator = tm = None

    @classmethod
    def _buildOptionParser(cls):
        parser = cls.option_parser
        parser.description = "NEO Storage node"
        cls.addCommonServerOptions('storage', '127.0.0.1')

        _ = parser.group('storage')
        _('a', 'adapter', choices=DATABASE_MANAGERS,
            help="database adapter to use")
        _('d', 'database', required=True,
            help="database connections string")
        _.float('w', 'wait',
            help="seconds to wait for backend to be available,"
                 " before erroring-out (-1 = infinite)")
        _.bool('disable-pack',
            help="do not process any pack order")
        _.bool('disable-drop-partitions',
            help="do not delete data of discarded cells, which is useful for"
                 " big databases because the current implementation is"
                 " inefficient (this option should disappear in the future)")
        _.bool('new-nid',
            help="request a new NID from a cluster that is already"
                 " operational, update the database with the new NID and exit,"
                 " which makes easier to quickly set up a replica by copying"
                 " the database of another node while it was stopped")

        _ = parser.group('database creation')
        _.int('i', 'nid',
            help="specify an NID to use for this process. Previously"
                 " assigned NID takes precedence (i.e. you should"
                 " always use reset with this switch)")
        _('e', 'engine', help="database engine (MySQL only)")
        _.bool('dedup',
            help="enable deduplication of data"
                 " when setting up a new storage node")
        # TODO: Forbid using "reset" along with any unneeded argument.
        #       "reset" is too dangerous to let user a chance of accidentally
        #       letting it slip through in a long option list.
        #       It should even be forbidden in configuration files.
        _.bool('reset',
            help="remove an existing database if any, and exit")

        parser.set_defaults(**option_defaults)

    def __init__(self, config):
        super(Application, self).__init__(
            config.get('ssl_credentials'), config.get('dynamic_master_list'))
        # set the cluster name
        self.name = config['cluster']

        self.dm = buildDatabaseManager(config['adapter'],
            (config['database'], config.get('engine'), config['wait']),
        )
        self.disable_drop_partitions = config.get('disable_drop_partitions',
                                                  False)
        self.disable_pack = config.get('disable_pack', False)
        self.nm.createMasters(config['masters'])

        # set the bind address
        self.server = config['bind']
        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None

        self.listening_conn = None
        self.master_conn = None
        self.master_node = None

        # operation related data
        self.operational = False

        self.dm.setup(reset=config.get('reset', False),
                      dedup=config.get('dedup', False))
        self.loadConfiguration()
        self.devpath = self.dm.getTopologyPath()

        if config.get('new_nid'):
            self.new_nid = [x[0] for x in self.dm.iterAssignedCells()]
            if not self.new_nid:
                sys.exit('database is empty')
            self.uuid = None
        else:
            self.new_nid = ()
            if 'nid' in config: # for testing purpose only
                self.uuid = config['nid']
                logging.node(self.name, self.uuid)

        registerLiveDebugger(on_log=self.log)
        self.dm.lock.release()

    def close(self):
        self.listening_conn = None
        self.dm.close()
        super(Application, self).close()

    def _poll(self):
        self.em.poll(1)

    def log(self):
        super(Application, self).log()
        if self.tm:
            self.tm.log()

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
        logging.node(self.name, self.uuid)

        logging.info('Configuration loaded:')
        logging.info('PTID      : %s', dump(dm.getPTID()))
        logging.info('Name      : %s', self.name)

    def loadPartitionTable(self):
        """Load a partition table from the database."""
        ptid = self.dm.getPTID()
        if ptid is None:
            self.pt = PartitionTable(0, 0)
            return
        row_list = []
        for offset, uuid, state in self.dm.getPartitionTable():
            while len(row_list) <= offset:
                row_list.append([])
            # register unknown nodes
            if self.nm.getByUUID(uuid) is None:
                self.nm.createStorage(uuid=uuid)
            row_list[offset].append((uuid, CellStates[state]))
        self.pt = object.__new__(PartitionTable)
        self.pt.load(ptid, self.dm.getNumReplicas(), row_list, self.nm)

    def run(self):
        try:
            with self.em.wakeup_fd(), self.dm.lock:
                self._run()
        except Exception:
            logging.exception('Pre-mortem data:')
            self.log()
            logging.flush()
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        assert self.name

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
            self.completed_pack_id = self.last_pack_id = ZERO_TID
            self.checker = Checker(self)
            self.replicator = Replicator(self)
            self.tm = TransactionManager(self)
            try:
                self.initialize()
                self.doOperation()
                assert False
            except StoppedOperation as msg:
                logging.error('operation stopped: %s', msg)
            except PrimaryFailure as msg:
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
        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, NodeTypes.STORAGE,
                                     None if self.new_nid else self.server,
                                     devpath=self.devpath, new_nid=self.new_nid)
        self.master_node, self.master_conn = bootstrap.getPrimaryConnection()
        self.dm.setUUID(self.uuid)

        # Reload a partition table from the database,
        # in case that we're in RECOVERING phase.
        self.loadPartitionTable()

    def initialize(self):
        logging.debug('initializing...')
        _poll = self._poll
        self.master_conn.setHandler(initialization.InitializationHandler(self))
        while not self.operational:
            _poll()
        self.master_conn.send(Packets.NotifyReady())

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
            with self.dm.operational(self):
                with self.dm.lock:
                    self.maybePack()
                while True:
                    if task_queue and isIdle():
                        with self.dm.lock:
                            while True:
                                try:
                                    next(task_queue[-1]) or task_queue.rotate()
                                except StopIteration:
                                    task_queue.pop()
                                    if not task_queue:
                                        break
                                else:
                                    _poll(0)
                                if not isIdle():
                                    break
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

    def notifyPackCompleted(self):
        if self.disable_pack:
            pack_id = self.last_pack_id
        else:
            packed = self.dm.getPackedIDs()
            if not packed:
                return
            pack_id = min(packed.itervalues())
        if self.completed_pack_id != pack_id:
            self.completed_pack_id = pack_id
            self.master_conn.send(Packets.NotifyPackCompleted(pack_id))

    def maybePack(self, info=None, min_id=None):
        ready = self.dm.isReadyToStartPack()
        if ready:
            packed_dict = self.dm.getPackedIDs(True)
            if packed_dict:
                packed = min(packed_dict.itervalues())
                if packed < self.last_pack_id:
                    if packed == ready[1]:
                        # Last completed pack for this storage node hasn't
                        # changed since the last call to dm.pack() so simply
                        # resume. No info needed.
                        pack_id = ready[0]
                        assert not info, (ready, info, min_id)
                    elif packed == min_id:
                        # New pack order to process and we've just received
                        # all needed information to start right now.
                        pack_id = info[0]
                    else:
                        # Time to process the next approved pack after 'packed'.
                        # We don't even know its id. Ask the master more
                        # information.
                        self.master_conn.ask(
                            Packets.AskPackOrders(add64(packed, 1)),
                            pack_id=packed)
                        return
                    self.dm.pack(self, info, packed,
                        self.replicator.filterPackable(pack_id,
                            (k for k, v in packed_dict.iteritems()
                                if v == packed)))
                else:
                    # All approved pack orders are processed.
                    self.dm.pack(self, None, None, ()) # for cleanup
            else:
                assert not self.pt.getReadableOffsetList(self.uuid)
