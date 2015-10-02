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

from neo.lib import logging
from neo.lib.app import BaseApplication
from neo.lib.connection import ListeningConnection
from neo.lib.exception import PrimaryFailure
from .handler import AdminEventHandler, MasterEventHandler, \
    MasterRequestEventHandler
from neo.lib.bootstrap import BootstrapManager
from neo.lib.pt import PartitionTable
from neo.lib.protocol import ClusterStates, Errors, \
    NodeTypes, NodeStates, Packets
from neo.lib.debug import register as registerLiveDebugger

class Application(BaseApplication):
    """The storage node application."""

    def __init__(self, config):
        super(Application, self).__init__(
            config.getSSL(), config.getDynamicMasterList())
        for address in config.getMasters():
            self.nm.createMaster(address=address)

        self.name = config.getCluster()
        self.server = config.getBind()

        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = config.getUUID()
        self.request_handler = MasterRequestEventHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.cluster_state = None
        self.reset()
        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        super(Application, self).close()

    def reset(self):
        self.bootstrapped = False
        self.master_conn = None
        self.master_node = None

    def log(self):
        self.em.log()
        self.nm.log()
        if self.pt is not None:
            self.pt.log()

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

        # Make a listening port.
        handler = AdminEventHandler(self)
        self.listening_conn = ListeningConnection(self, handler, self.server)

        while self.cluster_state != ClusterStates.STOPPING:
            self.connectToPrimary()
            try:
                while True:
                    self.em.poll(1)
            except PrimaryFailure:
                logging.error('primary master is down')
        self.listening_conn.close()
        while not self.em.isIdle():
            self.em.poll(1)

    def connectToPrimary(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage.
        """
        self.cluster_state = None
        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name, NodeTypes.ADMIN,
                self.uuid, self.server)
        data = bootstrap.getPrimaryConnection()
        (node, conn, uuid, num_partitions, num_replicas) = data
        self.master_node = node
        self.master_conn = conn
        self.uuid = uuid

        if self.pt is None:
            self.pt = PartitionTable(num_partitions, num_replicas)
        elif self.pt.getPartitions() != num_partitions:
            # XXX: shouldn't we recover instead of raising ?
            raise RuntimeError('the number of partitions is inconsistent')
        elif self.pt.getReplicas() != num_replicas:
            # XXX: shouldn't we recover instead of raising ?
            raise RuntimeError('the number of replicas is inconsistent')

        # passive handler
        self.master_conn.setHandler(self.master_event_handler)
        self.master_conn.ask(Packets.AskClusterState())
        self.master_conn.ask(Packets.AskNodeInformation())
        self.master_conn.ask(Packets.AskPartitionTable())

    def sendPartitionTable(self, conn, min_offset, max_offset, uuid):
        # we have a pt
        self.pt.log()
        row_list = []
        if max_offset == 0:
            max_offset = self.pt.getPartitions()
        try:
            for offset in xrange(min_offset, max_offset):
                row = []
                try:
                    for cell in self.pt.getCellList(offset):
                        if uuid is None or cell.getUUID() == uuid:
                            row.append((cell.getUUID(), cell.getState()))
                except TypeError:
                    pass
                row_list.append((offset, row))
        except IndexError:
            conn.notify(Errors.ProtocolError('invalid partition table offset'))
        else:
            conn.answer(Packets.AnswerPartitionList(self.pt.getID(), row_list))
