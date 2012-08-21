#
# Copyright (C) 2006-2012  Nexedi SA
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
from neo.lib.node import NodeManager
from neo.lib.event import EventManager
from neo.lib.connection import ListeningConnection
from neo.lib.exception import PrimaryFailure
from .handler import AdminEventHandler, MasterEventHandler, \
    MasterRequestEventHandler
from neo.lib.connector import getConnectorHandler
from neo.lib.bootstrap import BootstrapManager
from neo.lib.pt import PartitionTable
from neo.lib.protocol import ClusterStates, Errors, \
    NodeTypes, NodeStates, Packets
from neo.lib.debug import register as registerLiveDebugger

class Application(object):
    """The storage node application."""

    def __init__(self, config):
        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager(config.getDynamicMasterList())

        self.name = config.getCluster()
        self.server = config.getBind()

        self.master_addresses, connector_name = config.getMasters()
        self.connector_handler = getConnectorHandler(connector_name)
        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = config.getUUID()
        self.primary_master_node = None
        self.request_handler = MasterRequestEventHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.cluster_state = None
        self.reset()
        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        self.nm.close()
        self.em.close()
        del self.__dict__

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
            raise

    def _run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port.
        handler = AdminEventHandler(self)
        self.listening_conn = ListeningConnection(self.em, handler,
            addr=self.server, connector=self.connector_handler())

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
        at this stage."""

        nm = self.nm
        nm.init()
        self.cluster_state = None

        for address in self.master_addresses:
            self.nm.createMaster(address=address)

        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name, NodeTypes.ADMIN,
                self.uuid, self.server)
        data = bootstrap.getPrimaryConnection(self.connector_handler)
        (node, conn, uuid, num_partitions, num_replicas) = data
        nm.update([(node.getType(), node.getAddress(), node.getUUID(),
                    NodeStates.RUNNING)])
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
