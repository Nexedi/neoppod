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

from neo.lib import logging
from neo.lib.app import BaseApplication, buildOptionParser
from neo.lib.connection import ListeningConnection
from neo.lib.exception import PrimaryFailure
from .handler import AdminEventHandler, MasterEventHandler, \
    MasterRequestEventHandler
from neo.lib.bootstrap import BootstrapManager
from neo.lib.protocol import ClusterStates, Errors, NodeTypes, Packets
from neo.lib.debug import register as registerLiveDebugger

@buildOptionParser
class Application(BaseApplication):
    """The storage node application."""

    @classmethod
    def _buildOptionParser(cls):
        _ = cls.option_parser
        _.description = "NEO Admin node"
        cls.addCommonServerOptions('admin', '127.0.0.1:9999')

        _ = _.group('admin')
        _.int('i', 'nid',
            help="specify an NID to use for this process (testing purpose)")

    def __init__(self, config):
        super(Application, self).__init__(
            config.get('ssl'), config.get('dynamic_master_list'))
        for address in config['masters']:
            self.nm.createMaster(address=address)

        self.name = config['cluster']
        self.server = config['bind']

        logging.debug('IP address is %s, port is %d', *self.server)

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = config.get('nid')
        logging.node(self.name, self.uuid)
        self.request_handler = MasterRequestEventHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.cluster_state = None
        self.reset()
        registerLiveDebugger(on_log=self.log)

    def close(self):
        self.listening_conn = None
        super(Application, self).close()

    def reset(self):
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
        bootstrap = BootstrapManager(self, NodeTypes.ADMIN, self.server)
        self.master_node, self.master_conn = bootstrap.getPrimaryConnection()

        # passive handler
        self.master_conn.setHandler(self.master_event_handler)
        self.master_conn.ask(Packets.AskClusterState())

    def sendPartitionTable(self, conn, min_offset, max_offset, uuid):
        pt = self.pt
        if max_offset == 0:
            max_offset = pt.getPartitions()
        try:
            row_list = map(pt.getRow, xrange(min_offset, max_offset))
        except IndexError:
            conn.send(Errors.ProtocolError('invalid partition table offset'))
        else:
            conn.answer(Packets.AnswerPartitionList(
                pt.getID(), pt.getReplicas(), row_list))
