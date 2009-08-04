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

from neo.config import ConfigurationManager
from neo.node import NodeManager, MasterNode
from neo.event import EventManager
from neo.connection import ListeningConnection
from neo.exception import PrimaryFailure
from neo.admin.handler import MasterMonitoringEventHandler, AdminEventHandler, \
     MasterEventHandler, MasterRequestEventHandler
from neo.connector import getConnectorHandler
from neo.bootstrap import BootstrapManager
from neo.pt import PartitionTable
from neo import protocol

class Dispatcher:
    """Dispatcher use to redirect master request to handler"""

    def __init__(self):
        # associate conn/message_id to dispatch
        # message to connection
        self.message_table = {}

    def register(self, msg_id, conn, kw=None):
        self.message_table[msg_id] = conn, kw

    def pop(self, msg_id):
        return self.message_table.pop(msg_id)

    def registered(self, msg_id):
        return self.message_table.has_key(msg_id)


class Application(object):
    """The storage node application."""

    def __init__(self, filename, section, uuid=None):
        config = ConfigurationManager(filename, section)

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
        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = uuid
        self.primary_master_node = None
        self.ptid = None
        self.monitoring_handler = MasterMonitoringEventHandler(self)
        self.request_handler = MasterRequestEventHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.dispatcher = Dispatcher()
        self.cluster_state = None
        self.master_conn = None
        self.master_node = None

    def run(self):
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port.
        handler = AdminEventHandler(self)
        ListeningConnection(self.em, handler, addr = self.server,
                            connector_handler = self.connector_handler)

        # Connect to a primary master node, verify data, and
        # start the operation. This cycle will be executed permentnly,
        # until the user explicitly requests a shutdown.
        while 1:
            self.connectToPrimaryMaster()
            try:
                while 1:
                    self.em.poll(1)
            except PrimaryFailure:
                logging.error('primary master is down')


    def connectToPrimaryMaster(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""

        nm = self.nm
        nm.clear()
        for server in self.master_node_list:
            nm.add(MasterNode(server = server))

        # search, find, connect and identify to the primary master
        bootstrap = BootstrapManager(self, self.name, protocol.ADMIN_NODE_TYPE, 
                self.uuid, self.server)
        data = bootstrap.getPrimaryConnection(self.connector_handler)
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
        self.master_conn.ask(protocol.askNodeInformation())
        self.master_conn.ask(protocol.askPartitionTable([]))

    def sendPartitionTable(self, conn, min_offset, max_offset, uuid, msg_id):
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
                        if uuid is not None and cell.getUUID() != uuid:
                            continue
                        else:
                            row.append((cell.getUUID(), cell.getState()))
                except TypeError:
                    pass
                row_list.append((offset, row))
        except IndexError:
            p = protocol.protocolError('invalid partition table offset')
            conn.notify(p)
            return
        p = protocol.answerPartitionList(self.ptid, row_list)
        conn.answer(p, msg_id)
