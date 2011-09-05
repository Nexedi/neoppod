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

import neo.lib

from neo.lib.node import NodeManager
from neo.lib.event import EventManager
from neo.lib.connection import ListeningConnection
from neo.lib.exception import PrimaryFailure
from neo.admin.handler import AdminEventHandler, MasterEventHandler, \
    MasterRequestEventHandler
from neo.lib.connector import getConnectorHandler
from neo.lib.bootstrap import BootstrapManager
from neo.lib.pt import PartitionTable
from neo.lib.protocol import NodeTypes, NodeStates, Packets, Errors
from neo.lib.debug import register as registerLiveDebugger

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

    def clear(self):
        """
            Unregister packet expected for a given connection
        """
        self.message_table.clear()

class Application(object):
    """The storage node application."""

    def __init__(self, config):
        # Internal attributes.
        self.em = EventManager()
        self.nm = NodeManager()

        self.name = config.getCluster()
        self.server = config.getBind()

        self.master_addresses, connector_name = config.getMasters()
        self.connector_handler = getConnectorHandler(connector_name)
        neo.lib.logging.debug('IP address is %s, port is %d', *(self.server))

        # The partition table is initialized after getting the number of
        # partitions.
        self.pt = None
        self.uuid = config.getUUID()
        self.primary_master_node = None
        self.request_handler = MasterRequestEventHandler(self)
        self.master_event_handler = MasterEventHandler(self)
        self.dispatcher = Dispatcher()
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
        """Make sure that the status is sane and start a loop."""
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        # Make a listening port.
        handler = AdminEventHandler(self)
        self.listening_conn = ListeningConnection(self.em, handler,
            addr=self.server, connector=self.connector_handler())

        while True:
            self.connectToPrimary()
            try:
                while True:
                    self.em.poll(1)
            except PrimaryFailure:
                neo.lib.logging.error('primary master is down')


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
                        if uuid is not None and cell.getUUID() != uuid:
                            continue
                        else:
                            row.append((cell.getUUID(), cell.getState()))
                except TypeError:
                    pass
                row_list.append((offset, row))
        except IndexError:
            p = Errors.ProtocolError('invalid partition table offset')
            conn.notify(p)
            return
        p = Packets.AnswerPartitionList(self.pt.getID(), row_list)
        conn.answer(p)
