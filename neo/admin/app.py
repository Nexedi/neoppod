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

import logging
from time import time

from neo.config import ConfigurationManager
from neo.protocol import INVALID_UUID, INVALID_PTID, MASTER_NODE_TYPE
from neo.node import NodeManager, MasterNode
from neo.event import EventManager
from neo.connection import ListeningConnection, ClientConnection
from neo.exception import PrimaryFailure
from neo.admin.handler import MasterMonitoringEventHandler, AdminEventHandler, \
     MasterBootstrapEventHandler, MasterRequestEventHandler
from neo.connector import getConnectorHandler
from neo import protocol

class Dispatcher:
    """Dispatcher use to redirect master request to handler"""

    def __init__(self):
        # associate conn/message_id to dispatch
        # message to connection
        self.message_table = {}

    def register(self, msg_id, conn, kw=None):
        self.message_table[msg_id] = conn, kw

    def retrieve(self, msg_id):
        return self.message_table.pop(msg_id, None)

    def registered(self, msg_id):
        return self.message_table.has_key(msg_id)


class Application(object):
    """The storage node application."""

    def __init__(self, file, section):
        config = ConfigurationManager(file, section)

        self.num_partitions = None
        self.num_replicas = None
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
        self.uuid = INVALID_UUID
        self.primary_master_node = None
        self.ptid = INVALID_PTID
        self.monitoring_handler = MasterMonitoringEventHandler(self)
        self.request_handler = MasterRequestEventHandler(self)
        self.dispatcher = Dispatcher()
        self.cluster_state = None

    def run(self):
        """Make sure that the status is sane and start a loop."""
        if self.num_partitions is not None and self.num_partitions <= 0:
            raise RuntimeError, 'partitions must be more than zero'
        if len(self.name) == 0:
            raise RuntimeError, 'cluster name must be non-empty'

        for server in self.master_node_list:
            self.nm.add(MasterNode(server = server))

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
                # do not trust any longer our informations
                self.pt.clear()
                self.nm.clear(filter = lambda node: node.getNodeType() != MASTER_NODE_TYPE)


    def connectToPrimaryMaster(self):
        """Find a primary master node, and connect to it.

        If a primary master node is not elected or ready, repeat
        the attempt of a connection periodically.

        Note that I do not accept any connection from non-master nodes
        at this stage."""
        logging.info('connecting to a primary master node')

        handler = MasterBootstrapEventHandler(self)
        em = self.em
        nm = self.nm

        # First of all, make sure that I have no connection.
        for conn in em.getConnectionList():
            if not conn.isListeningConnection():
                conn.close()

        index = 0
        self.trying_master_node = None
        self.primary_master_node = None
        self.master_conn = None
        t = 0
        while 1:
            em.poll(1)
            if self.primary_master_node is not None:
                # If I know which is a primary master node, check if
                # I have a connection to it already.
                for conn in em.getConnectionList():
                    if not conn.isListeningConnection() and not conn.isServerConnection():
                        uuid = conn.getUUID()
                        if uuid is not None:
                            node = nm.getNodeByUUID(uuid)
                            if node is self.primary_master_node:
                                logging.info("connected to primary master node %s:%d" % node.getServer())
                                self.master_conn = conn
                                # Yes, I have.
                                return

            if self.trying_master_node is None and t + 1 < time():
                # Choose a master node to connect to.
                if self.primary_master_node is not None:
                    # If I know a primary master node, pinpoint it.
                    self.trying_master_node = self.primary_master_node
                else:
                    # Otherwise, check one by one.
                    master_list = nm.getMasterNodeList()
                    try:
                        self.trying_master_node = master_list[index]
                    except IndexError:
                        index = 0
                        self.trying_master_node = master_list[0]
                    index += 1
                print "connecting to %s:%d" % self.trying_master_node.getServer()
                ClientConnection(em, handler, \
                                 addr = self.trying_master_node.getServer(),
                                 connector_handler = self.connector_handler)
                t = time()

    def sendPartitionTable(self, conn, min_offset, max_offset, uuid, msg_id):
        # we have a pt
        self.pt.log()
        row_list = []
        if max_offset == 0:
            max_offset = self.num_partitions
        try:
            for offset in xrange(min_offset, max_offset):
                row = []
                try:
                    for cell in self.pt.getCellList(offset):
                        if uuid != INVALID_UUID and cell.getUUID() != uuid:
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
        conn.notify(p, msg_id)
