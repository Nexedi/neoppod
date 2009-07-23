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
from time import sleep

from neo.handler import EventHandler
from neo.node import MasterNode
from neo import protocol
from neo.util import dump
from neo.connection import ClientConnection

NO_SERVER = ('0.0.0.0', 0)

class BootstrapManager(EventHandler):
    """ 
    Manage the bootstrap stage, lookup for the primary master then connect to it
    """

    def __init__(self, app, name, node_type, uuid=None, server=NO_SERVER):
        EventHandler.__init__(self, app)
        self.primary = None
        self.server = server
        self.node_type = node_type
        self.uuid = uuid
        self.name = name
        self.num_replicas = None
        self.num_partitions = None

    def connectionCompleted(self, conn):
        EventHandler.connectionCompleted(self, conn)
        conn.ask(protocol.askPrimaryMaster())

    def connectionFailed(self, conn):
        EventHandler.connectionFailed(self, conn)
        self.current = None

    def connectionClosed(self, conn):
        EventHandler.connectionClosed(self, conn)
        self.current = None

    def timeoutExpired(self, conn):
        EventHandler.timeoutExpired(self, conn)
        self.current = None

    def peerBroken(self, conn):
        EventHandler.peerBroken(self, conn)
        self.current = None

    def handleNotReady(self, conn, packet, message):
        # master are still electing on of them
        self.current = None
        conn.close()

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        nm  = self.app.nm

        # Register new master nodes.
        for address, uuid in known_master_list:
            node = nm.getNodeByServer(address)
            if node is None:
                node = MasterNode(server=address)
                nm.add(node)
            node.setUUID(uuid)

        self.primary = nm.getNodeByUUID(primary_uuid)
        if self.primary is None or self.current is not self.primary:
            # three cases here:
            # - something goes wrong (unknown UUID)
            # - this master doesn't know who's the primary
            # - got the primary's uuid, so cut here
            self.current = None
            conn.close()
            return

        logging.info('connected to a primary master node')
        conn.ask(protocol.requestNodeIdentification(self.node_type,
                self.uuid, self.server, self.name))

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
           uuid, address, num_partitions, num_replicas, your_uuid):
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        if self.uuid != your_uuid:
            # got an uuid from the primary master
            self.uuid = your_uuid
            logging.info('Got a new UUID : %s' % dump(self.uuid))
        conn.setUUID(uuid)

    def getPrimaryConnection(self, connector_handler):

        logging.info('connecting to a primary master node')
        em, nm = self.app.em, self.app.nm
        index = 0
        self.current = nm.getMasterNodeList()[0]
        conn = None
        # retry until identified to the primary
        while self.primary is None or conn.getUUID() != self.primary.getUUID():
            if self.current is None:
                # conn closed 
                conn = None
            if self.current is None:
                # select a master
                master_list = nm.getMasterNodeList()
                index = (index + 1) % len(master_list)
                self.current = master_list[index]
                if index == 0:
                    # tried all known masters, sleep a bit
                    sleep(1)
            if conn is None:
                # open the connection
                addr = self.current.getServer()
                conn = ClientConnection(em, self, addr, connector_handler)
            # still processing 
            em.poll(1)
        node = nm.getNodeByUUID(conn.getUUID())
        return (node, conn, self.uuid, self.num_partitions, self.num_replicas)



