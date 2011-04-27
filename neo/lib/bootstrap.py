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

import neo
from time import sleep

from neo.lib.handler import EventHandler
from neo.lib.protocol import Packets
from neo.lib.util import dump
from neo.lib.connection import ClientConnection

NO_SERVER = ('0.0.0.0', 0)

class BootstrapManager(EventHandler):
    """
    Manage the bootstrap stage, lookup for the primary master then connect to it
    """

    def __init__(self, app, name, node_type, uuid=None, server=NO_SERVER):
        """
        Manage the bootstrap stage of a non-master node, it lookup for the
        primary master node, connect to it then returns when the master node
        is ready.
        """
        EventHandler.__init__(self, app)
        self.primary = None
        self.server = server
        self.node_type = node_type
        self.uuid = uuid
        self.name = name
        self.num_replicas = None
        self.num_partitions = None
        self.current = None

    def connectionCompleted(self, conn):
        """
        Triggered when the network connection is successful.
        Now ask who's the primary.
        """
        EventHandler.connectionCompleted(self, conn)
        self.current.setRunning()
        conn.ask(Packets.AskPrimary())

    def connectionFailed(self, conn):
        """
        Triggered when the network connection failed.
        Restart bootstrap.
        """
        EventHandler.connectionFailed(self, conn)
        self.current = None

    def connectionLost(self, conn, new_state):
        """
        Triggered when an established network connection is lost.
        Restart bootstrap.
        """
        self.current.setTemporarilyDown()
        self.current = None

    def notReady(self, conn, message):
        """
        The primary master send this message when it is still not ready to
        handle the client node.
        Close connection and restart.
        """
        conn.close()

    def answerPrimary(self, conn, primary_uuid, known_master_list):
        """
        A master answer who's the primary. If it's another node, connect to it.
        If it's itself then the primary is successfully found, ask
        identification.
        """
        nm  = self.app.nm

        # Register new master nodes.
        for address, uuid in known_master_list:
            node = nm.getByAddress(address)
            if node is None:
                node = nm.createMaster(address=address)
            node.setUUID(uuid)

        self.primary = nm.getByUUID(primary_uuid)
        if self.primary is None or self.current is not self.primary:
            # three cases here:
            # - something goes wrong (unknown UUID)
            # - this master doesn't know who's the primary
            # - got the primary's uuid, so cut here
            conn.close()
            return

        neo.lib.logging.info('connected to a primary master node')
        conn.ask(Packets.RequestIdentification(self.node_type,
                self.uuid, self.server, self.name))

    def acceptIdentification(self, conn, node_type,
           uuid, num_partitions, num_replicas, your_uuid):
        """
        The primary master has accepted the node.
        """
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        if self.uuid != your_uuid:
            # got an uuid from the primary master
            self.uuid = your_uuid
            neo.lib.logging.info('Got a new UUID : %s' % dump(self.uuid))
        conn.setUUID(uuid)

    def getPrimaryConnection(self, connector_handler):
        """
        Primary lookup/connection process.
        Returns when the connection is made.
        """
        neo.lib.logging.info('connecting to a primary master node')
        em, nm = self.app.em, self.app.nm
        index = 0
        self.current = nm.getMasterList()[0]
        conn = None
        # retry until identified to the primary
        while self.primary is None or conn.getUUID() != self.primary.getUUID():
            if self.current is None:
                # conn closed
                conn = None
                # select a master
                master_list = nm.getMasterList()
                index = (index + 1) % len(master_list)
                self.current = master_list[index]
                if index == 0:
                    # tried all known masters, sleep a bit
                    sleep(1)
            if conn is None:
                # open the connection
                addr = self.current.getAddress()
                conn = ClientConnection(em, self, addr, connector_handler())
            # still processing
            em.poll(1)
        node = nm.getByUUID(conn.getUUID())
        return (node, conn, self.uuid, self.num_partitions, self.num_replicas)



