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

from . import logging
from .handler import EventHandler
from .protocol import uuid_str, Packets
from .connection import ClientConnection


class BootstrapManager(EventHandler):
    """
    Manage the bootstrap stage, lookup for the primary master then connect to it
    """
    accepted = False

    def __init__(self, app, name, node_type, uuid=None, server=None):
        """
        Manage the bootstrap stage of a non-master node, it lookup for the
        primary master node, connect to it then returns when the master node
        is ready.
        """
        self.primary = None
        self.server = server
        self.node_type = node_type
        self.uuid = uuid
        self.name = name
        self.num_replicas = None
        self.num_partitions = None
        self.current = None

    def notifyNodeInformation(self, conn, node_list):
        pass

    def announcePrimary(self, conn):
        # We found the primary master early enough to be notified of election
        # end. Lucky. Anyway, we must carry on with identification request, so
        # nothing to do here.
        pass

    def connectionCompleted(self, conn):
        """
        Triggered when the network connection is successful.
        Now ask who's the primary.
        """
        EventHandler.connectionCompleted(self, conn)
        self.current.setRunning()
        conn.ask(Packets.RequestIdentification(self.node_type, self.uuid,
            self.server, self.name))

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

    def _acceptIdentification(self, node, uuid, num_partitions,
            num_replicas, your_uuid, primary, known_master_list):
        nm  = self.app.nm

        # Register new master nodes.
        for address, uuid in known_master_list:
            master_node = nm.getByAddress(address)
            if master_node is None:
                master_node = nm.createMaster(address=address)
            master_node.setUUID(uuid)

        self.primary = nm.getByAddress(primary)
        if self.primary is None or self.current is not self.primary:
            # three cases here:
            # - something goes wrong (unknown UUID)
            # - this master doesn't know who's the primary
            # - got the primary's uuid, so cut here
            node.getConnection().close()
            return

        logging.info('connected to a primary master node')
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        if self.uuid != your_uuid:
            # got an uuid from the primary master
            self.uuid = your_uuid
            logging.info('Got a new UUID: %s', uuid_str(self.uuid))
        self.accepted = True

    def getPrimaryConnection(self):
        """
        Primary lookup/connection process.
        Returns when the connection is made.
        """
        logging.info('connecting to a primary master node')
        app = self.app
        poll = app.em.poll
        index = 0
        self.current = None
        conn = None
        # retry until identified to the primary
        while not self.accepted:
            if self.current is None:
                # conn closed
                conn = None
                # select a master
                master_list = app.nm.getMasterList()
                index = (index + 1) % len(master_list)
                self.current = master_list[index]
            if conn is None:
                # open the connection
                conn = ClientConnection(app, self, self.current)
                # Yes, the connection may be already closed. This happens when
                # the kernel reacts so quickly to a closed port that 'connect'
                # fails on the first call. In such case, poll(1) would deadlock
                # if there's no other connection to timeout.
                if conn.isClosed():
                    continue
            # still processing
            poll(1)
        return (self.current, conn, self.uuid, self.num_partitions,
            self.num_replicas)



