#
# Copyright (C) 2006-2017  Nexedi SA
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
from .exception import PrimaryElected
from .handler import EventHandler
from .protocol import Packets
from .connection import ClientConnection


class BootstrapManager(EventHandler):
    """
    Manage the bootstrap stage, lookup for the primary master then connect to it
    """

    def __init__(self, app, node_type, server=None):
        """
        Manage the bootstrap stage of a non-master node, it lookup for the
        primary master node, connect to it then returns when the master node
        is ready.
        """
        self.server = server
        self.node_type = node_type
        self.num_replicas = None
        self.num_partitions = None
        app.nm.reset()

    uuid = property(lambda self: self.app.uuid)

    def connectionCompleted(self, conn):
        EventHandler.connectionCompleted(self, conn)
        conn.ask(Packets.RequestIdentification(self.node_type, self.uuid,
            self.server, self.app.name, None))

    def connectionFailed(self, conn):
        EventHandler.connectionFailed(self, conn)
        self.current = None

    def connectionLost(self, conn, new_state):
        self.current = None

    def _acceptIdentification(self, node, num_partitions, num_replicas):
        assert self.current is node, (self.current, node)
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

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
        # retry until identified to the primary
        while True:
            try:
                while self.current:
                    if self.current.isIdentified():
                        return (self.current, self.current.getConnection(),
                            self.num_partitions, self.num_replicas)
                    poll(1)
            except PrimaryElected, e:
                if self.current:
                    self.current.getConnection().close()
                self.current, = e.args
                index = app.nm.getMasterList().index(self.current)
            else:
                # select a master
                master_list = app.nm.getMasterList()
                index = (index + 1) % len(master_list)
                self.current = master_list[index]
            ClientConnection(app, self, self.current)
            # Note that the connection may be already closed. This happens when
            # the kernel reacts so quickly to a closed port that 'connect'
            # fails on the first call. In such case, poll(1) would deadlock
            # if there's no other connection to timeout.
