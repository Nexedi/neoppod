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

from threading import Thread, Lock
from Queue import Empty, Queue

from neo.protocol import PING, Packet, CLIENT_NODE_TYPE, FINISH_TRANSACTION
from neo.connection import MTClientConnection
from neo.node import MasterNode

from time import time
import logging

class Dispatcher(Thread):
    """Dispatcher class use to redirect request to thread."""

    def __init__(self, em, **kw):
        Thread.__init__(self, **kw)
        self.em = em
        # This dict is used to associate conn/message id to client thread queue
        # and thus redispatch answer to the original thread
        self.message_table = {}
        # Indicate if we are in process of connection to master node
        self.connecting_to_master_node = Lock()

    def run(self):
        while 1:
            # First check if we receive any new message from other node
            try:
                self.em.poll(None)
            except KeyError:
                # This happen when there is no connection
                logging.error('Dispatcher, run, poll returned a KeyError')

    def getQueue(self, conn, packet):
        key = (id(conn), packet.getId())
        return self.message_table.pop(key, None)

    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply. Thanks to GIL, it is
        safe not to use a lock here."""
        key = (id(conn), msg_id)
        self.message_table[key] = queue

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        searched_id = id(conn)
        for conn_id, msg_id in self.message_table.iterkeys():
            if searched_id == conn_id:
                return True
        return False

    def connectToPrimaryMasterNode(self, app, connector):
        """Connect to a primary master node.
        This can be called either at bootstrap or when
        client got disconnected during process"""
        # Indicate we are trying to connect to avoid multiple try a time
        acquired = self.connecting_to_master_node.acquire(0)
        if acquired:
            try:
                from neo.client.handler import ClientEventHandler
                if app.pt is not None:
                    app.pt.clear()
                master_index = 0
                t = 0
                conn = None
                # Make application execute remaining message if any
                app._waitMessage()
                handler = ClientEventHandler(app, app.dispatcher)
                while 1:
                    if t + 1 < time():
                        if app.pt is not None and app.pt.operational():
                            # Connected to primary master node and got all informations
                            break
                        app.local_var.node_not_ready = 0
                        if app.primary_master_node is None:
                            # Try with master node defined in config
                            try:
                                addr, port = app.master_node_list[master_index].split(':')                        
                            except IndexError:
                                master_index = 0
                                addr, port = app.master_node_list[master_index].split(':')
                            port = int(port)
                        else:
                            addr, port = app.primary_master_node.getServer()
                        # Request Node Identification
                        conn = MTClientConnection(app.em, handler, (addr, port), connector_handler=connector)
                        if app.nm.getNodeByServer((addr, port)) is None:
                            n = MasterNode(server = (addr, port))
                            app.nm.add(n)

                        conn.lock()
                        try:
                            msg_id = conn.getNextId()
                            p = Packet()
                            p.requestNodeIdentification(msg_id, CLIENT_NODE_TYPE, app.uuid,
                                                        '0.0.0.0', 0, app.name)

                            # Send message
                            conn.addPacket(p)
                            conn.expectMessage(msg_id)
                            self.register(conn, msg_id, app.getQueue())
                        finally:
                            conn.unlock()

                        # Wait for answer
                        while 1:
                            try:
                                self.em.poll(1)
                            except TypeError:
                                break
                            app._waitMessage()
                            # Now check result
                            if app.primary_master_node is not None:
                                if app.primary_master_node == -1:
                                    # Connection failed, try with another master node
                                    app.primary_master_node = None
                                    master_index += 1
                                    break
                                elif app.primary_master_node.getServer() != (addr, port):
                                    # Master node changed, connect to new one
                                    break
                                elif app.local_var.node_not_ready:
                                    # Wait a bit and reask again
                                    break
                                elif app.pt is not None and app.pt.operational():
                                    # Connected to primary master node
                                    break
                        t = time()

                logging.info("connected to primary master node %s:%d" %app.primary_master_node.getServer())
                app.master_conn = conn
            finally:
                self.connecting_to_master_node.release()
