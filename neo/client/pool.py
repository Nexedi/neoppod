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


from neo import logging
from neo.locking import RLock
from neo.protocol import NodeTypes, Packets
from neo.connection import MTClientConnection
from neo.client.exception import ConnectionClosed
from neo.profiling import profiler_decorator

class ConnectionPool(object):
    """This class manages a pool of connections to storage nodes."""

    def __init__(self, app, max_pool_size = 25):
        self.app = app
        self.max_pool_size = max_pool_size
        self.connection_dict = {}
        # Define a lock in order to create one connection to
        # a storage node at a time to avoid multiple connections
        # to the same node.
        l = RLock()
        self.connection_lock_acquire = l.acquire
        self.connection_lock_release = l.release

    @profiler_decorator
    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        addr = node.getAddress()
        if addr is None:
            return None

        app = self.app

        # Loop until a connection is obtained.
        while True:
            logging.debug('trying to connect to %s - %s', node, node.getState())
            app.setNodeReady()
            conn = MTClientConnection(app.local_var, app.em,
                app.storage_event_handler, addr,
                connector=app.connector_handler(), dispatcher=app.dispatcher)
            conn.lock()

            try:
                if conn.getConnector() is None:
                    # This happens, if a connection could not be established.
                    logging.error('Connection to %r failed', node)
                    return None

                p = Packets.RequestIdentification(NodeTypes.CLIENT,
                            app.uuid, None, app.name)
                msg_id = conn.ask(p)
            finally:
                conn.unlock()

            try:
                app._waitMessage(conn, msg_id,
                        handler=app.storage_bootstrap_handler)
            except ConnectionClosed:
                logging.error('Connection to %r failed', node)
                return None

            if app.isNodeReady():
                logging.info('Connected %r', node)
                return conn
            else:
                logging.info('%r not ready', node)
                return None

    @profiler_decorator
    def _dropConnections(self):
        """Drop connections."""
        for node_uuid, conn in self.connection_dict.items():
            # Drop first connection which looks not used
            conn.lock()
            try:
                if not conn.pending() and \
                        not self.app.dispatcher.registered(conn):
                    del self.connection_dict[conn.getUUID()]
                    conn.close()
                    logging.debug('_dropConnections : connection to storage ' \
                            'node %s:%d closed', *(conn.getAddress()))
                    if len(self.connection_dict) <= self.max_pool_size:
                        break
            finally:
                conn.unlock()

    @profiler_decorator
    def _createNodeConnection(self, node):
        """Create a connection to a given storage node."""
        if len(self.connection_dict) > self.max_pool_size:
            # must drop some unused connections
            self._dropConnections()

        conn = self._initNodeConnection(node)
        if conn is not None:
            self.connection_dict[node.getUUID()] = conn
        return conn

    @profiler_decorator
    def getConnForCell(self, cell):
        return self.getConnForNode(cell.getNode())

    @profiler_decorator
    def getConnForNode(self, node):
        """Return a locked connection object to a given node
        If no connection exists, create a new one"""
        if not node.isRunning():
            return None
        uuid = node.getUUID()
        self.connection_lock_acquire()
        try:
            try:
                conn = self.connection_dict[uuid]
                # Already connected to node
                return conn
            except KeyError:
                # Create new connection to node
                return self._createNodeConnection(node)
        finally:
            self.connection_lock_release()

    @profiler_decorator
    def removeConnection(self, node):
        """Explicitly remove connection when a node is broken."""
        self.connection_lock_acquire()
        try:
            try:
                del self.connection_dict[node.getUUID()]
            except KeyError:
                pass
        finally:
            self.connection_lock_release()

