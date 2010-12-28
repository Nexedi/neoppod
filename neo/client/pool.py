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
from neo.locking import RLock
from neo.protocol import NodeTypes, Packets
from neo.connection import MTClientConnection, ConnectionClosed
from neo.profiling import profiler_decorator
import time

# How long before we might retry a connection to a node to which connection
# failed in the past.
MAX_FAILURE_AGE = 600

# Cell list sort keys
#   We are connected to storage node hosting cell, high priority
CELL_CONNECTED = -1
#   normal priority
CELL_GOOD = 0
#   Storage node hosting cell failed recently, low priority
CELL_FAILED = 1

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
        self.node_failure_dict = {}

    @profiler_decorator
    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        addr = node.getAddress()
        if addr is None:
            return None

        app = self.app
        app.setNodeReady()
        neo.logging.debug('trying to connect to %s - %s', node,
            node.getState())
        conn = MTClientConnection(app.em, app.storage_event_handler, addr,
            connector=app.connector_handler(), dispatcher=app.dispatcher)
        conn.lock()

        try:
            if conn.getConnector() is None:
                # This happens, if a connection could not be established.
                neo.logging.error('Connection to %r failed', node)
                self.notifyFailure(node)
                return None

            p = Packets.RequestIdentification(NodeTypes.CLIENT,
                app.uuid, None, app.name)
            msg_id = conn.ask(p, queue=app.local_var.queue)
        finally:
            conn.unlock()

        try:
            app._waitMessage(conn, msg_id,
                handler=app.storage_bootstrap_handler)
        except ConnectionClosed:
            neo.logging.error('Connection to %r failed', node)
            self.notifyFailure(node)
            return None

        if app.isNodeReady():
            neo.logging.info('Connected %r', node)
            return conn
        else:
            neo.logging.info('%r not ready', node)
            self.notifyFailure(node)
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
                    neo.logging.debug('_dropConnections : connection to ' \
                        'storage node %s:%d closed', *(conn.getAddress()))
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
    def notifyFailure(self, node):
        self._notifyFailure(node.getUUID(), time.time() + MAX_FAILURE_AGE)

    def _notifyFailure(self, uuid, at):
        self.node_failure_dict[uuid] = at

    @profiler_decorator
    def getCellSortKey(self, cell):
        return self._getCellSortKey(cell.getUUID(), time.time())

    def _getCellSortKey(self, uuid, now):
        if uuid in self.connection_dict:
            result = CELL_CONNECTED
        else:
            failure = self.node_failure_dict.get(uuid)
            if failure is None or failure < now:
                result = CELL_GOOD
            else:
                result = CELL_FAILED
        return result

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
        self.connection_dict.pop(node.getUUID(), None)

    def flush(self):
        """Remove all connections"""
        self.connection_dict.clear()

