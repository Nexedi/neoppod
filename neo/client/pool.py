#
# Copyright (C) 2006-2012  Nexedi SA
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

import time
from random import shuffle

from neo.lib import logging
from neo.lib.locking import RLock
from neo.lib.protocol import NodeTypes, Packets
from neo.lib.connection import MTClientConnection, ConnectionClosed
from neo.lib.profiling import profiler_decorator
from neo.lib.exception import NodeNotReady
from .exception import NEOStorageError

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
        app = self.app
        logging.debug('trying to connect to %s - %s', node, node.getState())
        conn = MTClientConnection(app.em, app.storage_event_handler, node,
            connector=app.connector_handler(), dispatcher=app.dispatcher)
        p = Packets.RequestIdentification(NodeTypes.CLIENT,
            app.uuid, None, app.name)
        try:
            app._ask(conn, p, handler=app.storage_bootstrap_handler)
        except ConnectionClosed:
            logging.error('Connection to %r failed', node)
            self.notifyFailure(node)
            conn = None
        except NodeNotReady:
            logging.info('%r not ready', node)
            self.notifyFailure(node)
            conn = None
        else:
            logging.info('Connected %r', node)
        return conn

    @profiler_decorator
    def _dropConnections(self):
        """Drop connections."""
        for conn in self.connection_dict.values():
            # Drop first connection which looks not used
            conn.lock()
            try:
                if not conn.pending() and \
                        not self.app.dispatcher.registered(conn):
                    del self.connection_dict[conn.getUUID()]
                    conn.close()
                    logging.debug('_dropConnections: connection to '
                        'storage node %s:%d closed', *conn.getAddress())
                    if len(self.connection_dict) <= self.max_pool_size:
                        break
            finally:
                conn.unlock()

    @profiler_decorator
    def notifyFailure(self, node):
        self.node_failure_dict[node.getUUID()] = time.time() + MAX_FAILURE_AGE

    @profiler_decorator
    def getCellSortKey(self, cell):
        uuid = cell.getUUID()
        if uuid in self.connection_dict:
            return CELL_CONNECTED
        failure = self.node_failure_dict.get(uuid)
        if failure is None or failure < time.time():
            return CELL_GOOD
        return CELL_FAILED

    @profiler_decorator
    def getConnForCell(self, cell):
        return self.getConnForNode(cell.getNode())

    def iterateForObject(self, object_id, readable=False):
        """ Iterate over nodes managing an object """
        pt = self.app.getPartitionTable()
        if type(object_id) is str:
            object_id = pt.getPartition(object_id)
        cell_list = pt.getCellList(object_id, readable)
        if not cell_list:
            raise NEOStorageError('no storage available')
        getConnForNode = self.getConnForNode
        while cell_list:
            new_cell_list = []
            cell_list = [c for c in cell_list if c.getNode().isRunning()]
            shuffle(cell_list)
            cell_list.sort(key=self.getCellSortKey)
            for cell in cell_list:
                node = cell.getNode()
                conn = getConnForNode(node)
                if conn is not None:
                    yield (node, conn)
                elif node.isRunning():
                    new_cell_list.append(cell)
            cell_list = new_cell_list
            if new_cell_list:
                # wait a bit to avoid a busy loop
                time.sleep(1)

    @profiler_decorator
    def getConnForNode(self, node):
        """Return a locked connection object to a given node
        If no connection exists, create a new one"""
        if node.isRunning():
            uuid = node.getUUID()
            self.connection_lock_acquire()
            try:
                # Already connected to node
                return self.connection_dict[uuid]
            except KeyError:
                if len(self.connection_dict) > self.max_pool_size:
                    # must drop some unused connections
                    self._dropConnections()
                # Create new connection to node
                conn = self._initNodeConnection(node)
                if conn is not None:
                    self.connection_dict[uuid] = conn
                return conn
            finally:
                self.connection_lock_release()

    @profiler_decorator
    def removeConnection(self, node):
        """Explicitly remove connection when a node is broken."""
        self.connection_dict.pop(node.getUUID(), None)

    def flush(self):
        """Remove all connections"""
        self.connection_dict.clear()

