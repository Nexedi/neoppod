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

import time
from random import shuffle

from neo.lib import logging
from neo.lib.locking import Lock
from neo.lib.protocol import NodeTypes, Packets
from neo.lib.connection import MTClientConnection, ConnectionClosed
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
        self._lock = Lock()
        self.node_failure_dict = {}

    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        app = self.app
        logging.debug('trying to connect to %s - %s', node, node.getState())
        conn = MTClientConnection(app, app.storage_event_handler, node,
                                  dispatcher=app.dispatcher)
        p = Packets.RequestIdentification(NodeTypes.CLIENT,
            app.uuid, None, app.name)
        try:
            app._ask(conn, p, handler=app.storage_bootstrap_handler)
        except ConnectionClosed:
            logging.error('Connection to %r failed', node)
        except NodeNotReady:
            logging.info('%r not ready', node)
        else:
            logging.info('Connected %r', node)
            return conn
        self.notifyFailure(node)

    def _dropConnections(self):
        """Drop connections."""
        for conn in self.connection_dict.values():
            # Drop first connection which looks not used
            with conn.lock:
                if not conn.pending() and \
                        not self.app.dispatcher.registered(conn):
                    del self.connection_dict[conn.getUUID()]
                    conn.setReconnectionNoDelay()
                    conn.close()
                    logging.debug('_dropConnections: connection to '
                        'storage node %s:%d closed', *conn.getAddress())
                    if len(self.connection_dict) <= self.max_pool_size:
                        break

    def notifyFailure(self, node):
        self.node_failure_dict[node.getUUID()] = time.time() + MAX_FAILURE_AGE

    def getCellSortKey(self, cell):
        uuid = cell.getUUID()
        if uuid in self.connection_dict:
            return CELL_CONNECTED
        failure = self.node_failure_dict.get(uuid)
        if failure is None or failure < time.time():
            return CELL_GOOD
        return CELL_FAILED

    def getConnForCell(self, cell):
        return self.getConnForNode(cell.getNode())

    def iterateForObject(self, object_id, readable=False):
        """ Iterate over nodes managing an object """
        pt = self.app.pt
        if type(object_id) is str:
            object_id = pt.getPartition(object_id)
        cell_list = pt.getCellList(object_id, readable)
        if not cell_list:
            raise NEOStorageError('no storage available')
        getConnForNode = self.getConnForNode
        while 1:
            new_cell_list = []
            # Shuffle to randomise node to access...
            shuffle(cell_list)
            # ...and sort with non-unique keys, to prioritise ranges of
            # randomised entries.
            cell_list.sort(key=self.getCellSortKey)
            for cell in cell_list:
                node = cell.getNode()
                conn = getConnForNode(node)
                if conn is not None:
                    yield node, conn
                # Re-check if node is running, as our knowledge of its
                # state can have changed during connection attempt.
                elif node.isRunning():
                    new_cell_list.append(cell)
            if not new_cell_list or self.app.master_conn is None:
                break
            cell_list = new_cell_list

    def getConnForNode(self, node):
        """Return a locked connection object to a given node
        If no connection exists, create a new one"""
        if node.isRunning():
            uuid = node.getUUID()
            try:
                # Already connected to node
                return self.connection_dict[uuid]
            except KeyError:
                with self._lock:
                    # Second lookup, if another thread initiated connection
                    # while we were waiting for connection lock.
                    try:
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

    def removeConnection(self, node):
        """Explicitly remove connection when a node is broken."""
        self.connection_dict.pop(node.getUUID(), None)

    def flush(self):
        """Remove all connections"""
        self.connection_dict.clear()

