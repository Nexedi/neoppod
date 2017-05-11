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

import random, time
from neo.lib import logging
from neo.lib.locking import Lock
from neo.lib.protocol import NodeTypes, Packets
from neo.lib.connection import MTClientConnection, ConnectionClosed
from neo.lib.exception import NodeNotReady
from .exception import NEOPrimaryMasterLost

# How long before we might retry a connection to a node to which connection
# failed in the past.
MAX_FAILURE_AGE = 600


class ConnectionPool(object):
    """This class manages a pool of connections to storage nodes."""

    def __init__(self, app):
        self.app = app
        self.connection_dict = {}
        # Define a lock in order to create one connection to
        # a storage node at a time to avoid multiple connections
        # to the same node.
        self._lock = Lock()
        self.node_failure_dict = {}

    def _initNodeConnection(self, node):
        """Init a connection to a given storage node."""
        app = self.app
        if app.master_conn is None:
            raise NEOPrimaryMasterLost
        conn = MTClientConnection(app, app.storage_event_handler, node,
                                  dispatcher=app.dispatcher)
        p = Packets.RequestIdentification(NodeTypes.CLIENT,
            app.uuid, None, app.name, app.id_timestamp)
        try:
            app._ask(conn, p, handler=app.storage_bootstrap_handler)
        except ConnectionClosed:
            logging.error('Connection to %r failed', node)
        except NodeNotReady:
            logging.info('%r not ready', node)
        else:
            logging.info('Connected %r', node)
            return conn
        self.node_failure_dict[node.getUUID()] = time.time() + MAX_FAILURE_AGE

    def getCellSortKey(self, cell, random=random.random):
        # The use of 'random' suffles cells to randomise node to access.
        uuid = cell.getUUID()
        # First, prefer a connected node.
        if uuid in self.connection_dict:
            return random()
        # Then one that didn't fail recently.
        failure = self.node_failure_dict.get(uuid)
        if failure:
            if time.time() < failure:
                # At last, order by date of connection failure.
                return failure
            # Do not use 'del' statement: we didn't lock, so another
            # thread might have removed uuid from node_failure_dict.
            self.node_failure_dict.pop(uuid, None)
        return 1 + random()

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
                        # Create new connection to node
                        conn = self._initNodeConnection(node)
                        if conn is not None:
                            self.connection_dict[uuid] = conn
                            return conn

    def removeConnection(self, node):
        self.connection_dict.pop(node.getUUID(), None)

    def closeAll(self):
        with self._lock:
            while 1:
                try:
                    conn = self.connection_dict.popitem()[1]
                except KeyError:
                    break
                conn.setReconnectionNoDelay()
                conn.close()
