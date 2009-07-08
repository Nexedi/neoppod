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

from neo.storage.handlers import BaseStorageHandler
from neo.protocol import INVALID_UUID, MASTER_NODE_TYPE, STORAGE_NODE_TYPE
from neo.node import MasterNode
from neo import protocol
from neo.pt import PartitionTable
from neo.util import dump

class BootstrapHandler(BaseStorageHandler):
    """This class deals with events for a bootstrap phase."""

    def connectionCompleted(self, conn):
        conn.ask(protocol.askPrimaryMaster())
        BaseStorageHandler.connectionCompleted(self, conn)

    def connectionFailed(self, conn):
        app = self.app
        if app.trying_master_node is app.primary_master_node:
            # Tried to connect to a primary master node and failed.
            # So this would effectively mean that it is dead.
            app.primary_master_node = None
        app.trying_master_node = None
        BaseStorageHandler.connectionFailed(self, conn)

    def timeoutExpired(self, conn):
        app = self.app
        if app.trying_master_node is app.primary_master_node:
            # If a primary master node timeouts, I should not rely on it.
            app.primary_master_node = None
        app.trying_master_node = None
        BaseStorageHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        app = self.app
        if app.trying_master_node is app.primary_master_node:
            # If a primary master node closes, I should not rely on it.
            app.primary_master_node = None
        app.trying_master_node = None
        BaseStorageHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        app = self.app
        if app.trying_master_node is app.primary_master_node:
            # If a primary master node gets broken, I should not rely
            # on it.
            app.primary_master_node = None
        app.trying_master_node = None
        BaseStorageHandler.peerBroken(self, conn)

    def handleNotReady(self, conn, packet, message):
        app = self.app
        if app.trying_master_node is not None:
            app.trying_master_node = None
        conn.close()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        if node_type != MASTER_NODE_TYPE:
            # The peer is not a master node!
            logging.error('%s:%d is not a master node', ip_address, port)
            app.nm.remove(node)
            conn.close()
            return
        if conn.getAddress() != (ip_address, port):
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                          conn.getAddress()[0], conn.getAddress()[1], 
                          ip_address, port)
            app.nm.remove(node)
            conn.close()
            return

        if app.num_partitions is None or app.num_replicas is None or \
               app.num_replicas != num_replicas:
            # changing number of replicas is not an issue
            app.num_partitions = num_partitions
            app.dm.setNumPartitions(app.num_partitions)
            app.num_replicas = num_replicas
            app.dm.setNumReplicas(app.num_replicas)
            app.pt = PartitionTable(num_partitions, num_replicas)
            app.loadPartitionTable()
            app.ptid = app.dm.getPTID()
        elif app.num_partitions != num_partitions:
            raise RuntimeError('the number of partitions is inconsistent')


        if your_uuid != INVALID_UUID and app.uuid != your_uuid:
            # got an uuid from the primary master
            app.uuid = your_uuid
            app.dm.setUUID(app.uuid)
            logging.info('Got a new UUID from master : %s' % dump(app.uuid))

        conn.setUUID(uuid)
        #node.setUUID(uuid)
        # Node UUID was set in handleAnswerPrimaryMaster
        assert node.getUUID() == uuid

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        closed = False
        app = self.app
        # Register new master nodes.
        for ip_address, port, uuid in known_master_list:
            addr = (ip_address, port)
            n = app.nm.getNodeByServer(addr)
            if n is None:
                n = MasterNode(server = addr)
                app.nm.add(n)

            if uuid != INVALID_UUID:
                # If I don't know the UUID yet, believe what the peer
                # told me at the moment.
                if n.getUUID() is None or n.getUUID() != uuid:
                    n.setUUID(uuid)

        if primary_uuid != INVALID_UUID:
            primary_node = app.nm.getNodeByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('Unknown primary master UUID: %s. Ignoring.' % dump(primary_uuid))
            else:
                app.primary_master_node = primary_node
                if app.trying_master_node is primary_node:
                    # I am connected to the right one.
                    logging.info('connected to a primary master node')
                else:
                    app.trying_master_node = None
                    conn.close()
                    closed = True
        else:
            if app.primary_master_node is not None:
                # The primary master node is not a primary master node
                # any longer.
                app.primary_master_node = None

            app.trying_master_node = None
            conn.close()
            closed = True
        if not closed:
            p = protocol.requestNodeIdentification(STORAGE_NODE_TYPE, app.uuid,
                                        app.server[0], app.server[1], app.name)
            conn.ask(p)

