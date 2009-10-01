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

from neo import logging

from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.pt import MTPartitionTable as PartitionTable
from neo import protocol
from neo.protocol import NodeTypes
from neo.util import dump

class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def handleNotReady(self, conn, packet, message):
        app = self.app
        app.trying_master_node = None
        app.setNodeNotReady()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                   uuid, address, num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getByAddress(conn.getAddress())
        # this must be a master node
        if node_type != NodeTypes.MASTER:
            conn.close()
            return
        if conn.getAddress() != address:
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                          conn.getAddress()[0], conn.getAddress()[1], *address)
            app.nm.remove(node)
            conn.close()
            return

        conn.setUUID(uuid)
        node.setUUID(uuid)

        if your_uuid is not None:
            # got an uuid from the primary master
            app.uuid = your_uuid

        # Always create partition table 
        app.pt = PartitionTable(num_partitions, num_replicas)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        app = self.app
        # Register new master nodes.
        for address, uuid in known_master_list:
            n = app.nm.getByAddress(address)
            if n is None:
                app.nm.createMaster(address)
            if uuid is not None:
                # If I don't know the UUID yet, believe what the peer
                # told me at the moment.
                if n.getUUID() is None or n.getUUID() != uuid:
                    n.setUUID(uuid)

        if primary_uuid is not None:
            primary_node = app.nm.getByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('Unknown primary master UUID: %s. ' \
                                'Ignoring.' % dump(primary_uuid))
            else:
                app.primary_master_node = primary_node
                if app.trying_master_node is not primary_node:
                    app.trying_master_node = None
                    conn.close()
        else:
            if app.primary_master_node is not None:
                # The primary master node is not a primary master node
                # any longer.
                app.primary_master_node = None
 
            app.trying_master_node = None
            conn.close()
 
    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        pass
 
    def handleAnswerNodeInformation(self, conn, packet, node_list):
        pass

class PrimaryNotificationsHandler(BaseHandler):
    """ Handler that process the notifications from the primary master """

    def connectionClosed(self, conn):
        app = self.app
        logging.critical("connection to primary master node closed")
        conn.close()
        if app.master_conn is conn:
            app.master_conn = None
            app.primary_master_node = None
        else:
            logging.warn('app.master_conn is %s, but we are closing %s', app.master_conn, conn)
        super(PrimaryNotificationsHandler, self).connectionClosed(conn)

    def timeoutExpired(self, conn):
        app = self.app
        if app.master_conn is not None:
            assert conn is app.master_conn
            logging.critical("connection timeout to primary master node expired")
        BaseHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        app = self.app
        if app.master_conn is not None:
            assert conn is app.master_conn
            logging.critical("primary master node is broken")
        BaseHandler.peerBroken(self, conn)

    def handleStopOperation(self, conn, packet):
        logging.critical("master node ask to stop operation")

    def handleInvalidateObjects(self, conn, packet, oid_list, tid):
        app = self.app
        app._cache_lock_acquire()
        try:
            # ZODB required a dict with oid as key, so create it
            oids = {}
            for oid in oid_list:
                oids[oid] = tid
                try:
                    del app.mq_cache[oid]
                except KeyError:
                    pass
            db = app.getDB()
            if db is not None:
                db.invalidate(tid, oids)
        finally:
            app._cache_lock_release()

    # For the two methods below, we must not use app._getPartitionTable() 
    # to avoid a dead lock. It is safe to not check the master connection
    # because it's in the master handler, so the connection is already
    # established.
    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        pt = self.app.pt
        if pt.filled():
            pt.update(ptid, cell_list, self.app.nm)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        self.app.pt.load(ptid, row_list, self.app.nm)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if node_type != NodeTypes.STORAGE \
                    or state != protocol.RUNNING_STATE:
                continue
            # close connection to this storage if no longer running
            conn = self.app.em.getConnectionByUUID(uuid)
            if conn is not None:
                conn.close()
                if node_type == NodeTypes.STORAGE:
                    # Remove from pool connection
                    app.cp.removeConnection(conn)
                    self.dispatcher.unregister(conn)

class PrimaryAnswersHandler(AnswerBaseHandler):
    """ Handle that process expected packets from the primary master """

    def handleAnswerBeginTransaction(self, conn, packet, tid):
        app = self.app
        app.setTID(tid)

    def handleAnswerNewOIDs(self, conn, packet, oid_list):
        app = self.app
        app.new_oid_list = oid_list
        app.new_oid_list.reverse()

    def handleNotifyTransactionFinished(self, conn, packet, tid):
        app = self.app
        if tid == app.getTID():
            app.setTransactionFinished()

