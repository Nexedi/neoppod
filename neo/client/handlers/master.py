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

import neo.lib

from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.lib.pt import MTPartitionTable as PartitionTable
from neo.lib.protocol import NodeTypes, NodeStates, ProtocolError
from neo.lib.util import dump
from neo.client.exception import NEOStorageError

class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def notReady(self, conn, message):
        app = self.app
        app.trying_master_node = None

    def acceptIdentification(self, conn, node_type,
                   uuid, num_partitions, num_replicas, your_uuid):
        app = self.app
        # this must be a master node
        if node_type != NodeTypes.MASTER:
            conn.close()
            return

        # the master must give an UUID
        if your_uuid is None:
            raise ProtocolError('No UUID supplied')
        app.uuid = your_uuid
        neo.lib.logging.info('Got an UUID: %s', dump(app.uuid))

        node = app.nm.getByAddress(conn.getAddress())
        conn.setUUID(uuid)
        node.setUUID(uuid)

        # Always create partition table
        app.pt = PartitionTable(num_partitions, num_replicas)

    def answerPrimary(self, conn, primary_uuid,
                                  known_master_list):
        app = self.app
        # Register new master nodes.
        for address, uuid in known_master_list:
            n = app.nm.getByAddress(address)
            if uuid is not None and n.getUUID() != uuid:
                n.setUUID(uuid)

        if primary_uuid is not None:
            primary_node = app.nm.getByUUID(primary_uuid)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                neo.lib.logging.warning('Unknown primary master UUID: %s. ' \
                                'Ignoring.' % dump(primary_uuid))
            else:
                if app.trying_master_node is not primary_node:
                    app.trying_master_node = None
                    conn.close()
                app.primary_master_node = primary_node
        else:
            if app.primary_master_node is not None:
                # The primary master node is not a primary master node
                # any longer.
                app.primary_master_node = None

            app.trying_master_node = None
            conn.close()

    def answerPartitionTable(self, conn, ptid, row_list):
        assert row_list
        self.app.pt.load(ptid, row_list, self.app.nm)

    def answerNodeInformation(self, conn):
        pass

class PrimaryNotificationsHandler(BaseHandler):
    """ Handler that process the notifications from the primary master """

    def connectionClosed(self, conn):
        app = self.app
        if app.master_conn is not None:
            neo.lib.logging.critical("connection to primary master node closed")
            app.master_conn = None
        app.primary_master_node = None
        super(PrimaryNotificationsHandler, self).connectionClosed(conn)

    def stopOperation(self, conn):
        neo.lib.logging.critical("master node ask to stop operation")

    def invalidateObjects(self, conn, tid, oid_list):
        app = self.app
        app._cache_lock_acquire()
        try:
            invalidate = app._cache.invalidate
            for oid in oid_list:
                invalidate(oid, tid)
            db = app.getDB()
            if db is not None:
                db.invalidate(tid, oid_list)
        finally:
            app._cache_lock_release()

    # For the two methods below, we must not use app._getPartitionTable()
    # to avoid a dead lock. It is safe to not check the master connection
    # because it's in the master handler, so the connection is already
    # established.
    def notifyPartitionChanges(self, conn, ptid, cell_list):
        if self.app.pt.filled():
            self.app.pt.update(ptid, cell_list, self.app.nm)

    def notifyNodeInformation(self, conn, node_list):
        nm = self.app.nm
        nm.update(node_list)
        # XXX: 'update' automatically closes DOWN nodes. Do we really want
        #      to do the same thing for nodes in other non-running states ?
        for node_type, addr, uuid, state in node_list:
            if state != NodeStates.RUNNING:
                node = nm.getByUUID(uuid)
                if node and node.isConnected():
                    node.getConnection().close()


class PrimaryAnswersHandler(AnswerBaseHandler):
    """ Handle that process expected packets from the primary master """

    def answerBeginTransaction(self, conn, ttid):
        self.app.setHandlerData(ttid)

    def answerNewOIDs(self, conn, oid_list):
        self.app.new_oid_list = list(oid_list)

    def answerTransactionFinished(self, conn, _, tid):
        self.app.setHandlerData(tid)

    def answerPack(self, conn, status):
        if not status:
            raise NEOStorageError('Already packing')

    def answerLastTransaction(self, conn, ltid):
        self.app.setHandlerData(ltid)

