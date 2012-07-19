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

from neo.lib import logging
from neo.lib.pt import MTPartitionTable as PartitionTable
from neo.lib.protocol import NodeTypes, NodeStates, Packets, ProtocolError
from neo.lib.util import dump
from . import BaseHandler, AnswerBaseHandler
from ..exception import NEOStorageError

class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def notReady(self, conn, message):
        app = self.app
        app.trying_master_node = None

    def _acceptIdentification(self, node, uuid, num_partitions,
            num_replicas, your_uuid, primary, known_master_list):
        app = self.app

        # Register new master nodes.
        found = False
        conn_address = node.getAddress()
        for node_address, node_uuid in known_master_list:
            if node_address == conn_address:
                assert uuid == node_uuid, (dump(uuid), dump(node_uuid))
                found = True
            n = app.nm.getByAddress(node_address)
            if n is None:
                n = app.nm.createMaster(address=node_address)
            if node_uuid is not None and n.getUUID() != node_uuid:
                n.setUUID(node_uuid)
        assert found, (node, dump(uuid), known_master_list)

        conn = node.getConnection()
        if primary is not None:
            primary_node = app.nm.getByAddress(primary)
            if primary_node is None:
                # I don't know such a node. Probably this information
                # is old. So ignore it.
                logging.warning('Unknown primary master: %s. Ignoring.',
                                primary)
                return
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
            return

        # the master must give an UUID
        if your_uuid is None:
            raise ProtocolError('No UUID supplied')
        app.uuid = your_uuid
        logging.info('Got an UUID: %s', dump(app.uuid))

        # Always create partition table
        app.pt = PartitionTable(num_partitions, num_replicas)
        app.master_conn = conn

    def answerPartitionTable(self, conn, ptid, row_list):
        assert row_list
        self.app.pt.load(ptid, row_list, self.app.nm)

    def answerNodeInformation(self, conn):
        pass

class PrimaryNotificationsHandler(BaseHandler):
    """ Handler that process the notifications from the primary master """

    def packetReceived(self, conn, packet, kw={}):
        if type(packet) is Packets.AnswerTransactionFinished:
            callback = kw.pop('callback')
            if callback is not None:
                callback(packet.decode()[1])
        BaseHandler.packetReceived(self, conn, packet, kw)

    def connectionClosed(self, conn):
        app = self.app
        if app.master_conn is not None:
            logging.critical("connection to primary master node closed")
            app.master_conn = None
        app.primary_master_node = None
        super(PrimaryNotificationsHandler, self).connectionClosed(conn)

    def stopOperation(self, conn):
        logging.critical("master node ask to stop operation")

    def invalidateObjects(self, conn, tid, oid_list):
        app = self.app
        app._cache_lock_acquire()
        try:
            invalidate = app._cache.invalidate
            loading = app._loading_oid
            for oid in oid_list:
                try:
                    invalidate(oid, tid)
                except KeyError:
                    if oid == loading:
                        app._loading_oid = None
                        app._loading_invalidated = tid
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
        self.app.new_oid_list = oid_list

    def answerTransactionFinished(self, conn, _, tid):
        self.app.setHandlerData(tid)

    def answerPack(self, conn, status):
        if not status:
            raise NEOStorageError('Already packing')

    def answerLastTransaction(self, conn, ltid):
        self.app.setHandlerData(ltid)

