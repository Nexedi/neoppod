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

from neo.client.handlers import BaseHandler, AnswerBaseHandler
from neo.protocol import MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        INVALID_UUID, RUNNING_STATE, TEMPORARILY_DOWN_STATE
from neo.node import MasterNode, StorageNode
from neo.pt import MTPartitionTable as PartitionTable
from neo.util import dump
from neo import decorators

class PrimaryBootstrapHandler(AnswerBaseHandler):
    """ Bootstrap handler used when looking for the primary master """

    def handleNotReady(self, conn, packet, message):
        app = self.app
        app.trying_master_node = None
        app.setNodeNotReady()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        # this must be a master node
        if node_type != MASTER_NODE_TYPE:
            conn.lock()
            try:
                conn.close()
            finally:
                conn.release()
            return
        if conn.getAddress() != (ip_address, port):
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                          conn.getAddress()[0], conn.getAddress()[1],
                          ip_address, port)
            app.nm.remove(node)
            conn.lock()
            try:
                conn.close()
            finally:
                conn.release()
            return

        conn.setUUID(uuid)
        node.setUUID(uuid)

        if your_uuid != INVALID_UUID:
            # got an uuid from the primary master
            app.uuid = your_uuid

        # Always create partition table 
        app.pt = PartitionTable(num_partitions, num_replicas)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
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
                logging.warning('Unknown primary master UUID: %s. ' \
                                'Ignoring.' % dump(primary_uuid))
            else:
                app.primary_master_node = primary_node
                if app.trying_master_node is not primary_node:
                    app.trying_master_node = None
                    conn.lock()
                    try:
                        conn.close()
                    finally:
                        conn.release()
        else:
            if app.primary_master_node is not None:
                # The primary master node is not a primary master node
                # any longer.
                app.primary_master_node = None
 
            app.trying_master_node = None
            conn.lock()
            try:
                conn.close()
            finally:
                conn.release()
 
    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        pass
 
    def handleAnswerNodeInformation(self, conn, packet, node_list):
        pass

class PrimaryNotificationsHandler(BaseHandler):
    """ Handler that process the notifications from the primary master """

    def connectionClosed(self, conn):
        app = self.app
        if app.master_conn is not None:
            assert conn is app.master_conn
            logging.critical("connection to primary master node closed")
            conn.lock()
            try:
                app.master_conn.close()
            finally:
                conn.release()
            app.master_conn = None
            app.primary_master_node = None
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


    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        app = self.app
        nm = app.nm
        pt = app.pt

        if app.ptid >= ptid:
            # Ignore this packet.
            return
        app.ptid = ptid
        for offset, uuid, state in cell_list:
            node = nm.getNodeByUUID(uuid)
            if node is None:
                node = StorageNode(uuid = uuid)
                if uuid != app.uuid:
                    node.setState(TEMPORARILY_DOWN_STATE)
                nm.add(node)
            pt.setCell(offset, node, state)

    @decorators.identification_required
    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        # This handler is in PrimaryBootstrapHandler, since this
        # basicaly is an answer to askPrimaryMaster.
        # Extract from P-NEO-Protocol.Description:
        #  Connection to primary master node (PMN in service state)
        #   CN -> PMN : askPrimaryMaster
        #   PMN -> CN : answerPrimaryMaster containing primary uuid and no
        #               known master list
        #   PMN -> CN : notifyNodeInformation containing list of all
        #   ASK_STORE_TRANSACTION#   PMN -> CN : sendPartitionTable containing partition table id and
        #               list of rows
        # notifyNodeInformation is valid as asynchrounous event, but
        # sendPartitionTable is only triggered after askPrimaryMaster.
        uuid = conn.getUUID()
        app = self.app
        nm = app.nm
        pt = app.pt
        node = app.nm.getNodeByUUID(uuid)
        # This must be sent only by primary master node
        if node.getNodeType() != MASTER_NODE_TYPE:
            return

        if app.ptid != ptid:
            app.ptid = ptid
            pt.clear()
        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    node.setState(TEMPORARILY_DOWN_STATE)
                    nm.add(node)
                pt.setCell(offset, node, state)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        nm = app.nm
        for node_type, ip_address, port, uuid, state in node_list:
            logging.debug("notified of %s %s %d %s %s" %(node_type, ip_address, port, dump(uuid), state))
            # Register new nodes.
            addr = (ip_address, port)
            # Try to retrieve it from nm
            n = None
            if uuid != INVALID_UUID:
                n = nm.getNodeByUUID(uuid)
            if n is None:
                n = nm.getNodeByServer(addr)
                if n is not None and uuid != INVALID_UUID:
                    # node only exists by address, remove it
                    nm.remove(n)
                    n = None
            elif n.getServer() != addr:
                # same uuid but different address, remove it
                nm.remove(n)
                n = None
                 
            if node_type == MASTER_NODE_TYPE:
                if n is None:
                    n = MasterNode(server = addr)
                    nm.add(n)
                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)
            elif node_type == STORAGE_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue
                if n is None:
                    n = StorageNode(server = addr, uuid = uuid)
                    nm.add(n)
            elif node_type == CLIENT_NODE_TYPE:
                continue

            n.setState(state)
            # close connection to this node if no longer running
            if node_type in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE) and \
                   state != RUNNING_STATE:
                can_close = False
                for conn in self.app.em.getConnectionList():
                    if conn.getUUID() == n.getUUID():
                        conn.lock()
                        try:
                            conn.close()
                        finally:
                            conn.release()
                        can_close = True
                        break
                if can_close and node_type == STORAGE_NODE_TYPE:
                    # Remove from pool connection
                    app.cp.removeConnection(n)

                    # Put fake packets to task queues.
                    queue_set = set()
                    for key in self.dispatcher.message_table.keys():
                        if id(conn) == key[0]:
                            queue = self.dispatcher.message_table.pop(key)
                            queue_set.add(queue)
                    # Storage failure is notified to the primary master when the fake 
                    # packet if popped by a non-polling thread.
                    for queue in queue_set:
                        queue.put((conn, None))

class PrimaryAnswersHandler(AnswerBaseHandler):
    """ Handle that process expected packets from the primary master """

    def handleAnswerNewTID(self, conn, packet, tid):
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

