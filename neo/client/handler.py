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

from neo.handler import EventHandler
from neo.connection import MTClientConnection
from neo.protocol import Packet, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        INVALID_UUID, RUNNING_STATE, TEMPORARILY_DOWN_STATE, \
        BROKEN_STATE, PING
from neo.node import MasterNode, StorageNode, ClientNode
from neo.pt import PartitionTable
from neo.client.exception import NEOStorageError
from neo.exception import ElectionFailure
from neo.util import dump

from ZODB.TimeStamp import TimeStamp
from ZODB.utils import p64


class ClientEventHandler(EventHandler):
    """This class deals with events for a master."""

    def __init__(self, app, dispatcher):
        self.app = app
        self.dispatcher = dispatcher
        EventHandler.__init__(self)

    def dispatch(self, conn, packet):
        """Redirect all received packet to dispatcher thread."""
        dispatcher = self.dispatcher
        # Send message to waiting thread
        key = (id(conn), packet.getId())
        if key in dispatcher.message_table:
            queue = dispatcher.message_table.pop(key)
            queue.put((conn, packet))
        else:
            method_type = packet.getType()
            if method_type == PING:
                # must answer with no delay
                conn.lock()
                try:
                    conn.addPacket(Packet().pong(packet.getId()))
                finally:
                    conn.unlock()
            else:
                # put message in request queue
                dispatcher._request_queue.put((conn, packet))

    def _dealWithStorageFailure(self, conn, node, state):
        app = self.app

        # Remove from pool connection
        app.cp.removeConnection(node)

        # Put fake packets to task queues.
        queue_set = set()
        for key in self.dispatcher.message_table.keys():
            if id(conn) == key[0]:
                queue = self.dispatcher.message_table.pop(key)
                queue_set.add(queue)
        for queue in queue_set:
            queue.put((conn, None))

        # Notify the primary master node of the failure.
        conn = app.master_conn
        if conn is not None:
            conn.lock()
            try:
                msg_id = conn.getNextId()
                p = Packet()
                ip_address, port = node.getServer()
                node_list = [(STORAGE_NODE_TYPE, ip_address, port, 
                              node.getUUID(), state)]
                p.notifyNodeInformation(msg_id, node_list)
                conn.addPacket(p)
            finally:
                conn.unlock()
        
    def connectionFailed(self, conn):
        app = self.app
        uuid = conn.getUUID()
        if app.primary_master_node is None:
            # Failed to connect to a master node
            app.primary_master_node = -1
        elif self.app.primary_master_node is not None and uuid == \
                 self.app.primary_master_node.getUUID():
            logging.critical("connection to primary master node failed")
            self.dispatcher.connectToPrimaryMasterNode(app, conn)
        else:
            # Connection to a storage node failed
            node = app.nm.getNodeByServer(conn.getAddress())
            if isinstance(node, StorageNode):
                self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)

        EventHandler.connectionFailed(self, conn)

    def connectionClosed(self, conn):
        uuid = conn.getUUID()
        app = self.app
        if app.primary_master_node is None:
            # Failed to connect to a master node
            app.primary_master_node = -1
        if app.master_conn is not None and uuid == app.master_conn.getUUID():
            logging.critical("connection to primary master node closed")
            # Close connection
            app.master_conn.close()
            app.master_conn = None
            app.primary_master_node = None
            self.dispatcher.connectToPrimaryMasterNode(app, conn)
        else:
            node = app.nm.getNodeByServer(conn.getAddress())
            if isinstance(node, StorageNode):
                # Notify primary master node that a storage node is temporarily down
                logging.info("connection to storage node %s closed",
                             node.getServer())
                self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)

        EventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        uuid = conn.getUUID()
        app = self.app
        if app.primary_master_node is None:
            # Failed to connect to a master node
            app.primary_master_node = -1
        if app.master_conn is not None and uuid == app.primary_master_node.getUUID():
            logging.critical("connection timeout to primary master node expired")
            self.dispatcher.connectToPrimaryMasterNode(app, conn)
        else:
            node = app.nm.getNodeByServer(conn.getAddress())
            if isinstance(node, StorageNode):
                # Notify primary master node that a storage node is
                # temporarily down.
                self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)

        EventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        uuid = conn.getUUID()
        app = self.app
        if app.primary_master_node is None:
            # Failed to connect to a master node
            app.primary_master_node = -1
        if app.master_conn is not None and uuid == app.primary_master_node.getUUID():
            logging.critical("primary master node is broken")
            self.dispatcher.connectToPrimaryMasterNode(app, conn)
        else:
            node = app.nm.getNodeByServer(conn.getAddress())
            if isinstance(node, StorageNode):
                self._dealWithStorageFailure(conn, node, BROKEN_STATE)

        EventHandler.peerBroken(self, conn)


    def handleNotReady(self, conn, packet, message):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.local_var.node_not_ready = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas):
        if isinstance(conn, MTClientConnection):
            app = self.app
            node = app.nm.getNodeByServer(conn.getAddress())
            # It can be eiter a master node or a storage node
            if node_type == CLIENT_NODE_TYPE:
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

            conn.setUUID(uuid)
            node.setUUID(uuid)

            if node_type == MASTER_NODE_TYPE:
                # Create partition table if necessary
                if app.pt is None:
                    app.pt = PartitionTable(num_partitions, num_replicas)
                    app.num_partitions = num_partitions
                    app.num_replicas = num_replicas

                # Ask a primary master.
                conn.lock()
                try:
                    msg_id = conn.getNextId()
                    p = Packet()
                    p.askPrimaryMaster(msg_id)
                    conn.addPacket(p)
                    conn.expectMessage(msg_id)
                    app.dispatcher.register(conn, msg_id, app.getQueue())
                finally:
                    conn.unlock()
            elif node_type == STORAGE_NODE_TYPE:
                app.storage_node = node
        else:
            self.handleUnexpectedPacket(conn, packet)


    # Master node handler
    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        if isinstance(conn, MTClientConnection):
            uuid = conn.getUUID()
            if uuid is None:
                self.handleUnexpectedPacket(conn, packet)
                return

            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            # This must be sent only by primary master node
            if not isinstance(node, MasterNode):
                return
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
                    if n.getUUID() is None:
                        n.setUUID(uuid)

            if primary_uuid != INVALID_UUID:
                # The primary master is defined.
                if app.primary_master_node is not None \
                        and app.primary_master_node.getUUID() != primary_uuid:
                    # There are multiple primary master nodes. This is
                    # dangerous.
                    raise ElectionFailure, 'multiple primary master nodes'
                primary_node = app.nm.getNodeByUUID(primary_uuid)
                if primary_node is None:
                    # I don't know such a node. Probably this information
                    # is old. So ignore it.
                    pass
                else:
                    if primary_node.getUUID() == primary_uuid:
                        # Whatever the situation is, I trust this master.
                        app.primary_master_node = primary_node
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        if isinstance(conn, MTClientConnection):
            uuid = conn.getUUID()
            if uuid is None:
                self.handleUnexpectedPacket(conn, packet)
                return

            app = self.app
            nm = app.nm
            pt = app.pt
            node = app.nm.getNodeByUUID(uuid)
            # This must be sent only by primary master node
            if not isinstance(node, MasterNode):
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
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        if isinstance(conn, MTClientConnection):
            uuid = conn.getUUID()
            if uuid is None:
                self.handleUnexpectedPacket(conn, packet)
                return

            app = self.app
            nm = app.nm
            node = nm.getNodeByUUID(uuid)
            # This must be sent only by a primary master node.
            # Note that this may be sent before I know that it is
            # a primary master node.
            if not isinstance(node, MasterNode):
                logging.warn('ignoring notify node information from %s',
                             dump(uuid))
                return
            for node_type, ip_address, port, uuid, state in node_list:
                # Register new nodes.
                addr = (ip_address, port)

                if node_type == MASTER_NODE_TYPE:
                    n = nm.getNodeByServer(addr)
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
                    n = nm.getNodeByUUID(uuid)
                    if n is None:
                        n = StorageNode(server = addr, uuid = uuid)
                        nm.add(n)
                    else:
                        n.setServer(addr)
                elif node_type == CLIENT_NODE_TYPE:
                    if uuid == INVALID_UUID:
                        # No interest.
                        continue
                    if state == RUNNING_STATE:
                        n = app.nm.getNodeByUUID(uuid)
                        if n is None:
                            n = ClientNode(server = addr, uuid = uuid)
                            nm.add(n)
                    else:
                        n = app.nm.getNodeByUUID(uuid)
                        if n is not None:
                            app.nm.remove(n)
                    continue

                n.setState(state)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        if isinstance(conn, MTClientConnection):
            app = self.app
            nm = app.nm
            pt = app.pt
            uuid = conn.getUUID()
            if uuid is None:
                self.handleUnexpectedPacket(conn, packet)
                return

            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            # This must be sent only by primary master node
            if not isinstance(node, MasterNode) \
                   or app.primary_master_node is None \
                   or app.primary_master_node.getUUID() != uuid:
                return

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
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerNewTID(self, conn, packet, tid):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.tid = tid
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyTransactionFinished(self, conn, packet, tid):
        if isinstance(conn, MTClientConnection):
            app = self.app
            if tid != app.tid:
                app.txn_finished = -1
            else:
                app.txn_finished = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleInvalidateObjects(self, conn, packet, oid_list, tid):
        if isinstance(conn, MTClientConnection):
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
                if app._db is not None:
                    app._db.invalidate(tid, oids)
            finally:
                app._cache_lock_release()
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerNewOIDs(self, conn, packet, oid_list):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.new_oid_list = oid_list
            app.new_oid_list.reverse()
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        if isinstance(conn, MTClientConnection):
            logging.critical("master node ask to stop operation")
        else:
            self.handleUnexpectedPacket(conn, packet)


    # Storage node handler
    def handleAnswerObject(self, conn, packet, oid, start_serial, end_serial, compression,
                           checksum, data):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.local_var.asked_object = (oid, start_serial, end_serial, compression,
                                           checksum, data)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreObject(self, conn, packet, conflicting, oid, serial):
        if isinstance(conn, MTClientConnection):
            app = self.app
            if conflicting:
                app.txn_object_stored = -1, serial
            else:
                app.txn_object_stored = oid, serial
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreTransaction(self, conn, packet, tid):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.txn_voted = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        if isinstance(conn, MTClientConnection):
            app = self.app
            # transaction information are returned as a dict
            info = {}
            info['time'] = TimeStamp(tid).timeTime()
            info['user_name'] = user
            info['description'] = desc
            info['id'] = tid
            info['oids'] = oid_list
            app.local_var.txn_info = info
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        if isinstance(conn, MTClientConnection):
            app = self.app
            # history_list is a list of tuple (serial, size)
            app.local_var.history = oid, history_list
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleOidNotFound(self, conn, packet, message):
        if isinstance(conn, MTClientConnection):
            app = self.app
            # This can happen either when :
            # - loading an object
            # - asking for history
            app.local_var.asked_object = -1
            app.local_var.history = -1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleTidNotFound(self, conn, packet, message):
        if isinstance(conn, MTClientConnection):
            app = self.app
            # This can happen when requiring txn informations
            app.local_var.txn_info = -1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerTIDs(self, conn, packet, tid_list):
        if isinstance(conn, MTClientConnection):
            app = self.app
            app.local_var.node_tids[conn.getUUID()] = tid_list
        else:
            self.handleUnexpectedPacket(conn, packet)
