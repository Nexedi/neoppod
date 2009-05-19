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
        BROKEN_STATE, FEEDING_STATE, DISCARDED_STATE
from neo.node import MasterNode, StorageNode, ClientNode
from neo.pt import PartitionTable
from neo.client.exception import NEOStorageError
from neo.exception import ElectionFailure
from neo.util import dump

from ZODB.TimeStamp import TimeStamp
from ZODB.utils import p64

class BaseClientEventHandler(EventHandler):
    """Base class for client-side EventHandler implementations."""

    def __init__(self, app, dispatcher):
        self.app = app
        self.dispatcher = dispatcher
        super(BaseClientEventHandler, self).__init__()

    def dispatch(self, conn, packet):
        # Before calling superclass's dispatch method, lock the connection.
        # This covers the case where handler sends a response to received
        # packet.
        conn.lock()
        try:
            super(BaseClientEventHandler, self).dispatch(conn, packet)
        finally:
            conn.release()

    def packetReceived(self, conn, packet):
        """Redirect all received packet to dispatcher thread."""
        queue = self.dispatcher.getQueue(conn, packet)
        if queue is None:
            self.dispatch(conn, packet)
        else:
            queue.put((conn, packet))

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


class PrimaryBoostrapEventHandler(BaseClientEventHandler):
    # Bootstrap handler used when looking for the primary master

    def connectionFailed(self, conn):
        if self.app.primary_master_node is None:
            self.app.primary_master_node = -1
        super(PrimaryBoostrapEventHandler, self).connectionFailed(conn)
    
    def connectionClosed(self, conn):
        if self.app.primary_master_node is None:
            self.app.primary_master_node = -1
        super(PrimaryBoostrapEventHandler, self).connectionClosed(conn)

    def peerBroken(self, conn):
        if self.app.primary_master_node is None:
            self.app.primary_master_node = -1
        super(PrimaryBoostrapEventHandler, self).peerBroken(conn)

    def timeoutExpired(self, conn):
        if self.app.primary_master_node is None:
            self.app.primary_master_node = -1
        super(PrimaryBoostrapEventHandler, self).timeoutExpired(conn)

    def handleNotReady(self, conn, packet, message):
        self.app.setNodeNotReady()
        
    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        # this must be a master node
        if node_type != MASTER_NODE_TYPE:
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

        if your_uuid != INVALID_UUID:
            # got an uuid from the primary master
            app.uuid = your_uuid

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
            self.dispatcher.register(conn, msg_id, app.getQueue())
        finally:
            conn.unlock()

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # This must be sent only by master node
        if node.getNodeType() != MASTER_NODE_TYPE:
            conn.close()
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
                if n.getUUID() is None or n.getUUID() != uuid:
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

    def handleNotifyNodeInformation(self, conn, packet, node_list):
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
        if node.getNodeType() != MASTER_NODE_TYPE:
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
                continue

            n.setState(state)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        # This handler is in PrimaryBoostrapEventHandler, since this
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
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

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


class PrimaryEventHandler(BaseClientEventHandler):
    # Handler used to communicate with the primary master

    def connectionClosed(self, conn):
        logging.critical("connection to primary master node closed")
        # Close connection
        app = self.app
        app.master_conn.close()
        app.master_conn = None
        app.primary_master_node = None
        app.connectToPrimaryMasterNode()
        super(PrimaryEventHandler, self).peerBroken(conn)

    def timeoutExpired(self, conn):
        logging.critical("connection timeout to primary master node expired")
        self.app.connectToPrimaryMasterNode()
        super(PrimaryEventHandler, self).timeoutExpired(conn)

    def peerBroken(self, conn):
        logging.critical("primary master node is broken")
        self.app.connectToPrimaryMasterNode()
        super(PrimaryEventHandler, self).peerBroken(conn)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        nm = app.nm
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
                continue

            n.setState(state)

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
            if state in (DISCARDED_STATE, FEEDING_STATE):
                pt.dropNode(node)
            else:
                pt.setCell(offset, node, state)

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


class StorageBootstrapEventHandler(BaseClientEventHandler):
    # Handler used when connecting to a storage node

    def connectionFailed(self, conn):
        # Connection to a storage node failed
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)
        super(StorageBootstrapEventHandler, self).connectionFailed(conn)

    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        logging.info("connection to storage node %s closed", node.getServer())
        self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)
        super(StorageBootstrapEventHandler, self).connectionClosed(conn)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)
        super(StorageBootstrapEventHandler, self).timeoutExpired(conn)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node, BROKEN_STATE)
        super(StorageBootstrapEventHandler, self).peerBroken(conn)

    def handleNotReady(self, conn, packet, message):
        app = self.app
        app.setNodeNotReady()
        
    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        app = self.app
        node = app.nm.getNodeByServer(conn.getAddress())
        # It can be eiter a master node or a storage node
        if node_type != STORAGE_NODE_TYPE:
            conn.close()
            return
        if conn.getAddress() != (ip_address, port):
            # The server address is different! Then why was
            # the connection successful?
            logging.error('%s:%d is waiting for %s:%d',
                  conn.getAddress()[0], conn.getAddress()[1], ip_address, port)
            app.nm.remove(node)
            conn.close()
            return

        conn.setUUID(uuid)
        node.setUUID(uuid)


class StorageEventHandler(BaseClientEventHandler):
    # Handle all messages related to ZODB operations

    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        logging.info("connection to storage node %s closed", node.getServer())
        self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)
        super(StorageEventHandler, self).connectionClosed(conn)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node, TEMPORARILY_DOWN_STATE)
        super(StorageEventHandler, self).timeoutExpired(conn)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByServer(conn.getAddress())
        self._dealWithStorageFailure(conn, node, BROKEN_STATE)
        super(StorageEventHandler, self).peerBroken(conn)

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

    def handleStopOperation(self, conn, packet):
        logging.critical("master node ask to stop operation")
        
    def handleAnswerObject(self, conn, packet, oid, start_serial, end_serial, compression,
                           checksum, data):
        app = self.app
        app.local_var.asked_object = (oid, start_serial, end_serial, compression,
                                      checksum, data)

    def handleAnswerStoreObject(self, conn, packet, conflicting, oid, serial):
        app = self.app
        if conflicting:
            app.local_var.object_stored = -1, serial
        else:
            app.local_var.object_stored = oid, serial

    def handleAnswerStoreTransaction(self, conn, packet, tid):
        app = self.app
        app.setTransactionVoted()

    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        app = self.app
        # transaction information are returned as a dict
        info = {}
        info['time'] = TimeStamp(tid).timeTime()
        info['user_name'] = user
        info['description'] = desc
        info['id'] = tid
        info['oids'] = oid_list
        app.local_var.txn_info = info

    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        app = self.app
        # history_list is a list of tuple (serial, size)
        app.local_var.history = oid, history_list

    def handleOidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen either when :
        # - loading an object
        # - asking for history
        app.local_var.asked_object = -1
        app.local_var.history = -1

    def handleTidNotFound(self, conn, packet, message):
        app = self.app
        # This can happen when requiring txn informations
        app.local_var.txn_info = -1

    def handleAnswerTIDs(self, conn, packet, tid_list):
        app = self.app
        app.local_var.node_tids[conn.getUUID()] = tid_list

