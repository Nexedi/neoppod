import logging

from neo.handler import EventHandler
from neo.connection import ClientConnection
from neo.protocol import Packet, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        INVALID_UUID, TEMPORARILY_DOWN_STATE, BROKEN_STATE
from neo.node import MasterNode, StorageNode, ClientNode
from neo.pt import PartitionTable
from neo.client.NEOStorage import NEOStorageError
from neo.exception import ElectionFailure

from ZODB.TimeStamp import TimeStamp


class ClientEventHandler(EventHandler):
    """This class deals with events for a master."""

    def __init__(self, app, dispatcher):
        self.app = app
        self.dispatcher = dispatcher
        EventHandler.__init__(self)

    def packetReceived(self, conn, packet):
        """Redirect all received packet to dispatcher thread."""
        self.dispatcher.message.put((conn, packet), True)

    def connectionFailed(self, conn):
        app = self.app
        uuid = conn.getUUID()
        if app.primary_master_node is None:
            # Failed to connect to a master node
            app.primary_master_node = -1
        elif uuid == self.app.primary_master_node.getUUID():
            logging.critical("connection to primary master node failed")
            raise NEOStorageError("connection to primary master node failed")
        else:
            # Connection to a storage node failed
            app.storage_node = -1
        EventHandler.connectionFailed(self, conn)

    def connectionClosed(self, conn):
        uuid = conn.getUUID()
        if self.app.master_conn is None:
            EventHandler.connectionClosed(self, conn)
        elif uuid == self.app.master_conn.getUUID():
            logging.critical("connection to primary master node closed")
            raise NEOStorageError("connection to primary master node closed")
        else:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            logging.info("connection to storage node %s closed" %(node.getServer(),))
            if isinstance(node, StorageNode):
                # Notify primary master node that a storage node is temporarily down
                conn = app.master_conn
                msg_id = conn.getNextId()
                p = Packet()
                ip_address, port =  node.getServer()
                node_list = [(STORAGE_NODE_TYPE, ip_address, port, node.getUUID(),
                             TEMPORARILY_DOWN_STATE),]
                p.notifyNodeInformation(msg_id, node_list)
                app.queue.put((None, msg_id, conn, p), True)
                # Remove from pool connection
                app.cm.removeConnection(node)
        EventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        uuid = conn.getUUID()
        if uuid == self.app.primary_master_node.getUUID():
            logging.critical("connection timeout to primary master node expired")
            raise NEOStorageError("connection timeout to primary master node expired")
        else:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if isinstance(node, StorageNode):
                # Notify primary master node that a storage node is temporarily down
                conn = app.master_conn
                msg_id = conn.getNextId()
                p = Packet()
                ip_address, port =  node.getServer()
                node_list = [(STORAGE_NODE_TYPE, ip_address, port, node.getUUID(),
                             TEMPORARILY_DOWN_STATE),]
                p.notifyNodeInformation(msg_id, node_list)
                app.queue.put((None, msg_id, conn, p), True)
                # Remove from pool connection
                app.cm.removeConnection(node)
        EventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        uuid = conn.getUUID()
        if uuid == self.app.primary_master_node.getUUID():
            logging.critical("primary master node is broken")
            raise NEOStorageError("primary master node is broken")
        else:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if isinstance(node, StorageNode):
                # Notify primary master node that a storage node is broken
                conn = app.master_conn
                msg_id = conn.getNextId()
                p = Packet()
                ip_address, port =  node.getServer()
                node_list = [(STORAGE_NODE_TYPE, ip_address, port, node.getUUID(),
                             BROKEN_STATE),]
                p.notifyNodeInformation(msg_id, node_list)
                app.queue.put((None, msg_id, conn, p), True)
                # Remove from pool connection
                app.cm.removeConnection(node)
        EventHandler.peerBroken(self, conn)


    def handleNotReady(self, conn, packet, message):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.node_not_ready = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas):
        if isinstance(conn, ClientConnection):
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
                msg_id = conn.getNextId()
                p = Packet()
                p.askPrimaryMaster(msg_id)
                # send message to dispatcher
                app.queue.put((app.local_var.tmp_q, msg_id, conn, p), True)
            elif node_type == STORAGE_NODE_TYPE:
                app.storage_node = node
        else:
            self.handleUnexpectedPacket(conn, packet)


    # Master node handler
    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        if isinstance(conn, ClientConnection):
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
        if isinstance(conn, ClientConnection):
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
                        if uuid != app.uuid:
                            node.setState(TEMPORARILY_DOWN_STATE)
                        nm.add(node)
                    pt.setCell(offset, node, state)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        if isinstance(conn, ClientConnection):
            uuid = conn.getUUID()
            if uuid is None:
                self.handleUnexpectedPacket(conn, packet)
                return

            app = self.app
            nm = app.nm
            node = nm.getNodeByUUID(uuid)
            # This must be sent only by primary master node
            if not isinstance(node, MasterNode) \
                   or app.primary_master_node is None \
                   or app.primary_master_node.getUUID() != uuid:
                return

            for node_type, ip_address, port, uuid, state in node_list:
                # Register new nodes.
                addr = (ip_address, port)
                n = nm.getNodeByServer(addr)
                if n is None:
                    if node_type == MASTER_NODE_TYPE:
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
                        n = StorageNode(server = addr, uuid = uuid)
                        nm.add(n)
                    elif node_type == CLIENT_NODE_TYPE:
                        if uuid == INVALID_UUID:
                            # No interest.
                            continue
                        n = ClientNode(server = addr, uuid = uuid)
                        nm.add(n)
                    else:
                        continue

                n.setState(state)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        if isinstance(conn, ClientConnection):
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
        if isinstance(conn, ClientConnection):
            app = self.app
            app.tid = tid
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyTransactionFinished(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            if tid != app.tid:
                app.txn_finished = -1
            else:
                app.txn_finished = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleInvalidateObjects(self, conn, packet, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            app._cache_lock_acquire()
            try:
                for oid in oid_list:
                    if app.cache.has_key(oid):
                        del app.cache[oid]
            finally:
                app._cache_lock_release()
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerNewOIDs(self, conn, packet, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.new_oid_list = oid_list
            app.new_oid_list.reverse()
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        if isinstance(conn, ClientConnection):
            raise NEOStorageError('operation stopped')
        else:
            self.handleUnexpectedPacket(conn, packet)


    # Storage node handler
    def handleAnwserObject(self, conn, packet, oid, start_serial, end_serial, compression,
                           checksum, data):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.local_var.loaded_object = (oid, start_serial, end_serial, compression,
                                           checksum, data)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreObject(self, conn, packet, conflicting, oid, serial):
        if isinstance(conn, ClientConnection):
            app = self.app
            if conflicting == '1':
                app.object_stored = -1, serial
            else:
                app.object_stored = oid, serial
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreTransaction(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.txn_voted = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerTransactionInformation(self, conn, packet, tid, user, desc, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            # transaction information are returned as a dict
            info = {}
            info['time'] = TimeStamp(p64(long(tid))).timeTime()
            info['user_name'] = user
            info['description'] = desc
            info['id'] = p64(long(tid))
            info['oids'] = oid_list
            app.local_var.txn_info = info
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            # history_list is a list of tuple (serial, size)
            self.history = oid, history_list
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleOidNotFound(self, conn, packet, message):
        if isinstance(conn, ClientConnection):
            app = self.app
            # This can happen either when :
            # - loading an object
            # - asking for history
            app.local_var.asked_object = -1
            app.local_var.history = -1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleTidNotFound(self, conn, packet, message):
        if isinstance(conn, ClientConnection):
            app = self.app
            # This can happen when requiring txn informations
            app.local_var.txn_info = -1
        else:
            self.handleUnexpectedPacket(conn, packet)

