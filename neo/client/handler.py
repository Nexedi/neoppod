import logging

from neo.handler import EventHandler
from neo.connection import ClientConnection
from neo.protocol import Packet, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
     INVALID_UUID
from neo.node import MasterNode, StorageNode, ClientNode
from neo.pt import PartitionTable

from ZODB.TimeStamp import TimeStamp
from ZODB.utils import p64

class ClientEventHandler(EventHandler):
    """This class deals with events for a master."""

    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

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

                # Ask a primary master.
                msg_id = conn.getNextId()
                conn.addPacket(Packet().askPrimaryMaster(msg_id))
                conn.expectMessage(msg_id)
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
            node = app.nm.getNodeByUUID(uuid)
            # This must be sent only by primary master node
            if not isinstance(node, MasterNode) \
                   or app.primary_master_node is None \
                   or app.primary_master_node.getUUID() != uuid:
                return

            for offset, node in row_list:
                app.pt.setRow(offset, row)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        if isinstance(conn, ClientConnection):
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

            for node_type, ip_address, port, uuid, state in node_list:
                # Register new nodes.
                addr = (ip_address, port)
                if app.server == addr:
                    # This is self.
                    continue
                else:
                    n = app.app.nm.getNodeByServer(addr)
                    if n is None:
                        if node_type == MASTER_NODE:
                            n = MasterNode(server = addr)
                            if uuid != INVALID_UUID:
                                # If I don't know the UUID yet, believe what the peer
                                # told me at the moment.
                                if n.getUUID() is None:
                                    n.setUUID(uuid)
                        elif node_typ == STORAGE_NODE:
                            if uuid == INVALID_UUID:
                                # No interest.
                                continue
                            n = StorageNode(server = addr)
                        elif node_typ == CLIENT_NODE:
                            if uuid == INVALID_UUID:
                                # No interest.
                                continue
                            n = ClientNode(server = addr)
                        else:
                            continue
                        app.app.nm.add(n)
                    n.setState(state)

        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        if isinstance(conn, ClientConnection):
            app = self.app
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

            for cell in cell_list:
                app.pt.addNode(cell)
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
                # What's this ?
                raise NEOStorageError
            else:
                app.txn_finished = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleInvalidateObjects(self, conn, packet, oid_list):
        raise NotImplementedError('this method must be overridden')

    def handleAnswerNewOIDs(self, conn, packet, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.new_oid_list = oid_list
            app.new_oid_list.reverse()
        else:
            self.handleUnexpectedPacket(conn, packet)

    # Storage node handler
    def handleAnwserObjectByOID(self, oid, start_serial, end_serial, compression,
                                checksum, data):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.loaded_object = (oid, start_serial, end_serial, compression,
                                 checksum, data)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreObject(self, conflicting, oid, serial):
        if isinstance(conn, ClientConnection):
            app = self.app
            if conflicting == '1':
                app.object_stored = -1
            else:
                app.object_stored = oid, serial
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerStoreTransaction(self, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.txn_stored = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerTransactionInformation(self, tid, user, desc, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            # transaction information are returned as a dict
            info = {}
            info['time'] = TimeStamp(p64(long(tid))).timeTime()
            info['user_name'] = user
            info['description'] = desc
            info['id'] = p64(long(tid))
            info['oids'] = oid_list
            app.txn_info = info
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerObjectHistory(self, oid, history_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            # history_list is a list of tuple (serial, size)
            self.history = oid, history_list
        else:
            self.handleUnexpectedPacket(conn, packet)

