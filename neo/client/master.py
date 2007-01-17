import logging

from neo.handler import EventHandler
from neo.connection import ClientConnection
from neo.protocol import Packet, MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
     INVALID_UUID
from neo.node import MasterNode, StorageNode, ClientNode
from neo.pt import PartitionTable

class MasterEventHandler(EventHandler):
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
                              conn.getAddress()[0], conn.getAddress()[1], ip_address, port)
                app.nm.remove(node)
                conn.close()
                return

            conn.setUUID(uuid)
            node.setUUID(uuid)

            # Create partition table if necessary
            if app.pt is None:
                app.pt = PartitionTable(num_partitions, num_replicas)
            
            # Ask a primary master.
            msg_id = conn.getNextId()
            conn.addPacket(Packet().askPrimaryMaster(msg_id))
            conn.expectMessage(msg_id)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        if isinstance(conn, ClientConnection):
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

    def handleSendPartitionTable(self, conn, packet, row_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            for offset, node in row_list:
                app.pt.setRow(offset, row)                
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
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
                    elif node_typ == STORAGE_NODE:
                        n = StorageNode(server = addr)
                    elif node_typ == CLIENT_NODE:
                        n = ClientNode(server = addr)
                    else:
                        continue
                    app.app.nm.add(n)

                if uuid != INVALID_UUID:
                    # If I don't know the UUID yet, believe what the peer
                    # told me at the moment.
                    if n.getUUID() is None:
                        n.setUUID(uuid)
        
    def handleNotifyPartitionChanges(self, conn, packet, cell_list):
        if isinstance(conn, ClientConnection):
            app = self.app
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
                # what's this ?
                raise 
            else:
                app.txn_finished = 1
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleInvalidateObjects(self, conn, packet, oid_list):
        raise NotImplementedError('this method must be overridden')

    def handleAnswerNewOIDList(self, conn, packet, oid_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.new_oid_list = oid_list
            app.new_oid_list.reverse()
        else:
            self.handleUnexpectedPacket(conn, packet)
