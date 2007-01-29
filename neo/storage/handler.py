import logging

from neo.handler import EventHandler
from neo.protocol import INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE
from neo.util import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection

class StorageEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        raise NotImplementedError('this method must be overridden')

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas):
        raise NotImplementedError('this method must be overridden')

    def handleAskPrimaryMaster(self, conn, packet):
        """This should not be used in reality, because I am not a master
        node. But? If someone likes to ask me, I can help."""
        app = self.app

        if app.primary_master_node is not None:
            primary_uuid = app.primary_master_node.getUUID()
        else:
            primary_uuid = INVALID_UUID

        known_master_list = []
        for n in app.nm.getMasterNodeList():
            if n.getState() == BROKEN_STATE:
                continue
            info = n.getServer() + (n.getUUID() or INVALID_UUID,)
            known_master_list.append(info)

        p = Packet()
        p.answerPrimaryMaster(packet.getId(), primary_uuid, known_master_list)
        conn.addPacket(p)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        raise NotImplementedError('this method must be overridden')

    def handleAnnouncePrimaryMaster(self, conn, packet):
        """Theoretically speaking, I should not get this message,
        because the primary master election must happen when I am
        not connected to any master node."""
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            raise RuntimeError('I do not know the uuid %r' % dump(uuid))

        if not isinstance(node, MasterNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        if app.primary_master_node is None:
            # Hmm... I am somehow connected to the primary master already.
            app.primary_master_node = node
            if not isinstance(conn, ClientConnection):
                # I do not want a connection from any master node. I rather
                # want to connect from myself.
                conn.close()
        elif app.primary_master_node.getUUID() == uuid:
            # Yes, I know you are the primary master node.
            pass
        else:
            # It seems that someone else claims taking over the primary
            # master node...
            app.primary_master_node = None
            raise PrimaryFailure('another master node wants to take over')

    def handleReelectPrimaryMaster(self, conn, packet):
        raise PrimaryFailure('re-election occurs')

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        # XXX it might be better to implement this callback in each handler.
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, MasterNode) \
                or app.primary_master_node is None \
                or app.primary_master_node.getUUID() != uuid:
            return

        for node_type, ip_address, port, uuid, state in node_list:
            addr = (ip_address, port)

            if node_type == MASTER_NODE_TYPE:
                n = app.nm.getNodeByServer(addr)
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)

                n.setState(state)
                if uuid != INVALID_UUID:
                    if n.getUUID() is None:
                        n.setUUID(uuid)

            elif node_type == STORAGE_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue

                n = app.nm.getNodeByUUID(uuid)
                if n is None:
                    n = StorageNode(server = addr, uuid = uuid)
                    app.nm.add(n)
                else:
                    n.setServer(addr)

                n.setState(state)

            elif node_type == CLIENT_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue

                if state == RUNNING_STATE:
                    n = app.nm.getNodeByUUID(uuid)
                    if n is None:
                        n = ClientNode(uuid = uuid)
                        app.nm.add(n)
                else:
                    n = app.nm.getNodeByUUID(uuid)
                    if n is not None:
                        app.nm.remove(n)

    def handleAskLastIDs(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAskPartitionTable(self, conn, packet, offset_list):
        raise NotImplementedError('this method must be overridden')

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        raise NotImplementedError('this method must be overridden')

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        raise NotImplementedError('this method must be overridden')

    def handleStartOperation(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleStopOperation(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAskUnfinishedTransactions(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAskTransactionInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        raise NotImplementedError('this method must be overridden')

    def handleDeleteTransaction(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleCommitTransaction(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleLockInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleUnlockInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleAskObject(self, conn, packet, oid, serial, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskTIDs(self, conn, packet, first, last):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskObjectHistory(self, conn, packet, oid, length):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskStoreObject(self, conn, packet, msg_id, oid, serial,
                             compression, data, checksum, tid):
        self.handleUnexpectedPacket(conn, packet)

