import logging

from neo.storage.handler import StorageEventHandler
from neo.protocol import INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE
from neo.utils import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connetion import ClientConnection
from neo.protocol import Packet

class VerificationEventHandler(StorageEventHandler):
    """This class deals with events for a verification phase."""

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # I do not want to accept a connection at this phase, but
        # someone might mistake me as a master node.
        StorageEventHandler.connectionAccepted(self, conn, s, addr)

    def timeoutExpired(self, conn):
        if isinstance(conn, ClientConnection):
            # If a primary master node timeouts, I cannot continue.
            logging.critical('the primary master node times out')
            raise PrimaryFailure('the primary master node times out')

        StorageEventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        if isinstance(conn, ClientConnection):
            # If a primary master node closes, I cannot continue.
            logging.critical('the primary master node is dead')
            raise PrimaryFailure('the primary master node is dead')

        StorageEventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        if isinstance(conn, ClientConnection):
            # If a primary master node gets broken, I cannot continue.
            logging.critical('the primary master node is broken')
            raise PrimaryFailure('the primary master node is broken')

        StorageEventHandler.peerBroken(self, conn)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        if isinstance(conn, ClientConnection):
            self.handleUnexpectedPacket(conn, packet)
        else:
            if node_type != MASTER_NODE_TYPE:
                logging.info('reject a connection from a non-master')
                conn.addPacket(Packet().notReady(packet.getId(), 'retry later'))
                conn.abort()
                return
            if name != app.name:
                logging.error('reject an alien cluster')
                conn.addPacket(Packet().protocolError(packet.getId(),
                                                      'invalid cluster name'))
                conn.abort()
                return

            addr = (ip_address, port)
            node = app.nm.getNodeByServer(addr)
            if node is None:
                node = MasterNode(server = addr, uuid = uuid)
                app.nm.add(node)
            else:
                # If this node is broken, reject it.
                if node.getUUID() == uuid:
                    if node.getState() == BROKEN_STATE:
                        p = Packet()
                        p.brokenNodeDisallowedError(packet.getId(), 'go away')
                        conn.addPacket(p)
                        conn.abort()
                        return

            # Trust the UUID sent by the peer.
            node.setUUID(uuid)
            conn.setUUID(uuid)

            p = Packet()
            p.acceptNodeIdentification(packet.getId(), STORAGE_NODE_TYPE,
                                       app.uuid, app.server[0], app.server[1])
            conn.addPacket(p)

            # Now the master node should know that I am not the right one.
            conn.abort()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskLastIDs(self, conn, packet):
        if isinstance(conn, ClientConnection):
            app = self.app
            p = Packet()
            p.answerLastIDs(packet.getId(), app.dm.getLastOID(), 
                            app.dm.getLastTID(), app.ptid)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        if isinstance(conn, ClientConnection):
            app = self.app
            row_list = []
            try:
                for offset in offset_list:
                    row = []
                    for cell in app.pt.getCellList(offset):
                        row.append((cell.getUUID(), cell.getState()))
                    row_list.append((offset, row))
            except IndexError:
                p = Packet()
                p.protocolError(packet.getId(), 
                                'invalid partition table offset')
                conn.addPacket(p)
                return

            p = Packet()
            p.answerPartitionTable(packet.getId(), app.ptid, row_list)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        """A primary master node sends this packet to synchronize a partition
        table. Note that the message can be split into multiple packets."""
        if isinstance(conn, ClientConnection):
            app = self.app
            nm = app.nm
            pt = app.pt
            if app.ptid != ptid:
                app.ptid = ptid
                pt.clear()

            for offset, row in row_list:
                for uuid, state in row:
                    node = nm.getNodeByUUID(uuid)
                    if node is None:
                        node = StorageNode(uuid = uuid)
                        if uuid != self.uuid:
                            node.setState(TEMPORARILY_DOWN_STATE)
                        nm.add(node)

                    pt.setCell(offset, node, state)

            if pt.filled():
                # If the table is filled, I assume that the table is ready
                # to use. Thus install it into the database for persistency.
                cell_list = []
                for offset in xrange(app.num_partitions):
                    for cell in pt.getCellList(offset):
                        cell_list.append((offset, cell.getUUID(), 
                                          cell.getState()))
                app.dm.setPartitionTable(ptid, cell_list)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        if isinstance(conn, ClientConnection):
            app = self.app
            nm = app.nm
            pt = app.pt
            if app.ptid >= ptid:
                # Ignore this packet.
                return

            # First, change the table on memory.
            app.ptid = ptid
            for offset, uuid, state in cell_list:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    if uuid != self.uuid:
                        node.setState(TEMPORARILY_DOWN_STATE)
                    nm.add(node)

                pt.setCell(offset, node, state)

            # Then, the database.
            app.dm.changePartitionTable(ptid, cell_list)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStartOperation(self, conn, packet):
        if isinstance(conn, ClientConnection):
            self.app.operational = True
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        if isinstance(conn, ClientConnection):
            raise OperationFailure('operation stopped')
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        if isinstance(conn, ClientConnection):
            app = self.app
            tid_list = app.dm.getUnfinishedTIDList()
            p = Packet()
            p.answerUnfinishedTransactions(packet.getId(), tid_list)
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskTransactionInformation(self, conn, packet, tid):
        app = self.app
        if isinstance(conn, ClientConnection):
            # If this is from a primary master node, assume that it wants
            # to obtain information about the transaction, even if it has
            # not been finished.
            t = app.dm.getTransaction(tid, all = True)
        else:
            t = app.dm.getTransaction(tid)

        p = Packet()
        if t is None:
            p.tidNotFound(packet.getId(), '%s does not exist' % dump(tid))
        else:
            p.answerTransactionInformation(packet.getId(), tid, 
                                           t[1], t[2], t[0])
        conn.addPacket(p)

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            p = Packet()
            if app.dm.objectPresent(oid, tid):
                p.answerObjectPresent(packet.getId(), oid, tid)
            else:
                p.oidNotFound(packet.getId(), 
                              '%s:%s do not exist' % (dump(oid), dump(tid)))
            conn.addPacket(p)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleDeleteTransaction(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.dm.deleteTransaction(tid, all = True)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleCommitTransaction(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            app.dm.finishTransaction(tid)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleLockInformation(self, conn, packet, tid):
        pass

    def handleUnlockInformation(self, conn, packet, tid):
        pass
