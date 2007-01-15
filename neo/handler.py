import logging

from neo.protocol import Packet, ProtocolError
from neo.connection import ServerConnection

from protocol import ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
        PING, PONG, ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, ANNOUNCE_PRIMARY_MASTER, \
        REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION, START_OPERATION, \
        STOP_OPERATION, ASK_LAST_IDS, ANSWER_LAST_IDS, ASK_PARTITION_TABLE, \
        ANSWER_PARTITION_TABLE, SEND_PARTITION_TABLE, NOTIFY_PARTITION_CHANGES, \
        ASK_UNFINISHED_TRANSACTIONS, ANSWER_UNFINISHED_TRANSACTIONS, \
        ASK_OIDS_BY_TID, ANSWER_OIDS_BY_TID, ASK_OBJECT_PRESENT, ANSWER_OBJECT_PRESENT, \
        DELETE_TRANSACTION, COMMIT_TRANSACTION, ASK_NEW_TID, ANSWER_NEW_TID, \
        FINISH_TRANSACTION, NOTIFY_TRANSACTION_FINISHED, LOCK_INFORMATION, \
        NOTIFY_INFORMATION_LOCKED, INVALIDATE_OBJECTS, UNLOCK_INFORMATION, \
        NOT_READY_CODE, OID_NOT_FOUND_CODE, SERIAL_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
        PROTOCOL_ERROR_CODE, TIMEOUT_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE, \
        INTERNAL_ERROR_CODE

class EventHandler(object):
    """This class handles events."""
    def __init__(self):
        self.initPacketDispatchTable()
        self.initErrorDispatchTable()

    def connectionStarted(self, conn):
        """Called when a connection is started."""
        logging.debug('connection started for %s:%d', *(conn.getAddress()))

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        logging.debug('connection completed for %s:%d', *(conn.getAddress()))

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        logging.debug('connection failed for %s:%d', *(conn.getAddress()))

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        logging.debug('connection accepted from %s:%d', *addr)
        new_conn = ServerConnection(conn.getEventManager(), conn.getHandler(),
                                    s = s, addr = addr)
        # A request for a node identification should arrive.
        new_conn.expectMessage(timeout = 10, additional_timeout = 0)

    def timeoutExpired(self, conn):
        """Called when a timeout event occurs."""
        logging.debug('timeout expired for %s:%d', *(conn.getAddress()))

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        logging.debug('connection closed for %s:%d', *(conn.getAddress()))

    def packetReceived(self, conn, packet):
        """Called when a packet is received."""
        logging.debug('packet received from %s:%d', *(conn.getAddress()))
        self.dispatch(conn, packet)

    def packetMalformed(self, conn, packet, error_message):
        """Called when a packet is malformed."""
        logging.info('malformed packet from %s:%d: %s', 
                     conn.getAddress()[0], conn.getAddress()[1], error_message)
        conn.addPacket(Packet().protocolError(packet.getId(), error_message))
        conn.abort()
        self.peerBroken(conn)

    def peerBroken(self, conn):
        """Called when a peer is broken."""
        logging.error('%s:%d is broken', *(conn.getAddress()))

    def dispatch(self, conn, packet):
        """This is a helper method to handle various packet types."""
        t = packet.getType()
        try:
            method = self.packet_dispatch_table[t]
            args = packet.decode() or ()
            method(conn, packet, *args)
        except (KeyError, ValueError):
            self.handleUnexpectedPacket(conn, packet)
        except ProtocolError, m:
            self.packetMalformed(conn, packet, m[1])

    def handleUnexpectedPacket(self, conn, packet, message = None):
        """Handle an unexpected packet."""
        if message is None:
            message = 'unexpected packet type %d' % packet.getType()
        else:
            message = 'unexpected packet: ' + message
        logging.info('%s', message)
        conn.addPacket(Packet().protocolError(packet.getId(), message))
        conn.abort()
        self.peerBroken(conn)

    # Packet handlers.

    def handleError(self, conn, packet, code, message):
        try:
            method = self.error_dispatch_table[code]
            method(conn, packet, message)
        except ValueError:
            self.handleUnexpectedPacket(conn, packet, message)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        self.handleUnexpectedPacket(conn, packet)

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port):
        self.handleUnexpectedPacket(conn, packet)

    def handlePing(self, conn, packet):
        logging.info('got a ping packet; am I overloaded?')
        conn.addPacket(Packet().pong(packet.getId()))

    def handlePong(self, conn, packet):
        pass

    def handleAskPrimaryMaster(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid, known_master_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnnouncePrimaryMaster(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleReelectPrimaryMaster(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskLastIDs(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPartitionTable(self, conn, packet, row_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleSendPartitionTable(self, conn, packet, row_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, cell_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleStartOperation(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskOIDsByTID(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerOIDsByTID(self, conn, packet, oid_list, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleDeleteTransaction(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleCommitTransaction(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskNewTID(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerNewTID(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyTransactionFinished(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleLockInformation(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyInformationLocked(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleInvalidateObjects(self, conn, packet, oid_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleUnlockInformation(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)


    # Error packet handlers.

    handleNotReady = handleUnexpectedPacket
    handleOidNotFound = handleUnexpectedPacket
    handleSerialNotFound = handleUnexpectedPacket
    handleTidNotFound = handleUnexpectedPacket

    def handleProtocolError(self, conn, packet, message):
        raise RuntimeError, 'protocol error: %s' % (message,)

    def handleTimeoutError(self, conn, packet, message):
        raise RuntimeError, 'timeout error: %s' % (message,)

    def handleBrokenNodeDisallowedError(self, conn, packet, message):
        raise RuntimeError, 'broken node disallowed error: %s' % (message,)

    def handleInternalError(self, conn, packet, message):
        self.peerBroken(conn)
        conn.close()

    def initPacketDispatchTable(self):
        d = {}

        d[ERROR] = self.handleError
        d[REQUEST_NODE_IDENTIFICATION] = self.handleRequestNodeIdentification
        d[ACCEPT_NODE_IDENTIFICATION] = self.handleAcceptNodeIdentification
        d[PING] = self.handlePing
        d[PONG] = self.handlePong
        d[ASK_PRIMARY_MASTER] = self.handleAskPrimaryMaster
        d[ANSWER_PRIMARY_MASTER] = self.handleAnswerPrimaryMaster
        d[ANNOUNCE_PRIMARY_MASTER] = self.handleAnnouncePrimaryMaster
        d[REELECT_PRIMARY_MASTER] = self.handleReelectPrimaryMaster
        d[NOTIFY_NODE_INFORMATION] = self.handleNotifyNodeInformation
        d[ASK_LAST_IDS] = self.handleAskLastIDs
        d[ANSWER_LAST_IDS] = self.handleAnswerLastIDs
        d[ASK_PARTITION_TABLE] = self.handleAskPartitionTable
        d[ANSWER_PARTITION_TABLE] = self.handleAnswerPartitionTable
        d[SEND_PARTITION_TABLE] = self.handleSendPartitionTable
        d[NOTIFY_PARTITION_CHANGES] = self.handleNotifyPartitionChanges
        d[START_OPERATION] = self.handleStartOperation
        d[STOP_OPERATION] = self.handleStopOperation
        d[ASK_UNFINISHED_TRANSACTIONS] = self.handleAskUnfinishedTransactions
        d[ANSWER_UNFINISHED_TRANSACTIONS] = self.handleAnswerUnfinishedTransactions
        d[ASK_OIDS_BY_TID] = self.handleAskOIDsByTID
        d[ANSWER_OIDS_BY_TID] = self.handleAnswerOIDsByTID
        d[ASK_OBJECT_PRESENT] = self.handleAskObjectPresent
        d[ANSWER_OBJECT_PRESENT] = self.handleAnswerObjectPresent
        d[DELETE_TRANSACTION] = self.handleDeleteTransaction
        d[COMMIT_TRANSACTION] = self.handleCommitTransaction
        d[ASK_NEW_TID] = self.handleAskNewTID
        d[ANSWER_NEW_TID] = self.handleAnswerNewTID
        d[FINISH_TRANSACTION] = self.handleFinishTransaction
        d[NOTIFY_TRANSACTION_FINISHED] = self.handleNotifyTransactionFinished
        d[LOCK_INFORMATION] = self.handleLockInformation
        d[NOTIFY_INFORMATION_LOCKED] = self.handleNotifyInformationLocked
        d[INVALIDATE_OBJECTS] = self.handleInvalidateObjects
        d[UNLOCK_INFORMATION] = self.handleUnlockInformation

        self.packet_dispatch_table = d

    def initErrorDispatchTable(self):
        d = {}

        d[NOT_READY_CODE] = self.handleNotReady
        d[OID_NOT_FOUND_CODE] = self.handleOidNotFound
        d[SERIAL_NOT_FOUND_CODE] = self.handleSerialNotFound
        d[TID_NOT_FOUND_CODE] = self.handleTidNotFound
        d[PROTOCOL_ERROR_CODE] = self.handleProtocolError
        d[TIMEOUT_ERROR_CODE] = self.handleTimeoutError
        d[BROKEN_NODE_DISALLOWED_CODE] = self.handleBrokenNodeDisallowedError
        d[INTERNAL_ERROR_CODE] = self.handleInternalError

        self.error_dispatch_table = d
