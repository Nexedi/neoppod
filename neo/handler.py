import logging

from neo.protocol import Packet, ProtocolError
from neo.connection import ServerConnection

from protocol import ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
        PING, PONG, ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, ANNOUNCE_PRIMARY_MASTER, \
        REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION, START_OPERATION, \
        STOP_OPERATION, ASK_FINISHING_TRANSACTIONS, ANSWER_FINISHING_TRANSACTIONS, \
        FINISH_TRANSACTIONS, \
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
        pass

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        pass

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        pass

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        new_conn = ServerConnection(conn.getEventManager(), conn.getHandler(),
                                    s = s, addr = addr)
        # A request for a node identification should arrive.
        new_conn.expectMessage(timeout = 10, additional_timeout = 0)

    def timeoutExpired(self, conn):
        """Called when a timeout event occurs."""
        pass

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        pass

    def packetReceived(self, conn, packet):
        """Called when a packet is received."""
        self.dispatch(conn, packet)

    def packetMalformed(self, conn, packet, error_message):
        """Called when a packet is malformed."""
        logging.info('malformed packet: %s', error_message)
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
            args = packet.decode()
            method(conn, packet, *args)
        except ValueError:
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

    def handleAskPrimaryNode(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryNode(self, conn, packet, primary_uuid, known_master_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAnnouncePrimaryMaster(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleReelectPrimaryMaster(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
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
