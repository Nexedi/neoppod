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

from neo import logging
from neo import protocol
from neo.protocol import NodeStates, ErrorCodes, PacketTypes
from neo.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError


class EventHandler(object):
    """This class handles events."""

    def __init__(self, app):
        self.app = app
        self.packet_dispatch_table = self.initPacketDispatchTable()
        self.error_dispatch_table = self.initErrorDispatchTable()

    def __packetMalformed(self, conn, packet, message='', *args):
        """Called when a packet is malformed."""
        args = (conn.getAddress()[0], conn.getAddress()[1], message)
        if packet is None:
            # if decoding fail, there's no packet instance 
            logging.error('malformed packet from %s:%d: %s', *args)
        else:
            logging.error('malformed packet %s from %s:%d: %s', packet.getType(), *args)
        response = protocol.protocolError(message)
        if packet is not None:
            conn.answer(response, packet.getId())
        else:
            conn.notify(response)
        conn.abort()
        self.peerBroken(conn)

    def __unexpectedPacket(self, conn, packet, message=None):
        """Handle an unexpected packet."""
        if message is None:
            message = 'unexpected packet type %s in %s' % (packet.getType(),
                    self.__class__.__name__)
        else:
            message = 'unexpected packet: %s in %s' % (message,
                    self.__class__.__name__)
        logging.error(message)
        conn.answer(protocol.protocolError(message), packet.getId())
        conn.abort()
        self.peerBroken(conn)

    def dispatch(self, conn, packet):
        """This is a helper method to handle various packet types."""
        try:
            try:
                method = self.packet_dispatch_table[packet.getType()]
            except KeyError:
                raise UnexpectedPacketError('no handler found')
            args = packet.decode() or ()
            method(conn, packet, *args)
        except UnexpectedPacketError, e:
            self.__unexpectedPacket(conn, packet, *e.args)
        except PacketMalformedError, e:
            self.__packetMalformed(conn, packet, *e.args)
        except BrokenNodeDisallowedError:
            answer_packet = protocol.brokenNodeDisallowedError('go away')
            conn.answer(answer_packet, packet.getId())
            conn.abort()
        except NotReadyError, message:
            if not message.args:
                message = 'Retry Later'
            message = str(message)
            conn.answer(protocol.notReady(message), packet.getId())
            conn.abort()
        except ProtocolError, message:
            message = str(message)
            conn.answer(protocol.protocolError(message), packet.getId())
            conn.abort()

    def checkClusterName(self, name):
        # raise an exception if the fiven name mismatch the current cluster name
        if self.app.name != name:
            logging.error('reject an alien cluster')
            raise protocol.ProtocolError('invalid cluster name')


    # Network level handlers

    def packetReceived(self, conn, packet):
        """Called when a packet is received."""
        self.dispatch(conn, packet)

    def connectionStarted(self, conn):
        """Called when a connection is started."""
        logging.debug('connection started for %s:%d', *(conn.getAddress()))

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        logging.debug('connection completed for %s:%d', *(conn.getAddress()))

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        logging.debug('connection failed for %s:%d', *(conn.getAddress()))

    def connectionAccepted(self, conn):
        """Called when a connection is accepted."""
        # A request for a node identification should arrive.
        conn.expectMessage(timeout = 10, additional_timeout = 0)

    def timeoutExpired(self, conn):
        """Called when a timeout event occurs."""
        logging.debug('timeout expired for %s:%d', *(conn.getAddress()))
        self.connectionLost(conn, NodeStates.TEMPORARILY_DOWN)

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        logging.debug('connection closed for %s:%d', *(conn.getAddress()))
        self.connectionLost(conn, NodeStates.TEMPORARILY_DOWN)

    def peerBroken(self, conn):
        """Called when a peer is broken."""
        logging.error('%s:%d is broken', *(conn.getAddress()))
        self.connectionLost(conn, NodeStates.BROKEN)

    def connectionLost(self, conn, new_state):
        """ this is a method to override in sub-handlers when there is no need
        to make distinction from the kind event that closed the connection  """
        pass


    # Packet handlers.

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        raise UnexpectedPacketError

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                       uuid, address, num_partitions, num_replicas, your_uuid):
        raise UnexpectedPacketError

    def handleAskPrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        raise UnexpectedPacketError

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def handleReelectPrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def handleAskLastIDs(self, conn, packet):
        raise UnexpectedPacketError

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        raise UnexpectedPacketError

    def handleAskPartitionTable(self, conn, packet, offset_list):
        raise UnexpectedPacketError

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        raise UnexpectedPacketError

    def handleStartOperation(self, conn, packet):
        raise UnexpectedPacketError

    def handleStopOperation(self, conn, packet):
        raise UnexpectedPacketError

    def handleAskUnfinishedTransactions(self, conn, packet):
        raise UnexpectedPacketError

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        raise UnexpectedPacketError

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        raise UnexpectedPacketError

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        raise UnexpectedPacketError

    def handleDeleteTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleCommitTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAskBeginTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAnswerBeginTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAskNewOIDs(self, conn, packet, num_oids):
        raise UnexpectedPacketError

    def handleAnswerNewOIDs(self, conn, packet, num_oids):
        raise UnexpectedPacketError

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        raise UnexpectedPacketError

    def handleNotifyTransactionFinished(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleLockInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleNotifyInformationLocked(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleInvalidateObjects(self, conn, packet, oid_list, tid):
        raise UnexpectedPacketError

    def handleUnlockInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAskStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        raise UnexpectedPacketError

    def handleAnswerStoreObject(self, conn, packet, conflicting, oid, serial):
        raise UnexpectedPacketError

    def handleAbortTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAskStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        raise UnexpectedPacketError

    def handleAnswerStoreTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAskObject(self, conn, packet, oid, serial, tid):
        raise UnexpectedPacketError

    def handleAnswerObject(self, conn, packet, oid, serial_start,
                           serial_end, compression, checksum, data):
        raise UnexpectedPacketError

    def handleAskTIDs(self, conn, packet, first, last, partition):
        raise UnexpectedPacketError

    def handleAnswerTIDs(self, conn, packet, tid_list):
        raise UnexpectedPacketError

    def handleAskTransactionInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def handleAnswerTransactionInformation(self, conn, packet, tid, 
                                           user, desc, ext, oid_list):
        raise UnexpectedPacketError

    def handleAskObjectHistory(self, conn, packet, oid, first, last):
        raise UnexpectedPacketError

    def handleAnswerObjectHistory(self, conn, packet, oid, history_list):
        raise UnexpectedPacketError

    def handleAskOIDs(self, conn, packet, first, last, partition):
        raise UnexpectedPacketError

    def handleAnswerOIDs(self, conn, packet, oid_list):
        raise UnexpectedPacketError

    def handleAskPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        raise UnexpectedPacketError

    def handleAnswerPartitionList(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def handleAskNodeList(self, conn, packet, offset_list):
        raise UnexpectedPacketError

    def handleAnswerNodeList(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def handleSetNodeState(self, conn, packet, uuid, state, modify_partition_table):
        raise UnexpectedPacketError

    def handleAnswerNodeState(self, conn, packet, uuid, state):
        raise UnexpectedPacketError

    def handleAddPendingNodes(self, conn, packet, uuid_list):
        raise UnexpectedPacketError

    def handleAnswerNewNodes(self, conn, packet, uuid_list):
        raise UnexpectedPacketError

    def handleAskNodeInformation(self, conn, packet):
        raise UnexpectedPacketError

    def handleAnswerNodeInformation(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def handleAskClusterState(self, conn, packet):
        raise UnexpectedPacketError

    def handleAnswerClusterState(self, conn, packet, state):
        raise UnexpectedPacketError

    def handleSetClusterState(self, conn, packet, state):
        raise UnexpectedPacketError

    def handleNotifyClusterInformation(self, conn, packet, state):
        raise UnexpectedPacketError

    def handleNotifyLastOID(self, conn, packet, oid):
        raise UnexpectedPacketError


    # Error packet handlers.

    def handleError(self, conn, packet, code, message):
        try:
            method = self.error_dispatch_table[code]
            method(conn, packet, message)
        except ValueError:
            raise UnexpectedPacketError(message)

    def handleNotReady(self, conn, packet, message):
        raise UnexpectedPacketError

    def handleOidNotFound(self, conn, packet, message):
        raise UnexpectedPacketError

    def handleTidNotFound(self, conn, packet, message):
        raise UnexpectedPacketError

    def handleProtocolError(self, conn, packet, message):
        # the connection should have been closed by the remote peer
        logging.error('protocol error: %s' % (message,))

    def handleTimeoutError(self, conn, packet, message):
        logging.error('timeout error: %s' % (message,))

    def handleBrokenNodeDisallowedError(self, conn, packet, message):
        raise RuntimeError, 'broken node disallowed error: %s' % (message,)

    def handleInternalError(self, conn, packet, message):
        self.peerBroken(conn)
        conn.close()

    def handleNoError(self, conn, packet, message):
        logging.debug("no error message : %s" % (message))


    # Fetch tables initialization

    def initPacketDispatchTable(self):
        d = {}

        d[PacketTypes.ERROR] = self.handleError
        d[PacketTypes.REQUEST_NODE_IDENTIFICATION] = self.handleRequestNodeIdentification
        d[PacketTypes.ACCEPT_NODE_IDENTIFICATION] = self.handleAcceptNodeIdentification
        d[PacketTypes.ASK_PRIMARY_MASTER] = self.handleAskPrimaryMaster
        d[PacketTypes.ANSWER_PRIMARY_MASTER] = self.handleAnswerPrimaryMaster
        d[PacketTypes.ANNOUNCE_PRIMARY_MASTER] = self.handleAnnouncePrimaryMaster
        d[PacketTypes.REELECT_PRIMARY_MASTER] = self.handleReelectPrimaryMaster
        d[PacketTypes.NOTIFY_NODE_INFORMATION] = self.handleNotifyNodeInformation
        d[PacketTypes.ASK_LAST_IDS] = self.handleAskLastIDs
        d[PacketTypes.ANSWER_LAST_IDS] = self.handleAnswerLastIDs
        d[PacketTypes.ASK_PARTITION_TABLE] = self.handleAskPartitionTable
        d[PacketTypes.ANSWER_PARTITION_TABLE] = self.handleAnswerPartitionTable
        d[PacketTypes.SEND_PARTITION_TABLE] = self.handleSendPartitionTable
        d[PacketTypes.NOTIFY_PARTITION_CHANGES] = self.handleNotifyPartitionChanges
        d[PacketTypes.START_OPERATION] = self.handleStartOperation
        d[PacketTypes.STOP_OPERATION] = self.handleStopOperation
        d[PacketTypes.ASK_UNFINISHED_TRANSACTIONS] = self.handleAskUnfinishedTransactions
        d[PacketTypes.ANSWER_UNFINISHED_TRANSACTIONS] = self.handleAnswerUnfinishedTransactions
        d[PacketTypes.ASK_OBJECT_PRESENT] = self.handleAskObjectPresent
        d[PacketTypes.ANSWER_OBJECT_PRESENT] = self.handleAnswerObjectPresent
        d[PacketTypes.DELETE_TRANSACTION] = self.handleDeleteTransaction
        d[PacketTypes.COMMIT_TRANSACTION] = self.handleCommitTransaction
        d[PacketTypes.ASK_BEGIN_TRANSACTION] = self.handleAskBeginTransaction
        d[PacketTypes.ANSWER_BEGIN_TRANSACTION] = self.handleAnswerBeginTransaction
        d[PacketTypes.FINISH_TRANSACTION] = self.handleFinishTransaction
        d[PacketTypes.NOTIFY_TRANSACTION_FINISHED] = self.handleNotifyTransactionFinished
        d[PacketTypes.LOCK_INFORMATION] = self.handleLockInformation
        d[PacketTypes.NOTIFY_INFORMATION_LOCKED] = self.handleNotifyInformationLocked
        d[PacketTypes.INVALIDATE_OBJECTS] = self.handleInvalidateObjects
        d[PacketTypes.UNLOCK_INFORMATION] = self.handleUnlockInformation
        d[PacketTypes.ASK_NEW_OIDS] = self.handleAskNewOIDs
        d[PacketTypes.ANSWER_NEW_OIDS] = self.handleAnswerNewOIDs
        d[PacketTypes.ASK_STORE_OBJECT] = self.handleAskStoreObject
        d[PacketTypes.ANSWER_STORE_OBJECT] = self.handleAnswerStoreObject
        d[PacketTypes.ABORT_TRANSACTION] = self.handleAbortTransaction
        d[PacketTypes.ASK_STORE_TRANSACTION] = self.handleAskStoreTransaction
        d[PacketTypes.ANSWER_STORE_TRANSACTION] = self.handleAnswerStoreTransaction
        d[PacketTypes.ASK_OBJECT] = self.handleAskObject
        d[PacketTypes.ANSWER_OBJECT] = self.handleAnswerObject
        d[PacketTypes.ASK_TIDS] = self.handleAskTIDs
        d[PacketTypes.ANSWER_TIDS] = self.handleAnswerTIDs
        d[PacketTypes.ASK_TRANSACTION_INFORMATION] = self.handleAskTransactionInformation
        d[PacketTypes.ANSWER_TRANSACTION_INFORMATION] = self.handleAnswerTransactionInformation
        d[PacketTypes.ASK_OBJECT_HISTORY] = self.handleAskObjectHistory
        d[PacketTypes.ANSWER_OBJECT_HISTORY] = self.handleAnswerObjectHistory
        d[PacketTypes.ASK_OIDS] = self.handleAskOIDs
        d[PacketTypes.ANSWER_OIDS] = self.handleAnswerOIDs
        d[PacketTypes.ASK_PARTITION_LIST] = self.handleAskPartitionList
        d[PacketTypes.ANSWER_PARTITION_LIST] = self.handleAnswerPartitionList
        d[PacketTypes.ASK_NODE_LIST] = self.handleAskNodeList
        d[PacketTypes.ANSWER_NODE_LIST] = self.handleAnswerNodeList
        d[PacketTypes.SET_NODE_STATE] = self.handleSetNodeState
        d[PacketTypes.ANSWER_NODE_STATE] = self.handleAnswerNodeState
        d[PacketTypes.SET_CLUSTER_STATE] = self.handleSetClusterState
        d[PacketTypes.ADD_PENDING_NODES] = self.handleAddPendingNodes
        d[PacketTypes.ANSWER_NEW_NODES] = self.handleAnswerNewNodes
        d[PacketTypes.ASK_NODE_INFORMATION] = self.handleAskNodeInformation
        d[PacketTypes.ANSWER_NODE_INFORMATION] = self.handleAnswerNodeInformation
        d[PacketTypes.ASK_CLUSTER_STATE] = self.handleAskClusterState
        d[PacketTypes.ANSWER_CLUSTER_STATE] = self.handleAnswerClusterState
        d[PacketTypes.NOTIFY_CLUSTER_INFORMATION] = self.handleNotifyClusterInformation
        d[PacketTypes.NOTIFY_LAST_OID] = self.handleNotifyLastOID

        return d

    def initErrorDispatchTable(self):
        d = {}

        d[ErrorCodes.NO_ERROR] = self.handleNoError
        d[ErrorCodes.NOT_READY] = self.handleNotReady
        d[ErrorCodes.OID_NOT_FOUND] = self.handleOidNotFound
        d[ErrorCodes.TID_NOT_FOUND] = self.handleTidNotFound
        d[ErrorCodes.PROTOCOL_ERROR] = self.handleProtocolError
        d[ErrorCodes.BROKEN_NODE] = self.handleBrokenNodeDisallowedError
        d[ErrorCodes.INTERNAL_ERROR] = self.handleInternalError

        return d

