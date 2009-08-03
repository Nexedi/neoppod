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
from neo.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError
from neo.connection import ServerConnection

from neo import protocol
from protocol import ERROR, REQUEST_NODE_IDENTIFICATION, ACCEPT_NODE_IDENTIFICATION, \
        ASK_PRIMARY_MASTER, ANSWER_PRIMARY_MASTER, ANNOUNCE_PRIMARY_MASTER, \
        REELECT_PRIMARY_MASTER, NOTIFY_NODE_INFORMATION, START_OPERATION, \
        STOP_OPERATION, ASK_LAST_IDS, ANSWER_LAST_IDS, ASK_PARTITION_TABLE, \
        ANSWER_PARTITION_TABLE, SEND_PARTITION_TABLE, NOTIFY_PARTITION_CHANGES, \
        ASK_UNFINISHED_TRANSACTIONS, ANSWER_UNFINISHED_TRANSACTIONS, \
        ASK_OBJECT_PRESENT, ANSWER_OBJECT_PRESENT, \
        DELETE_TRANSACTION, COMMIT_TRANSACTION, ASK_BEGIN_TRANSACTION, ANSWER_BEGIN_TRANSACTION, \
        FINISH_TRANSACTION, NOTIFY_TRANSACTION_FINISHED, LOCK_INFORMATION, \
        NOTIFY_INFORMATION_LOCKED, INVALIDATE_OBJECTS, UNLOCK_INFORMATION, \
        ASK_NEW_OIDS, ANSWER_NEW_OIDS, ASK_STORE_OBJECT, ANSWER_STORE_OBJECT, \
        ABORT_TRANSACTION, ASK_STORE_TRANSACTION, ANSWER_STORE_TRANSACTION, \
        ASK_OBJECT, ANSWER_OBJECT, ASK_TIDS, ANSWER_TIDS, ASK_TRANSACTION_INFORMATION, \
        ANSWER_TRANSACTION_INFORMATION, ASK_OBJECT_HISTORY, ANSWER_OBJECT_HISTORY, \
        ASK_OIDS, ANSWER_OIDS, ADD_PENDING_NODES, ANSWER_NEW_NODES, \
        NOT_READY_CODE, OID_NOT_FOUND_CODE, TID_NOT_FOUND_CODE, \
        PROTOCOL_ERROR_CODE, BROKEN_NODE_DISALLOWED_CODE, \
        INTERNAL_ERROR_CODE, ASK_PARTITION_LIST, ANSWER_PARTITION_LIST, ASK_NODE_LIST, \
        ANSWER_NODE_LIST, SET_NODE_STATE, ANSWER_NODE_STATE, SET_CLUSTER_STATE, \
        ASK_NODE_INFORMATION, ANSWER_NODE_INFORMATION, NO_ERROR_CODE, \
        ASK_CLUSTER_STATE, ANSWER_CLUSTER_STATE, NOTIFY_CLUSTER_INFORMATION, \
        NOTIFY_LAST_OID


class EventHandler(object):
    """This class handles events."""

    def __init__(self, app):
        self.app = app
        self.packet_dispatch_table = self.initPacketDispatchTable()
        self.error_dispatch_table = self.initErrorDispatchTable()

    # XXX: there is an inconsistency between connection* and handle* names. As
    # we are in an hander, I think that's redondant to prefix with 'handle'

    def _packetMalformed(self, conn, packet, message='', *args):
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

    def _unexpectedPacket(self, conn, packet, message=None):
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
            self._unexpectedPacket(conn, packet, *e.args)
        except PacketMalformedError, e:
            self._packetMalformed(conn, packet, *e.args)
        except BrokenNodeDisallowedError:
            answer_packet = protocol.brokenNodeDisallowedError('go away')
            conn.answer(answer_packet, packet.getId())
            conn.abort()
        except NotReadyError:
            conn.answer(protocol.notReady('retry later'), packet.getId())
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

    def connectionAccepted(self, conn, connector, addr):
        """Called when a connection is accepted."""
        logging.debug('connection accepted from %s:%d', *addr)
        new_conn = ServerConnection(conn.getEventManager(), conn.getHandler(),
                                    connector=connector, addr=addr)
        # A request for a node identification should arrive.
        new_conn.expectMessage(timeout = 10, additional_timeout = 0)

    def timeoutExpired(self, conn):
        """Called when a timeout event occurs."""
        logging.debug('timeout expired for %s:%d', *(conn.getAddress()))
        self.connectionLost(conn, protocol.TEMPORARILY_DOWN_STATE)

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        logging.debug('connection closed for %s:%d', *(conn.getAddress()))
        self.connectionLost(conn, protocol.TEMPORARILY_DOWN_STATE)

    def peerBroken(self, conn):
        """Called when a peer is broken."""
        logging.error('%s:%d is broken', *(conn.getAddress()))
        self.connectionLost(conn, protocol.BROKEN_STATE)

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

    def handleAnswerStoreObject(self, conn, packet, status, oid):
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

        d[ERROR] = self.handleError
        d[REQUEST_NODE_IDENTIFICATION] = self.handleRequestNodeIdentification
        d[ACCEPT_NODE_IDENTIFICATION] = self.handleAcceptNodeIdentification
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
        d[ASK_OBJECT_PRESENT] = self.handleAskObjectPresent
        d[ANSWER_OBJECT_PRESENT] = self.handleAnswerObjectPresent
        d[DELETE_TRANSACTION] = self.handleDeleteTransaction
        d[COMMIT_TRANSACTION] = self.handleCommitTransaction
        d[ASK_BEGIN_TRANSACTION] = self.handleAskBeginTransaction
        d[ANSWER_BEGIN_TRANSACTION] = self.handleAnswerBeginTransaction
        d[FINISH_TRANSACTION] = self.handleFinishTransaction
        d[NOTIFY_TRANSACTION_FINISHED] = self.handleNotifyTransactionFinished
        d[LOCK_INFORMATION] = self.handleLockInformation
        d[NOTIFY_INFORMATION_LOCKED] = self.handleNotifyInformationLocked
        d[INVALIDATE_OBJECTS] = self.handleInvalidateObjects
        d[UNLOCK_INFORMATION] = self.handleUnlockInformation
        d[ASK_NEW_OIDS] = self.handleAskNewOIDs
        d[ANSWER_NEW_OIDS] = self.handleAnswerNewOIDs
        d[ASK_STORE_OBJECT] = self.handleAskStoreObject
        d[ANSWER_STORE_OBJECT] = self.handleAnswerStoreObject
        d[ABORT_TRANSACTION] = self.handleAbortTransaction
        d[ASK_STORE_TRANSACTION] = self.handleAskStoreTransaction
        d[ANSWER_STORE_TRANSACTION] = self.handleAnswerStoreTransaction
        d[ASK_OBJECT] = self.handleAskObject
        d[ANSWER_OBJECT] = self.handleAnswerObject
        d[ASK_TIDS] = self.handleAskTIDs
        d[ANSWER_TIDS] = self.handleAnswerTIDs
        d[ASK_TRANSACTION_INFORMATION] = self.handleAskTransactionInformation
        d[ANSWER_TRANSACTION_INFORMATION] = self.handleAnswerTransactionInformation
        d[ASK_OBJECT_HISTORY] = self.handleAskObjectHistory
        d[ANSWER_OBJECT_HISTORY] = self.handleAnswerObjectHistory
        d[ASK_OIDS] = self.handleAskOIDs
        d[ANSWER_OIDS] = self.handleAnswerOIDs
        d[ASK_PARTITION_LIST] = self.handleAskPartitionList
        d[ANSWER_PARTITION_LIST] = self.handleAnswerPartitionList
        d[ASK_NODE_LIST] = self.handleAskNodeList
        d[ANSWER_NODE_LIST] = self.handleAnswerNodeList
        d[SET_NODE_STATE] = self.handleSetNodeState
        d[ANSWER_NODE_STATE] = self.handleAnswerNodeState
        d[SET_CLUSTER_STATE] = self.handleSetClusterState
        d[ADD_PENDING_NODES] = self.handleAddPendingNodes
        d[ANSWER_NEW_NODES] = self.handleAnswerNewNodes
        d[ASK_NODE_INFORMATION] = self.handleAskNodeInformation
        d[ANSWER_NODE_INFORMATION] = self.handleAnswerNodeInformation
        d[ASK_CLUSTER_STATE] = self.handleAskClusterState
        d[ANSWER_CLUSTER_STATE] = self.handleAnswerClusterState
        d[NOTIFY_CLUSTER_INFORMATION] = self.handleNotifyClusterInformation
        d[NOTIFY_LAST_OID] = self.handleNotifyLastOID

        return d

    def initErrorDispatchTable(self):
        d = {}

        d[NO_ERROR_CODE] = self.handleNoError
        d[NOT_READY_CODE] = self.handleNotReady
        d[OID_NOT_FOUND_CODE] = self.handleOidNotFound
        d[TID_NOT_FOUND_CODE] = self.handleTidNotFound
        d[PROTOCOL_ERROR_CODE] = self.handleProtocolError
        d[BROKEN_NODE_DISALLOWED_CODE] = self.handleBrokenNodeDisallowedError
        d[INTERNAL_ERROR_CODE] = self.handleInternalError

        return d

