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
        self.packet_dispatch_table = self.__initPacketDispatchTable()
        self.error_dispatch_table = self.__initErrorDispatchTable()

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

    def requestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        raise UnexpectedPacketError

    def acceptNodeIdentification(self, conn, packet, node_type,
                       uuid, address, num_partitions, num_replicas, your_uuid):
        raise UnexpectedPacketError

    def askPrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def answerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        raise UnexpectedPacketError

    def announcePrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def reelectPrimaryMaster(self, conn, packet):
        raise UnexpectedPacketError

    def notifyNodeInformation(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def askLastIDs(self, conn, packet):
        raise UnexpectedPacketError

    def answerLastIDs(self, conn, packet, loid, ltid, lptid):
        raise UnexpectedPacketError

    def askPartitionTable(self, conn, packet, offset_list):
        raise UnexpectedPacketError

    def answerPartitionTable(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def sendPartitionTable(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def notifyPartitionChanges(self, conn, packet, ptid, cell_list):
        raise UnexpectedPacketError

    def startOperation(self, conn, packet):
        raise UnexpectedPacketError

    def stopOperation(self, conn, packet):
        raise UnexpectedPacketError

    def askUnfinishedTransactions(self, conn, packet):
        raise UnexpectedPacketError

    def answerUnfinishedTransactions(self, conn, packet, tid_list):
        raise UnexpectedPacketError

    def askObjectPresent(self, conn, packet, oid, tid):
        raise UnexpectedPacketError

    def answerObjectPresent(self, conn, packet, oid, tid):
        raise UnexpectedPacketError

    def deleteTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def commitTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def askBeginTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def answerBeginTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def askNewOIDs(self, conn, packet, num_oids):
        raise UnexpectedPacketError

    def answerNewOIDs(self, conn, packet, num_oids):
        raise UnexpectedPacketError

    def finishTransaction(self, conn, packet, oid_list, tid):
        raise UnexpectedPacketError

    def notifyTransactionFinished(self, conn, packet, tid):
        raise UnexpectedPacketError

    def lockInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def notifyInformationLocked(self, conn, packet, tid):
        raise UnexpectedPacketError

    def invalidateObjects(self, conn, packet, oid_list, tid):
        raise UnexpectedPacketError

    def unlockInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def askStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        raise UnexpectedPacketError

    def answerStoreObject(self, conn, packet, conflicting, oid, serial):
        raise UnexpectedPacketError

    def abortTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def askStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        raise UnexpectedPacketError

    def answerStoreTransaction(self, conn, packet, tid):
        raise UnexpectedPacketError

    def askObject(self, conn, packet, oid, serial, tid):
        raise UnexpectedPacketError

    def answerObject(self, conn, packet, oid, serial_start,
                           serial_end, compression, checksum, data):
        raise UnexpectedPacketError

    def askTIDs(self, conn, packet, first, last, partition):
        raise UnexpectedPacketError

    def answerTIDs(self, conn, packet, tid_list):
        raise UnexpectedPacketError

    def askTransactionInformation(self, conn, packet, tid):
        raise UnexpectedPacketError

    def answerTransactionInformation(self, conn, packet, tid, 
                                           user, desc, ext, oid_list):
        raise UnexpectedPacketError

    def askObjectHistory(self, conn, packet, oid, first, last):
        raise UnexpectedPacketError

    def answerObjectHistory(self, conn, packet, oid, history_list):
        raise UnexpectedPacketError

    def askOIDs(self, conn, packet, first, last, partition):
        raise UnexpectedPacketError

    def answerOIDs(self, conn, packet, oid_list):
        raise UnexpectedPacketError

    def askPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        raise UnexpectedPacketError

    def answerPartitionList(self, conn, packet, ptid, row_list):
        raise UnexpectedPacketError

    def askNodeList(self, conn, packet, offset_list):
        raise UnexpectedPacketError

    def answerNodeList(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def setNodeState(self, conn, packet, uuid, state, modify_partition_table):
        raise UnexpectedPacketError

    def answerNodeState(self, conn, packet, uuid, state):
        raise UnexpectedPacketError

    def addPendingNodes(self, conn, packet, uuid_list):
        raise UnexpectedPacketError

    def answerNewNodes(self, conn, packet, uuid_list):
        raise UnexpectedPacketError

    def askNodeInformation(self, conn, packet):
        raise UnexpectedPacketError

    def answerNodeInformation(self, conn, packet, node_list):
        raise UnexpectedPacketError

    def askClusterState(self, conn, packet):
        raise UnexpectedPacketError

    def answerClusterState(self, conn, packet, state):
        raise UnexpectedPacketError

    def setClusterState(self, conn, packet, state):
        raise UnexpectedPacketError

    def notifyClusterInformation(self, conn, packet, state):
        raise UnexpectedPacketError

    def notifyLastOID(self, conn, packet, oid):
        raise UnexpectedPacketError


    # Error packet handlers.

    def error(self, conn, packet, code, message):
        try:
            method = self.error_dispatch_table[code]
            method(conn, packet, message)
        except ValueError:
            raise UnexpectedPacketError(message)

    def notReady(self, conn, packet, message):
        raise UnexpectedPacketError

    def oidNotFound(self, conn, packet, message):
        raise UnexpectedPacketError

    def tidNotFound(self, conn, packet, message):
        raise UnexpectedPacketError

    def protocolError(self, conn, packet, message):
        # the connection should have been closed by the remote peer
        logging.error('protocol error: %s' % (message,))

    def timeoutError(self, conn, packet, message):
        logging.error('timeout error: %s' % (message,))

    def brokenNodeDisallowedError(self, conn, packet, message):
        raise RuntimeError, 'broken node disallowed error: %s' % (message,)

    def noError(self, conn, packet, message):
        logging.debug("no error message : %s" % (message))


    # Fetch tables initialization

    def __initPacketDispatchTable(self):
        d = {}

        d[PacketTypes.ERROR] = self.error
        d[PacketTypes.REQUEST_NODE_IDENTIFICATION] = self.requestNodeIdentification
        d[PacketTypes.ACCEPT_NODE_IDENTIFICATION] = self.acceptNodeIdentification
        d[PacketTypes.ASK_PRIMARY_MASTER] = self.askPrimaryMaster
        d[PacketTypes.ANSWER_PRIMARY_MASTER] = self.answerPrimaryMaster
        d[PacketTypes.ANNOUNCE_PRIMARY_MASTER] = self.announcePrimaryMaster
        d[PacketTypes.REELECT_PRIMARY_MASTER] = self.reelectPrimaryMaster
        d[PacketTypes.NOTIFY_NODE_INFORMATION] = self.notifyNodeInformation
        d[PacketTypes.ASK_LAST_IDS] = self.askLastIDs
        d[PacketTypes.ANSWER_LAST_IDS] = self.answerLastIDs
        d[PacketTypes.ASK_PARTITION_TABLE] = self.askPartitionTable
        d[PacketTypes.ANSWER_PARTITION_TABLE] = self.answerPartitionTable
        d[PacketTypes.SEND_PARTITION_TABLE] = self.sendPartitionTable
        d[PacketTypes.NOTIFY_PARTITION_CHANGES] = self.notifyPartitionChanges
        d[PacketTypes.START_OPERATION] = self.startOperation
        d[PacketTypes.STOP_OPERATION] = self.stopOperation
        d[PacketTypes.ASK_UNFINISHED_TRANSACTIONS] = self.askUnfinishedTransactions
        d[PacketTypes.ANSWER_UNFINISHED_TRANSACTIONS] = self.answerUnfinishedTransactions
        d[PacketTypes.ASK_OBJECT_PRESENT] = self.askObjectPresent
        d[PacketTypes.ANSWER_OBJECT_PRESENT] = self.answerObjectPresent
        d[PacketTypes.DELETE_TRANSACTION] = self.deleteTransaction
        d[PacketTypes.COMMIT_TRANSACTION] = self.commitTransaction
        d[PacketTypes.ASK_BEGIN_TRANSACTION] = self.askBeginTransaction
        d[PacketTypes.ANSWER_BEGIN_TRANSACTION] = self.answerBeginTransaction
        d[PacketTypes.FINISH_TRANSACTION] = self.finishTransaction
        d[PacketTypes.NOTIFY_TRANSACTION_FINISHED] = self.notifyTransactionFinished
        d[PacketTypes.LOCK_INFORMATION] = self.lockInformation
        d[PacketTypes.NOTIFY_INFORMATION_LOCKED] = self.notifyInformationLocked
        d[PacketTypes.INVALIDATE_OBJECTS] = self.invalidateObjects
        d[PacketTypes.UNLOCK_INFORMATION] = self.unlockInformation
        d[PacketTypes.ASK_NEW_OIDS] = self.askNewOIDs
        d[PacketTypes.ANSWER_NEW_OIDS] = self.answerNewOIDs
        d[PacketTypes.ASK_STORE_OBJECT] = self.askStoreObject
        d[PacketTypes.ANSWER_STORE_OBJECT] = self.answerStoreObject
        d[PacketTypes.ABORT_TRANSACTION] = self.abortTransaction
        d[PacketTypes.ASK_STORE_TRANSACTION] = self.askStoreTransaction
        d[PacketTypes.ANSWER_STORE_TRANSACTION] = self.answerStoreTransaction
        d[PacketTypes.ASK_OBJECT] = self.askObject
        d[PacketTypes.ANSWER_OBJECT] = self.answerObject
        d[PacketTypes.ASK_TIDS] = self.askTIDs
        d[PacketTypes.ANSWER_TIDS] = self.answerTIDs
        d[PacketTypes.ASK_TRANSACTION_INFORMATION] = self.askTransactionInformation
        d[PacketTypes.ANSWER_TRANSACTION_INFORMATION] = self.answerTransactionInformation
        d[PacketTypes.ASK_OBJECT_HISTORY] = self.askObjectHistory
        d[PacketTypes.ANSWER_OBJECT_HISTORY] = self.answerObjectHistory
        d[PacketTypes.ASK_OIDS] = self.askOIDs
        d[PacketTypes.ANSWER_OIDS] = self.answerOIDs
        d[PacketTypes.ASK_PARTITION_LIST] = self.askPartitionList
        d[PacketTypes.ANSWER_PARTITION_LIST] = self.answerPartitionList
        d[PacketTypes.ASK_NODE_LIST] = self.askNodeList
        d[PacketTypes.ANSWER_NODE_LIST] = self.answerNodeList
        d[PacketTypes.SET_NODE_STATE] = self.setNodeState
        d[PacketTypes.ANSWER_NODE_STATE] = self.answerNodeState
        d[PacketTypes.SET_CLUSTER_STATE] = self.setClusterState
        d[PacketTypes.ADD_PENDING_NODES] = self.addPendingNodes
        d[PacketTypes.ANSWER_NEW_NODES] = self.answerNewNodes
        d[PacketTypes.ASK_NODE_INFORMATION] = self.askNodeInformation
        d[PacketTypes.ANSWER_NODE_INFORMATION] = self.answerNodeInformation
        d[PacketTypes.ASK_CLUSTER_STATE] = self.askClusterState
        d[PacketTypes.ANSWER_CLUSTER_STATE] = self.answerClusterState
        d[PacketTypes.NOTIFY_CLUSTER_INFORMATION] = self.notifyClusterInformation
        d[PacketTypes.NOTIFY_LAST_OID] = self.notifyLastOID

        return d

    def __initErrorDispatchTable(self):
        d = {}

        d[ErrorCodes.NO_ERROR] = self.noError
        d[ErrorCodes.NOT_READY] = self.notReady
        d[ErrorCodes.OID_NOT_FOUND] = self.oidNotFound
        d[ErrorCodes.TID_NOT_FOUND] = self.tidNotFound
        d[ErrorCodes.PROTOCOL_ERROR] = self.protocolError
        d[ErrorCodes.BROKEN_NODE] = self.brokenNodeDisallowedError

        return d

