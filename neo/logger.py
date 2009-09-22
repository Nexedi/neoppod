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
from neo.util import dump

class PacketLogger(object):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        self.fetch_table = self.initFetchTable()

    def log(self, conn, packet, direction):
        """This is a helper method to handle various packet types."""
        # default log message 
        type = packet.getType()
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        logging.debug('#0x%08x %-30s %s %s (%s:%d)', packet.getId(),
                type, direction, uuid, ip, port)
        logger = self.fetch_table.get(type, None)
        if logger is None:
            logging.warning('No logger found for packet %s' % type)
            return
        # enhanced log
        args = packet.decode() or ()
        log_message = logger(conn, packet, *args)
        if log_message is not None:
            logging.debug('#0x%08x %s', packet.getId(), log_message)


    # Packet loggers

    def error(self, conn, packet, code, message):
        return "%s (%s)" % (code, message)

    def requestNodeIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        pass

    def acceptNodeIdentification(self, conn, packet, node_type,
                       uuid, address, num_partitions, num_replicas, your_uuid):
        pass

    def askPrimaryMaster(self, conn, packet):
        pass

    def answerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        pass

    def announcePrimaryMaster(self, conn, packet):
        pass

    def reelectPrimaryMaster(self, conn, packet):
        pass

    def notifyNodeInformation(self, conn, packet, node_list):
        pass

    def askLastIDs(self, conn, packet):
        pass

    def answerLastIDs(self, conn, packet, loid, ltid, lptid):
        pass

    def askPartitionTable(self, conn, packet, offset_list):
        pass

    def answerPartitionTable(self, conn, packet, ptid, row_list):
        pass

    def sendPartitionTable(self, conn, packet, ptid, row_list):
        pass

    def notifyPartitionChanges(self, conn, packet, ptid, cell_list):
        pass

    def startOperation(self, conn, packet):
        pass

    def stopOperation(self, conn, packet):
        pass

    def askUnfinishedTransactions(self, conn, packet):
        pass

    def answerUnfinishedTransactions(self, conn, packet, tid_list):
        pass

    def askObjectPresent(self, conn, packet, oid, tid):
        pass

    def answerObjectPresent(self, conn, packet, oid, tid):
        pass

    def deleteTransaction(self, conn, packet, tid):
        pass

    def commitTransaction(self, conn, packet, tid):
        pass

    def askBeginTransaction(self, conn, packet, tid):
        pass

    def answerBeginTransaction(self, conn, packet, tid):
        pass

    def askNewOIDs(self, conn, packet, num_oids):
        pass

    def answerNewOIDs(self, conn, packet, num_oids):
        pass

    def finishTransaction(self, conn, packet, oid_list, tid):
        pass

    def notifyTransactionFinished(self, conn, packet, tid):
        pass

    def lockInformation(self, conn, packet, tid):
        pass

    def notifyInformationLocked(self, conn, packet, tid):
        pass

    def invalidateObjects(self, conn, packet, oid_list, tid):
        pass

    def unlockInformation(self, conn, packet, tid):
        pass

    def askStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        pass

    def answerStoreObject(self, conn, packet, conflicting, oid, serial):
        pass

    def abortTransaction(self, conn, packet, tid):
        pass

    def askStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        pass

    def answerStoreTransaction(self, conn, packet, tid):
        pass

    def askObject(self, conn, packet, oid, serial, tid):
        pass

    def answerObject(self, conn, packet, oid, serial_start,
                           serial_end, compression, checksum, data):
        pass

    def askTIDs(self, conn, packet, first, last, partition):
        pass

    def answerTIDs(self, conn, packet, tid_list):
        pass

    def askTransactionInformation(self, conn, packet, tid):
        pass

    def answerTransactionInformation(self, conn, packet, tid, 
                                           user, desc, ext, oid_list):
        pass

    def askObjectHistory(self, conn, packet, oid, first, last):
        pass

    def answerObjectHistory(self, conn, packet, oid, history_list):
        pass

    def askOIDs(self, conn, packet, first, last, partition):
        pass

    def answerOIDs(self, conn, packet, oid_list):
        pass

    def askPartitionList(self, conn, packet, min_offset, max_offset, uuid):
        pass

    def answerPartitionList(self, conn, packet, ptid, row_list):
        pass

    def askNodeList(self, conn, packet, offset_list):
        pass

    def answerNodeList(self, conn, packet, node_list):
        pass

    def setNodeState(self, conn, packet, uuid, state, modify_partition_table):
        pass

    def answerNodeState(self, conn, packet, uuid, state):
        pass

    def addPendingNodes(self, conn, packet, uuid_list):
        pass

    def answerNewNodes(self, conn, packet, uuid_list):
        pass

    def askNodeInformation(self, conn, packet):
        pass

    def answerNodeInformation(self, conn, packet, node_list):
        pass

    def askClusterState(self, conn, packet):
        pass

    def answerClusterState(self, conn, packet, state):
        pass

    def setClusterState(self, conn, packet, state):
        pass

    def notifyClusterInformation(self, conn, packet, state):
        pass

    def notifyLastOID(self, conn, packet, oid):
        pass


    # Fetch tables initialization
    def initFetchTable(self):
        d = {}
        d[protocol.ERROR] = self.error
        d[protocol.REQUEST_NODE_IDENTIFICATION] = self.requestNodeIdentification
        d[protocol.ACCEPT_NODE_IDENTIFICATION] = self.acceptNodeIdentification
        d[protocol.ASK_PRIMARY_MASTER] = self.askPrimaryMaster
        d[protocol.ANSWER_PRIMARY_MASTER] = self.answerPrimaryMaster
        d[protocol.ANNOUNCE_PRIMARY_MASTER] = self.announcePrimaryMaster
        d[protocol.REELECT_PRIMARY_MASTER] = self.reelectPrimaryMaster
        d[protocol.NOTIFY_NODE_INFORMATION] = self.notifyNodeInformation
        d[protocol.ASK_LAST_IDS] = self.askLastIDs
        d[protocol.ANSWER_LAST_IDS] = self.answerLastIDs
        d[protocol.ASK_PARTITION_TABLE] = self.askPartitionTable
        d[protocol.ANSWER_PARTITION_TABLE] = self.answerPartitionTable
        d[protocol.SEND_PARTITION_TABLE] = self.sendPartitionTable
        d[protocol.NOTIFY_PARTITION_CHANGES] = self.notifyPartitionChanges
        d[protocol.START_OPERATION] = self.startOperation
        d[protocol.STOP_OPERATION] = self.stopOperation
        d[protocol.ASK_UNFINISHED_TRANSACTIONS] = self.askUnfinishedTransactions
        d[protocol.ANSWER_UNFINISHED_TRANSACTIONS] = self.answerUnfinishedTransactions
        d[protocol.ASK_OBJECT_PRESENT] = self.askObjectPresent
        d[protocol.ANSWER_OBJECT_PRESENT] = self.answerObjectPresent
        d[protocol.DELETE_TRANSACTION] = self.deleteTransaction
        d[protocol.COMMIT_TRANSACTION] = self.commitTransaction
        d[protocol.ASK_BEGIN_TRANSACTION] = self.askBeginTransaction
        d[protocol.ANSWER_BEGIN_TRANSACTION] = self.answerBeginTransaction
        d[protocol.FINISH_TRANSACTION] = self.finishTransaction
        d[protocol.NOTIFY_TRANSACTION_FINISHED] = self.notifyTransactionFinished
        d[protocol.LOCK_INFORMATION] = self.lockInformation
        d[protocol.NOTIFY_INFORMATION_LOCKED] = self.notifyInformationLocked
        d[protocol.INVALIDATE_OBJECTS] = self.invalidateObjects
        d[protocol.UNLOCK_INFORMATION] = self.unlockInformation
        d[protocol.ASK_NEW_OIDS] = self.askNewOIDs
        d[protocol.ANSWER_NEW_OIDS] = self.answerNewOIDs
        d[protocol.ASK_STORE_OBJECT] = self.askStoreObject
        d[protocol.ANSWER_STORE_OBJECT] = self.answerStoreObject
        d[protocol.ABORT_TRANSACTION] = self.abortTransaction
        d[protocol.ASK_STORE_TRANSACTION] = self.askStoreTransaction
        d[protocol.ANSWER_STORE_TRANSACTION] = self.answerStoreTransaction
        d[protocol.ASK_OBJECT] = self.askObject
        d[protocol.ANSWER_OBJECT] = self.answerObject
        d[protocol.ASK_TIDS] = self.askTIDs
        d[protocol.ANSWER_TIDS] = self.answerTIDs
        d[protocol.ASK_TRANSACTION_INFORMATION] = self.askTransactionInformation
        d[protocol.ANSWER_TRANSACTION_INFORMATION] = self.answerTransactionInformation
        d[protocol.ASK_OBJECT_HISTORY] = self.askObjectHistory
        d[protocol.ANSWER_OBJECT_HISTORY] = self.answerObjectHistory
        d[protocol.ASK_OIDS] = self.askOIDs
        d[protocol.ANSWER_OIDS] = self.answerOIDs
        d[protocol.ASK_PARTITION_LIST] = self.askPartitionList
        d[protocol.ANSWER_PARTITION_LIST] = self.answerPartitionList
        d[protocol.ASK_NODE_LIST] = self.askNodeList
        d[protocol.ANSWER_NODE_LIST] = self.answerNodeList
        d[protocol.SET_NODE_STATE] = self.setNodeState
        d[protocol.ANSWER_NODE_STATE] = self.answerNodeState
        d[protocol.SET_CLUSTER_STATE] = self.setClusterState
        d[protocol.ADD_PENDING_NODES] = self.addPendingNodes
        d[protocol.ANSWER_NEW_NODES] = self.answerNewNodes
        d[protocol.ASK_NODE_INFORMATION] = self.askNodeInformation
        d[protocol.ANSWER_NODE_INFORMATION] = self.answerNodeInformation
        d[protocol.ASK_CLUSTER_STATE] = self.askClusterState
        d[protocol.ANSWER_CLUSTER_STATE] = self.answerClusterState
        d[protocol.NOTIFY_CLUSTER_INFORMATION] = self.notifyClusterInformation
        d[protocol.NOTIFY_LAST_OID] = self.notifyLastOID
        return d


PACKET_LOGGER = PacketLogger()
