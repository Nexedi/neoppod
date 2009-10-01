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
from neo.protocol import PacketTypes
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


PACKET_LOGGER = PacketLogger()
