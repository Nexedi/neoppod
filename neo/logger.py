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
from neo.protocol import Packets, PacketMalformedError
from neo.util import dump

class PacketLogger(object):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        self.fetch_table = self.initFetchTable()

    def log(self, conn, packet, direction):
        """This is a helper method to handle various packet types."""
        # default log message 
        klass = packet.getType()
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        logging.debug('#0x%08x %-30s %s %s (%s:%d)', packet.getId(),
                packet.__class__.__name__, direction, uuid, ip, port)
        logger = self.fetch_table.get(klass, None)
        if logger is None:
            logging.warning('No logger found for packet %s' % klass)
            return
        # enhanced log
        try:
            args = packet.decode() or ()
        except PacketMalformedError:
            logging.warning("Can't decode packet for logging")
            return
        log_message = logger(conn, packet, *args)
        if log_message is not None:
            logging.debug('#0x%08x %s', packet.getId(), log_message)


    # Packet loggers

    def error(self, conn, packet, code, message):
        return "%s (%s)" % (code, message)

    def requestIdentification(self, conn, packet, node_type,
                                        uuid, address, name):
        logging.debug('Request identification for cluster %s' % (name, ))
        pass

    def acceptIdentification(self, conn, packet, node_type,
                       uuid, address, num_partitions, num_replicas, your_uuid):
        pass

    def askPrimary(self, conn, packet):
        pass

    def answerPrimary(self, conn, packet, primary_uuid,
                                  known_master_list):
        pass

    def announcePrimary(self, conn, packet):
        pass

    def reelectPrimary(self, conn, packet):
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

    def answerNodeInformation(self, conn, packet):
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
        d[Packets.Error] = self.error
        d[Packets.RequestIdentification] = self.requestIdentification
        d[Packets.AcceptIdentification] = self.acceptIdentification
        d[Packets.AskPrimary] = self.askPrimary
        d[Packets.AnswerPrimary] = self.answerPrimary
        d[Packets.AnnouncePrimary] = self.announcePrimary
        d[Packets.ReelectPrimary] = self.reelectPrimary
        d[Packets.NotifyNodeInformation] = self.notifyNodeInformation
        d[Packets.AskLastIDs] = self.askLastIDs
        d[Packets.AnswerLastIDs] = self.answerLastIDs
        d[Packets.AskPartitionTable] = self.askPartitionTable
        d[Packets.AnswerPartitionTable] = self.answerPartitionTable
        d[Packets.SendPartitionTable] = self.sendPartitionTable
        d[Packets.NotifyPartitionChanges] = self.notifyPartitionChanges
        d[Packets.StartOperation] = self.startOperation
        d[Packets.StopOperation] = self.stopOperation
        d[Packets.AskUnfinishedTransactions] = self.askUnfinishedTransactions
        d[Packets.AnswerUnfinishedTransactions] = self.answerUnfinishedTransactions
        d[Packets.AskObjectPresent] = self.askObjectPresent
        d[Packets.AnswerObjectPresent] = self.answerObjectPresent
        d[Packets.DeleteTransaction] = self.deleteTransaction
        d[Packets.CommitTransaction] = self.commitTransaction
        d[Packets.AskBeginTransaction] = self.askBeginTransaction
        d[Packets.AnswerBeginTransaction] = self.answerBeginTransaction
        d[Packets.FinishTransaction] = self.finishTransaction
        d[Packets.NotifyTransactionFinished] = self.notifyTransactionFinished
        d[Packets.LockInformation] = self.lockInformation
        d[Packets.NotifyInformationLocked] = self.notifyInformationLocked
        d[Packets.InvalidateObjects] = self.invalidateObjects
        d[Packets.UnlockInformation] = self.unlockInformation
        d[Packets.AskNewOIDs] = self.askNewOIDs
        d[Packets.AnswerNewOIDs] = self.answerNewOIDs
        d[Packets.AskStoreObject] = self.askStoreObject
        d[Packets.AnswerStoreObject] = self.answerStoreObject
        d[Packets.AbortTransaction] = self.abortTransaction
        d[Packets.AskStoreTransaction] = self.askStoreTransaction
        d[Packets.AnswerStoreTransaction] = self.answerStoreTransaction
        d[Packets.AskObject] = self.askObject
        d[Packets.AnswerObject] = self.answerObject
        d[Packets.AskTIDs] = self.askTIDs
        d[Packets.AnswerTIDs] = self.answerTIDs
        d[Packets.AskTransactionInformation] = self.askTransactionInformation
        d[Packets.AnswerTransactionInformation] = self.answerTransactionInformation
        d[Packets.AskObjectHistory] = self.askObjectHistory
        d[Packets.AnswerObjectHistory] = self.answerObjectHistory
        d[Packets.AskOIDs] = self.askOIDs
        d[Packets.AnswerOIDs] = self.answerOIDs
        d[Packets.AskPartitionList] = self.askPartitionList
        d[Packets.AnswerPartitionList] = self.answerPartitionList
        d[Packets.AskNodeList] = self.askNodeList
        d[Packets.AnswerNodeList] = self.answerNodeList
        d[Packets.SetNodeState] = self.setNodeState
        d[Packets.AnswerNodeState] = self.answerNodeState
        d[Packets.SetClusterState] = self.setClusterState
        d[Packets.AddPendingNodes] = self.addPendingNodes
        d[Packets.AnswerNewNodes] = self.answerNewNodes
        d[Packets.AskNodeInformation] = self.askNodeInformation
        d[Packets.AnswerNodeInformation] = self.answerNodeInformation
        d[Packets.AskClusterState] = self.askClusterState
        d[Packets.AnswerClusterState] = self.answerClusterState
        d[Packets.NotifyClusterInformation] = self.notifyClusterInformation
        d[Packets.NotifyLastOID] = self.notifyLastOID
        return d


PACKET_LOGGER = PacketLogger()
