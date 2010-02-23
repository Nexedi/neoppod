#
# Copyright (C) 2006-2010  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging
from neo.protocol import PacketMalformedError
from neo.util import dump
from neo.handler import EventHandler

class PacketLogger(EventHandler):
    """ Logger at packet level (for debugging purpose) """

    def __init__(self):
        EventHandler.__init__(self, None)

    def dispatch(self, conn, packet, direction):
        """This is a helper method to handle various packet types."""
        # default log message
        klass = packet.getType()
        uuid = dump(conn.getUUID())
        ip, port = conn.getAddress()
        logging.debug('#0x%08x %-30s %s %s (%s:%d)', packet.getId(),
                packet.__class__.__name__, direction, uuid, ip, port)
        logger = self.packet_dispatch_table.get(klass, None)
        if logger is None:
            return
        # enhanced log
        try:
            args = packet.decode() or ()
        except PacketMalformedError:
            logging.warning("Can't decode packet for logging")
            return
        log_message = logger(conn, *args)
        if log_message is not None:
            logging.debug('#0x%08x %s', packet.getId(), log_message)


    # Packet loggers

    def error(self, conn, code, message):
        return "%s (%s)" % (code, message)

    def requestIdentification(self, conn, node_type,
                                        uuid, address, name):
        logging.debug('Request identification for cluster %s' % (name, ))
        pass

    def acceptIdentification(self, conn, node_type,
                       uuid, num_partitions, num_replicas, your_uuid):
        pass

    def askPrimary(self, conn):
        pass

    def answerPrimary(self, conn, primary_uuid,
                                  known_master_list):
        pass

    def announcePrimary(self, conn):
        pass

    def reelectPrimary(self, conn):
        pass

    def notifyNodeInformation(self, conn, node_list):
        for node_type, address, uuid, state in node_list:
            if address is not None:
                address = '%s:%d' % address
            else:
                address = '?'
            node = (dump(uuid), node_type, address, state)
            logging.debug(' ! %s | %8s | %22s | %s' % node)

    def askLastIDs(self, conn):
        pass

    def answerLastIDs(self, conn, loid, ltid, lptid):
        pass

    def askPartitionTable(self, conn, offset_list):
        pass

    def answerPartitionTable(self, conn, ptid, row_list):
        pass

    def sendPartitionTable(self, conn, ptid, row_list):
        pass

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        pass

    def startOperation(self, conn):
        pass

    def stopOperation(self, conn):
        pass

    def askUnfinishedTransactions(self, conn):
        pass

    def answerUnfinishedTransactions(self, conn, tid_list):
        pass

    def askObjectPresent(self, conn, oid, tid):
        pass

    def answerObjectPresent(self, conn, oid, tid):
        pass

    def deleteTransaction(self, conn, tid):
        pass

    def commitTransaction(self, conn, tid):
        pass

    def askBeginTransaction(self, conn, tid):
        pass

    def answerBeginTransaction(self, conn, tid):
        pass

    def askNewOIDs(self, conn, num_oids):
        pass

    def answerNewOIDs(self, conn, num_oids):
        pass

    def askFinishTransaction(self, conn, oid_list, tid):
        pass

    def answerTransactionFinished(self, conn, tid):
        pass

    def askLockInformation(self, conn, tid):
        pass

    def answerInformationLocked(self, conn, tid):
        pass

    def invalidateObjects(self, conn, oid_list, tid):
        pass

    def notifyUnlockInformation(self, conn, tid):
        pass

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, tid):
        pass

    def answerStoreObject(self, conn, conflicting, oid, serial):
        pass

    def abortTransaction(self, conn, tid):
        pass

    def askStoreTransaction(self, conn, tid, user, desc,
                                  ext, oid_list):
        pass

    def answerStoreTransaction(self, conn, tid):
        pass

    def askObject(self, conn, oid, serial, tid):
        pass

    def answerObject(self, conn, oid, serial_start,
                           serial_end, compression, checksum, data):
        pass

    def askTIDs(self, conn, first, last, partition):
        pass

    def answerTIDs(self, conn, tid_list):
        pass

    def askTransactionInformation(self, conn, tid):
        pass

    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        pass

    def askObjectHistory(self, conn, oid, first, last):
        pass

    def answerObjectHistory(self, conn, oid, history_list):
        pass

    def askOIDs(self, conn, first, last, partition):
        pass

    def answerOIDs(self, conn, oid_list):
        pass

    def askPartitionList(self, conn, min_offset, max_offset, uuid):
        pass

    def answerPartitionList(self, conn, ptid, row_list):
        pass

    def askNodeList(self, conn, offset_list):
        pass

    def answerNodeList(self, conn, node_list):
        pass

    def setNodeState(self, conn, uuid, state, modify_partition_table):
        pass

    def answerNodeState(self, conn, uuid, state):
        pass

    def addPendingNodes(self, conn, uuid_list):
        pass

    def answerNewNodes(self, conn, uuid_list):
        pass

    def askNodeInformation(self, conn):
        pass

    def answerNodeInformation(self, conn):
        pass

    def askClusterState(self, conn):
        pass

    def answerClusterState(self, conn, state):
        pass

    def setClusterState(self, conn, state):
        pass

    def notifyClusterInformation(self, conn, state):
        pass

    def notifyLastOID(self, conn, oid):
        pass

    def notifyReplicationDone(self, conn, offset):
        pass


PACKET_LOGGER = PacketLogger()
