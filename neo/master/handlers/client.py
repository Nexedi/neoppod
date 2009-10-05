#
# Copyright (C) 2006-2009  Nexedi SA
 
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
from neo.protocol import NodeStates, Packets, UnexpectedPacketError
from neo.master.handlers import BaseServiceHandler
from neo.util import dump, getNextTID

class FinishingTransaction(object):
    """This class describes a finishing transaction."""

    def __init__(self, conn):
        self._conn = conn
        self._msg_id = None
        self._oid_list = None
        self._uuid_set = None
        self._locked_uuid_set = set()

    def getConnection(self):
        return self._conn

    def setMessageId(self, msg_id):
        self._msg_id = msg_id

    def getMessageId(self):
        return self._msg_id

    def setOIDList(self, oid_list):
        self._oid_list = oid_list

    def getOIDList(self):
        return self._oid_list

    def setUUIDSet(self, uuid_set):
        self._uuid_set = uuid_set

    def getUUIDSet(self):
        return self._uuid_set

    def addLockedUUID(self, uuid):
        if uuid in self._uuid_set:
            self._locked_uuid_set.add(uuid)

    def allLocked(self):
        return self._uuid_set == self._locked_uuid_set


class ClientServiceHandler(BaseServiceHandler):
    """ Handler dedicated to client during service state """

    def connectionCompleted(self, conn):
        pass

    def nodeLost(self, conn, node):
        app = self.app
        for tid, t in app.finishing_transaction_dict.items():
            if t.getConnection() is conn:
                del app.finishing_transaction_dict[tid]

    def abortTransaction(self, conn, packet, tid):
        try:
            del self.app.finishing_transaction_dict[tid]
        except KeyError:
            logging.warn('aborting transaction %s does not exist', dump(tid))

    def askBeginTransaction(self, conn, packet, tid):
        app = self.app
        if tid is not None and tid < app.ltid:
            # supplied TID is in the past
            raise protocol.ProtocolError('invalid TID requested')
        if tid is None:
            # give a new transaction ID
            tid = getNextTID(app.ltid)
        app.ltid = tid
        app.finishing_transaction_dict[tid] = FinishingTransaction(conn)
        conn.answer(Packets.AnswerBeginTransaction(tid), packet.getId())

    def askNewOIDs(self, conn, packet, num_oids):
        oid_list = self.app.getNewOIDList(num_oids)
        conn.answer(Packets.AnswerNewOIDs(oid_list), packet.getId())

    def finishTransaction(self, conn, packet, oid_list, tid):
        app = self.app
        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            raise UnexpectedPacketError

        # Collect partitions related to this transaction.
        getPartition = app.pt.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        for part in partition_set:
            uuid_set.update((cell.getUUID() for cell in app.pt.getCellList(part) \
                             if cell.getNodeState() != NodeStates.HIDDEN))

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        used_uuid_set = set()
        for c in app.em.getConnectionList():
            if c.getUUID() in uuid_set:
                c.ask(Packets.LockInformation(tid), timeout=60)
                used_uuid_set.add(c.getUUID())

        try:
            t = app.finishing_transaction_dict[tid]
            t.setOIDList(oid_list)
            t.setUUIDSet(used_uuid_set)
            t.setMessageId(packet.getId())
        except KeyError:
            logging.warn('finishing transaction %s does not exist', dump(tid))

