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

import logging

from neo import protocol
from neo.protocol import HIDDEN_STATE
from neo.master.handlers import BaseServiceHandler
from neo.protocol import UnexpectedPacketError
from neo.util import dump

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

    def _nodeLost(self, conn, node):
        app = self.app
        for tid, t in app.finishing_transaction_dict.items():
            if t.getConnection() is conn:
                del app.finishing_transaction_dict[tid]

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        for node_type, addr, uuid, state in node_list:
            # XXX: client must notify only about storage failures, so remove
            # this assertion when done
            assert node_type == protocol.STORAGE_NODE_TYPE
            assert state in (protocol.TEMPORARILY_DOWN_STATE, protocol.BROKEN_STATE)
            node = self.app.nm.getNodeByUUID(uuid)
            assert node is not None
            if self.app.em.getConnectionByUUID(uuid) is None:
                # trust this notification only if I don't have a connexion to
                # this node
                node.setState(state)
            self.app.broadcastNodeInformation(node)

    def handleAbortTransaction(self, conn, packet, tid):
        try:
            del self.app.finishing_transaction_dict[tid]
        except KeyError:
            logging.warn('aborting transaction %s does not exist', dump(tid))

    def handleAskNewTID(self, conn, packet):
        app = self.app
        tid = app.getNextTID()
        app.finishing_transaction_dict[tid] = FinishingTransaction(conn)
        conn.answer(protocol.answerNewTID(tid), packet)

    def handleAskNewOIDs(self, conn, packet, num_oids):
        app = self.app
        oid_list = app.getNewOIDList(num_oids)
        conn.answer(protocol.answerNewOIDs(oid_list), packet)

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        app = self.app
        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            raise UnexpectedPacketError

        # Collect partitions related to this transaction.
        getPartition = app.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        for part in partition_set:
            uuid_set.update((cell.getUUID() for cell in app.pt.getCellList(part) \
                             if cell.getNodeState() != HIDDEN_STATE))

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        used_uuid_set = set()
        for c in app.em.getConnectionList():
            if c.getUUID() in uuid_set:
                c.ask(protocol.lockInformation(tid), timeout=60)
                used_uuid_set.add(c.getUUID())

        try:
            t = app.finishing_transaction_dict[tid]
            t.setOIDList(oid_list)
            t.setUUIDSet(used_uuid_set)
            t.setMessageId(packet.getId())
        except KeyError:
            logging.warn('finishing transaction %s does not exist', dump(tid))

