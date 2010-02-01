#
# Copyright (C) 2006-2010  Nexedi SA

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

from neo.protocol import NodeStates, Packets, ProtocolError
from neo.master.handlers import BaseServiceHandler
from neo.util import dump


class ClientServiceHandler(BaseServiceHandler):
    """ Handler dedicated to client during service state """

    def connectionCompleted(self, conn):
        pass

    def nodeLost(self, conn, node):
        # cancel it's transactions and forgot the node
        self.app.tm.abortFor(node)
        self.app.nm.remove(node)

    def abortTransaction(self, conn, tid):
        if tid in self.app.tm:
            self.app.tm.remove(tid)
        else:
            logging.warn('aborting transaction %s does not exist', dump(tid))

    def askBeginTransaction(self, conn, tid):
        node = self.app.nm.getByUUID(conn.getUUID())
        tid = self.app.tm.begin(node, tid)
        conn.answer(Packets.AnswerBeginTransaction(tid))

    def askNewOIDs(self, conn, num_oids):
        oid_list = self.app.getNewOIDList(num_oids)
        conn.answer(Packets.AnswerNewOIDs(oid_list))

    def finishTransaction(self, conn, oid_list, tid):
        app = self.app
        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if tid > self.app.tm.getLastTID():
            raise ProtocolError('TID too big')

        # Collect partitions related to this transaction.
        getPartition = app.pt.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        for part in partition_set:
            uuid_set.update((cell.getUUID() for cell in app.pt.getCellList(part)
                             if cell.getNodeState() != NodeStates.HIDDEN))

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        used_uuid_set = set()
        for c in app.em.getConnectionList():
            if c.getUUID() in uuid_set:
                c.ask(Packets.LockInformation(tid), timeout=60)
                used_uuid_set.add(c.getUUID())

        app.tm.prepare(tid, oid_list, used_uuid_set, conn.getPeerId())

