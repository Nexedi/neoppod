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

import neo

from neo.protocol import NodeStates, Packets, ProtocolError
from neo.master.handlers import MasterHandler
from neo.util import dump


class ClientServiceHandler(MasterHandler):
    """ Handler dedicated to client during service state """

    def connectionCompleted(self, conn):
        pass

    def connectionLost(self, conn, new_state):
        # cancel it's transactions and forgot the node
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        assert node is not None
        app.tm.abortFor(node)
        node.setState(NodeStates.DOWN)
        app.broadcastNodesInformation([node])
        app.nm.remove(node)

    def askNodeInformation(self, conn):
        # send informations about master and storages only
        nm = self.app.nm
        node_list = []
        node_list.extend(n.asTuple() for n in nm.getMasterList())
        node_list.extend(n.asTuple() for n in nm.getStorageList())
        conn.notify(Packets.NotifyNodeInformation(node_list))
        conn.answer(Packets.AnswerNodeInformation())

    def askBeginTransaction(self, conn):
        """
            A client request a TID, nothing is kept about it until the finish.
        """
        conn.answer(Packets.AnswerBeginTransaction(self.app.tm.begin()))

    def askNewOIDs(self, conn, num_oids):
        conn.answer(Packets.AnswerNewOIDs(self.app.tm.getNextOIDList(num_oids)))
        self.app.broadcastLastOID()

    def askFinishTransaction(self, conn, tid, oid_list):
        app = self.app

        # Collect partitions related to this transaction.
        getPartition = app.pt.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        isStorageReady = app.isStorageReady
        for part in partition_set:
            uuid_set.update((uuid for uuid in (
                    cell.getUUID() for cell in app.pt.getCellList(part)
                    if cell.getNodeState() != NodeStates.HIDDEN)
                if isStorageReady(uuid)))

        # check if greater and foreign OID was stored
        if self.app.tm.updateLastOID(oid_list):
            self.app.broadcastLastOID()

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        p = Packets.AskLockInformation(tid, oid_list)
        used_uuid_set = set()
        for node in self.app.nm.getIdentifiedList(pool_set=uuid_set):
            node.ask(p, timeout=60)
            used_uuid_set.add(node.getUUID())

        node = self.app.nm.getByUUID(conn.getUUID())
        app.tm.prepare(node, tid, oid_list, used_uuid_set, conn.getPeerId())

    def askPack(self, conn, tid):
        app = self.app
        if app.packing is None:
            storage_list = self.app.nm.getStorageList(only_identified=True)
            app.packing = (conn, conn.getPeerId(),
                set(x.getUUID() for x in storage_list))
            p = Packets.AskPack(tid)
            for storage in storage_list:
                storage.getConnection().ask(p)
        else:
            conn.answer(Packets.AnswerPack(False))

