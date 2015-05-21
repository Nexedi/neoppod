#
# Copyright (C) 2006-2015  Nexedi SA

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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from neo.lib.protocol import NodeStates, Packets, ProtocolError
from . import MasterHandler

class ClientServiceHandler(MasterHandler):
    """ Handler dedicated to client during service state """

    def connectionCompleted(self, conn):
        pass

    def connectionLost(self, conn, new_state):
        # cancel its transactions and forgot the node
        app = self.app
        if app.listening_conn: # if running
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

    def askBeginTransaction(self, conn, tid):
        """
            A client request a TID, nothing is kept about it until the finish.
        """
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        conn.answer(Packets.AnswerBeginTransaction(app.tm.begin(node, tid)))

    def askNewOIDs(self, conn, num_oids):
        conn.answer(Packets.AnswerNewOIDs(self.app.tm.getNextOIDList(num_oids)))

    def askFinishTransaction(self, conn, ttid, oid_list):
        app = self.app
        pt = app.pt

        # Collect partitions related to this transaction.
        partition_set = set(map(pt.getPartition, oid_list))
        partition_set.add(pt.getPartition(ttid))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_list = filter(app.isStorageReady, {cell.getUUID()
            for part in partition_set
            for cell in pt.getCellList(part)
            if cell.getNodeState() != NodeStates.HIDDEN})
        if not uuid_list:
            raise ProtocolError('No storage node ready for transaction')

        identified_node_list = app.nm.getIdentifiedList(pool_set=set(uuid_list))

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        p = Packets.AskLockInformation(
            ttid,
            app.tm.prepare(
                ttid,
                pt.getPartitions(),
                oid_list,
                {x.getUUID() for x in identified_node_list},
                conn.getPeerId(),
            ),
            oid_list,
        )
        for node in identified_node_list:
            node.ask(p, timeout=60)

    def askPack(self, conn, tid):
        app = self.app
        if app.packing is None:
            storage_list = app.nm.getStorageList(only_identified=True)
            app.packing = (conn, conn.getPeerId(),
                {x.getUUID() for x in storage_list})
            p = Packets.AskPack(tid)
            for storage in storage_list:
                storage.getConnection().ask(p)
        else:
            conn.answer(Packets.AnswerPack(False))

    def abortTransaction(self, conn, tid):
        self.app.tm.remove(conn.getUUID(), tid)

