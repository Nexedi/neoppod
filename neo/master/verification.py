#
# Copyright (C) 2006-2016  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from collections import defaultdict
from neo.lib import logging
from neo.lib.protocol import ClusterStates, Packets, NodeStates
from .handlers import BaseServiceHandler


class VerificationManager(BaseServiceHandler):

    def __init__(self, app):
        self._locked_dict = {}
        self._voted_dict = defaultdict(set)
        self._uuid_set = set()

    def _askStorageNodesAndWait(self, packet, node_list):
        poll = self.app.em.poll
        uuid_set = self._uuid_set
        uuid_set.clear()
        for node in node_list:
            uuid_set.add(node.getUUID())
            node.ask(packet)
        while uuid_set:
            poll(1)

    def getHandler(self):
        return self

    def identifyStorageNode(self, known):
        """
            Returns the handler to manager the given node
        """
        if known:
            state = NodeStates.RUNNING
        else:
            # if node is unknown, it has been forget when the current
            # partition was validated by the admin
            # Here the uuid is not cleared to allow lookup pending nodes by
            # uuid from the test framework. It's safe since nodes with a
            # conflicting UUID are rejected in the identification handler.
            state = NodeStates.PENDING
        return state, self

    def run(self):
        app = self.app
        app.changeClusterState(ClusterStates.VERIFYING)
        app.tm.reset()
        if not app.backup_tid:
            self.verifyData()
        # This is where storages truncate if requested:
        # - we make sure all nodes are running with a truncate_tid value saved
        # - there's no unfinished data
        # - just before they return the last tid/oid
        self._askStorageNodesAndWait(Packets.AskLastIDs(),
            [x for x in app.nm.getIdentifiedList() if x.isStorage()])
        app.setLastTransaction(app.tm.getLastTID())
        # Just to not return meaningless information in AnswerRecovery.
        app.truncate_tid = None

    def verifyData(self):
        app = self.app
        logging.info('start to verify data')
        getIdentifiedList = app.nm.getIdentifiedList

        # Gather all transactions that may have been partially finished.
        # It's safe to query outdated cells from nodes with readable cells.
        # For other nodes, it's more complicated:
        #  1. pt: U|U  ltid: 10
        #  2. S1: restart with voted ttid=13
        #     S2: stop with locked ttid=13
        #  3. pt: U|O  ltid: 10
        #  4. verification drops ttid=13 because it's not locked
        #  5. new commits -> ltid: 20
        #  6. S1 restarted, S2 started
        #  7. ttid=13 must be dropped
        # And we can't ignore ttid < last tid for all nodes, even if the
        # master serializes unlock notifications:
        #  1. pt: U.|.U  ltid: 15
        #  2. unlock ttid=18 to S1
        #  3. unlock ttid=20 to S2
        #  4. S1 stopped before unlocking ttid=18
        #  5. S2 unlocks ttid=20
        #  6. back to recovery, S1 started
        #  7. verification must validate ttid=18
        # So for nodes without any readable cell, and only for them, we only
        # check if they have locked transactions. Replication will do the rest.
        self._askStorageNodesAndWait(Packets.AskLockedTransactions(),
            [x for x in getIdentifiedList() if x.isStorage()])

        # Some nodes may have already unlocked these transactions and
        # _locked_dict is incomplete, but we can ask them the final tid.
        for ttid, voted_set in self._voted_dict.iteritems():
            if ttid in self._locked_dict:
                continue
            partition = app.pt.getPartition(ttid)
            for node in getIdentifiedList(pool_set={cell.getUUID()
                    # If an outdated cell had unlocked ttid, then either
                    # it is already in _locked_dict or a readable cell also
                    # unlocked it.
                    for cell in app.pt.getCellList(partition, readable=True)
                    } - voted_set):
                self._askStorageNodesAndWait(Packets.AskFinalTID(ttid), (node,))
                if self._tid is not None:
                    self._locked_dict[ttid] = self._tid
                    break
            else:
                # Transaction not locked. No need to tell nodes to delete it,
                # since they drop any unfinished data just before being
                # operational.
                pass

        # Finish all transactions for which we know that tpc_finish was called
        # but not fully processed. This may include replicas with transactions
        # that were not even locked.
        for ttid, tid in self._locked_dict.iteritems():
            uuid_set = self._voted_dict.get(ttid)
            if uuid_set:
                packet = Packets.ValidateTransaction(ttid, tid)
                for node in getIdentifiedList(pool_set=uuid_set):
                    node.notify(packet)

    def answerLastIDs(self, conn, loid, ltid):
        self._uuid_set.remove(conn.getUUID())
        tm = self.app.tm
        tm.setLastOID(loid)
        tm.setLastTID(ltid)

    def answerLockedTransactions(self, conn, tid_dict):
        uuid = conn.getUUID()
        self._uuid_set.remove(uuid)
        app = self.app
        node = app.nm.getByUUID(uuid)
        vote = any(x[1].isReadable() for x in app.pt.iterNodeCell(node))
        for ttid, tid in tid_dict.iteritems():
            if tid:
                self._locked_dict[ttid] = tid
            if vote:
                self._voted_dict[ttid].add(uuid)

    def answerFinalTID(self, conn, tid):
        self._uuid_set.remove(conn.getUUID())
        self._tid = tid

    def connectionLost(self, conn, new_state):
        self._uuid_set.discard(conn.getUUID())
        super(VerificationManager, self).connectionLost(conn, new_state)
