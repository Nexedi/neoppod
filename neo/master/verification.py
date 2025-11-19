#
# Copyright (C) 2006-2019  Nexedi SA
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
from neo import *
from neo.lib import logging
from neo.lib.protocol import ClusterStates, Packets, NodeStates, ZERO_TID
from neo.lib.util import add64
from .handlers import BaseServiceHandler


class VerificationManager(BaseServiceHandler):

    _last_tid = ZERO_TID

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
            app.nm.getStorageList(only_identified=True))
        app.setLastTransaction(self._last_tid)
        # Just to not return meaningless information in AnswerRecovery.
        app.truncate_tid = None
        # Set up pack manager.
        node_set = app.pt.getNodeSet(readable=True)
        try:
            pack_id = add64(min(node.completed_pack_id
                for node in node_set
                if hasattr(node, "completed_pack_id")), 1)
        except ValueError:
            pack_id = ZERO_TID
        self._askStorageNodesAndWait(Packets.AskPackOrders(pack_id), node_set)

    def verifyData(self):
        app = self.app
        logging.info('start to verify data')
        getIdentifiedList = app.nm.getIdentifiedList

        # Gather all transactions that may have been partially finished.
        # It's safe to query outdated cells from nodes with readable cells.
        # On the other hand, we must ignore temporary data from other nodes:
        #  1. S1:U  S2:U  ltid:10
        #  2. S1: restart with voted ttid=13
        #     S2: stop with locked ttid=13
        #  3. S1:U  S2:O  ltid:10
        #  4. verification drops ttid=13 because it's not locked
        #  5. new commits -> ltid:20
        #  6. S1 restarted, S2 started
        #  7. ttid=13 must be dropped
        # Replication will fix them if the data should have been validated.
        # And in the case such node had information about locked transactions,
        # a node with readable cells would have unlocked them.
        node_set = app.pt.getNodeSet(readable=True)
        assert all(node.isIdentified() for node in node_set), node_set
        self._askStorageNodesAndWait(Packets.AskLockedTransactions(), node_set)

        # Some nodes may have already unlocked these transactions and
        # _locked_dict is incomplete, but we can ask them the final tid.
        for ttid, voted_set in six.iteritems(self._voted_dict):
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
        for ttid, tid in six.iteritems(self._locked_dict):
            uuid_set = self._voted_dict.get(ttid)
            if uuid_set:
                packet = Packets.ValidateTransaction(ttid, tid)
                for node in getIdentifiedList(pool_set=uuid_set):
                    node.send(packet)

    def notifyPackCompleted(self, conn, pack_id):
        self.app.nm.getByUUID(conn.getUUID()).completed_pack_id = pack_id

    def answerLastIDs(self, conn, ltid, loid, ftid):
        self._uuid_set.remove(conn.getUUID())
        if None is not ltid > self._last_tid:
            self._last_tid = ltid
        tm = self.app.tm
        tm.setLastOID(loid)
        tm.setFirstTID(ftid)

    def answerPackOrders(self, conn, pack_list):
        self._uuid_set.remove(conn.getUUID())
        add = self.app.pm.add
        for pack_order in pack_list:
            add(*pack_order)

    def answerLockedTransactions(self, conn, tid_dict):
        uuid = conn.getUUID()
        self._uuid_set.remove(uuid)
        for ttid, tid in six.iteritems(tid_dict):
            if tid:
                self._locked_dict[ttid] = tid
            self._voted_dict[ttid].add(uuid)

    def answerFinalTID(self, conn, tid):
        self._uuid_set.remove(conn.getUUID())
        self._tid = tid

    def connectionLost(self, conn, new_state):
        self._uuid_set.discard(conn.getUUID())
        super(VerificationManager, self).connectionLost(conn, new_state)
