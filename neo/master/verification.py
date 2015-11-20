#
# Copyright (C) 2006-2015  Nexedi SA
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
from neo.lib.util import dump
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
        if not app.backup_tid:
            self.verifyData()
        app.setLastTransaction(app.tm.getLastTID())

    def verifyData(self):
        app = self.app
        logging.info('start to verify data')
        getIdentifiedList = app.nm.getIdentifiedList

        # Gather all transactions that may have been partially finished.
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
        all_set = set()
        for ttid, tid in self._locked_dict.iteritems():
            uuid_set = self._voted_dict.get(ttid)
            if uuid_set:
                all_set |= uuid_set
                packet = Packets.ValidateTransaction(ttid, tid)
                for node in getIdentifiedList(pool_set=uuid_set):
                    node.notify(packet)

        # Ask last oid/tid again for nodes that recovers locked transactions.
        # In fact, this is mainly for the last oid since the last tid can be
        # deduced from max(self._locked_dict.values()).
        # If getLastIDs is not always instantaneous for some backends, we
        # should split AskLastIDs to not ask the last oid/tid at the end of
        # recovery phase (and instead ask all nodes once, here).
        # With this request, we also prefer to make sure all nodes validate
        # successfully before switching to RUNNING state.
        self._askStorageNodesAndWait(Packets.AskLastIDs(),
            getIdentifiedList(all_set))

    def answerLastIDs(self, conn, loid, ltid, lptid, backup_tid):
        self._uuid_set.remove(conn.getUUID())
        tm = self.app.tm
        tm.setLastOID(loid)
        tm.setLastTID(ltid)
        ptid = self.app.pt.getID()
        assert lptid < ptid if None != lptid != ptid else not backup_tid

    def answerLockedTransactions(self, conn, tid_dict):
        uuid = conn.getUUID()
        self._uuid_set.remove(uuid)
        for ttid, tid in tid_dict.iteritems():
            if tid:
                self._locked_dict[ttid] = tid
            self._voted_dict[ttid].add(uuid)

    def answerFinalTID(self, conn, tid):
        self._uuid_set.remove(conn.getUUID())
        self._tid = tid

    def connectionCompleted(self, conn):
        pass

    def connectionLost(self, conn, new_state):
        self._uuid_set.discard(conn.getUUID())
        super(VerificationManager, self).connectionLost(conn, new_state)
