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

import neo
from neo.lib.util import dump
from neo.lib.protocol import ClusterStates, Packets, NodeStates
from neo.master.handlers import BaseServiceHandler


class VerificationFailure(Exception):
    """
        Exception raised each time the cluster integrity failed.
          - An required storage node is missing
          - A transaction or an object is missing on a node
    """
    pass


class VerificationManager(BaseServiceHandler):
    """
      Manager for verification step of a NEO cluster:
        - Wait for at least one available storage per partition
        - Check if all expected content is present
    """

    def __init__(self, app):
        BaseServiceHandler.__init__(self, app)
        self._oid_set = set()
        self._tid_set = set()
        self._uuid_set = set()
        self._object_present = False

    def _askStorageNodesAndWait(self, packet, node_list):
        poll = self.app.em.poll
        operational = self.app.pt.operational
        uuid_set = self._uuid_set
        uuid_set.clear()
        for node in node_list:
            uuid_set.add(node.getUUID())
            node.ask(packet)
        while True:
            poll(1)
            if not operational():
                raise VerificationFailure
            if not uuid_set:
                break

    def _gotAnswerFrom(self, uuid):
        """
        Returns True if answer from given uuid is waited upon by
        _askStorageNodesAndWait, False otherwise.

        Also, mark this uuid as having answered, so it stops being waited upon
        by _askStorageNodesAndWait.
        """
        try:
            self._uuid_set.remove(uuid)
        except KeyError:
            result = False
        else:
            result = True
        return result

    def getHandler(self):
        return self

    def identifyStorageNode(self, uuid, node):
        """
            Returns the handler to manager the given node
        """
        state = NodeStates.RUNNING
        if uuid is None or node is None:
            # if node is unknown, it has been forget when the current
            # partition was validated by the admin
            # Here the uuid is not cleared to allow lookup pending nodes by
            # uuid from the test framework. It's safe since nodes with a
            # conflicting UUID are rejected in the identification handler.
            state = NodeStates.PENDING
        return (uuid, state, self)

    def run(self):

        self.app.changeClusterState(ClusterStates.VERIFYING)
        while True:
            try:
                self.verifyData()
            except VerificationFailure:
                continue
            break
        # At this stage, all non-working nodes are out-of-date.
        cell_list = self.app.pt.outdate()

        # Tweak the partition table, if the distribution of storage nodes
        # is not uniform.
        cell_list.extend(self.app.pt.tweak())

        # If anything changed, send the changes.
        self.app.broadcastPartitionChanges(cell_list)

    def verifyData(self):
        """Verify the data in storage nodes and clean them up, if necessary."""

        em, nm = self.app.em, self.app.nm

        # wait for any missing node
        neo.lib.logging.debug('waiting for the cluster to be operational')
        while not self.app.pt.operational():
            em.poll(1)

        neo.lib.logging.info('start to verify data')

        # Gather all unfinished transactions.
        self._askStorageNodesAndWait(Packets.AskUnfinishedTransactions(),
            [x for x in self.app.nm.getIdentifiedList() if x.isStorage()])

        # Gather OIDs for each unfinished TID, and verify whether the
        # transaction can be finished or must be aborted. This could be
        # in parallel in theory, but not so easy. Thus do it one-by-one
        # at the moment.
        for tid in self._tid_set:
            uuid_set = self.verifyTransaction(tid)
            if uuid_set is None:
                packet = Packets.DeleteTransaction(tid, self._oid_set or [])
                # Make sure that no node has this transaction.
                for node in self.app.nm.getIdentifiedList():
                    if node.isStorage():
                        node.notify(packet)
            else:
                packet = Packets.CommitTransaction(tid)
                for node in self.app.nm.getIdentifiedList(pool_set=uuid_set):
                    node.notify(packet)
            self._oid_set = set()

            # If possible, send the packets now.
            em.poll(0)

    def verifyTransaction(self, tid):
        em = self.app.em
        nm = self.app.nm
        uuid_set = set()

        # Determine to which nodes I should ask.
        partition = self.app.pt.getPartition(tid)
        uuid_list = [cell.getUUID() for cell \
                in self.app.pt.getCellList(partition, readable=True)]
        if len(uuid_list) == 0:
            raise VerificationFailure
        uuid_set.update(uuid_list)

        # Gather OIDs.
        node_list = self.app.nm.getIdentifiedList(pool_set=uuid_list)
        if len(node_list) == 0:
            raise VerificationFailure
        self._askStorageNodesAndWait(Packets.AskTransactionInformation(tid),
            node_list)

        if self._oid_set is None or len(self._oid_set) == 0:
            # Not commitable.
            return None
        # Verify that all objects are present.
        for oid in self._oid_set:
            partition = self.app.pt.getPartition(oid)
            object_uuid_list = [cell.getUUID() for cell \
                        in self.app.pt.getCellList(partition, readable=True)]
            if len(object_uuid_list) == 0:
                raise VerificationFailure
            uuid_set.update(object_uuid_list)

            self._object_present = True
            self._askStorageNodesAndWait(Packets.AskObjectPresent(oid, tid),
                nm.getIdentifiedList(pool_set=object_uuid_list))
            if not self._object_present:
                # Not commitable.
                return None

        return uuid_set

    def answerLastIDs(self, conn, loid, ltid, lptid):
        # FIXME: this packet should not allowed here, the master already
        # accepted the current partition table end IDs. As there were manually
        # approved during recovery, there is no need to check them here.
        pass

    def answerUnfinishedTransactions(self, conn, max_tid, tid_list):
        uuid = conn.getUUID()
        neo.lib.logging.info('got unfinished transactions %s from %r',
            [dump(tid) for tid in tid_list], conn)
        if not self._gotAnswerFrom(uuid):
            return
        self._tid_set.update(tid_list)

    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        uuid = conn.getUUID()
        app = self.app
        if not self._gotAnswerFrom(uuid):
            return
        oid_set = set(oid_list)
        if self._oid_set is None:
            # Someone does not agree.
            pass
        elif len(self._oid_set) == 0:
            # This is the first answer.
            self._oid_set.update(oid_set)
        elif self._oid_set != oid_set:
            raise ValueError, "Inconsistent transaction %s" % \
                (dump(tid, ))

    def tidNotFound(self, conn, message):
        uuid = conn.getUUID()
        neo.lib.logging.info('TID not found: %s', message)
        if not self._gotAnswerFrom(uuid):
            return
        self._oid_set = None

    def answerObjectPresent(self, conn, oid, tid):
        uuid = conn.getUUID()
        neo.lib.logging.info('object %s:%s found', dump(oid), dump(tid))
        self._gotAnswerFrom(uuid)

    def oidNotFound(self, conn, message):
        uuid = conn.getUUID()
        neo.lib.logging.info('OID not found: %s', message)
        app = self.app
        if not self._gotAnswerFrom(uuid):
            return
        app._object_present = False

    def connectionCompleted(self, conn):
        pass

    def nodeLost(self, conn, node):
        if not self.app.pt.operational():
            raise VerificationFailure, 'cannot continue verification'

