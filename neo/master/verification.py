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
from neo.util import dump
from neo.protocol import ClusterStates, Packets, NodeStates
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
        self._uuid_dict = {}
        self._object_present = False

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
        logging.debug('waiting for the cluster to be operational')
        while not self.app.pt.operational():
            em.poll(1)

        logging.info('start to verify data')

        # Gather all unfinished transactions.
        for node in self.app.nm.getIdentifiedList():
            if node.isStorage():
                self._uuid_dict[node.getUUID()] = False
                node.ask(Packets.AskUnfinishedTransactions())

        while True:
            em.poll(1)
            if not self.app.pt.operational():
                raise VerificationFailure
            if False not in self._uuid_dict.values():
                break

        # Gather OIDs for each unfinished TID, and verify whether the
        # transaction can be finished or must be aborted. This could be
        # in parallel in theory, but not so easy. Thus do it one-by-one
        # at the moment.
        for tid in self._tid_set:
            uuid_set = self.verifyTransaction(tid)
            if uuid_set is None:
                # Make sure that no node has this transaction.
                for node in self.app.nm.getIdentifiedList():
                    if node.isStorage():
                        node.notify(Packets.DeleteTransaction(tid))
            else:
                for node in self.app.nm.getIdentifiedList(pool_set=uuid_set):
                    node.notify(Packets.CommitTransaction(tid))

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
        self._uuid_dict = {}
        for node in self.app.nm.getIdentifiedList(pool_set=uuid_list):
            self._uuid_dict[node.getUUID()] = False
            node.ask(Packets.AskTransactionInformation(tid))
        if len(self._uuid_dict) == 0:
            raise VerificationFailure

        while True:
            em.poll(1)
            if not self.app.pt.operational():
                raise VerificationFailure
            if False not in self._uuid_dict.values():
                break

        if self._oid_set is None or len(self._oid_set) == 0:
            # Not commitable.
            return None
        # Verify that all objects are present.
        for oid in self._oid_set:
            self._uuid_dict.clear()
            partition = self.app.pt.getPartition(oid)
            object_uuid_list = [cell.getUUID() for cell \
                        in self.app.pt.getCellList(partition, readable=True)]
            if len(object_uuid_list) == 0:
                raise VerificationFailure
            uuid_set.update(object_uuid_list)

            self._object_present = True
            for node in nm.getIdentifiedList(pool_set=object_uuid_list):
                self._uuid_dict[node.getUUID()] = False
                node.ask(Packets.AskObjectPresent(oid, tid))

            while True:
                em.poll(1)
                if not self.app.pt.operational():
                    raise VerificationFailure
                if False not in self._uuid_dict.values():
                    break

            if not self._object_present:
                # Not commitable.
                return None

        return uuid_set

    def answerLastIDs(self, conn, loid, ltid, lptid):
        # FIXME: this packet should not allowed here, the master already
        # accepted the current partition table end IDs. As there were manually
        # approved during recovery, there is no need to check them here.
        pass

    def answerUnfinishedTransactions(self, conn, tid_list):
        uuid = conn.getUUID()
        logging.info('got unfinished transactions %s from %s:%d',
                tid_list, *(conn.getAddress()))
        if self._uuid_dict.get(uuid, True):
            # No interest.
            return
        self._tid_set.update(tid_list)
        self._uuid_dict[uuid] = True

    def answerTransactionInformation(self, conn, tid,
                                           user, desc, ext, packed, oid_list):
        uuid = conn.getUUID()
        app = self.app
        if self._uuid_dict.get(uuid, True):
            # No interest.
            return
        oid_set = set(oid_list)
        if self._oid_set is None:
            # Someone does not agree.
            pass
        elif len(self._oid_set) == 0:
            # This is the first answer.
            self._oid_set.update(oid_set)
        elif self._oid_set != oid_set:
            self._oid_set = None
        self._uuid_dict[uuid] = True

    def tidNotFound(self, conn, message):
        uuid = conn.getUUID()
        logging.info('TID not found: %s', message)
        if self._uuid_dict.get(uuid, True):
            # No interest.
            return
        self._oid_set = None
        self._uuid_dict[uuid] = True

    def answerObjectPresent(self, conn, oid, tid):
        uuid = conn.getUUID()
        logging.info('object %s:%s found', dump(oid), dump(tid))
        if self._uuid_dict.get(uuid, True):
            # No interest.
            return
        self._uuid_dict[uuid] = True

    def oidNotFound(self, conn, message):
        uuid = conn.getUUID()
        logging.info('OID not found: %s', message)
        app = self.app
        if self._uuid_dict.get(uuid, True):
            # No interest.
            return
        app.object_present = False
        self._uuid_dict[uuid] = True

    def connectionCompleted(self, conn):
        pass

    def nodeLost(self, conn, node):
        if not self.app.pt.operational():
            raise VerificationFailure, 'cannot continue verification'

