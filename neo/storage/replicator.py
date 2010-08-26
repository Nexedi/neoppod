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
from random import choice

from neo.storage.handlers import replication
from neo.protocol import NodeTypes, NodeStates, CellStates, Packets, ZERO_TID
from neo.connection import ClientConnection
from neo.util import dump

class Partition(object):
    """This class abstracts the state of a partition."""

    def __init__(self, rid):
        self.rid = rid
        self.tid = None

    def getRID(self):
        return self.rid

    def getCriticalTID(self):
        return self.tid

    def setCriticalTID(self, tid):
        if tid is None:
            tid = ZERO_TID
        self.tid = tid

    def safe(self, min_pending_tid):
        tid = self.tid
        return tid is not None and (
            min_pending_tid is None or tid < min_pending_tid)

class Replicator(object):
    """This class handles replications of objects and transactions.

    Assumptions:

        - Client nodes recognize partition changes reasonably quickly.

        - When an out of date partition is added, next transaction ID
          is given after the change is notified and serialized.

    Procedures:

        - Get the last TID right after a partition is added. This TID
          is called a "critical TID", because this and TIDs before this
          may not be present in this storage node yet. After a critical
          TID, all transactions must exist in this storage node.

        - Check if a primary master node still has pending transactions
          before and at a critical TID. If so, I must wait for them to be
          committed or aborted.

        - In order to copy data, first get the list of TIDs. This is done
          part by part, because the list can be very huge. When getting
          a part of the list, I verify if they are in my database, and
          ask data only for non-existing TIDs. This is performed until
          the check reaches a critical TID.

        - Next, get the list of OIDs. And, for each OID, ask the history,
          namely, a list of serials. This is also done part by part, and
          I ask only non-existing data. """

    def __init__(self, app):
        self.app = app

    def populate(self):
        """
        Populate partitions to replicate. Must be called when partition
        table is the one accepted by primary master.
        Implies a reset.
        """
        self.new_partition_dict = self._getOutdatedPartitionList()
        self.critical_tid_dict = {}
        self.reset()

    def reset(self):
        """Reset attributes to restart replicating."""
        self.current_partition = None
        self.current_connection = None
        self.waiting_for_unfinished_tids = False
        self.unfinished_tid_list = None
        self.replication_done = True
        self.partition_dict = {}

    def _getOutdatedPartitionList(self):
        app = self.app
        partition_dict = {}
        for offset in xrange(app.pt.getPartitions()):
            for uuid, state in app.pt.getRow(offset):
                if uuid == app.uuid and state == CellStates.OUT_OF_DATE:
                    partition_dict[offset] = Partition(offset)
        return partition_dict

    def pending(self):
        """Return whether there is any pending partition."""
        return len(self.partition_dict) or len(self.new_partition_dict)

    def setCriticalTID(self, uuid, tid):
        """This is a callback from MasterOperationHandler."""
        try:
            partition_list = self.critical_tid_dict[uuid]
            logging.debug('setting critical TID %s to %s', dump(tid),
                         ', '.join([str(p.getRID()) for p in partition_list]))
            for partition in self.critical_tid_dict[uuid]:
                partition.setCriticalTID(tid)
            del self.critical_tid_dict[uuid]
        except KeyError:
            logging.debug("setCriticalTID raised KeyError for %s" %
                    (dump(uuid), ))

    def _askCriticalTID(self):
        conn = self.app.master_conn
        conn.ask(Packets.AskLastIDs())
        uuid = conn.getUUID()
        self.critical_tid_dict[uuid] = self.new_partition_dict.values()
        self.partition_dict.update(self.new_partition_dict)
        self.new_partition_dict = {}

    def setUnfinishedTIDList(self, tid_list):
        """This is a callback from MasterOperationHandler."""
        logging.debug('setting unfinished TIDs %s',
                      ','.join([dump(tid) for tid in tid_list]))
        self.waiting_for_unfinished_tids = False
        self.unfinished_tid_list = tid_list

    def _askUnfinishedTIDs(self):
        conn = self.app.master_conn
        conn.ask(Packets.AskUnfinishedTransactions())
        self.waiting_for_unfinished_tids = True

    def _startReplication(self):
        # Choose a storage node for the source.
        app = self.app
        try:
            cell_list = app.pt.getCellList(self.current_partition.getRID(),
                                           readable=True)
            node_list = [cell.getNode() for cell in cell_list
                            if cell.getNodeState() == NodeStates.RUNNING]
            node = choice(node_list)
        except IndexError:
            # Not operational.
            logging.error('not operational', exc_info = 1)
            self.current_partition = None
            return

        addr = node.getAddress()
        if addr is None:
            logging.error("no address known for the selected node %s" %
                    (dump(node.getUUID()), ))
            return
        if self.current_connection is not None:
            if self.current_connection.getAddress() != addr:
                self.current_connection.close()
                self.current_connection = None

        if self.current_connection is None:
            handler = replication.ReplicationHandler(app)
            self.current_connection = ClientConnection(app.em, handler,
                   addr=addr, connector=app.connector_handler())
            p = Packets.RequestIdentification(NodeTypes.STORAGE,
                    app.uuid, app.server, app.name)
            self.current_connection.ask(p)

        p = Packets.AskTIDsFrom(ZERO_TID, 1000,
            self.current_partition.getRID())
        self.current_connection.ask(p, timeout=300)

        self.replication_done = False

    def _finishReplication(self):
        app = self.app
        # TODO: remove try..except: pass
        try:
            self.partition_dict.pop(self.current_partition.getRID())
            # Notify to a primary master node that my cell is now up-to-date.
            conn = self.app.master_conn
            offset = self.current_partition.getRID()
            conn.notify(Packets.NotifyReplicationDone(offset))
        except KeyError:
            pass
        self.current_partition = None

    def act(self):
        # If the new partition list is not empty, I must ask a critical
        # TID to a primary master node.
        if self.new_partition_dict:
            self._askCriticalTID()

        if self.current_partition is not None:
            if self.replication_done:
                # finish a replication
                logging.info('replication is done for %s' %
                        (self.current_partition.getRID(), ))
                self._finishReplication()
            return

        if self.waiting_for_unfinished_tids:
            # Still waiting.
            logging.debug('waiting for unfinished tids')
            return

        if self.unfinished_tid_list is None:
            # Ask pending transactions.
            logging.debug('asking unfinished tids')
            self._askUnfinishedTIDs()
            return

        # Try to select something.
        if len(self.unfinished_tid_list):
            min_unfinished_tid = min(self.unfinished_tid_list)
        else:
            min_unfinished_tid = None
        self.unfinished_tid_list = None
        for partition in self.partition_dict.values():
            if partition.safe(min_unfinished_tid):
                self.current_partition = partition
                break
        else:
            # Not yet.
            logging.debug('not ready yet')
            return

        self._startReplication()

    def removePartition(self, rid):
        """This is a callback from MasterOperationHandler."""
        self.partition_dict.pop(rid, None)
        self.new_partition_dict.pop(rid, None)

    def addPartition(self, rid):
        """This is a callback from MasterOperationHandler."""
        if not self.partition_dict.has_key(rid) \
                and not self.new_partition_dict.has_key(rid):
            self.new_partition_dict[rid] = Partition(rid)

