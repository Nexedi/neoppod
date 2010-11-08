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

class Task(object):
    """
    A Task is a callable to execute at another time, with given parameters.
    Execution result is kept and can be retrieved later.
    """

    _func = None
    _args = None
    _kw = None
    _result = None
    _processed = False

    def __init__(self, func, args=(), kw=None):
        self._func = func
        self._args = args
        if kw is None:
            kw = {}
        self._kw = kw

    def process(self):
        if self._processed:
            raise ValueError, 'You cannot process a single Task twice'
        self._processed = True
        self._result = self._func(*self._args, **self._kw)

    def getResult(self):
        # Should we instead execute immediately rather than raising ?
        if not self._processed:
            raise ValueError, 'You cannot get a result until task is executed'
        return self._result

    def __repr__(self):
        fmt = '<%s at %x %r(*%r, **%r)%%s>' % (self.__class__.__name__,
            id(self), self._func, self._args, self._kw)
        if self._processed:
            extra = ' => %r' % (self._result, )
        else:
            extra = ''
        return fmt % (extra, )

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

    # new_partition_dict
    #   outdated partitions for which no critical tid was asked to primary
    #   master yet
    # critical_tid_list
    #   outdated partitions for which a critical tid was asked to primary
    #   master, but not answered so far
    # partition_dict
    #   outdated partitions (with or without a critical tid - if without, it
    #   was asked to primary master)
    # current_partition
    #   partition being currently synchronised
    # current_connection
    #   connection to a storage node we are replicating from
    # waiting_for_unfinished_tids
    #   unfinished_tid_list has been asked to primary master node, but it
    #   didn't answer yet.
    # unfinished_tid_list
    #   The list of unfinished TIDs known by master node.
    # replication_done
    #   False if we know there is something to replicate.
    #   True when current_partition is replicated, or we don't know yet if
    #   there is something to replicate

    current_partition = None
    current_connection = None
    waiting_for_unfinished_tids = False
    unfinished_tid_list = None
    replication_done = True

    def __init__(self, app):
        self.app = app
        self.new_partition_dict = {}
        self.critical_tid_list = []
        self.partition_dict = {}
        self.task_list = []
        self.task_dict = {}

    def masterLost(self):
        """
        When connection to primary master is lost, stop waiting for unfinished
        transactions.
        """
        self.critical_tid_list = []
        self.waiting_for_unfinished_tids = False

    def storageLost(self):
        """
        Restart replicating.
        """
        self.reset()

    def populate(self):
        """
        Populate partitions to replicate. Must be called when partition
        table is the one accepted by primary master.
        Implies a reset.
        """
        self.new_partition_dict = self._getOutdatedPartitionList()
        self.partition_dict = {}
        self.reset()

    def reset(self):
        """Reset attributes to restart replicating."""
        self.task_list = []
        self.task_dict = {}
        self.current_partition = None
        self.current_connection = None
        self.unfinished_tid_list = None
        self.replication_done = True

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

    def getCurrentRID(self):
        assert self.current_partition is not None
        return self.current_partition.getRID()

    def getCurrentCriticalTID(self):
        assert self.current_partition is not None
        return self.current_partition.getCriticalTID()

    def setReplicationDone(self):
        """ Callback from ReplicationHandler """
        self.replication_done = True

    def isCurrentConnection(self, conn):
        return self.current_connection is conn

    def setCriticalTID(self, tid):
        """This is a callback from MasterOperationHandler."""
        neo.logging.debug('setting critical TID %s to %s', dump(tid),
            ', '.join([str(p.getRID()) for p in self.critical_tid_list]))
        for partition in self.critical_tid_list:
            partition.setCriticalTID(tid)
        self.critical_tid_list = []

    def _askCriticalTID(self):
        self.app.master_conn.ask(Packets.AskLastIDs())
        self.critical_tid_list.extend(self.new_partition_dict.values())
        self.partition_dict.update(self.new_partition_dict)
        self.new_partition_dict = {}

    def setUnfinishedTIDList(self, tid_list):
        """This is a callback from MasterOperationHandler."""
        neo.logging.debug('setting unfinished TIDs %s',
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
        cell_list = app.pt.getCellList(self.current_partition.getRID(),
                                       readable=True)
        node_list = [cell.getNode() for cell in cell_list
                        if cell.getNodeState() == NodeStates.RUNNING]
        try:
            node = choice(node_list)
        except IndexError:
            # Not operational.
            neo.logging.error('not operational', exc_info = 1)
            self.current_partition = None
            return

        addr = node.getAddress()
        if addr is None:
            neo.logging.error("no address known for the selected node %s" %
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
        else:
            self.current_connection.getHandler().startReplication(
                self.current_connection)
        self.replication_done = False

    def _finishReplication(self):
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
        if not self.pending():
            self.current_connection.close()
            self.current_connection = None

    def act(self):
        # If the new partition list is not empty, I must ask a critical
        # TID to a primary master node.
        if self.new_partition_dict:
            self._askCriticalTID()

        if self.current_partition is not None:
            # Don't end replication until we have received all expected
            # answers, as we might have asked object data just before the last
            # AnswerCheckSerialRange.
            if self.replication_done and \
                    not self.current_connection.isPending():
                # finish a replication
                neo.logging.info('replication is done for %s' %
                        (self.current_partition.getRID(), ))
                self._finishReplication()
            return

        if self.waiting_for_unfinished_tids:
            # Still waiting.
            neo.logging.debug('waiting for unfinished tids')
            return

        if self.unfinished_tid_list is None:
            # Ask pending transactions.
            neo.logging.debug('asking unfinished tids')
            self._askUnfinishedTIDs()
            return

        # Try to select something.
        if len(self.unfinished_tid_list):
            min_unfinished_tid = min(self.unfinished_tid_list)
        else:
            min_unfinished_tid = None
        for partition in self.partition_dict.values():
            if partition.safe(min_unfinished_tid):
                self.current_partition = partition
                break
        else:
            # Not yet.
            self.unfinished_tid_list = None
            neo.logging.debug('not ready yet')
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

    def _addTask(self, key, func, args=(), kw=None):
        task = Task(func, args, kw)
        task_dict = self.task_dict
        if key in task_dict:
            raise ValueError, 'Task with key %r already exists (%r), cannot ' \
                'add %r' % (key, task_dict[key], task)
        task_dict[key] = task
        self.task_list.append(task)

    def processDelayedTasks(self):
        task_list = self.task_list
        if task_list:
            for task in task_list:
                task.process()
            self.task_list = []

    def checkTIDRange(self, min_tid, length, partition):
        app = self.app
        self._addTask(('TID', min_tid, length), app.dm.checkTIDRange,
            (min_tid, length, app.pt.getPartitions(), partition))

    def checkSerialRange(self, min_oid, min_serial, length, partition):
        app = self.app
        self._addTask(('Serial', min_oid, min_serial, length),
            app.dm.checkSerialRange, (min_oid, min_serial, length,
            app.pt.getPartitions(), partition))

    def getTIDsFrom(self, min_tid, max_tid, length, partition):
        app = self.app
        self._addTask('TIDsFrom',
            app.dm.getReplicationTIDList, (min_tid, max_tid, length,
            app.pt.getPartitions(), partition))

    def getObjectHistoryFrom(self, min_oid, min_serial, max_serial, length,
            partition):
        app = self.app
        self._addTask('ObjectHistoryFrom',
            app.dm.getObjectHistoryFrom, (min_oid, min_serial, max_serial,
            length, app.pt.getPartitions(), partition))

    def _getCheckResult(self, key):
        return self.task_dict.pop(key).getResult()

    def getTIDCheckResult(self, min_tid, length):
        return self._getCheckResult(('TID', min_tid, length))

    def getSerialCheckResult(self, min_oid, min_serial, length):
        return self._getCheckResult(('Serial', min_oid, min_serial, length))

    def getTIDsFromResult(self):
        return self._getCheckResult('TIDsFrom')

    def getObjectHistoryFromResult(self):
        return self._getCheckResult('ObjectHistoryFrom')

