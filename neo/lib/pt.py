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

from functools import wraps
import neo

from neo.lib import protocol
from neo.lib.protocol import CellStates
from neo.lib.util import dump, u64
from neo.lib.locking import RLock

class PartitionTableException(Exception):
    """
        Base class for partition table exceptions
    """

class Cell(object):
    """This class represents a cell in a partition table."""

    def __init__(self, node, state = CellStates.UP_TO_DATE):
        self.node = node
        self.state = state

    def __repr__(self):
        return "<Cell(uuid=%s, address=%s, state=%s)>" % (
            dump(self.getUUID()),
            self.getAddress(),
            self.getState(),
        )

    def getState(self):
        return self.state

    def setState(self, state):
        self.state = state

    def isUpToDate(self):
        return self.state == CellStates.UP_TO_DATE

    def isOutOfDate(self):
        return self.state == CellStates.OUT_OF_DATE

    def isFeeding(self):
        return self.state == CellStates.FEEDING

    def getNode(self):
        return self.node

    def getNodeState(self):
        """This is a short hand."""
        return self.node.getState()

    def getUUID(self):
        return self.node.getUUID()

    def getAddress(self):
        return self.node.getAddress()


class PartitionTable(object):
    """This class manages a partition table."""

    def __init__(self, num_partitions, num_replicas):
        self._id = None
        self.np = num_partitions
        self.nr = num_replicas
        self.num_filled_rows = 0
        # Note: don't use [[]] * num_partition construct, as it duplicates
        # instance *references*, so the outer list contains really just one
        # inner list instance.
        self.partition_list = [[] for _ in xrange(num_partitions)]
        self.count_dict = {}

    def getID(self):
        return self._id

    def getPartitions(self):
        return self.np

    def getReplicas(self):
        return self.nr

    def clear(self):
        """Forget an existing partition table."""
        self._id = None
        self.num_filled_rows = 0
        # Note: don't use [[]] * self.np construct, as it duplicates
        # instance *references*, so the outer list contains really just one
        # inner list instance.
        self.partition_list = [[] for _ in xrange(self.np)]
        self.count_dict.clear()

    def getAssignedPartitionList(self, uuid):
        """ Return the partition assigned to the specified UUID """
        assigned_partitions = []
        for offset in xrange(self.np):
            for cell in self.getCellList(offset, readable=True):
                if cell.getUUID() == uuid:
                    assigned_partitions.append(offset)
                    break
        return assigned_partitions

    def hasOffset(self, offset):
        try:
            return len(self.partition_list[offset]) > 0
        except IndexError:
            return False

    def getNodeList(self):
        """Return all used nodes."""
        return [node for node, count in self.count_dict.iteritems() \
                if count > 0]

    def getCellList(self, offset, readable=False, writable=False):
        # allow all cell states
        state_set = set(CellStates.values())
        if readable or writable:
            # except non readables
            state_set.remove(CellStates.DISCARDED)
        if readable:
            # except non writables
            state_set.remove(CellStates.OUT_OF_DATE)
        try:
            return [cell for cell in self.partition_list[offset] \
                    if cell is not None and cell.getState() in state_set]
        except (TypeError, KeyError):
            return []

    def getCellListForTID(self, tid, readable=False, writable=False):
        return self.getCellList(self.getPartition(tid), readable, writable)

    def getCellListForOID(self, oid, readable=False, writable=False):
        return self.getCellList(self.getPartition(oid), readable, writable)

    def getPartition(self, oid_or_tid):
        return u64(oid_or_tid) % self.getPartitions()

    def getOutdatedOffsetListFor(self, uuid):
        return [
            offset for offset in xrange(self.np)
            for c in self.partition_list[offset]
            if c.getUUID() == uuid and c.getState() == CellStates.OUT_OF_DATE
        ]

    def isAssigned(self, oid, uuid):
        """ Check if the oid is assigned to the given node """
        for cell in self.partition_list[u64(oid) % self.np]:
            if cell.getUUID() == uuid:
                return True
        return False

    def setCell(self, offset, node, state):
        if state == CellStates.DISCARDED:
            return self.removeCell(offset, node)
        if node.isBroken() or node.isDown():
            raise PartitionTableException('Invalid node state')

        self.count_dict.setdefault(node, 0)
        row = self.partition_list[offset]
        if len(row) == 0:
            # Create a new row.
            row = [Cell(node, state), ]
            if state != CellStates.FEEDING:
                self.count_dict[node] += 1
            self.partition_list[offset] = row

            self.num_filled_rows += 1
        else:
            # XXX this can be slow, but it is necessary to remove a duplicate,
            # if any.
            for cell in row:
                if cell.getNode() == node:
                    row.remove(cell)
                    if not cell.isFeeding():
                        self.count_dict[node] -= 1
                    break
            row.append(Cell(node, state))
            if state != CellStates.FEEDING:
                self.count_dict[node] += 1
        return (offset, node.getUUID(), state)

    def removeCell(self, offset, node):
        row = self.partition_list[offset]
        assert row is not None
        for cell in row:
            if cell.getNode() == node:
                row.remove(cell)
                if not cell.isFeeding():
                    self.count_dict[node] -= 1
                break
        return (offset, node.getUUID(), CellStates.DISCARDED)

    def load(self, ptid, row_list, nm):
        """
        Load the partition table with the specified PTID, discard all previous
        content.
        """
        self.clear()
        self._id = ptid
        for offset, row in row_list:
            if offset >= self.getPartitions():
                raise IndexError
            for uuid, state in row:
                node = nm.getByUUID(uuid)
                # the node must be known by the node manager
                assert node is not None
                self.setCell(offset, node, state)
        neo.lib.logging.debug('partition table loaded (ptid=%s)', ptid)
        self.log()

    def update(self, ptid, cell_list, nm):
        """
        Update the partition with the cell list supplied. Ignore those changes
        if the partition table ID is not greater than the current one. If a node
        is not known, it is created in the node manager and set as unavailable
        """
        if ptid <= self._id:
            neo.lib.logging.warning('ignoring older partition changes')
            return
        self._id = ptid
        for offset, uuid, state in cell_list:
            node = nm.getByUUID(uuid)
            assert node is not None, 'No node found for uuid %r' % (dump(uuid), )
            self.setCell(offset, node, state)
        neo.lib.logging.debug('partition table updated (ptid=%s)', ptid)
        self.log()

    def filled(self):
        return self.num_filled_rows == self.np

    def log(self):
        for line in self._format():
            neo.lib.logging.debug(line)

    def format(self):
        return '\n'.join(self._format())

    def _format(self):
        """Help debugging partition table management.

        Output sample:
        DEBUG:root:pt: node 0: ad7ffe8ceef4468a0c776f3035c7a543, R
        DEBUG:root:pt: node 1: a68a01e8bf93e287bd505201c1405bc2, R
        DEBUG:root:pt: node 2: 67ae354b4ed240a0594d042cf5c01b28, R
        DEBUG:root:pt: node 3: df57d7298678996705cd0092d84580f4, R
        DEBUG:root:pt: 00000000: .UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.
        DEBUG:root:pt: 00000009: U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U

        Here, there are 4 nodes in RUNNING state.
        The first partition has 2 replicas in UP_TO_DATE state, on nodes 1 and
        2 (nodes 0 and 3 are displayed as unused for that partition by
        displaying a dot).
        The 8-digits number on the left represents the number of the first
        partition on the line (here, line length is 9 to keep the docstring
        width under 80 column).
        """
        result = []
        append = result.append
        node_list = self.count_dict.keys()
        node_list = [k for k, v in self.count_dict.items() if v != 0]
        node_list.sort()
        node_dict = {}
        for i, node in enumerate(node_list):
            uuid = node.getUUID()
            node_dict[uuid] = i
            append('pt: node %d: %s, %s' % (i, dump(uuid),
                protocol.node_state_prefix_dict[node.getState()]))
        line = []
        max_line_len = 20 # XXX: hardcoded number of partitions per line
        cell_state_dict = protocol.cell_state_prefix_dict
        for offset, row in enumerate(self.partition_list):
            if len(line) == max_line_len:
                append('pt: %08d: %s' % (offset - max_line_len,
                              '|'.join(line)))
                line = []
            if row is None:
                line.append('X' * len(node_list))
            else:
                cell = []
                cell_dict = dict([(node_dict.get(x.getUUID(), None), x)
                    for x in row])
                for node in xrange(len(node_list)):
                    if node in cell_dict:
                        cell.append(cell_state_dict[cell_dict[node].getState()])
                    else:
                        cell.append('.')
                line.append(''.join(cell))
        if len(line):
            append('pt: %08d: %s' % (offset - len(line) + 1,
                          '|'.join(line)))
        return result

    def operational(self):
        if not self.filled():
            return False
        for row in self.partition_list:
            for cell in row:
                if (cell.isUpToDate() or cell.isFeeding()) and \
                        cell.getNode().isRunning():
                    break
            else:
                return False
        return True

    def getRow(self, offset):
        row = self.partition_list[offset]
        if row is None:
            return []
        return [(cell.getUUID(), cell.getState()) for cell in row]

    def getRowList(self):
        getRow = self.getRow
        return [(x, getRow(x)) for x in xrange(self.np)]

    def getNodeMap(self):
        """ Return a list of 2-tuple: (uuid, partition_list) """
        uuid_map = {}
        for index, row in enumerate(self.partition_list):
            for cell in row:
                uuid_map.setdefault(cell.getNode(), []).append(index)
        return uuid_map

def thread_safe(method):
    def wrapper(self, *args, **kwargs):
        self.lock()
        try:
            return method(self, *args, **kwargs)
        finally:
            self.unlock()
    return wraps(method)(wrapper)


class MTPartitionTable(PartitionTable):
    """ Thread-safe aware version of the partition table, override only methods
        used in the client """

    def __init__(self, *args, **kwargs):
        self._lock = RLock()
        PartitionTable.__init__(self, *args, **kwargs)

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

    @thread_safe
    def getCellListForTID(self, *args, **kwargs):
        return PartitionTable.getCellListForTID(self, *args, **kwargs)

    @thread_safe
    def getCellListForOID(self, *args, **kwargs):
        return PartitionTable.getCellListForOID(self, *args, **kwargs)

    @thread_safe
    def setCell(self, *args, **kwargs):
        return PartitionTable.setCell(self, *args, **kwargs)

    @thread_safe
    def clear(self, *args, **kwargs):
        return PartitionTable.clear(self, *args, **kwargs)

    @thread_safe
    def operational(self, *args, **kwargs):
        return PartitionTable.operational(self, *args, **kwargs)

    @thread_safe
    def getNodeList(self, *args, **kwargs):
        return PartitionTable.getNodeList(self, *args, **kwargs)

    @thread_safe
    def getNodeMap(self, *args, **kwargs):
        return PartitionTable.getNodeMap(self, *args, **kwargs)

