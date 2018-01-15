#
# Copyright (C) 2006-2017  Nexedi SA
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

import math
from . import logging, protocol
from .locking import Lock
from .protocol import uuid_str, CellStates
from .util import u64

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
            uuid_str(self.getUUID()),
            self.getAddress(),
            self.getState(),
        )

    def getState(self):
        return self.state

    def setState(self, state):
        assert state != CellStates.DISCARDED
        self.state = state

    def isUpToDate(self):
        return self.state == CellStates.UP_TO_DATE

    def isOutOfDate(self):
        return self.state == CellStates.OUT_OF_DATE

    def isFeeding(self):
        return self.state == CellStates.FEEDING

    def isCorrupted(self):
        return self.state == CellStates.CORRUPTED

    def isReadable(self):
        return self.state == CellStates.UP_TO_DATE or \
               self.state == CellStates.FEEDING

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

    # Flushing logs whenever a cell becomes out-of-date would flood them.
    _first_outdated_message = \
        'a cell became non-readable whereas all cells were readable'

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

    def getNodeSet(self, readable=False):
        if readable:
            return {x.getNode() for row in self.partition_list for x in row
                                   if x.isReadable()}
        return {x.getNode() for row in self.partition_list for x in row}

    def getConnectedNodeList(self):
        return [node for node in self.getNodeSet() if node.isConnected()]

    def getCellList(self, offset, readable=False):
        if readable:
            return filter(Cell.isReadable, self.partition_list[offset])
        return list(self.partition_list[offset])

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

    def getCell(self, offset, uuid):
        for cell in self.partition_list[offset]:
            if cell.getUUID() == uuid:
                return cell

    def _setCell(self, offset, node, state):
        if state == CellStates.DISCARDED:
            return self.removeCell(offset, node)
        if node.isUnknown():
            raise PartitionTableException('Invalid node state')

        self.count_dict.setdefault(node, 0)
        for cell in self.partition_list[offset]:
            if cell.getNode() is node:
                if not cell.isFeeding():
                    self.count_dict[node] -= 1
                cell.setState(state)
                break
        else:
            row = self.partition_list[offset]
            self.num_filled_rows += not row
            row.append(Cell(node, state))
        if state != CellStates.FEEDING:
            self.count_dict[node] += 1

    def removeCell(self, offset, node):
        row = self.partition_list[offset]
        for cell in row:
            if cell.getNode() == node:
                row.remove(cell)
                self.num_filled_rows -= not row
                if not cell.isFeeding():
                    self.count_dict[node] -= 1
                break

    def dropNode(self, node):
        count = self.count_dict.get(node)
        if count == 0:
            del self.count_dict[node]
        return not count

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
                self._setCell(offset, node, state)
        logging.debug('partition table loaded (ptid=%s)', ptid)
        self.log()

    def update(self, ptid, cell_list, nm):
        """
        Update the partition with the cell list supplied. If a node
        is not known, it is created in the node manager and set as unavailable
        """
        assert self._id < ptid, (self._id, ptid)
        self._id = ptid
        readable_list = []
        for row in self.partition_list:
            if not all(cell.isReadable() for cell in row):
                del readable_list[:]
                break
            readable_list += row
        for offset, uuid, state in cell_list:
            node = nm.getByUUID(uuid)
            assert node is not None, 'No node found for uuid ' + uuid_str(uuid)
            self._setCell(offset, node, state)
        self.logUpdated()
        if not all(cell.isReadable() for cell in readable_list):
            logging.warning(self._first_outdated_message)

    def filled(self):
        return self.num_filled_rows == self.np

    def logUpdated(self):
        logging.debug('partition table updated (ptid=%s)', self._id)
        self.log()

    def log(self):
        logging.debug(self.format())

    def format(self):
        return '\n'.join(self._format())

    def _format(self):
        """Help debugging partition table management.

        Output sample:
          pt: node 0: S1, R
          pt: node 1: S2, R
          pt: node 2: S3, R
          pt: node 3: S4, R
          pt: 00: .UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.
          pt: 11: U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U

        Here, there are 4 nodes in RUNNING state.
        The first partition has 2 replicas in UP_TO_DATE state, on nodes 1 and
        2 (nodes 0 and 3 are displayed as unused for that partition by
        displaying a dot).
        The first number on the left represents the number of the first
        partition on the line (here, line length is 11 to keep the docstring
        width under 80 column).
        """
        node_list = sorted(self.count_dict)
        result = ['pt: node %u: %s, %s' % (i, uuid_str(node.getUUID()),
                     protocol.node_state_prefix_dict[node.getState()])
                  for i, node in enumerate(node_list)]
        append = result.append
        line = []
        max_line_len = 20 # XXX: hardcoded number of partitions per line
        prefix = 0
        prefix_len = int(math.ceil(math.log10(self.np)))
        for offset, row in enumerate(self._formatRows(node_list)):
            if len(line) == max_line_len:
                append('pt: %0*u: %s' % (prefix_len, prefix, '|'.join(line)))
                line = []
                prefix = offset
            line.append(row)
        if line:
            append('pt: %0*u: %s' % (prefix_len, prefix, '|'.join(line)))
        return result

    def _formatRows(self, node_list):
        cell_state_dict = protocol.cell_state_prefix_dict
        for row in self.partition_list:
            if row is None:
                yield 'X' * len(node_list)
            else:
                cell_dict = {x.getNode(): cell_state_dict[x.getState()]
                             for x in row}
                yield ''.join(cell_dict.get(x, '.') for x in node_list)

    def operational(self, exclude_list=()):
        if not self.filled():
            return False
        for row in self.partition_list:
            for cell in row:
                if cell.isReadable():
                    node = cell.getNode()
                    if node.isRunning() and node.getUUID() not in exclude_list:
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

class MTPartitionTable(PartitionTable):
    """ Thread-safe aware version of the partition table, override only methods
        used in the client """

    def __init__(self, *args, **kw):
        self._lock = Lock()
        PartitionTable.__init__(self, *args, **kw)

    def update(self, *args, **kw):
        with self._lock:
            return PartitionTable.update(self, *args, **kw)

    def clear(self, *args, **kw):
        with self._lock:
            return PartitionTable.clear(self, *args, **kw)

    def operational(self, *args, **kw):
        with self._lock:
            return PartitionTable.operational(self, *args, **kw)
