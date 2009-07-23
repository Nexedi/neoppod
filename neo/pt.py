#
# Copyright (C) 2006-2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import logging

from neo.protocol import UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, \
        DISCARDED_STATE, RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        BROKEN_STATE, VALID_CELL_STATE_LIST, HIDDEN_STATE, PENDING_STATE
from neo.util import dump, u64
from neo.locking import RLock


class Cell(object):
    """This class represents a cell in a partition table."""

    def __init__(self, node, state = UP_TO_DATE_STATE):
        self.node = node
        assert state in VALID_CELL_STATE_LIST
        self.state = state

    def getState(self):
        return self.state

    def setState(self, state):
        assert state in VALID_CELL_STATE_LIST
        self.state = state

    def getNode(self):
        return self.node

    def getNodeState(self):
        """This is a short hand."""
        return self.node.getState()

    def getUUID(self):
        return self.node.getUUID()

    def getServer(self):
        return self.node.getServer()

class PartitionTable(object):
    """This class manages a partition table."""

    def __init__(self, num_partitions, num_replicas):
        self.id = None
        self.np = num_partitions
        self.nr = num_replicas
        self.num_filled_rows = 0
        # Note: don't use [[]] * num_partition construct, as it duplicates
        # instance *references*, so the outer list contains really just one
        # inner list instance.
        self.partition_list = [[] for x in xrange(num_partitions)]
        self.count_dict = {}

    def getID(self):
        return self.id

    def getPartitions(self):
        return self.np

    def getReplicas(self):
        return self.nr

    def clear(self):
        """Forget an existing partition table."""
        self.num_filled_rows = 0
        # Note: don't use [[]] * self.np construct, as it duplicates
        # instance *references*, so the outer list contains really just one
        # inner list instance.
        self.partition_list = [[] for x in xrange(self.np)]
        self.count_dict.clear()

    def hasOffset(self, offset):
        try:
            return len(self.partition_list[offset]) > 0
        except IndexError:
            return False

    def getNodeList(self):
        """Return all used nodes."""
        node_list = []
        for node, count in self.count_dict.iteritems():
            if count > 0:
                node_list.append(node)
        return node_list

    def getCellList(self, offset, readable=False, writable=False):
        # allow all cell states
        state_set = set(VALID_CELL_STATE_LIST)
        if readable or writable:
            # except non readables
            state_set.remove(DISCARDED_STATE)
        if readable:
            # except non writables
            state_set.remove(OUT_OF_DATE_STATE)
        allowed_states = tuple(state_set)
        try:
            return [cell for cell in self.partition_list[offset] \
                    if cell is not None and cell.getState() in allowed_states]
        except (TypeError, KeyError):
            return []

    def getCellListForTID(self, tid, readable=False, writable=False):
        return self.getCellList(self._getPartitionFromIndex(u64(tid)),
                                readable, writable)

    def getCellListForOID(self, oid, readable=False, writable=False):
        return self.getCellList(self._getPartitionFromIndex(u64(oid)),
                                readable, writable)

    def _getPartitionFromIndex(self, index):
        return index % self.np

    def setCell(self, offset, node, state):
        assert state in VALID_CELL_STATE_LIST
        if state == DISCARDED_STATE:
            return self.removeCell(offset, node)
        if node.getState() in (BROKEN_STATE, DOWN_STATE):
            return

        row = self.partition_list[offset]
        if len(row) == 0:
            # Create a new row.
            row = [Cell(node, state), ]
            if state != FEEDING_STATE:
                self.count_dict[node] = self.count_dict.get(node, 0) + 1
            self.partition_list[offset] = row

            self.num_filled_rows += 1
        else:
            # XXX this can be slow, but it is necessary to remove a duplicate,
            # if any.
            for cell in row:
                if cell.getNode() == node:
                    row.remove(cell)
                    if cell.getState() != FEEDING_STATE:
                        self.count_dict[node] = self.count_dict.get(node, 0) - 1
                    break
            row.append(Cell(node, state))
            if state != FEEDING_STATE:
                self.count_dict[node] = self.count_dict.get(node, 0) + 1

    def removeCell(self, offset, node):
        row = self.partition_list[offset]
        if row is not None:
            for cell in row:
                if cell.getNode() == node:
                    row.remove(cell)
                    if cell.getState() != FEEDING_STATE:
                        self.count_dict[node] = self.count_dict.get(node, 0) - 1
                    break

    # XXX: node manager is given here just to verify that any node in the
    # partition table is known, this will be removed when checked.
    def update(self, cell_list, nm):
        for offset, uuid, state in cell_list:
            node = nm.getNodeByUUID(uuid) 
            if node is None:
                logging.warning('Updating partition table with an unknown UUID : %s', 
                        dump(uuid))
                from neo.node import StorageNode
                node = StorageNode(uuid=uuid)
                nm.add(node)
            self.setCell(offset, node, state)
        logging.debug('partition table updated')
        self.log()

    def filled(self):
        return self.num_filled_rows == self.np

    def log(self):
        """Help debugging partition table management.

        Output sample:
        DEBUG:root:pt: node 0: ad7ffe8ceef4468a0c776f3035c7a543, R
        DEBUG:root:pt: node 1: a68a01e8bf93e287bd505201c1405bc2, R
        DEBUG:root:pt: node 2: 67ae354b4ed240a0594d042cf5c01b28, R
        DEBUG:root:pt: node 3: df57d7298678996705cd0092d84580f4, R
        DEBUG:root:pt: 00000000: .UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.
        DEBUG:root:pt: 00000009: U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U|.UU.|U..U

        Here, there are 4 nodes in RUNNING_STATE.
        The first partition has 2 replicas in UP_TO_DATE_STATE, on nodes 1 and
        2 (nodes 0 and 3 are displayed as unused for that partition by
        displaying a dot).
        The 8-digits number on the left represents the number of the first
        partition on the line (here, line length is 9 to keep the docstring
        width under 80 column).
        """
        node_state_dict = { RUNNING_STATE: 'R',
                            TEMPORARILY_DOWN_STATE: 'T',
                            DOWN_STATE: 'D',
                            BROKEN_STATE: 'B',
                            HIDDEN_STATE: 'H',
                            PENDING_STATE: 'P'}
        cell_state_dict = { UP_TO_DATE_STATE: 'U', 
                            OUT_OF_DATE_STATE: 'O', 
                            FEEDING_STATE: 'F',
                            DISCARDED_STATE: 'D'}
        node_list = self.count_dict.keys()
        node_list.sort()
        node_dict = {}
        for i, node in enumerate(node_list):
            node_dict[node] = i
        for node, i in node_dict.iteritems():
            logging.debug('pt: node %d: %s, %s', i, dump(node.getUUID()),
                          node_state_dict[node.getState()])
        line = []
        max_line_len = 20 # XXX: hardcoded number of partitions per line
        for offset, row in enumerate(self.partition_list):
            if len(line) == max_line_len:
                logging.debug('pt: %08d: %s', offset - max_line_len,
                              '|'.join(line))
                line = []
            if row is None:
                line.append('X' * len(node_list))
            else:
                cell = []
                cell_dict = dict([(node_dict[x.getNode()], x) for x in row])
                for node in xrange(len(node_list)):
                    if node in cell_dict:
                        cell.append(cell_state_dict[cell_dict[node].getState()])
                    else:
                        cell.append('.')
                line.append(''.join(cell))
        if len(line):
            logging.debug('pt: %08d: %s', offset - len(line) + 1,
                          '|'.join(line))


    def operational(self):        
        if not self.filled():
            return False

        # FIXME it is better to optimize this code, as this could be extremely
        # slow. The possible fix is to have a handler to notify a change on
        # a node state, and record which rows are ready.
        for row in self.partition_list:
            for cell in row:
                if cell.getState() in (UP_TO_DATE_STATE, FEEDING_STATE) \
                        and cell.getNodeState() == RUNNING_STATE:
                    break
            else:
                return False

        return True

    def getRow(self, offset):
        row = self.partition_list[offset]
        if row is None:
            return []
        return [(cell.getUUID(), cell.getState()) for cell in row]


def thread_safe(method):
    def wrapper(self, *args, **kwargs):
        self.lock()
        try:
            return method(self, *args, **kwargs)
        finally:
            self.unlock()
    return wrapper


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

