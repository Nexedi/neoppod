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
        BROKEN_STATE
from neo.util import dump

class Cell(object):
    """This class represents a cell in a partition table."""

    def __init__(self, node, state = UP_TO_DATE_STATE):
        self.node = node
        self.state = state

    def getState(self):
        return self.state

    def setState(self, state):
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
        self.np = num_partitions
        self.nr = num_replicas
        self.num_filled_rows = 0
        self.partition_list = [[] for x in xrange(num_partitions)]
        self.count_dict = {}

    def clear(self):
        """Forget an existing partition table."""
        self.num_filled_rows = 0
        self.partition_list = [[] for x in xrange(self.np)]
        self.count_dict.clear()

    def getNodeList(self):
        """Return all used nodes."""
        node_list = []
        for node, count in self.count_dict.iteritems():
            if count > 0:
                node_list.append(node)
        return node_list

    def getCellList(self, offset, usable_only = False):
        if usable_only:
            try:
                return [cell for cell in self.partition_list[offset] \
                        if cell is not None and cell.getState() in (UP_TO_DATE_STATE, FEEDING_STATE)]
            except (TypeError, KeyError):
                return []
        try:
            return self.partition_list[offset]
        except KeyError:
            return []

    def make(self, node_list):
        """Make a new partition table from scratch."""
        # First, filter the list of nodes.
        node_list = [n for n in node_list \
                if n.getState() == RUNNING_STATE and n.getUUID() is not None]
        if len(node_list) == 0:
            # Impossible.
            raise RuntimeError, \
                    'cannot make a partition table with an empty storage node list'

        # Take it into account that the number of storage nodes may be less than the
        # number of replicas.
        repeats = min(self.nr, len(node_list))
        index = 0
        for offset in xrange(self.np):
            row = []
            for i in xrange(repeats):
                node = node_list[index]
                row.append(Cell(node))
                self.count_dict[node] = self.count_dict.get(node, 0) + 1
                index += 1
                if index == len(node_list):
                    index = 0
            self.partition_list[offset] = row

        self.num_filled_rows = self.np

    def setCell(self, offset, node, state):
        if state == DISCARDED_STATE:
            return self.removeCell(offset, node)
        if node.getState() in (BROKEN_STATE, DOWN_STATE):
            return

        row = self.partition_list[offset]
        if len(row) == 0:
            # Create a new row.
            row = [Cell(node, state),]
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

    def filled(self):
        return self.num_filled_rows == self.np

    def hasOffset(self, offset):
        try:
            return len(self.partition_list[offset]) > 0
        except IndexError:
            return False

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
        2 (nodes 0 and 4 are displayed as unused for that partition by
        displaying a dot).
        The 8-digits number on the left represents the number of the first
        partition on the line (here, line length is 9 to keep the docstring
        width under 80 column).
        """
        node_state_dict = { RUNNING_STATE: 'R',
                            TEMPORARILY_DOWN_STATE: 'T',
                            DOWN_STATE: 'D',
                            BROKEN_STATE: 'B' }
        cell_state_dict = { UP_TO_DATE_STATE: 'U', 
                            OUT_OF_DATE_STATE: 'O', 
                            FEEDING_STATE: 'F' }
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
            logging.debug('pt: %08d: %s', offset - len(line),
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

    def findLeastUsedNode(self, excluded_node_list = ()):
        min_count = self.np + 1
        min_node = None
        for node, count in self.count_dict.iteritems():
            if min_count > count \
                    and node not in excluded_node_list \
                    and node.getState() == RUNNING_STATE:
                min_node = node
                min_count = count
        return min_node

    def dropNode(self, node):
        cell_list = []
        uuid = node.getUUID()
        for offset, row in enumerate(self.partition_list):
            if row is not None:
                for cell in row:
                    if cell.getNode() is node:
                        if cell.getState() != FEEDING_STATE:
                            # If this cell is not feeding, find another node
                            # to be added.
                            node_list = [c.getNode() for c in row]
                            n = self.findLeastUsedNode(node_list)
                            if n is not None:
                                row.append(Cell(n, OUT_OF_DATE_STATE))
                                self.count_dict[n] += 1
                                cell_list.append((offset, n.getUUID(),
                                                  OUT_OF_DATE_STATE))
                        row.remove(cell)
                        cell_list.append((offset, uuid, DISCARDED_STATE))
                        break

        try:
            del self.count_dict[node]
        except KeyError:
            pass

        return cell_list

    def addNode(self, node):
        """Add a node. Take it into account that it might not be really a new
        node. The strategy is, if a row does not contain a good number of
        cells, add this node to the row, unless the node is already present
        in the same row. Otherwise, check if this node should replace another
        cell."""
        cell_list = []
        node_count = self.count_dict.get(node, 0)
        for offset, row in enumerate(self.partition_list):
            feeding_cell = None
            max_count = 0
            max_cell = None
            num_cells = 0
            skip = False
            for cell in row:
                if cell.getNode() == node:
                    skip = True
                    break
                if cell.getState() == FEEDING_STATE:
                    feeding_cell = cell
                else:
                    num_cells += 1
                    count = self.count_dict[cell.getNode()]
                    if count > max_count:
                        max_count = count
                        max_cell = cell

            if skip:
                continue

            if num_cells < self.nr:
                row.append(Cell(node, OUT_OF_DATE_STATE))
                cell_list.append((offset, node.getUUID(), OUT_OF_DATE_STATE))
                node_count += 1
            else:
                if max_count - node_count > 1:
                    if feeding_cell is not None \
                            or max_cell.getState() == OUT_OF_DATE_STATE:
                        # If there is a feeding cell already or it is
                        # out-of-date, just drop the node.
                        row.remove(max_cell)
                        cell_list.append((offset, max_cell.getUUID(), 
                                          DISCARDED_STATE))
                        self.count_dict[max_cell.getNode()] -= 1
                    else:
                        # Otherwise, use it as a feeding cell for safety.
                        max_cell.setState(FEEDING_STATE)
                        cell_list.append((offset, max_cell.getUUID(), 
                                          FEEDING_STATE))
                        # Don't count a feeding cell.
                        self.count_dict[max_cell.getNode()] -= 1
                    row.append(Cell(node, OUT_OF_DATE_STATE))
                    cell_list.append((offset, node.getUUID(), 
                                      OUT_OF_DATE_STATE))
                    node_count += 1

        self.count_dict[node] = node_count
        self.log()
        return cell_list

    def getRow(self, offset):
        row = self.partition_list[offset]
        if row is None:
            return ()
        return [(cell.getUUID(), cell.getState()) for cell in row]

    def tweak(self):
        """Test if nodes are distributed uniformly. Otherwise, correct the
        partition table."""
        changed_cell_list = []

        for offset, row in enumerate(self.partition_list):
            removed_cell_list = []
            feeding_cell = None
            out_of_date_cell_present = False
            out_of_date_cell_list = []
            up_to_date_cell_list = []
            for cell in row:
                if cell.getNodeState() == BROKEN_STATE:
                    # Remove a broken cell.
                    removed_cell_list.append(cell)
                elif cell.getState() == FEEDING_STATE:
                    if feeding_cell is None:
                        feeding_cell = cell
                    else:
                        # Remove an excessive feeding cell.
                        removed_cell_list.append(cell)
                elif cell.getState() == OUT_OF_DATE_STATE:
                    out_of_date_cell_list.append(cell)
                else:
                    up_to_date_cell_list.append(cell)

            # If all cells are up-to-date, a feeding cell is not required.
            if len(out_of_date_cell_list) == 0 and feeding_cell is not None:
                removed_cell_list.append(feeding_cell)

            ideal_num = self.nr
            while len(out_of_date_cell_list) + len(up_to_date_cell_list) > ideal_num:
                # This row contains too many cells.
                if len(up_to_date_cell_list) > 1:
                    # There are multiple up-to-date cells, so choose whatever
                    # used too much.
                    cell_list = out_of_date_cell_list + up_to_date_cell_list
                else:
                    # Drop an out-of-date cell.
                    cell_list = out_of_date_cell_list

                max_count = 0
                chosen_cell = None
                for cell in cell_list:
                    count = self.count_dict[cell.getNode()]
                    if max_count < count:
                        max_count = count
                        chosen_cell = cell
                removed_cell_list.append(chosen_cell)
                try:
                    out_of_date_cell_list.remove(chosen_cell)
                except ValueError:
                    up_to_date_cell_list.remove(chosen_cell)

            # Now remove cells really.
            for cell in removed_cell_list:
                row.remove(cell)
                if cell.getState() != FEEDING_STATE:
                    self.count_dict[cell.getNode()] -= 1
                changed_cell_list.append((offset, cell.getUUID(), DISCARDED_STATE))

        # Add cells, if a row contains less than the number of replicas.
        for offset, row in enumerate(self.partition_list):
            num_cells = 0
            for cell in row:
                if cell.getState() != FEEDING_STATE:
                    num_cells += 1
            while num_cells < self.nr:
                node = self.findLeastUsedNode([cell.getNode() for cell in row])
                if node is None:
                    break
                row.append(Cell(node, OUT_OF_DATE_STATE))
                changed_cell_list.append((offset, node.getUUID(), OUT_OF_DATE_STATE))
                self.count_dict[node] += 1
                num_cells += 1

        # FIXME still not enough. It is necessary to check if it is possible
        # to reduce differences between frequently used nodes and rarely used
        # nodes by replacing cells.

        self.log()
        return changed_cell_list

    def outdate(self):
        """Outdate all non-working nodes."""
        cell_list = []
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                if cell.getNodeState() != RUNNING_STATE \
                        and cell.getState() != OUT_OF_DATE_STATE:
                    cell.setState(OUT_OF_DATE_STATE)
                    cell_list.append((offset, cell.getUUID(), OUT_OF_DATE_STATE))
        return cell_list
