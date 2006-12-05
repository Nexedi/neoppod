import logging

from neo.protocol import UP_TO_DATE_STATE, OUT_OF_DATE_STATE, FEEDING_STATE, \
        DISCARDED_STATE

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

class PartitionTable(object):
    """This class manages a partition table."""

    def __init__(self, num_partitions, num_replicas):
        self.np = num_partitions
        self.nr = num_replicas
        self.num_filled_rows = 0
        self.partition_list = [None] * num_partitions

    def clear(self):
        """Forget an existing partition table."""
        self.num_filled_rows = 0
        self.partition_list = [None] * self.np

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
                row.append(Cell(node_list[index]))
                index += 1
                if index == len(uuid_list):
                    index = 0
            self.partition_list[offset] = row

        self.num_filled_rows = self.np

    def setCell(self, offset, node, state):
        row = self.partition_list[offset]
        if row is None:
            # Create a new row.
            row = [Cell(node, state)]
            self.partition_list[offset]

            self.num_filled_rows += 1
        else:
            # XXX this can be slow, but it is necessary to remove a duplicate,
            # if any.
            for cell in row:
                if cell.getNode() == node:
                    row.remove(cell)
                    break
            row.append(Cell(node, state))

    def filled(self):
        return self.num_filled_rows == self.np

    def hasOffset(self, offset):
        return self.partition_list[offset] is not None

    def operational(self):
        if not self.filled():
            return False

        # FIXME it is better to optimize this code, as this could be extremely
        # slow. The possible fix is to have a handler to notify a change on
        # a node state, and record which rows are ready.
        for row in self.partition_list:
            for cell in row:
                if cell.getState() == UP_TO_DATE_STATE \
                        and cell.getNodeState() == RUNNING_STATE:
                    break
            else:
                return False

        return True
