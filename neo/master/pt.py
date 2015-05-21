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
import neo.lib.pt
from neo.lib.protocol import CellStates, ZERO_TID


class Cell(neo.lib.pt.Cell):

    replicating = ZERO_TID

    def setState(self, state):
        readable = self.isReadable()
        super(Cell, self).setState(state)
        if readable and not self.isReadable():
            try:
                del self.backup_tid, self.replicating
            except AttributeError:
                pass

neo.lib.pt.Cell = Cell


class MappedNode(object):

    def __init__(self, node):
        self.node = node
        self.assigned = set()

    def __getattr__(self, attr):
        return getattr(self.node, attr)


class PartitionTable(neo.lib.pt.PartitionTable):
    """This class manages a partition table for the primary master node"""

    def setID(self, id):
        assert isinstance(id, (int, long)) or id is None, id
        self._id = id

    def setNextID(self):
        if self._id is None:
            raise RuntimeError, 'I do not know the last Partition Table ID'
        self._id += 1
        return self._id

    def make(self, node_list):
        """Make a new partition table from scratch."""
        # start with the first PTID
        self._id = 1
        # First, filter the list of nodes.
        node_list = [n for n in node_list if n.isRunning() \
                and n.getUUID() is not None]
        if len(node_list) == 0:
            # Impossible.
            raise RuntimeError, 'cannot make a partition table with an ' \
                    'empty storage node list'

        # Take it into account that the number of storage nodes may be less
        # than the number of replicas.
        repeats = min(self.nr + 1, len(node_list))
        index = 0
        for offset in xrange(self.np):
            row = []
            for _ in xrange(repeats):
                node = node_list[index]
                row.append(Cell(node))
                self.count_dict[node] = self.count_dict.get(node, 0) + 1
                index += 1
                if index == len(node_list):
                    index = 0
            self.partition_list[offset] = row

        self.num_filled_rows = self.np

    def dropNodeList(self, node_list, simulate=False):
        partition_list = []
        change_list = []
        feeding_list = []
        for offset, row in enumerate(self.partition_list):
            new_row = []
            partition_list.append(new_row)
            feeding = None
            drop_readable = uptodate = False
            for cell in row:
                node = cell.getNode()
                if node in node_list:
                    change_list.append((offset, node.getUUID(),
                                        CellStates.DISCARDED))
                    if cell.isReadable():
                        drop_readable = True
                else:
                    new_row.append(cell)
                    if cell.isFeeding():
                        feeding = cell
                    elif cell.isUpToDate():
                        uptodate = True
            if feeding is not None:
                if len(new_row) < len(row):
                    change_list.append((offset, feeding.getUUID(),
                                        CellStates.UP_TO_DATE))
                    feeding_list.append(feeding)
            elif drop_readable and not uptodate:
                raise neo.lib.pt.PartitionTableException(
                    "Refuse to drop nodes that contain the only readable"
                    " copies of partition %u" % offset)
        if not simulate:
            self.partition_list = partition_list
            for cell in feeding_list:
                cell.setState(CellStates.UP_TO_DATE)
                self.count_dict[cell.getNode()] += 1
            for node in node_list:
                self.count_dict.pop(node, None)
            self.num_filled_rows = len(filter(None, self.partition_list))
        return change_list

    def load(self, ptid, row_list, nm):
        """
        Load a partition table from a storage node during the recovery.
        Return the new storage nodes registered
        """
        # check offsets
        for offset, _row in row_list:
            if offset >= self.getPartitions():
                raise IndexError, offset
        # store the partition table
        self.clear()
        self._id = ptid
        new_nodes = []
        for offset, row in row_list:
            for uuid, state in row:
                node = nm.getByUUID(uuid)
                if node is None:
                    node = nm.createStorage(uuid=uuid)
                    new_nodes.append(node.asTuple())
                self.setCell(offset, node, state)
        return new_nodes

    def setUpToDate(self, node, offset):
        """Set a cell as up-to-date"""
        uuid = node.getUUID()
        # check the partition is assigned and known as outdated
        for cell in self.getCellList(offset):
            if cell.getUUID() == uuid:
                if cell.isOutOfDate():
                    break
                return
        else:
            raise neo.lib.pt.PartitionTableException('Non-assigned partition')

        # update the partition table
        cell_list = [self.setCell(offset, node, CellStates.UP_TO_DATE)]

        # If the partition contains a feeding cell, drop it now.
        for feeding_cell in self.getCellList(offset):
            if feeding_cell.isFeeding():
                cell_list.append(self.removeCell(offset,
                    feeding_cell.getNode()))
                break

        return cell_list

    def addNodeList(self, node_list):
        """Add nodes"""
        added_list = []
        for node in node_list:
            if node not in self.count_dict:
                self.count_dict[node] = 0
                added_list.append(node)
        return added_list

    def tweak(self, drop_list=()):
        """Optimize partition table

        This is done by computing a minimal diff between current partition table
        and what make() would do.
        """
        assigned_dict = {x: {} for x in self.count_dict}
        readable_list = [set() for x in xrange(self.np)]
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                if cell.isReadable():
                    readable_list[offset].add(cell)
                assigned_dict[cell.getNode()][offset] = cell
        pt = PartitionTable(self.np, self.nr)
        drop_list = set(drop_list).intersection(assigned_dict)
        node_set = {MappedNode(x) for x in assigned_dict
                                  if x not in drop_list}
        pt.make(node_set)
        for offset, row in enumerate(pt.partition_list):
            for cell in row:
                if cell.isReadable():
                    cell.getNode().assigned.add(offset)
        def map_nodes():
            node_list = []
            for node, assigned in assigned_dict.iteritems():
                if node in drop_list:
                    yield node, frozenset()
                    continue
                readable = {offset for offset, cell in assigned.iteritems()
                                   if cell.isReadable()}
                # the criterion on UUID is purely cosmetic
                node_list.append((len(readable), len(assigned),
                                  -node.getUUID(), readable, node))
            node_list.sort(reverse=1)
            for _, _, _, readable, node in node_list:
                assigned = assigned_dict[node]
                mapped = min(node_set, key=lambda m: (
                    len(m.assigned.symmetric_difference(assigned)),
                    len(m.assigned ^ readable)))
                node_set.remove(mapped)
                yield node, mapped.assigned
            assert not node_set
        changed_list = []
        uptodate_set = set()
        remove_dict = defaultdict(list)
        for node, mapped in map_nodes():
            uuid = node.getUUID()
            assigned = assigned_dict[node]
            for offset, cell in assigned.iteritems():
                if offset in mapped:
                    if cell.isReadable():
                        uptodate_set.add(offset)
                        readable_list[offset].remove(cell)
                        if cell.isFeeding():
                            self.count_dict[node] += 1
                            state = CellStates.UP_TO_DATE
                            cell.setState(state)
                            changed_list.append((offset, uuid, state))
                else:
                    if not cell.isFeeding():
                        self.count_dict[node] -= 1
                    remove_dict[offset].append(cell)
            for offset in mapped.difference(assigned):
                self.count_dict[node] += 1
                state = CellStates.OUT_OF_DATE
                self.partition_list[offset].append(Cell(node, state))
                changed_list.append((offset, uuid, state))
        count_dict = self.count_dict.copy()
        for offset, cell_list in remove_dict.iteritems():
            row = self.partition_list[offset]
            feeding = None if offset in uptodate_set else min(
                readable_list[offset], key=lambda x: count_dict[x.getNode()])
            for cell in cell_list:
                if cell is feeding:
                    count_dict[cell.getNode()] += 1
                    if cell.isFeeding():
                        continue
                    state = CellStates.FEEDING
                    cell.setState(state)
                else:
                    state = CellStates.DISCARDED
                    row.remove(cell)
                changed_list.append((offset, cell.getUUID(), state))
        assert self.num_filled_rows == len(filter(None, self.partition_list))
        return changed_list

    def outdate(self, lost_node=None):
        """Outdate all non-working nodes

        Do not outdate cells of 'lost_node' for partitions it was the last node
        to serve. This allows a cluster restart.
        """
        change_list = []
        for offset, row in enumerate(self.partition_list):
            lost = lost_node
            cell_list = []
            for cell in row:
                if cell.isReadable():
                    if cell.getNode().isRunning():
                        lost = None
                    else :
                        cell_list.append(cell)
            for cell in cell_list:
                if cell.getNode() is not lost:
                    cell.setState(CellStates.OUT_OF_DATE)
                    change_list.append((offset, cell.getUUID(),
                        CellStates.OUT_OF_DATE))
        return change_list

    def iterNodeCell(self, node):
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                if cell.getNode() is node:
                    yield offset, cell
                    break

    def getReadableCellNodeSet(self):
        """
        Return a set of all nodes which are part of at least one UP TO DATE
        partition.
        """
        return {cell.getNode()
            for row in self.partition_list
            for cell in row
            if cell.isReadable()}

    def clearReplicating(self):
        for row in self.partition_list:
            for cell in row:
                try:
                    del cell.replicating
                except AttributeError:
                    pass

    def setBackupTidDict(self, backup_tid_dict):
        for row in self.partition_list:
            for cell in row:
                if cell.isReadable():
                    cell.backup_tid = backup_tid_dict.get(cell.getUUID(),
                                                          ZERO_TID)

    def getBackupTid(self, mean=max):
        try:
            return min(mean(x.backup_tid for x in row if x.isReadable())
                       for row in self.partition_list)
        except ValueError:
            return ZERO_TID

    def getCheckTid(self, partition_list):
        try:
            return min(min(cell.backup_tid
                           for cell in self.partition_list[offset]
                           if cell.isReadable())
                       for offset in partition_list)
        except ValueError:
            return ZERO_TID
