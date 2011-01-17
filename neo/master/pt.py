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

import neo.lib.pt
from struct import pack, unpack
from neo.lib.protocol import CellStates
from neo.lib.pt import PartitionTableException
from neo.lib.pt import PartitionTable

class PartitionTable(PartitionTable):
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
                row.append(neo.lib.pt.Cell(node))
                self.count_dict[node] = self.count_dict.get(node, 0) + 1
                index += 1
                if index == len(node_list):
                    index = 0
            self.partition_list[offset] = row

        self.num_filled_rows = self.np

    def findLeastUsedNode(self, excluded_node_list = ()):
        min_count = self.np + 1
        min_node = None
        for node, count in self.count_dict.iteritems():
            if min_count > count \
                    and node not in excluded_node_list \
                    and node.isRunning():
                min_node = node
                min_count = count
        return min_node

    def dropNode(self, node):
        cell_list = []
        uuid = node.getUUID()
        for offset, row in enumerate(self.partition_list):
            if row is None:
                continue
            for cell in row:
                if cell.getNode() is node:
                    if not cell.isFeeding():
                        # If this cell is not feeding, find another node
                        # to be added.
                        node_list = [c.getNode() for c in row]
                        n = self.findLeastUsedNode(node_list)
                        if n is not None:
                            row.append(neo.lib.pt.Cell(n,
                                    CellStates.OUT_OF_DATE))
                            self.count_dict[n] += 1
                            cell_list.append((offset, n.getUUID(),
                                              CellStates.OUT_OF_DATE))
                    row.remove(cell)
                    cell_list.append((offset, uuid, CellStates.DISCARDED))
                    break

        try:
            del self.count_dict[node]
        except KeyError:
            pass

        return cell_list

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
                if not cell.isOutOfDate():
                    raise PartitionTableException('Non-oudated partition')
                break
        else:
            raise PartitionTableException('Non-assigned partition')

        # update the partition table
        cell_list = [self.setCell(offset, node, CellStates.UP_TO_DATE)]
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE)]

        # If the partition contains a feeding cell, drop it now.
        for feeding_cell in self.getCellList(offset):
            if feeding_cell.isFeeding():
                cell_list.append(self.removeCell(offset,
                    feeding_cell.getNode()))
                break

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
                if cell.isFeeding():
                    feeding_cell = cell
                else:
                    num_cells += 1
                    count = self.count_dict[cell.getNode()]
                    if count > max_count:
                        max_count = count
                        max_cell = cell

            if skip:
                continue

            if num_cells <= self.nr:
                row.append(neo.lib.pt.Cell(node, CellStates.OUT_OF_DATE))
                cell_list.append((offset, node.getUUID(),
                    CellStates.OUT_OF_DATE))
                node_count += 1
            else:
                if max_count - node_count > 1:
                    if feeding_cell is not None or max_cell.isOutOfDate():
                        # If there is a feeding cell already or it is
                        # out-of-date, just drop the node.
                        row.remove(max_cell)
                        cell_list.append((offset, max_cell.getUUID(),
                                          CellStates.DISCARDED))
                        self.count_dict[max_cell.getNode()] -= 1
                    else:
                        # Otherwise, use it as a feeding cell for safety.
                        max_cell.setState(CellStates.FEEDING)
                        cell_list.append((offset, max_cell.getUUID(),
                                          CellStates.FEEDING))
                        # Don't count a feeding cell.
                        self.count_dict[max_cell.getNode()] -= 1
                    row.append(neo.lib.pt.Cell(node, CellStates.OUT_OF_DATE))
                    cell_list.append((offset, node.getUUID(),
                                      CellStates.OUT_OF_DATE))
                    node_count += 1

        self.count_dict[node] = node_count
        self.log()
        return cell_list

    def tweak(self):
        """Test if nodes are distributed uniformly. Otherwise, correct the
        partition table."""
        changed_cell_list = []

        for offset, row in enumerate(self.partition_list):
            removed_cell_list = []
            feeding_cell = None
            out_of_date_cell_list = []
            up_to_date_cell_list = []
            for cell in row:
                if cell.getNode().isBroken():
                    # Remove a broken cell.
                    removed_cell_list.append(cell)
                elif cell.isFeeding():
                    if feeding_cell is None:
                        feeding_cell = cell
                    else:
                        # Remove an excessive feeding cell.
                        removed_cell_list.append(cell)
                elif cell.isOutOfDate():
                    out_of_date_cell_list.append(cell)
                else:
                    up_to_date_cell_list.append(cell)

            # If all cells are up-to-date, a feeding cell is not required.
            if len(out_of_date_cell_list) == 0 and feeding_cell is not None:
                removed_cell_list.append(feeding_cell)

            ideal_num = self.nr + 1
            while len(out_of_date_cell_list) + len(up_to_date_cell_list) > \
                    ideal_num:
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
                if not cell.isFeeding():
                    self.count_dict[cell.getNode()] -= 1
                changed_cell_list.append((offset, cell.getUUID(),
                    CellStates.DISCARDED))

        # Add cells, if a row contains less than the number of replicas.
        for offset, row in enumerate(self.partition_list):
            num_cells = 0
            for cell in row:
                if not cell.isFeeding():
                    num_cells += 1
            while num_cells <= self.nr:
                node = self.findLeastUsedNode([cell.getNode() for cell in row])
                if node is None:
                    break
                row.append(neo.lib.pt.Cell(node, CellStates.OUT_OF_DATE))
                changed_cell_list.append((offset, node.getUUID(),
                    CellStates.OUT_OF_DATE))
                self.count_dict[node] += 1
                num_cells += 1

        self.log()
        return changed_cell_list

    def outdate(self):
        """Outdate all non-working nodes."""
        cell_list = []
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                if not cell.getNode().isRunning() and not cell.isOutOfDate():
                    cell.setState(CellStates.OUT_OF_DATE)
                    cell_list.append((offset, cell.getUUID(),
                        CellStates.OUT_OF_DATE))
        return cell_list

