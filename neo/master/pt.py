#
# Copyright (C) 2006-2019  Nexedi SA
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

from collections import Counter, defaultdict
import neo.lib.pt
from neo.lib import logging
from neo.lib.protocol import CellStates, ZERO_TID


class Cell(neo.lib.pt.Cell):

    replicating = ZERO_TID
    updatable = False

    def setState(self, state):
        readable = self.isReadable()
        super(Cell, self).setState(state)
        if self.isReadable():
            return
        try:
            del self.updatable
        except AttributeError:
            pass
        if readable:
            try:
                del self.backup_tid, self.replicating
            except AttributeError:
                pass

neo.lib.pt.Cell = Cell


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

    def setReplicas(self, num_replicas):
        assert num_replicas >= 0, num_replicas
        self.nr = num_replicas

    def make(self, node_list):
        """Make a new partition table from scratch."""
        assert self._id is None and node_list, (self._id, node_list)
        for node in node_list:
            assert node.isRunning() and node.getUUID() is not None, node
        self.addNodeList(node_list)
        self.tweak()
        for node, count in self.count_dict.items():
            if not count:
                del self.count_dict[node]

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

    def load(self, ptid, num_replicas, row_list, nm):
        """
        Load a partition table from a storage node during the recovery.
        Return the new storage nodes registered
        """
        new_nodes = []
        def getByUUID(nid):
            node = nm.getByUUID(nid)
            if node is None:
                node = nm.createStorage(uuid=nid)
                new_nodes.append(node.asTuple())
            return node
        self._load(ptid, num_replicas, row_list, getByUUID)
        return new_nodes

    def setUpToDate(self, node, offset):
        """Set a cell as up-to-date"""
        uuid = node.getUUID()
        # Check the partition is assigned and known as outdated.
        row = self.partition_list[offset]
        for cell in row:
            if cell.getUUID() == uuid:
                if cell.isOutOfDate() and cell.updatable:
                    break
                return
        else:
            raise neo.lib.pt.PartitionTableException('Non-assigned partition')

        # Update the partition table.
        self._setCell(offset, node, CellStates.UP_TO_DATE)
        cell_list = [(offset, uuid, CellStates.UP_TO_DATE)]

        # Do no keep too many feeding cells.
        readable_list = filter(Cell.isReadable, row)
        iter_feeding = (cell.getNode() for cell in readable_list
                                       if cell.isFeeding())
        # If all cells are readable, we can now drop all feeding cells.
        if len(readable_list) != len(row):
            # Else we normally discard at most 1 cell. In the case that cells
            # became non-readable since the last tweak, we want to avoid going
            # below the wanted number of replicas. Also first try to discard
            # feeding cells from nodes that it was decided to drop.
            iter_feeding = sorted(iter_feeding, key=lambda node: not all(
                cell.isFeeding() for _, cell in self.iterNodeCell(node)
                ))[:max(0, len(readable_list) - self.nr)]
        for node in iter_feeding:
            self.removeCell(offset, node)
            cell_list.append((offset, node.getUUID(), CellStates.DISCARDED))

        return cell_list

    def tweak(self, drop_list=()):
        """Optimize partition table

        This reassigns cells in 4 ways:
        - Discard cells of nodes listed in 'drop_list'. For partitions with too
          few readable cells, some cells are instead marked as FEEDING. This is
          a preliminary step to drop these nodes, otherwise the partition table
          could become non-operational.
          In fact, the code touching these cells is disabled (see NOTE below).
        - Other nodes must have the same number of non-feeding cells, off by 1.
        - When a transaction creates new objects (oids are roughly allocated
          sequentially), we expect better performance by maximizing the number
          of involved nodes (i.e. parallelizing writes).
        - For maximum resiliency, cells of each partition are assigned as far
          as possible from each other, by checking the topology path of nodes.

        Examples of optimal partition tables with np=10, nr=1 and 5 nodes:

          UU...  ..UU.
          ..UU.  U...U
          U...U  .UU..
          .UU..  ...UU
          ...UU  UU...
          UU...  ..UU.
          ..UU.  U...U
          U...U  .UU..
          .UU..  ...UU
          ...UU  UU...

        The above 2 PT only differ by permutation of nodes, and this method
        plays on it to minimize the resulting amount of replication.
        For performance reasons, this algorithm uses a heuristic.

        When (np * nr) is not a multiple of the number of nodes, some nodes
        have 1 extra cell compared to other. In such case, other optimal PT
        could be considered by rotation of the partitions. Actually np times
        more, but it's not worth it since they don't differ enough (if np is
        big enough) and we don't already do an exhaustive search.
        Example with np=3, nr=1 and 2 nodes:

          U.  .U  U.
          .U  U.  U.
          U.  U.  .U

        For the topology, let's consider an example with paths of the form
        (room, machine, disk):
        - if there are more rooms than the number of replicas, 2 cells of the
          same partition must not be assigned in the same room;
        - otherwise, topology paths are checked at a deeper depth,
          e.g. not on the same machine and distributed evenly
               (off by 1) among rooms.
        But the topology is expected to be optimal, otherwise it is ignored.
        In some cases, we could fall back to a non-optimal topology but
        that would cause extra replication if the user wants to fix it.
        """
        # Collect some data in a usable form for the rest of the method.
        node_list = {node: {} for node in self.count_dict
                              if node not in drop_list}
        if not node_list:
            raise neo.lib.pt.PartitionTableException("Can't remove all nodes.")
        drop_list = defaultdict(list)
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                cell_dict = node_list.get(cell.getNode())
                if cell_dict is None:
                    drop_list[offset].append(cell)
                else:
                    cell_dict[offset] = cell
        # The sort by node id is cosmetic, to prefer result like the first one
        # in __doc__.
        node_list = sorted(node_list.iteritems(), key=lambda x: x[0].getUUID())

        # Generate an optimal PT.
        node_count = len(node_list)
        repeats = min(self.nr + 1, node_count)
        x = [[] for _ in xrange(node_count)]
        i = 0
        for offset in xrange(self.np):
            for _ in xrange(repeats):
                x[i % node_count].append(offset)
                i += 1
        option_dict = Counter(map(tuple, x))

        # Initialize variables/functions to optimize the topology.
        devpath_max = []
        devpaths = [()] * node_count
        if repeats > 1:
            _devpaths = [x[0].extra.get('devpath', ()) for x in node_list]
            max_depth = min(map(len, _devpaths))
            depth = 0
            while 1:
                if depth < max_depth:
                    depth += 1
                    x = Counter(x[:depth] for x in _devpaths)
                    n = len(x)
                    x = set(x.itervalues())
                    # TODO: Prove it works. If the code turns out to be:
                    #       - too pessimistic, the topology is ignored when
                    #         resiliency could be maximized;
                    #       - or worse too optimistic, in which case this
                    #         method raises, possibly after a very long time.
                    if len(x) == 1 or max(x) * repeats <= node_count:
                        i, x = divmod(repeats, n)
                        devpath_max.append((i + 1, x) if x else (i, n))
                        if n < repeats:
                            continue
                        devpaths = [x[:depth] for x in _devpaths]
                        break
                logging.warning("Can't maximize resiliency: fix the topology"
                    " of your storage nodes and make sure they're all running."
                    " %s storage device failure(s) may be enough to lose all"
                    " the database." % (repeats - 1))
                break
        topology = [{} for _ in xrange(self.np)]
        def update_topology():
            for offset in option:
                n = topology[offset]
                for i, (j, k) in zip(devpath, devpath_max):
                    try:
                        i, x = n[i]
                    except KeyError:
                        n[i] = i, x = [0, {}]
                    if i == j or i + 1 == j and k == sum(
                          1 for i in n.itervalues() if i[0] == j):
                        # Too many cells would be assigned at this topology
                        # node.
                        return False
                    n = x
            # The topology may be optimal with this option. Apply it.
            for offset in option:
                n = topology[offset]
                for i in devpath:
                    n = n[i]
                    n[0] += 1
                    n = n[1]
            return True
        def revert_topology():
            for offset in option:
                n = topology[offset]
                for i in devpath:
                    n = n[i]
                    n[0] -= 1
                    n = n[1]

        # Strategies to find the "best" permutation of nodes.
        def node_options():
            # The second part of the key goes with the above cosmetic sort.
            option_list = sorted(option_dict, key=lambda x: (-len(x), x))
            # 1. Search for solution that does not cause extra replication.
            #    This is important because tweak() must does nothing if it's
            #    called a second time whereas the list of nodes hasn't changed.
            result = []
            for i, (_, cell_dict) in enumerate(node_list):
                option = {offset for offset, cell in cell_dict.iteritems()
                                 if not cell.isFeeding()}
                x = filter(option.issubset, option_list)
                if not x:
                    break
                result.append((i, x))
            else:
                yield result
            # 2. We have to move cells. Evaluating all options would have
            #    a complexity of O(node_count!), which is clearly too slow,
            #    so we use a heuristic.
            #    For each node, we compare the resulting amount of replication
            #    in the best (min_cost) and worst (max_cost) case, and we first
            #    iterate over nodes with the biggest difference. This minimizes
            #    the impact of bad allocation patterns for the last nodes.
            result = []
            np_complement = frozenset(xrange(self.np)).difference
            for i, (_, cell_dict) in enumerate(node_list):
                cost_list = []
                for x, option in enumerate(option_list):
                    discard = [0, 0]
                    for offset in np_complement(option):
                        cell = cell_dict.get(offset)
                        if cell:
                            discard[cell.isReadable()] += 1
                    cost_list.append(((discard[1], discard[0]), x))
                cost_list.sort()
                min_cost = cost_list[0][0]
                max_cost = cost_list[-1][0]
                result.append((
                    min_cost[0] - max_cost[0],
                    min_cost[1] - max_cost[1],
                    i, [option_list[x[1]] for x in cost_list]))
            result.sort()
            yield result

        # The main loop, which is where we evaluate options.
        new = []   # the solution
        stack = [] # data recursion
        def options():
            x = node_options[len(new)]
            return devpaths[x[-2]], iter(x[-1])
        for node_options in node_options(): # for each strategy
            devpath, iter_option = options()
            while 1:
                try:
                    option = next(iter_option)
                except StopIteration:
                    if new:
                        devpath, iter_option = stack.pop()
                        option = new.pop()
                        revert_topology()
                        option_dict[option] += 1
                        continue
                    break
                if option_dict[option] and update_topology():
                    new.append(option)
                    if len(new) == node_count:
                        break
                    stack.append((devpath, iter_option))
                    devpath, iter_option = options()
                    option_dict[option] -= 1
            if new:
                break
        else:
            raise AssertionError

        # Apply the solution.

        if self._id is None:
            self._id = 1
            self.num_filled_rows = self.np
            new_state = CellStates.UP_TO_DATE
        else:
            new_state = CellStates.OUT_OF_DATE

        changed_list = []
        outdated_list = [repeats] * self.np
        discard_list = defaultdict(list)
        for i, offset_list in enumerate(new):
            node, cell_dict = node_list[node_options[i][-2]]
            for offset in offset_list:
                cell = cell_dict.pop(offset, None)
                if cell is None:
                    self.count_dict[node] += 1
                    self.partition_list[offset].append(Cell(node, new_state))
                    changed_list.append((offset, node.getUUID(), new_state))
                elif cell.isReadable():
                    if cell.isFeeding():
                        cell.setState(CellStates.UP_TO_DATE)
                        changed_list.append((offset, node.getUUID(),
                                             CellStates.UP_TO_DATE))
                    outdated_list[offset] -= 1
            for offset, cell in cell_dict.iteritems():
                discard_list[offset].append(cell)
        # NOTE: The following line disables the next 2 lines, which actually
        #       causes cells in drop_list to be discarded, now or later;
        #       drop_list could be renamed into ignore_list.
        #       1. Deleting data partition per partition is a lot of work, so
        #          why ask nodes in drop_list to do that when the goal is
        #          simply to trash the whole underlying database?
        #       2. By excluding nodes from a tweak, it becomes possible to have
        #          parts of the partition table that are tweaked differently.
        #          This may require to temporarily change the number of
        #          replicas for the part being tweaked. In the future, this
        #          number may be specified in the 'tweak' command, to avoid
        #          race conditions with setUpToDate().
        #       Overall, a common use case is when importing a ZODB to NEO,
        #       to keep the initial importing node up until the database is
        #       split and replicated to the final nodes.
        drop_list = {}
        for offset, drop_list in drop_list.iteritems():
            discard_list[offset] += drop_list
        # We have sorted cells to discard in order to first deallocate nodes
        # in drop_list, and have feeding cells in other nodes.
        # The following loop also makes sure not to discard cells too quickly,
        # by keeping a minimum of 'repeats' readable cells.
        for offset, outdated in enumerate(outdated_list):
            row = self.partition_list[offset]
            for cell in discard_list[offset]:
                if outdated and cell.isReadable():
                    outdated -= 1
                    if cell.isFeeding():
                        continue
                    state = CellStates.FEEDING
                    cell.setState(state)
                else:
                    self.count_dict[cell.getNode()] -= 1
                    state = CellStates.DISCARDED
                    row.remove(cell)
                changed_list.append((offset, cell.getUUID(), state))

        assert self.operational(), changed_list
        return changed_list

    def outdate(self, lost_node=None):
        """Outdate all non-working nodes

        Do not outdate cells of 'lost_node' for partitions it was the last node
        to serve. This allows a cluster restart.
        """
        change_list = []
        fully_readable = all(cell.isReadable()
                             for row in self.partition_list
                             for cell in row)
        for offset, row in enumerate(self.partition_list):
            lost = lost_node
            cell_list = []
            for cell in row:
                if cell.isReadable():
                    if cell.getNode().isRunning():
                        lost = None
                    else:
                        cell_list.append(cell)
            for cell in cell_list:
                node = cell.getNode()
                if node is not lost:
                    if cell.isFeeding():
                        self.removeCell(offset, node)
                        state = CellStates.DISCARDED
                    else:
                        state = CellStates.OUT_OF_DATE
                        cell.setState(state)
                    change_list.append((offset, node.getUUID(), state))
        if fully_readable and change_list:
            logging.warning(self._first_outdated_message)
        return change_list

    def updatable(self, uuid, offset_list):
        for offset in offset_list:
            for cell in self.partition_list[offset]:
                if cell.getUUID() == uuid and not cell.isReadable():
                    cell.updatable = True

    def iterNodeCell(self, node):
        for offset, row in enumerate(self.partition_list):
            for cell in row:
                if cell.getNode() is node:
                    yield offset, cell
                    break

    def getOperationalNodeSet(self):
        """
        Return a set of all nodes which are part of at least one UP TO DATE
        partition. An empty list is returned if these nodes aren't enough to
        become operational.
        """
        node_set = set()
        for row in self.partition_list:
            if not any(cell.isReadable() and cell.getNode().isPending()
                       for cell in row):
                return () # not operational
            node_set.update(cell.getNode() for cell in row if cell.isReadable())
        return node_set

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
