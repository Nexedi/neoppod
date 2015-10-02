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

from operator import itemgetter
from .neoctl import NeoCTL, NotReadyException
from neo.lib.util import p64, u64, tidFromTime
from neo.lib.protocol import uuid_str, ClusterStates, NodeTypes, \
    UUID_NAMESPACES, ZERO_TID

action_dict = {
    'print': {
        'ids': 'getLastIds',
        'pt': 'getPartitionRowList',
        'node': 'getNodeList',
        'cluster': 'getClusterState',
        'primary': 'getPrimary',
    },
    'set': {
        'cluster': 'setClusterState',
    },
    'check': 'checkReplicas',
    'start': 'startCluster',
    'add': 'enableStorageList',
    'tweak': 'tweakPartitionTable',
    'drop': 'dropNode',
    'kill': 'killNode',
}

uuid_int = (lambda ns: lambda uuid:
    (ns[uuid[0]] << 24) + int(uuid[1:])
    )({str(k)[0]: v for k, v in UUID_NAMESPACES.iteritems()})

class TerminalNeoCTL(object):
    def __init__(self, *args, **kw):
        self.neoctl = NeoCTL(*args, **kw)

    def __del__(self):
        self.neoctl.close()

    # Utility methods (could be functions)
    def asNodeType(self, value):
        return getattr(NodeTypes, value.upper())

    def asClusterState(self, value):
        return getattr(ClusterStates, value.upper())

    def asTID(self, value):
        if '.' in value:
            return tidFromTime(float(value))
        return p64(int(value, 0))

    asNode = staticmethod(uuid_int)

    def formatRowList(self, row_list):
        return '\n'.join('%03d | %s' % (offset,
            ''.join('%s - %s |' % (uuid_str(uuid), state)
            for (uuid, state) in cell_list))
            for (offset, cell_list) in row_list)

    def formatNodeList(self, node_list, _sort_key=itemgetter(2, 0, 1)):
        if not node_list:
            return 'Empty list!'
        node_list.sort(key=_sort_key)
        return '\n'.join(
            '%s - %s - %s - %s' % (node_type, uuid_str(uuid),
                                   address and '%s:%s' % address, state)
            for node_type, address, uuid, state in node_list)

    # Actual actions
    def getLastIds(self, params):
        """
          Get last ids.
        """
        assert not params
        r = self.neoctl.getLastIds()
        if r[3]:
            return "last_tid = 0x%x" % u64(self.neoctl.getLastTransaction())
        return "last_oid = 0x%x\nlast_tid = 0x%x\nlast_ptid = %u" % (
            u64(r[0]), u64(r[1]), r[2])

    def getPartitionRowList(self, params):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
          Parameters: [min [max [node]]]
            min: offset of the first row to fetch (starts at 0)
            max: offset of the last row to fetch (0 for no limit)
            node: filters the list of nodes serving a line to this node
        """
        params = params + [0, 0, None][len(params):]
        min_offset, max_offset, node = params
        min_offset = int(min_offset)
        max_offset = int(max_offset)
        if node is not None:
            node = self.asNode(node)
        ptid, row_list = self.neoctl.getPartitionRowList(
                min_offset=min_offset, max_offset=max_offset, node=node)
        # TODO: return ptid
        return self.formatRowList(row_list)

    def getNodeList(self, params):
        """
          Get a list of nodes, filtering with given type.
          Parameters: [type]
            type: type of node to display
        """
        assert len(params) < 2
        if len(params):
            node_type = self.asNodeType(params[0])
        else:
            node_type = None
        node_list = self.neoctl.getNodeList(node_type=node_type)
        return self.formatNodeList(node_list)

    def getClusterState(self, params):
        """
          Get cluster state.
        """
        assert len(params) == 0
        return str(self.neoctl.getClusterState())

    def setClusterState(self, params):
        """
          Set cluster state.
          Parameters: state
            state: state to put the cluster in
        """
        assert len(params) == 1
        return self.neoctl.setClusterState(self.asClusterState(params[0]))

    def startCluster(self, params):
        """
          Starts cluster operation after a startup.
          Equivalent to:
            set cluster verifying
        """
        assert len(params) == 0
        return self.neoctl.startCluster()

    def enableStorageList(self, params):
        """
          Enable cluster to make use of pending storages.
          Parameters: all
                      node [node [...]]
            node: if "all", add all pending storage nodes.
                  otherwise, the list of storage nodes to enable.
        """
        if len(params) == 1 and params[0] == 'all':
            node_list = self.neoctl.getNodeList(NodeTypes.STORAGE)
            uuid_list = [node[2] for node in node_list]
        else:
            uuid_list = map(self.asNode, params)
        return self.neoctl.enableStorageList(uuid_list)

    def tweakPartitionTable(self, params):
        """
          Optimize partition table.
          No partitition will be assigned to specified storage nodes.
          Parameters: [node [...]]
        """
        return self.neoctl.tweakPartitionTable(map(self.asNode, params))

    def killNode(self, params):
        """
          Kill redundant nodes (either a storage or a secondary master).
          Parameters: node
        """
        return self.neoctl.killNode(self.asNode(*params))

    def dropNode(self, params):
        """
          Remove storage node permanently.
          Parameters: node
        """
        return self.neoctl.dropNode(self.asNode(*params))

    def getPrimary(self, params):
        """
          Get primary master node.
        """
        return uuid_str(self.neoctl.getPrimary())

    def checkReplicas(self, params):
        """
          Test whether partitions have corrupted metadata

          Any corrupted cell is put in CORRUPTED state, possibly make the
          cluster non operational.

          Parameters: [partition]:[reference] ... [min_tid [max_tid]]
            reference: node id of a storage with known good data
              If not given, and if the cluster is in backup mode, an upstream
              cell is automatically taken as reference.
        """
        partition_dict = {}
        params = iter(params)
        min_tid = ZERO_TID
        max_tid = None
        for p in params:
            try:
                partition, source = p.split(':')
            except ValueError:
                min_tid = self.asTID(p)
                try:
                    max_tid = self.asTID(params.next())
                except StopIteration:
                    pass
                break
            source = self.asNode(source) if source else None
            if partition:
                partition_dict[int(partition)] = source
            else:
                assert not partition_dict
                np = len(self.neoctl.getPartitionRowList()[1])
                partition_dict = dict.fromkeys(xrange(np), source)
        self.neoctl.checkReplicas(partition_dict, min_tid, max_tid)

class Application(object):
    """The storage node application."""

    def __init__(self, *args, **kw):
        self.neoctl = TerminalNeoCTL(*args, **kw)

    def execute(self, args):
        """Execute the command given."""
        # print node type : print list of node of the given type
        # (STORAGE_NODE_TYPE, MASTER_NODE_TYPE...)
        # set node uuid state [1|0] : set the node for the given uuid to the
        # state (RUNNING, DOWN...) and modify the partition if asked
        # set cluster name [shutdown|operational] : either shutdown the
        # cluster or mark it as operational
        current_action = action_dict
        level = 0
        while current_action is not None and \
              level < len(args) and \
              isinstance(current_action, dict):
            current_action = current_action.get(args[level])
            level += 1
        action = None
        if isinstance(current_action, basestring):
            action = getattr(self.neoctl, current_action, None)
        if action is None:
            return self.usage('unknown command')
        try:
            return action(args[level:])
        except NotReadyException, message:
            return 'ERROR: %s' % (message, )

    def _usage(self, action_dict, level=0):
        result = []
        append = result.append
        sub_level = level + 1
        for name, action in action_dict.iteritems():
            append('%s%s' % ('  ' * level, name))
            if isinstance(action, dict):
                append(self._usage(action, level=sub_level))
            else:
                real_action = getattr(self.neoctl, action, None)
                if real_action is None:
                    continue
                docstring = getattr(real_action, '__doc__', None)
                if docstring is None:
                    docstring = '(no docstring)'
                docstring_line_list = docstring.split('\n')
                # Strip empty lines at begining & end of line list
                for end in (0, -1):
                    while len(docstring_line_list) \
                          and docstring_line_list[end] == '':
                        docstring_line_list.pop(end)
                # Get the indentation of first line, to preserve other lines
                # relative indentation.
                first_line = docstring_line_list[0]
                base_indentation = len(first_line) - len(first_line.lstrip())
                result.extend([('  ' * sub_level) + x[base_indentation:] \
                               for x in docstring_line_list])
        return '\n'.join(result)

    def usage(self, message):
        output_list = (message, 'Available commands:', self._usage(action_dict),
            "TID arguments can be either integers or timestamps as floats,"
            " e.g. '257684787499560686', '0x3937af2eeeeeeee' or '1325421296.'"
            " for 2012-01-01 12:34:56 UTC")
        return '\n'.join(output_list)

