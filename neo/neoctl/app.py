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

import json, sys
from .neoctl import NeoCTL, NotReadyException
from neo.lib.node import NodeManager
from neo.lib.pt import PartitionTable
from neo.lib.util import p64, u64, datetimeFromTID, tidFromTime
from neo.lib.protocol import uuid_str, formatNodeList, \
    ClusterStates, NodeStates, NodeTypes, UUID_NAMESPACES, ZERO_TID

action_dict = {
    'print': {
        'ids': 'getLastIds',
        'pt': 'getPartitionRowList',
        'node': 'getNodeList',
        'cluster': 'getClusterState',
        'primary': 'getPrimary',
        'summary': 'getSummary',
    },
    'set': {
        'cluster': 'setClusterState',
        'replicas': 'setNumReplicas',
    },
    'check': 'checkReplicas',
    'start': 'startCluster',
    'add': 'enableStorageList',
    'tweak': 'tweakPartitionTable',
    'drop': 'dropNode',
    'kill': 'killNode',
    'prune_orphan': 'pruneOrphan',
    'truncate': 'truncate',
    'flush_log': 'flushLog',
}

uuid_int = (lambda ns: lambda uuid:
    (ns[uuid[0]] << 24) + int(uuid[1:])
    )({str(k)[0]: v for k, v in UUID_NAMESPACES.iteritems()})


class dummy_app:
    id_timestamp = uuid = 0


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

    def formatPartitionTable(self, row_list):
        nm = NodeManager()
        nm.update(dummy_app, 1,
            self.neoctl.getNodeList(node_type=NodeTypes.STORAGE))
        pt = object.__new__(PartitionTable)
        pt._load(None, None, row_list, nm.getByUUID)
        pt.addNodeList(nm.getByStateList(NodeStates.RUNNING))
        return '\n'.join(line[4:] for line in pt._format())

    def formatRowList(self, row_list):
        return '\n'.join('%03d |%s' % (offset,
            ''.join(' %s - %s |' % (uuid_str(uuid), state)
            for (uuid, state) in cell_list))
            for (offset, cell_list) in row_list)

    # Actual actions
    def getLastIds(self, params):
        """
          Get last ids.
        """
        assert not params
        ptid, backup_tid, truncate_tid = self.neoctl.getRecovery()
        if backup_tid:
            ltid = self.neoctl.getLastTransaction()
            r = "backup_tid = 0x%x (%s)" % (u64(backup_tid),
                                            datetimeFromTID(backup_tid))
        else:
            ltid, loid = self.neoctl.getLastIds()
            r = "last_oid = 0x%x" % (u64(loid))
        return r + "\nlast_tid = 0x%x (%s)\nlast_ptid = %s" % \
                                    (u64(ltid), datetimeFromTID(ltid), ptid)

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
        ptid, num_replicas, row_list = self.neoctl.getPartitionRowList(
                min_offset=min_offset, max_offset=max_offset, node=node)
        return '# ptid: %s, replicas: %s\n%s' % (ptid, num_replicas,
            self.formatRowList(enumerate(row_list, min_offset))
            if min_offset or max_offset else
            self.formatPartitionTable(row_list))

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
        return '\n'.join(formatNodeList(node_list)) or 'Empty list!'

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

    def getSummary(self, params):
        """
          Get a summary of the health of this cluster and backups.
          The first line reports severities: it is a commented json dump of
            {severity: [backup_name | null]}
          where severity is either "warning" or "problem"
            and null refers to this cluster
        """
        assert len(params) == 0
        warning, problem, summary = self.neoctl.getMonitorInformation()
        return "# %s\n%s" % (json.dumps({k: v for k, v in zip(
            ('warning', 'problem'),
            (warning, problem),
            ) if v}), summary)

    def setNumReplicas(self, params):
        """
          Set number of replicas.
          Parameters: nr
            nr: positive number (0 means no redundancy)
        """
        assert len(params) == 1
        nr = int(params[0])
        if nr < 0:
            sys.exit('invalid number of replicas')
        return self.neoctl.setNumReplicas(nr)

    def startCluster(self, params):
        """
          Starts cluster operation after a startup.
          Equivalent to:
            set cluster verifying
        """
        assert len(params) == 0
        return self.neoctl.startCluster()

    def _getStorageList(self, params):
        if len(params) == 1 and params[0] == 'all':
            node_list = self.neoctl.getNodeList(NodeTypes.STORAGE)
            return [node[2] for node in node_list]
        return map(self.asNode, params)

    def enableStorageList(self, params):
        """
          Enable cluster to make use of pending storages.
          Parameters: node [node [...]]
            node: if "all", add all pending storage nodes,
                  otherwise, the list of storage nodes to enable.
        """
        return self.neoctl.enableStorageList(self._getStorageList(params))

    def tweakPartitionTable(self, params):
        """
          Optimize partition table.
          No change is done to the specified/down storage nodes and they don't
          count as replicas. The purpose of listing nodes is usually to drop
          them once the data is replicated to other nodes.
          Parameters: [-n] [node [...]]
            -n: dry run
        """
        dry_run = params[0] == '-n' if params else False
        changed, row_list = self.neoctl.tweakPartitionTable(
            map(self.asNode, params[dry_run:]), dry_run)
        if changed:
            return self.formatPartitionTable(row_list)
        return 'No change done.'

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

    def pruneOrphan(self, params):
        """
          Fix database by deleting unreferenced raw data

          This can take a long time.

          Parameters: dry_run node [node [...]]
            dry_run: 0 or 1
            node: if "all", ask all connected storage nodes to repair,
                  otherwise, only the given list of storage nodes.
        """
        dry_run = bool("01".index(params.pop(0)))
        return self.neoctl.repair(self._getStorageList(params), dry_run)

    def truncate(self, params):
        """
          Truncate the database at the given tid.

          The cluster must be in RUNNING state, without any pending transaction.
          This causes the cluster to go back in RECOVERING state, waiting all
          nodes to be pending (do not use 'start' command unless you're sure
          the missing nodes don't need to be truncated).

          Parameters: tid
        """
        self.neoctl.truncate(self.asTID(*params))

    def checkReplicas(self, params):
        """
          Test whether partitions have corrupted metadata by comparing replicas

          Any corrupted cell is put in CORRUPTED state, possibly make the
          cluster non operational.

          EXPERIMENTAL - This operation is not aware that differences happen
                         during pack operations and you could easily break
                         your database. Since there's anyway no mechanism to
                         repair cells, the primary master only logs possible
                         corruption rather than mark cells as CORRUPTED.

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

    def flushLog(self, params):
        """
          Ask all nodes in the cluster to flush their logs.

          If there are backup clusters, only their primary masters flush.
        """
        assert not params
        self.neoctl.flushLog()

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
        if not args:
            return self.usage()
        current_action = action_dict
        level = 0
        try:
            while level < len(args) and \
                  isinstance(current_action, dict):
                current_action = current_action[args[level]]
                level += 1
        except KeyError:
            sys.exit('invalid command: ' + ' '.join(args))
        action = getattr(self.neoctl, current_action)
        try:
            return action(args[level:])
        except NotReadyException, message:
            sys.exit('ERROR: ' + message)

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
                # Strip empty lines at beginning & end of line list
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

    def usage(self):
        output_list = ('Available commands:', self._usage(action_dict),
            "TID arguments can be either integers or timestamps as floats,"
            " e.g. '257684787499560686', '0x3937af2eeeeeeee' or '1325421296.'"
            " for 2012-01-01 12:34:56 UTC")
        return '\n'.join(output_list)
