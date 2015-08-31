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

import json, socket
from urllib import URLopener, urlencode
from neo.lib.protocol import CellStates, ClusterStates, NodeTypes, NodeStates, \
    ZERO_TID
from neo.lib.util import u64

class NotReadyException(Exception):
    pass

class NeoCTL(object):

    def __init__(self, address):
        host, port = address
        if ":" in host:
            host = "[%s]" % host
        self.base_url = "http://%s:%s/" % (host, port)
        self._open = URLopener().open

    def _ask(self, path, **kw):
        if kw:
            path += "?" + urlencode(sorted(x for x in kw.iteritems()
                                             if '' is not x[1] is not None))
        try:
            return self._open(self.base_url + path).read()
        except IOError, e:
            e0 = e[0]
            if e0 == 'socket error' or e0 == 'http error' and e[1] == 503:
                raise NotReadyException
            raise

    def enableStorageList(self, uuid_list):
        """
          Put all given storage nodes in "running" state.
        """
        self._ask('enableStorageList', node_list=','.join(map(str, uuid_list)))

    def tweakPartitionTable(self, uuid_list=()):
        self._ask('tweakPartitionTable', node_list=','.join(map(str, uuid_list)))

    def setClusterState(self, state):
        """
          Set cluster state.
        """
        self._ask('setClusterState', state=state)

    def getClusterState(self):
        """
          Get cluster state.
        """
        state = self._ask('getClusterState')
        if state:
            return getattr(ClusterStates, state)

    def getNodeList(self, node_type=None):
        """
          Get a list of nodes, filtering with given type.
        """
        node_list = json.loads(self._ask('getNodeList', node_type=node_type))
        return ((getattr(NodeTypes, node_type), address and tuple(address),
                 uuid, getattr(NodeStates, state))
            for node_type, address, uuid, state in node_list)

    def getPartitionRowList(self, min_offset=0, max_offset=0, node=None):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
        """
        ptid, row_list = json.loads(self._ask('getPartitionRowList',
            min_offset=min_offset, max_offset=max_offset, node=node))
        return ptid, [(offset, [(node, getattr(CellStates, state))
                                for node, state in row])
                      for offset, row in row_list]

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        self._ask('startCluster')

    def killNode(self, node):
        self._ask('killNode', node=node)

    def dropNode(self, node):
        self._ask('dropNode', node=node)

    def getPrimary(self):
        """
          Return the primary master UUID.
        """
        return int(self._ask('getPrimary'))

    def checkReplicas(self, partition_dict, min_tid=ZERO_TID, max_tid=None):
        kw = {'pt': ','.join('%s:%s' % (k, '' if v is None else v)
                             for k, v in partition_dict.iteritems())}
        if max_tid is not None:
            kw['max_tid'] = u64(max_tid)
        self._ask('checkReplicas', min_tid=u64(min_tid), **kw)
