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

import json, socket, sys
from urllib import urlencode
from urllib2 import urlopen, HTTPError, URLError
from neo.lib.protocol import CellStates, ClusterStates, NodeTypes, NodeStates, \
    ZERO_TID
from neo.lib.util import p64, u64

def ptFromJson(row_list):
    return tuple(
        tuple(sorted((uuid, getattr(CellStates, state))
            for state, uuid_list in row.iteritems()
            for uuid in uuid_list))
        for row in row_list)

class NotReadyException(Exception):
    pass

class NeoCTL(object):

    _open = staticmethod(urlopen)

    def __init__(self, address, ssl=None):
        host, port = address
        if ":" in host:
            host = "[%s]" % host
        self.base_url = "http://%s:%s/" % (host, port)

    def _ask(self, path, **kw):
        if kw:
            path += "?" + urlencode(sorted(x for x in kw.iteritems()
                                             if '' is not x[1] is not None))
        try:
            return self._open(self.base_url + path).read()
        except URLError, e:
            if isinstance(e, HTTPError):
                body = e.read()
                if e.code != 503:
                    sys.exit(body)
            else:
                body = str(e)
            raise NotReadyException(body)

    def enableStorageList(self, uuid_list):
        """
          Put all given storage nodes in "running" state.
        """
        return self._ask('enableStorageList',
            node_list=','.join(map(str, uuid_list)))

    def tweakPartitionTable(self, uuid_list=(), dry_run=False):
        changed, row_list = json.loads(self._ask('tweakPartitionTable',
            node_list=','.join(map(str, uuid_list)),
            dry_run=int(dry_run)))
        return changed, ptFromJson(row_list)

    def setNumReplicas(self, nr):
        return self._ask('setNumReplicas', nr=nr)

    def setClusterState(self, state):
        """
          Set cluster state.
        """
        return self._ask('setClusterState', state=state)

    def getClusterState(self):
        """
          Get cluster state.
        """
        state = self._ask('getClusterState')
        if state:
            return getattr(ClusterStates, state)

    def getLastIds(self):
        loid, ltid = json.loads(self._ask('getLastIds'))
        return p64(loid), p64(ltid)

    def getLastTransaction(self):
        return p64(int(self._ask('getLastTransaction')))

    def getRecovery(self):
        ptid, backup_tid, truncate_tid = json.loads(self._ask('getRecovery'))
        return (ptid,
            None if backup_tid is None else p64(backup_tid),
            None if truncate_tid is None else p64(truncate_tid),
            )

    def getNodeList(self, node_type=None):
        """
          Get a list of nodes, filtering with given type.
        """
        node_list = json.loads(self._ask('getNodeList', node_type=node_type))
        return [(getattr(NodeTypes, node_type), address and tuple(address),
                 uuid, getattr(NodeStates, state), id_timestamp)
            for node_type, address, uuid, state, id_timestamp in node_list]

    def getPartitionRowList(self, min_offset=0, max_offset=0, node=None):
        """
          Get a list of partition rows, bounded by min & max and involving
          given node.
        """
        ptid, nr, row_list = json.loads(self._ask('getPartitionRowList',
            min_offset=min_offset, max_offset=max_offset, node=node))
        from pprint import pprint as pp
        return ptid, nr, ptFromJson(row_list)

    def startCluster(self):
        """
          Set cluster into "verifying" state.
        """
        return self._ask('startCluster')

    def killNode(self, node):
        return self._ask('killNode', node=node)

    def dropNode(self, node):
        return self._ask('dropNode', node=node)

    def getPrimary(self):
        """
          Return the primary master UUID.
        """
        return int(self._ask('getPrimary'))

    def repair(self, uuid_list, dry_run):
        return self._ask('repair',
            node_list=','.join(map(str, uuid_list)),
            dry_run=int(dry_run))

    def truncate(self, tid):
        return self._ask('truncate', tid=u64(tid))

    def checkReplicas(self, partition_dict, min_tid=ZERO_TID, max_tid=None):
        kw = {'pt': ','.join('%s:%s' % (k, '' if v is None else v)
                             for k, v in partition_dict.iteritems())}
        if max_tid is not None:
            kw['max_tid'] = u64(max_tid)
        return self._ask('checkReplicas', min_tid=u64(min_tid), **kw)

    def flushLog(self):
        return self._ask('flushLog')
