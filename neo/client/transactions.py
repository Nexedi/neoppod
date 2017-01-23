#
# Copyright (C) 2017  Nexedi SA
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

from ZODB.POSException import StorageTransactionError
from neo.lib.connection import ConnectionClosed
from neo.lib.locking import SimpleQueue
from .exception import NEOStorageError


class Transaction(object):

    cache_size = 0  # size of data in cache_dict
    data_size = 0   # size of data in data_dict
    error = None
    voted = False
    ttid = None     # XXX: useless, except for testBackupReadOnlyAccess

    def __init__(self, txn):
        self.queue = SimpleQueue()
        self.txn = txn
        # data being stored
        self.data_dict = {}
        # data stored: this will go to the cache on tpc_finish
        self.cache_dict = {}
        # conflicts to resolve
        self.conflict_dict = {}                  # {oid: (base_serial, serial)}
        # resolved conflicts
        self.resolved_dict = {}                  # {oid: serial}
        # Keys are node ids instead of Node objects because a node may
        # disappear from the cluster. In any case, we always have to check
        # if the id is still known by the NodeManager.
        # status: 0 -> check only, 1 -> store, 2 -> failed
        self.involved_nodes = {}                 # {node_id: status}

    def write(self, app, packet, object_id, store=1, **kw):
        uuid_list = []
        pt = app.pt
        involved = self.involved_nodes
        object_id = pt.getPartition(object_id)
        for cell in pt.getCellList(object_id):
            node = cell.getNode()
            uuid = node.getUUID()
            status = involved.setdefault(uuid, store)
            if status < store:
                involved[uuid] = store
            elif status > 1:
                continue
            conn = app.cp.getConnForNode(node)
            if conn is not None:
                try:
                    conn.ask(packet, queue=self.queue, **kw)
                    uuid_list.append(uuid)
                    continue
                except ConnectionClosed:
                    pass
            involved[uuid] = 2
        if uuid_list:
            return uuid_list
        raise NEOStorageError(
            'no storage available for write to partition %s' % object_id)


class TransactionContainer(dict):
    # IDEA: Drop this container and use the new set_data/data API on
    #       transactions (requires transaction >= 1.6).

    def pop(self, txn):
        return dict.pop(self, id(txn), None)

    def get(self, txn):
        try:
            return self[id(txn)]
        except KeyError:
            raise StorageTransactionError("unknown transaction %r" % txn)

    def new(self, txn):
        key = id(txn)
        if key in self:
            raise StorageTransactionError("commit of transaction %r"
                                          " already started" % txn)
        context = self[key] = Transaction(txn)
        return context
