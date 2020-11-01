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
from neo.lib.protocol import Packets
from .exception import NEOStorageError

@apply
class _WakeupPacket(object):

    handler_method_name = 'pong'
    decode = tuple
    getId = int

class Transaction(object):

    cache_size = 0  # size of data in cache_dict
    data_size = 0   # size of data in data_dict
    error = None
    locking_tid = None
    voted = False
    ttid = None     # XXX: useless, except for testBackupReadOnlyAccess

    def __init__(self, txn):
        self.queue = SimpleQueue()
        self.txn = txn
        # data being stored
        self.data_dict = {}                 # {oid: (value, serial, [node_id])}
        # data stored: this will go to the cache on tpc_finish
        self.cache_dict = {}                # {oid: value}
        # conflicts to resolve
        self.conflict_dict = {}             # {oid: serial}
        # resolved conflicts
        self.resolved_dict = {}             # {oid: serial}
        # Keys are node ids instead of Node objects because a node may
        # disappear from the cluster. In any case, we always have to check
        # if the id is still known by the NodeManager.
        # status: 0 -> check only, 1 -> store, 2 -> failed
        self.involved_nodes = {}            # {node_id: status}

    def wakeup(self, conn):
        self.queue.put((conn, _WakeupPacket, {}))

    def write(self, app, packet, object_id, store=1, **kw):
        uuid_list = []
        pt = app.pt
        involved = self.involved_nodes
        object_id = pt.getPartition(object_id)
        for cell in pt.getCellList(object_id):
            node = cell.getNode()
            uuid = node.getUUID()
            status = involved.get(uuid, -1)
            if status < store:
                involved[uuid] = store
            elif status > 1:
                continue
            conn = app.cp.getConnForNode(node)
            if conn is not None:
                try:
                    if status < 0 and self.locking_tid and 'oid' in kw:
                        # A deadlock happened but this node is not aware of it.
                        # Tell it to write-lock with the same locking tid as
                        # for the other nodes. The condition on kw is because
                        # we don't need that for transaction metadata.
                        conn.ask(Packets.AskRebaseTransaction(
                            self.ttid, self.locking_tid), queue=self.queue)
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

    def written(self, app, uuid, oid):
        # When a node that is being disconnected by the master because it was
        # not part of the transaction that caused a conflict, we may receive a
        # positive answer (not to be confused with lockless stores) before the
        # conflict. Because we have no way to identify such case, we must keep
        # the data in self.data_dict until all nodes have answered so we remain
        # able to resolve conflicts.
        try:
            data, serial, uuid_list = self.data_dict[oid]
            uuid_list.remove(uuid)
        except KeyError:
            # 1. store to S1 and S2
            # 2. S2 reports a conflict
            # 3. store to S1 and S2 # conflict resolution
            # 4. S1 does not report a conflict (lockless)
            # 5. S2 answers before S1 for the second store
            return
        except ValueError:
            # The most common case for this exception is because nodeLost()
            # tries all oids blindly. Other possible cases:
            # - like above (KeyError), but with S2 answering last
            # - answer to resolved conflict before the first answer from a
            #   node that was being disconnected by the master
            return
        if uuid_list:
            return
        del self.data_dict[oid]
        if type(data) is bytes:
            size = len(data)
            self.data_size -= size
            size += self.cache_size
            if size < app._cache._max_size:
                self.cache_size = size
            else:
                # Do not cache data past cache max size, as it
                # would just flush it on tpc_finish. This also
                # prevents memory errors for big transactions.
                data = None
        self.cache_dict[oid] = data

    def nodeLost(self, app, uuid):
        self.involved_nodes[uuid] = 2
        for oid in list(self.data_dict):
            self.written(app, uuid, oid)


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
