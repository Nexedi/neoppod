#
# Copyright (C) 2017-2019  Nexedi SA
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
from ZODB.POSException import StorageTransactionError
from neo.lib.connection import ConnectionClosed
from neo.lib.locking import SimpleQueue
from neo.lib.protocol import Packets
from neo.lib.util import dump
from .exception import NEOStorageError

@apply
class _WakeupPacket(object):

    handler_method_name = 'pong'
    _args = ()
    getId = int

class Transaction(object):

    cache_size = 0  # size of data in cache_dict
    data_size = 0   # size of data in data_dict
    error = None
    locking_tid = None
    voted = False
    ttid = None     # XXX: useless, except for testBackupReadOnlyAccess
    lockless_dict = None                    # {partition: {uuid}}

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
        # involved storage nodes; connection is None is connection was lost
        self.conn_dict = {}                 # {node_id: connection}

    def __repr__(self):
        error = self.error
        return ("<%s ttid=%s locking_tid=%s voted=%u"
                " #queue=%s #writing=%s #written=%s%s>") % (
            self.__class__.__name__,
            dump(self.ttid), dump(self.locking_tid), self.voted,
            len(self.queue._queue), len(self.data_dict), len(self.cache_dict),
            ' error=%r' % error if error else '')

    def wakeup(self, conn):
        self.queue.put((conn, _WakeupPacket, {}))

    def write(self, app, packet, object_id, **kw):
        uuid_list = []
        pt = app.pt
        conn_dict = self.conn_dict
        object_id = pt.getPartition(object_id)
        for cell in pt.getCellList(object_id):
            node = cell.getNode()
            uuid = node.getUUID()
            try:
                try:
                    conn = conn_dict[uuid]
                except KeyError:
                    conn = conn_dict[uuid] = app.getStorageConnection(node)
                    if self.locking_tid and 'oid' in kw:
                        # A deadlock happened but this node is not aware of it.
                        # Tell it to write-lock with the same locking tid as
                        # for the other nodes. The condition on kw is to
                        # distinguish whether we're writing an oid or
                        # transaction metadata.
                        conn.ask(Packets.AskRebaseTransaction(
                            self.ttid, self.locking_tid), queue=self.queue)
                conn.ask(packet, queue=self.queue, **kw)
                uuid_list.append(uuid)
            except AttributeError:
                if conn is not None:
                    raise
            except ConnectionClosed:
                conn_dict[uuid] = None
        if uuid_list:
            return uuid_list
        raise NEOStorageError(
            'no storage available for write to partition %s' % object_id)

    def written(self, app, uuid, oid, lockless=None):
        # When a node is being disconnected by the master because it was
        # not part of the transaction that caused a conflict, we may receive a
        # positive answer (not to be confused with lockless stores) before the
        # conflict. Because we have no way to identify such case, we must keep
        # the data in self.data_dict until all nodes have answered so we remain
        # able to resolve conflicts.
        data, serial, uuid_list = self.data_dict[oid]
        try:
            uuid_list.remove(uuid)
        except ValueError:
            # The most common case for this exception is because nodeLost()
            # tries all oids blindly.
            # Another possible case is when we receive several positive answers
            # from a node that is being disconnected by the master, whereas the
            # first one (at least) should actually be conflict answer.
            return
        if lockless:
            if lockless != serial: # late lockless write
                assert lockless < serial, (lockless, serial)
                uuid_list.append(uuid)
                return
            # It's safe to do this after the above excepts: either the cell is
            # already marked as lockless or the node will be reported as failed.
            lockless = self.lockless_dict
            if not lockless:
                lockless = self.lockless_dict = defaultdict(set)
            lockless[app.pt.getPartition(oid)].add(uuid)
            if oid in self.conflict_dict:
                # In the case of a rebase, uuid_list may not contain the id
                # of the node reporting a conflict.
                return
        if uuid_list:
            return
        del self.data_dict[oid]
        if type(data) is bytes:
            size = len(data)
            self.data_size -= size
            size += self.cache_size
            if size < app._cache.max_size:
                self.cache_size = size
            else:
                # Do not cache data past cache max size, as it
                # would just flush it on tpc_finish. This also
                # prevents memory errors for big transactions.
                data = None
        self.cache_dict[oid] = data

    def nodeLost(self, app, uuid):
        # The following line is sometimes redundant
        # with the one in `except ConnectionClosed:` clauses.
        self.conn_dict[uuid] = None
        for oid in list(self.data_dict):
            # Exclude case of 1 conflict error immediately followed by a
            # connection loss, possibly with lockless writes to replicas.
            if oid not in self.conflict_dict:
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
