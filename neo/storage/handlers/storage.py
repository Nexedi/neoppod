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

import weakref
from functools import wraps
from neo.lib.connection import ConnectionClosed
from neo.lib.exception import ProtocolError
from neo.lib.handler import DelayEvent
from neo.lib.protocol import Errors, Packets, ZERO_HASH
from . import EventHandler

def checkConnectionIsReplicatorConnection(func):
    def wrapper(self, conn, *args, **kw):
        if self.app.replicator.isReplicatingConnection(conn):
            return func(self, conn, *args, **kw)
    return wraps(func)(wrapper)

def checkFeedingConnection(check):
    def decorator(func):
        def wrapper(self, conn, partition, *args, **kw):
            app = self.app
            cell = app.pt.getCell(partition, app.uuid)
            if cell is None or (cell.isOutOfDate() if check else
                                not cell.isReadable()):
                p = Errors.CheckingError if check else Errors.ReplicationError
                return conn.answer(p("partition %u not readable" % partition))
            conn.asServer()
            return func(self, conn, partition, *args, **kw)
        return wraps(func)(wrapper)
    return decorator

class StorageOperationHandler(EventHandler):
    """This class handles events for replications."""

    def connectionLost(self, conn, new_state):
        app = self.app
        if app.operational and conn.isClient():
            with app.dm.lock:
                uuid = conn.getUUID()
                if uuid:
                    node = app.nm.getByUUID(uuid)
                else:
                    node = app.nm.getByAddress(conn.getAddress())
                    node.setUnknown()
                replicator = app.replicator
                if replicator.current_node is node:
                    replicator.abort()
                app.checker.connectionLost(conn)

    # Client

    def connectionFailed(self, conn):
        app = self.app
        if app.operational:
            with app.dm.lock:
                app.replicator.abort()

    def _acceptIdentification(self, node, *args):
        self.app.replicator.connected(node)
        self.app.checker.connected(node)

    @checkConnectionIsReplicatorConnection
    def answerFetchTransactions(self, conn, next_tid, tid_list, completed_pack):
        app = self.app
        if tid_list:
            deleteTransaction = app.dm.deleteTransaction
            for tid in tid_list:
                deleteTransaction(tid)
        if completed_pack is not None:
            app.dm.updateCompletedPackByReplication(
                app.replicator.current_partition, completed_pack)
        if next_tid:
            app.replicator.fetchTransactions(next_tid)
        else:
            app.replicator.fetchObjects()

    @checkConnectionIsReplicatorConnection
    def addTransaction(self, conn, tid, user, desc, ext, packed, ttid,
                                   oid_list, pack):
        # Directly store the transaction.
        self.app.dm.storeTransaction(tid, (),
            (oid_list, user, desc, ext, packed, ttid), False)
        if pack:
            self.app.dm.storePackOrder(tid, *pack)

    @checkConnectionIsReplicatorConnection
    def answerFetchObjects(self, conn, next_tid, next_oid, object_dict):
        if object_dict:
            deleteObject = self.app.dm.deleteObject
            for serial, oid_list in object_dict.iteritems():
                for oid in oid_list:
                    deleteObject(oid, serial)
        if next_tid:
            # TODO also provide feedback to master about current replication state (tid)
            self.app.replicator.fetchObjects(next_tid, next_oid)
        else:
            # This will also commit.
            self.app.replicator.finish()

    @checkConnectionIsReplicatorConnection
    def addObject(self, conn, oid, serial, compression,
                              checksum, data, data_serial):
        dm = self.app.dm
        if not data and checksum == ZERO_HASH:
            checksum = data = None
        data_id = dm.storeData(checksum, oid, data, compression, data_serial)
        dm.storeTransaction(serial, ((oid, data_id, data_serial),), None, False)

    @checkConnectionIsReplicatorConnection
    def replicationError(self, conn, message):
        self.app.replicator.abort('source message: ' + message)

    def checkingError(self, conn, message):
        try:
            self.app.checker.connectionLost(conn)
        finally:
            self.app.closeClient(conn)

    @property
    def answerCheckTIDRange(self):
        return self.app.checker.checkRange

    @property
    def answerCheckSerialRange(self):
        return self.app.checker.checkRange

    # Server (all methods must set connection as server so that it isn't closed
    #         if client tasks are finished)
    #
    # These are all low-priority packets, in that we don't want to delay
    # answers to clients, so tasks are used to postpone work when we're idle.

    def getEventQueue(self):
        return self.app.tm.read_queue

    @checkFeedingConnection(check=True)
    def askCheckTIDRange(self, conn, *args):
        app = self.app
        if app.tm.isLockedTid(args[3]): # max_tid
            raise DelayEvent
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        def check():
            r = app.dm.checkTIDRange(*args)
            try:
                conn.send(Packets.AnswerCheckTIDRange(*r), msg_id)
            except (weakref.ReferenceError, ConnectionClosed):
                pass
            # Splitting this task would cause useless overhead. However, a
            # generator function is expected, hence the following fake yield
            # so that iteration stops immediately.
            return; yield
        app.newTask(check())

    @checkFeedingConnection(check=True)
    def askCheckSerialRange(self, conn, *args):
        app = self.app
        if app.tm.isLockedTid(args[3]): # max_tid
            raise ProtocolError("transactions must be checked before objects")
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        def check():
            r = app.dm.checkSerialRange(*args)
            try:
                conn.send(Packets.AnswerCheckSerialRange(*r), msg_id)
            except (weakref.ReferenceError, ConnectionClosed):
                pass
            return; yield # same as in askCheckTIDRange
        app.newTask(check())

    @checkFeedingConnection(check=False)
    def askFetchTransactions(self, conn, partition, length, min_tid, max_tid,
            tid_list, ask_pack_info):
        app = self.app
        if app.tm.isLockedTid(max_tid):
            # Wow, backup cluster is fast. Requested transactions are still in
            # ttrans/ttobj so wait a little.
            # This can also happen for internal replication, when
            #   NotifyTransactionFinished(M->S) + AskFetchTransactions(S->S)
            # is faster than
            #   NotifyUnlockInformation(M->S)
            raise DelayEvent
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        peer_tid_set = set(tid_list)
        dm = app.dm
        completed_pack = dm.getPackedIDs()[partition] if ask_pack_info else None
        tid_list = dm.getReplicationTIDList(min_tid, max_tid, length + 1,
            partition)
        next_tid = tid_list.pop() if length < len(tid_list) else None
        def push():
            try:
                for tid in tid_list:
                    if tid in peer_tid_set:
                        peer_tid_set.remove(tid)
                    else:
                        t = dm.getTransaction(tid)
                        if t is None:
                            conn.send(Errors.ReplicationError(
                                "partition %u dropped"
                                % partition), msg_id)
                            return
                        oid_list, user, desc, ext, packed, ttid, pack = t
                        # Sending such packet does not mark the connection
                        # for writing if there's too little data in the buffer.
                        conn.send(Packets.AddTransaction(tid, user,
                            desc, ext, packed, ttid, oid_list, pack), msg_id)
                        # To avoid delaying several connections simultaneously,
                        # and also prevent the backend from scanning different
                        # parts of the DB at the same time, we ask the
                        # scheduler not to switch to another background task.
                        # Ideally, we are filling a buffer while the kernel
                        # is flushing another one for a concurrent connection.
                        yield conn.buffering
                conn.send(Packets.AnswerFetchTransactions(
                    next_tid, peer_tid_set, completed_pack), msg_id)
                yield
            except (weakref.ReferenceError, ConnectionClosed):
                pass
        app.newTask(push())

    @checkFeedingConnection(check=False)
    def askFetchObjects(self, conn, partition, length, min_tid, max_tid,
            min_oid, object_dict):
        app = self.app
        if app.tm.isLockedTid(max_tid):
            raise ProtocolError("transactions must be fetched before objects")
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        dm = app.dm
        object_list = dm.getReplicationObjectList(min_tid, max_tid, length + 1,
            partition, min_oid)
        if length < len(object_list):
            next_tid, next_oid = object_list.pop()
        else:
            next_tid = next_oid = None
        def push():
            try:
                for serial, oid in object_list:
                    oid_set = object_dict.get(serial)
                    if oid_set:
                        if type(oid_set) is tuple:
                            object_dict[serial] = oid_set = set(oid_set)
                        if oid in oid_set:
                            oid_set.remove(oid)
                            if not oid_set:
                                del object_dict[serial]
                            continue
                    object = dm.fetchObject(oid, serial)
                    if not object:
                        conn.send(Errors.ReplicationError(
                            "partition %u dropped or truncated"
                            % partition), msg_id)
                        return
                    if not object[2]: # creation undone
                        object = object[0], 0, ZERO_HASH, '', object[4]
                    # Same as in askFetchTransactions.
                    conn.send(Packets.AddObject(oid, *object), msg_id)
                    yield conn.buffering
                conn.send(Packets.AnswerFetchObjects(
                    next_tid, next_oid, object_dict), msg_id)
                yield
            except (weakref.ReferenceError, ConnectionClosed):
                pass
        app.newTask(push())
