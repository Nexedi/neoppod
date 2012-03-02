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

import weakref
from functools import wraps
import neo.lib
from neo.lib.connector import ConnectorConnectionClosedException
from neo.lib.handler import EventHandler
from neo.lib.protocol import Errors, NodeStates, Packets, \
    ZERO_HASH, ZERO_TID, ZERO_OID
from neo.lib.util import add64

def checkConnectionIsReplicatorConnection(func):
    def decorator(self, conn, *args, **kw):
        assert self.app.replicator.getCurrentConnection() is conn
        return func(self, conn, *args, **kw)
    return wraps(func)(decorator)

class StorageOperationHandler(EventHandler):
    """This class handles events for replications."""

    def connectionLost(self, conn, new_state):
        if self.app.listening_conn and conn.isClient():
            # XXX: Connection and Node should merged.
            uuid = conn.getUUID()
            if uuid:
                node = self.app.nm.getByUUID(uuid)
            else:
                node = self.app.nm.getByAddress(conn.getAddress())
                node.setState(NodeStates.DOWN)
            replicator = self.app.replicator
            if replicator.current_node is node:
                replicator.abort()

    # Client

    def connectionFailed(self, conn):
        if self.app.listening_conn:
            self.app.replicator.abort()

    @checkConnectionIsReplicatorConnection
    def acceptIdentification(self, conn, node_type,
                       uuid, num_partitions, num_replicas, your_uuid):
        self.app.replicator.fetchTransactions()

    @checkConnectionIsReplicatorConnection
    def answerFetchTransactions(self, conn, pack_tid, next_tid, tid_list):
        if tid_list:
            deleteTransaction = self.app.dm.deleteTransaction
            for tid in tid_list:
                deleteTransaction(tid)
        assert not pack_tid, "TODO"
        if next_tid:
            self.app.replicator.fetchTransactions(next_tid)
        else:
            self.app.replicator.fetchObjects()

    @checkConnectionIsReplicatorConnection
    def addTransaction(self, conn, tid, user, desc, ext, packed, oid_list):
        # Directly store the transaction.
        self.app.dm.storeTransaction(tid, (),
            (oid_list, user, desc, ext, packed), False)

    @checkConnectionIsReplicatorConnection
    def answerFetchObjects(self, conn, pack_tid, next_tid,
                                       next_oid, object_dict):
        if object_dict:
            deleteObject = self.app.dm.deleteObject
            for serial, oid_list in object_dict.iteritems():
                for oid in oid_list:
                    delObject(oid, serial)
        assert not pack_tid, "TODO"
        if next_tid:
            self.app.replicator.fetchObjects(next_tid, next_oid)
        else:
            self.app.replicator.finish()

    @checkConnectionIsReplicatorConnection
    def addObject(self, conn, oid, serial, compression,
                              checksum, data, data_serial):
        dm = self.app.dm
        if data or checksum != ZERO_HASH:
            data_id = dm.storeData(checksum, data, compression)
        else:
            data_id = None
        # Directly store the transaction.
        obj = oid, data_id, data_serial
        dm.storeTransaction(serial, (obj,), None, False)

    @checkConnectionIsReplicatorConnection
    def replicationError(self, conn, message):
        self.app.replicator.abort('source message: ' + message)

    # Server (all methods must set connection as server so that it isn't closed
    #         if client tasks are finished)

    def askCheckTIDRange(self, conn, min_tid, max_tid, length, partition):
        conn.asServer()
        count, tid_checksum, max_tid = self.app.dm.checkTIDRange(min_tid,
            max_tid, length, partition)
        conn.answer(Packets.AnswerCheckTIDRange(min_tid, length,
            count, tid_checksum, max_tid))

    def askCheckSerialRange(self, conn, min_oid, min_serial, max_tid, length,
            partition):
        conn.asServer()
        count, oid_checksum, max_oid, serial_checksum, max_serial = \
            self.app.dm.checkSerialRange(min_oid, min_serial, max_tid, length,
                partition)
        conn.answer(Packets.AnswerCheckSerialRange(min_oid, min_serial, length,
            count, oid_checksum, max_oid, serial_checksum, max_serial))

    def askFetchTransactions(self, conn, partition, length, min_tid, max_tid,
            tid_list):
        app = self.app
        cell = app.pt.getCell(partition, app.uuid)
        if cell is None or cell.isOutOfDate():
            return conn.answer(Errors.ReplicationError(
                "partition %u not readable" % partition))
        conn.asServer()
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        peer_tid_set = set(tid_list)
        dm = app.dm
        tid_list = dm.getReplicationTIDList(min_tid, max_tid, length + 1,
            partition)
        next_tid = tid_list.pop() if length < len(tid_list) else None
        def push():
            try:
                pack_tid = None # TODO
                for tid in tid_list:
                    if tid in peer_tid_set:
                        peer_tid_set.remove(tid)
                    else:
                        t = dm.getTransaction(tid)
                        if t is None:
                            conn.answer(Errors.ReplicationError(
                                "partition %u dropped" % partition))
                            return
                        oid_list, user, desc, ext, packed = t
                        conn.notify(Packets.AddTransaction(
                            tid, user, desc, ext, packed, oid_list))
                        yield
                conn.answer(Packets.AnswerFetchTransactions(
                    pack_tid, next_tid, peer_tid_set), msg_id)
                yield
            except (weakref.ReferenceError, ConnectorConnectionClosedException):
                pass
        app.newTask(push())

    def askFetchObjects(self, conn, partition, length, min_tid, max_tid,
            min_oid, object_dict):
        app = self.app
        cell = app.pt.getCell(partition, app.uuid)
        if cell is None or cell.isOutOfDate():
            return conn.answer(Errors.ReplicationError(
                "partition %u not readable" % partition))
        conn.asServer()
        msg_id = conn.getPeerId()
        conn = weakref.proxy(conn)
        dm = app.dm
        object_list = dm.getReplicationObjectList(min_tid, max_tid, length,
            partition, min_oid)
        if length < len(object_list):
            next_tid, next_oid = object_list.pop()
        else:
            next_tid = next_oid = None
        def push():
            try:
                pack_tid = None # TODO
                for serial, oid in object_list:
                    oid_set = object_dict.get(serial)
                    if oid_set:
                        if type(oid_set) is list:
                            object_dict[serial] = oid_set = set(oid_set)
                        if oid in oid_set:
                            oid_set.remove(oid)
                            if not oid_set:
                                del object_dict[serial]
                            continue
                    object = dm.getObject(oid, serial)
                    if object is None:
                        conn.answer(Errors.ReplicationError(
                            "partition %u dropped" % partition))
                        return
                    conn.notify(Packets.AddObject(oid, serial, *object[2:]))
                    yield
                conn.answer(Packets.AnswerFetchObjects(
                    pack_tid, next_tid, next_oid, object_dict), msg_id)
                yield
            except (weakref.ReferenceError, ConnectorConnectionClosedException):
                pass
        app.newTask(push())
