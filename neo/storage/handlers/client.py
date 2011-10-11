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

import neo.lib
from neo.lib import protocol
from neo.lib.util import dump, makeChecksum
from neo.lib.protocol import Packets, LockState, Errors, ZERO_HASH
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.storage.transactions import ConflictError, DelayedError
from neo.storage.exception import AlreadyPendingError
import time

# Log stores taking (incl. lock delays) more than this many seconds.
# Set to None to disable.
SLOW_STORE = 2

class ClientOperationHandler(BaseClientAndStorageOperationHandler):

    def _askObject(self, oid, serial, ttid):
        return self.app.dm.getObject(oid, serial, ttid)

    def connectionLost(self, conn, new_state):
        uuid = conn.getUUID()
        node = self.app.nm.getByUUID(uuid)
        if self.app.listening_conn: # if running
            assert node is not None, conn
            self.app.nm.remove(node)

    def abortTransaction(self, conn, ttid):
        self.app.tm.abort(ttid)

    def askStoreTransaction(self, conn, ttid, user, desc, ext, oid_list):
        self.app.tm.register(conn.getUUID(), ttid)
        self.app.tm.storeTransaction(ttid, oid_list, user, desc, ext, False)
        conn.answer(Packets.AnswerStoreTransaction(ttid))

    def _askStoreObject(self, conn, oid, serial, compression, checksum, data,
            data_serial, ttid, unlock, request_time):
        if ttid not in self.app.tm:
            # transaction was aborted, cancel this event
            neo.lib.logging.info('Forget store of %s:%s by %s delayed by %s',
                    dump(oid), dump(serial), dump(ttid),
                    dump(self.app.tm.getLockingTID(oid)))
            # send an answer as the client side is waiting for it
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))
            return
        try:
            self.app.tm.storeObject(ttid, serial, oid, compression,
                    checksum, data, data_serial, unlock)
        except ConflictError, err:
            # resolvable or not
            ttid_or_serial = err.getTID()
            conn.answer(Packets.AnswerStoreObject(1, oid, ttid_or_serial))
        except DelayedError:
            # locked by a previous transaction, retry later
            # If we are unlocking, we want queueEvent to raise
            # AlreadyPendingError, to avoid making lcient wait for an unneeded
            # response.
            try:
                self.app.queueEvent(self._askStoreObject, conn, (oid, serial,
                    compression, checksum, data, data_serial, ttid,
                    unlock, request_time), key=(oid, ttid),
                    raise_on_duplicate=unlock)
            except AlreadyPendingError:
                conn.answer(Errors.AlreadyPending(dump(oid)))
        else:
            if SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    neo.lib.logging.info('StoreObject delay: %.02fs', duration)
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))

    def askStoreObject(self, conn, oid, serial,
            compression, checksum, data, data_serial, ttid, unlock):
        # register the transaction
        self.app.tm.register(conn.getUUID(), ttid)
        if data or checksum != ZERO_HASH:
            # TODO: return an appropriate error packet
            assert makeChecksum(data) == checksum
            assert data_serial is None
        else:
            checksum = data = None
        self._askStoreObject(conn, oid, serial, compression, checksum, data,
            data_serial, ttid, unlock, time.time())

    def askTIDsFrom(self, conn, min_tid, max_tid, length, partition_list):
        app = self.app
        getReplicationTIDList = app.dm.getReplicationTIDList
        partitions = app.pt.getPartitions()
        tid_list = []
        extend = tid_list.extend
        for partition in partition_list:
            extend(getReplicationTIDList(min_tid, max_tid, length,
                partitions, partition))
        conn.answer(Packets.AnswerTIDsFrom(tid_list))

    def askTIDs(self, conn, first, last, partition):
        # This method is complicated, because I must return TIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise protocol.ProtocolError('invalid offsets')

        app = self.app
        if partition == protocol.INVALID_PARTITION:
            partition_list = app.pt.getAssignedPartitionList(app.uuid)
        else:
            partition_list = [partition]

        tid_list = app.dm.getTIDList(first, last - first,
                             app.pt.getPartitions(), partition_list)
        conn.answer(Packets.AnswerTIDs(tid_list))

    def askObjectUndoSerial(self, conn, ttid, ltid, undone_tid, oid_list):
        app = self.app
        findUndoTID = app.dm.findUndoTID
        getObjectFromTransaction = app.tm.getObjectFromTransaction
        object_tid_dict = {}
        for oid in oid_list:
            current_serial, undo_serial, is_current = findUndoTID(oid, ttid,
                ltid, undone_tid, getObjectFromTransaction(ttid, oid))
            if current_serial is None:
                p = Errors.OidNotFound(dump(oid))
                break
            object_tid_dict[oid] = (current_serial, undo_serial, is_current)
        else:
            p = Packets.AnswerObjectUndoSerial(object_tid_dict)
        conn.answer(p)

    def askHasLock(self, conn, ttid, oid):
        locking_tid = self.app.tm.getLockingTID(oid)
        neo.lib.logging.info('%r check lock of %r:%r', conn,
                        dump(ttid), dump(oid))
        if locking_tid is None:
            state = LockState.NOT_LOCKED
        elif locking_tid is ttid:
            state = LockState.GRANTED
        else:
            state = LockState.GRANTED_TO_OTHER
        conn.answer(Packets.AnswerHasLock(oid, state))

    def askObjectHistory(self, conn, oid, first, last):
        if first >= last:
            raise protocol.ProtocolError( 'invalid offsets')

        app = self.app
        history_list = app.dm.getObjectHistory(oid, first, last - first)
        if history_list is None:
            p = Errors.OidNotFound(dump(oid))
        else:
            p = Packets.AnswerObjectHistory(oid, history_list)
        conn.answer(p)

    def askCheckCurrentSerial(self, conn, ttid, serial, oid):
        self._askCheckCurrentSerial(conn, ttid, serial, oid, time.time())

    def _askCheckCurrentSerial(self, conn, ttid, serial, oid, request_time):
        if ttid not in self.app.tm:
            # transaction was aborted, cancel this event
            neo.lib.logging.info(
                'Forget serial check of %s:%s by %s delayed by '
                '%s', dump(oid), dump(serial), dump(ttid),
                dump(self.app.tm.getLockingTID(oid)))
            # send an answer as the client side is waiting for it
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))
            return
        try:
            self.app.tm.checkCurrentSerial(ttid, serial, oid)
        except ConflictError, err:
            # resolvable or not
            conn.answer(Packets.AnswerCheckCurrentSerial(1, oid,
                err.getTID()))
        except DelayedError:
            # locked by a previous transaction, retry later
            try:
                self.app.queueEvent(self._askCheckCurrentSerial, conn, (ttid,
                    serial, oid, request_time), key=(oid, ttid))
            except AlreadyPendingError:
                conn.answer(Errors.AlreadyPending(dump(oid)))
        else:
            if SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    neo.lib.logging.info('CheckCurrentSerial delay: %.02fs',
                        duration)
            conn.answer(Packets.AnswerCheckCurrentSerial(0, oid, serial))

