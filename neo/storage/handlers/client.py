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

from neo import logging
from neo import protocol
from neo.util import dump
from neo.protocol import Packets, LockState, Errors
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.storage.transactions import ConflictError, DelayedError
import time

# Log stores taking (incl. lock delays) more than this many seconds.
# Set to None to disable.
SLOW_STORE = 2

class ClientOperationHandler(BaseClientAndStorageOperationHandler):

    def _askObject(self, oid, serial, tid):
        return self.app.dm.getObject(oid, serial, tid)

    def connectionLost(self, conn, new_state):
        uuid = conn.getUUID()
        node = self.app.nm.getByUUID(uuid)
        assert node is not None, conn
        self.app.nm.remove(node)

    def abortTransaction(self, conn, tid):
        self.app.tm.abort(tid)

    def askStoreTransaction(self, conn, tid, user, desc, ext, oid_list):
        self.app.tm.register(conn.getUUID(), tid)
        self.app.tm.storeTransaction(tid, oid_list, user, desc, ext, False)
        conn.answer(Packets.AnswerStoreTransaction(tid))

    def _askStoreObject(self, conn, oid, serial, compression, checksum, data,
            data_serial, tid, request_time):
        if tid not in self.app.tm:
            # transaction was aborted, cancel this event
            logging.info('Forget store of %s:%s by %s delayed by %s',
                    dump(oid), dump(serial), dump(tid),
                    dump(self.app.tm.getLockingTID(oid)))
            # send an answer as the client side is waiting for it
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))
            return
        try:
            self.app.tm.storeObject(tid, serial, oid, compression,
                    checksum, data, data_serial)
        except ConflictError, err:
            # resolvable or not
            tid_or_serial = err.getTID()
            conn.answer(Packets.AnswerStoreObject(1, oid, tid_or_serial))
        except DelayedError:
            # locked by a previous transaction, retry later
            self.app.queueEvent(self._askStoreObject, conn, oid, serial,
                compression, checksum, data, data_serial, tid, request_time)
        else:
            if SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('StoreObject delay: %.02fs', duration)
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, data_serial, tid):
        # register the transaction
        self.app.tm.register(conn.getUUID(), tid)
        if data_serial is not None:
            assert data == '', repr(data)
            # Change data to None here, to do it only once, even if store gets
            # delayed.
            data = None
        self._askStoreObject(conn, oid, serial, compression, checksum, data,
            data_serial, tid, time.time())

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

    def askObjectUndoSerial(self, conn, tid, undone_tid, oid_list):
        app = self.app
        findUndoTID = app.dm.findUndoTID
        getObjectFromTransaction = app.tm.getObjectFromTransaction
        object_tid_dict = {}
        for oid in oid_list:
            current_serial, undo_serial, is_current = findUndoTID(oid, tid,
                undone_tid, getObjectFromTransaction(tid, oid))
            if current_serial is None:
                p = Errors.OidNotFound(dump(oid))
                break
            object_tid_dict[oid] = (current_serial, undo_serial, is_current)
        else:
            p = Packets.AnswerObjectUndoSerial(object_tid_dict)
        conn.answer(p)

    def askHasLock(self, conn, tid, oid):
        locking_tid = self.app.tm.getLockingTID(oid)
        logging.info('%r check lock of %r:%r', conn, dump(tid), dump(oid))
        if locking_tid is None:
            state = LockState.NOT_LOCKED
        elif locking_tid is tid:
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
            history_list = []
        conn.answer(Packets.AnswerObjectHistory(oid, history_list))

