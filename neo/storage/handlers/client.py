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
from neo.protocol import Packets, LockState
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

    def askStoreTransaction(self, conn, tid, user, desc,
                                  ext, oid_list):
        self.app.tm.register(conn.getUUID(), tid)
        self.app.tm.storeTransaction(tid, oid_list, user, desc, ext,
            False)
        conn.answer(Packets.AnswerStoreTransaction(tid))

    def _askStoreObject(self, conn, oid, serial, compression, checksum, data,
            tid, request_time):
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
                    checksum, data, None)
        except ConflictError, err:
            # resolvable or not
            tid_or_serial = err.getTID()
            conn.answer(Packets.AnswerStoreObject(1, oid, tid_or_serial))
        except DelayedError:
            # locked by a previous transaction, retry later
            self.app.queueEvent(self._askStoreObject, conn, oid, serial,
                    compression, checksum, data, tid, request_time)
        else:
            if SLOW_STORE is not None:
                duration = time.time() - request_time
                if duration > SLOW_STORE:
                    logging.info('StoreObject delay: %.02fs', duration)
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, tid):
        # register the transaction
        self.app.tm.register(conn.getUUID(), tid)
        self._askStoreObject(conn, oid, serial, compression, checksum, data,
            tid, time.time())

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

    def askUndoTransaction(self, conn, tid, undone_tid):
        app = self.app
        tm = app.tm
        storeObject = tm.storeObject
        uuid = conn.getUUID()
        oid_list = []
        error_oid_list = []
        conflict_oid_list = []

        undo_tid_dict = app.dm.getTransactionUndoData(tid, undone_tid,
            tm.getObjectFromTransaction)
        for oid, (current_serial, undone_value_serial) in \
                undo_tid_dict.iteritems():
            if undone_value_serial == -1:
                # Some data were modified by a later transaction
                # This must be propagated to client, who will
                # attempt a conflict resolution, and store resolved
                # data.
                to_append_list = error_oid_list
            else:
                try:
                    self.app.tm.register(uuid, tid)
                    storeObject(tid, current_serial, oid, None,
                        None, None, undone_value_serial)
                except ConflictError:
                    to_append_list = conflict_oid_list
                except DelayedError:
                    app.queueEvent(self.askUndoTransaction, conn, tid,
                        undone_tid)
                    return
                else:
                    to_append_list = oid_list
            to_append_list.append(oid)
        conn.answer(Packets.AnswerUndoTransaction(oid_list, error_oid_list,
            conflict_oid_list))

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

