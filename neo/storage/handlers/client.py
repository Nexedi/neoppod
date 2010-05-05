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

from neo import protocol
from neo.protocol import Packets
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.storage.transactions import ConflictError, DelayedError

class ClientOperationHandler(BaseClientAndStorageOperationHandler):

    def _askObject(self, oid, serial, tid):
        return self.app.dm.getObject(oid, serial, tid)

    def connectionLost(self, conn, new_state):
        self.app.tm.abortFor(conn.getUUID())

    def abortTransaction(self, conn, tid):
        self.app.tm.abort(tid)

    def askStoreTransaction(self, conn, tid, user, desc,
                                  ext, oid_list):
        uuid = conn.getUUID()
        self.app.tm.storeTransaction(uuid, tid, oid_list, user, desc, ext,
            False)
        conn.answer(Packets.AnswerStoreTransaction(tid))

    def askStoreObject(self, conn, oid, serial,
                             compression, checksum, data, tid):
        uuid = conn.getUUID()
        try:
            self.app.tm.storeObject(uuid, tid, serial, oid, compression,
                    checksum, data, None)
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))
        except ConflictError, err:
            # resolvable or not
            tid_or_serial = err.getTID()
            conn.answer(Packets.AnswerStoreObject(1, oid, tid_or_serial))
        except DelayedError:
            # locked by a previous transaction, retry later
            self.app.queueEvent(self.askStoreObject, conn, oid, serial,
                    compression, checksum, data, tid)

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
                    storeObject(uuid, tid, current_serial, oid, None,
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

