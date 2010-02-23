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

from neo.protocol import Packets
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.storage.transactions import ConflictError, DelayedError

class ClientOperationHandler(BaseClientAndStorageOperationHandler):

    def timeoutExpired(self, conn):
        self.app.tm.abortFor(conn.getUUID())
        BaseClientAndStorageOperationHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        self.app.tm.abortFor(conn.getUUID())
        BaseClientAndStorageOperationHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        self.app.tm.abortFor(conn.getUUID())
        BaseClientAndStorageOperationHandler.peerBroken(self, conn)

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
                    checksum, data)
            conn.answer(Packets.AnswerStoreObject(0, oid, serial))
        except ConflictError, err:
            # resolvable or not
            tid_or_serial = err.getTID()
            conn.answer(Packets.AnswerStoreObject(1, oid, tid_or_serial))
        except DelayedError:
            # locked by a previous transaction, retry later
            self.app.queueEvent(self.askStoreObject, conn, oid, serial,
                    compression, checksum, data, tid)

