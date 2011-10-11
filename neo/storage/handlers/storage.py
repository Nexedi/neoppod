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

from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.lib.protocol import Packets

class StorageOperationHandler(BaseClientAndStorageOperationHandler):

    def _askObject(self, oid, serial, tid):
        result = self.app.dm.getObject(oid, serial, tid)
        if result and result[5]:
            return result[:2] + (None, None, None) + result[4:]
        return result

    def askLastIDs(self, conn):
        app = self.app
        oid = app.dm.getLastOID()
        tid = app.dm.getLastTID()
        conn.answer(Packets.AnswerLastIDs(oid, tid, app.pt.getID()))

    def askTIDsFrom(self, conn, min_tid, max_tid, length, partition_list):
        assert len(partition_list) == 1, partition_list
        partition = partition_list[0]
        app = self.app
        tid_list = app.dm.getReplicationTIDList(min_tid, max_tid, length,
            app.pt.getPartitions(), partition)
        conn.answer(Packets.AnswerTIDsFrom(tid_list))

    def askObjectHistoryFrom(self, conn, min_oid, min_serial, max_serial,
            length, partition):
        app = self.app
        object_dict = app.dm.getObjectHistoryFrom(min_oid, min_serial, max_serial,
            length, app.pt.getPartitions(), partition)
        conn.answer(Packets.AnswerObjectHistoryFrom(object_dict))

    def askCheckTIDRange(self, conn, min_tid, max_tid, length, partition):
        app = self.app
        count, tid_checksum, max_tid = app.dm.checkTIDRange(min_tid, max_tid,
            length, app.pt.getPartitions(), partition)
        conn.answer(Packets.AnswerCheckTIDRange(min_tid, length,
            count, tid_checksum, max_tid))

    def askCheckSerialRange(self, conn, min_oid, min_serial, max_tid, length,
            partition):
        app = self.app
        count, oid_checksum, max_oid, serial_checksum, max_serial = \
            app.dm.checkSerialRange(min_oid, min_serial, max_tid, length,
                app.pt.getPartitions(), partition)
        conn.answer(Packets.AnswerCheckSerialRange(min_oid, min_serial, length,
            count, oid_checksum, max_oid, serial_checksum, max_serial))

