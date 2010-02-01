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
from neo.storage.handlers import BaseClientAndStorageOperationHandler
from neo.protocol import Packets

class StorageOperationHandler(BaseClientAndStorageOperationHandler):

    def askLastIDs(self, conn):
        app = self.app
        oid = app.dm.getLastOID()
        tid = app.dm.getLastTID()
        conn.answer(Packets.AnswerLastIDs(oid, tid, app.pt.getID()))

    def askOIDs(self, conn, first, last, partition):
        # This method is complicated, because I must return OIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise protocol.ProtocolError('invalid offsets')

        app = self.app

        if partition == protocol.INVALID_PARTITION:
            partition_list = app.pt.getAssignedPartitionList(app.uuid)
        else:
            partition_list = [partition]

        oid_list = app.dm.getOIDList(first, last - first,
                                     app.pt.getPartitions(), partition_list)
        conn.answer(Packets.AnswerOIDs(oid_list))

