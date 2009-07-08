#
# Copyright (C) 2006-2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import logging

from neo import protocol
from neo.storage.handlers.handler import BaseClientAndStorageOperationHandler


class StorageOperationHandler(BaseClientAndStorageOperationHandler):

    def connectionCompleted(self, conn):
        BaseClientAndStorageOperationHandler.connectionCompleted(self, conn)

    def handleAskLastIDs(self, conn, packet):
        app = self.app
        oid = app.dm.getLastOID() or protocol.INVALID_OID
        tid = app.dm.getLastTID() or protocol.INVALID_TID
        p = protocol.answerLastIDs(oid, tid, app.ptid)
        conn.answer(p, packet)

    def handleAskOIDs(self, conn, packet, first, last, partition):
        # This method is complicated, because I must return OIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise protocol.ProtocolError('invalid offsets')

        app = self.app

        if partition == protocol.INVALID_PARTITION:
            # Collect all usable partitions for me.
            getCellList = app.pt.getCellList
            partition_list = []
            for offset in xrange(app.num_partitions):
                for cell in getCellList(offset, readable=True):
                    if cell.getUUID() == app.uuid:
                        partition_list.append(offset)
                        break
        else:
            partition_list = [partition]
        oid_list = app.dm.getOIDList(first, last - first,
                                     app.num_partitions, partition_list)
        conn.answer(protocol.answerOIDs(oid_list), packet)

