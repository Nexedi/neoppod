#
# Copyright (C) 2006-2016  Nexedi SA

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

from neo.lib import logging
from neo.lib.protocol import CellStates, ClusterStates, Packets, ProtocolError
from neo.lib.exception import StoppedOperation
from neo.lib.pt import PartitionTableException
from . import BaseServiceHandler


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn, new):
        app = self.app
        uuid = conn.getUUID()
        app.setStorageNotReady(uuid)
        if new:
            super(StorageServiceHandler, self).connectionCompleted(conn, new)
        if app.nm.getByUUID(uuid).isRunning(): # node may be PENDING
            conn.notify(Packets.StartOperation(app.backup_tid))

    def connectionLost(self, conn, new_state):
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        super(StorageServiceHandler, self).connectionLost(conn, new_state)
        app.tm.storageLost(conn.getUUID())
        if (app.getClusterState() == ClusterStates.BACKINGUP
            # Also check if we're exiting, because backup_app is not usable
            # in this case. Maybe cluster state should be set to something
            # else, like STOPPING, during cleanup (__del__/close).
            and app.listening_conn):
            app.backup_app.nodeLost(node)
        if app.packing is not None:
            self.answerPack(conn, False)

    def askUnfinishedTransactions(self, conn):
        app = self.app
        if app.backup_tid:
            last_tid = app.pt.getBackupTid(min)
            pending_list = ()
        else:
            last_tid = app.tm.getLastTID()
            pending_list = app.tm.registerForNotification(conn.getUUID())
        p = Packets.AnswerUnfinishedTransactions(last_tid, pending_list)
        conn.answer(p)

    def answerInformationLocked(self, conn, ttid):
        self.app.tm.lock(ttid, conn.getUUID())

    def notifyPartitionCorrupted(self, conn, partition, cell_list):
        change_list = []
        for cell in self.app.pt.getCellList(partition):
            if cell.getUUID() in cell_list:
                cell.setState(CellStates.CORRUPTED)
                change_list.append((partition, cell.getUUID(),
                                    CellStates.CORRUPTED))
        self.app.broadcastPartitionChanges(change_list)
        if not self.app.pt.operational():
            raise StoppedOperation

    def notifyReplicationDone(self, conn, offset, tid):
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        if app.backup_tid:
            cell_list = app.backup_app.notifyReplicationDone(node, offset, tid)
            if not cell_list:
                return
        else:
            try:
                cell_list = self.app.pt.setUpToDate(node, offset)
                if not cell_list:
                    raise ProtocolError('Non-outdated partition')
            except PartitionTableException, e:
                raise ProtocolError(str(e))
        logging.debug("%s is up for offset %s", node, offset)
        self.app.broadcastPartitionChanges(cell_list)

    def answerPack(self, conn, status):
        app = self.app
        if app.packing is not None:
            client, msg_id, uid_set = app.packing
            uid_set.remove(conn.getUUID())
            if not uid_set:
                app.packing = None
                if not client.isClosed():
                    client.answer(Packets.AnswerPack(True), msg_id=msg_id)

