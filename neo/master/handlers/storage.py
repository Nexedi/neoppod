#
# Copyright (C) 2006-2019  Nexedi SA

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
from neo.lib.exception import ProtocolError, StoppedOperation
from neo.lib.protocol import CellStates, ClusterStates, Packets, uuid_str
from neo.lib.pt import PartitionTableException
from neo.lib.util import dump
from . import BaseServiceHandler


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def handlerSwitched(self, conn, new):
        app = self.app
        if new:
            super(StorageServiceHandler, self).handlerSwitched(conn, new)
        node = app.nm.getByUUID(conn.getUUID())
        if node.isRunning(): # node may be PENDING
            app.startStorage(node)

    def notifyReady(self, conn):
        self.app.setStorageReady(conn.getUUID())

    def connectionLost(self, conn, new_state):
        app = self.app
        uuid = conn.getUUID()
        node = app.nm.getByUUID(uuid)
        super(StorageServiceHandler, self).connectionLost(conn, new_state)
        app.setStorageNotReady(uuid)
        app.tm.storageLost(uuid)
        if (app.getClusterState() == ClusterStates.BACKINGUP
            # Also check if we're exiting, because backup_app is not usable
            # in this case. Maybe cluster state should be set to something
            # else, like STOPPING, during cleanup (__del__/close).
            and app.listening_conn):
            app.backup_app.nodeLost(node)
        if app.packing is not None:
            self.answerPack(conn, False)

    def askUnfinishedTransactions(self, conn, offset_list):
        app = self.app
        if app.backup_tid:
            last_tid = app.pt.getBackupTid(min)
            pending_list = ()
        else:
            # This can't be app.tm.getLastTID() for imported transactions,
            # because outdated cells must at least wait that they're locked
            # at source side. For normal transactions, it would not matter.
            last_tid = app.getLastTransaction()
            pending_list = app.tm.registerForNotification(conn.getUUID())
        p = Packets.AnswerUnfinishedTransactions(last_tid, pending_list)
        conn.answer(p)
        app.pt.updatable(conn.getUUID(), offset_list)

    def answerInformationLocked(self, conn, ttid):
        app = self.app
        # XXX: see testAnswerInformationLockedDuringRecovery
        if ClusterStates.RUNNING != app.cluster_state != ClusterStates.STOPPING:
            assert app.cluster_state == ClusterStates.RECOVERING
        else:
            app.tm.lock(ttid, conn.getUUID())

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
        uuid = conn.getUUID()
        node = app.nm.getByUUID(uuid)
        if app.backup_tid:
            cell_list = app.backup_app.notifyReplicationDone(node, offset, tid)
            if not cell_list:
                return
        else:
            try:
                cell_list = self.app.pt.setUpToDate(node, offset)
            except PartitionTableException, e:
                raise ProtocolError(str(e))
            if not cell_list:
                logging.info("ignored late notification that"
                    " %s has replicated partition %s up to %s",
                    uuid_str(uuid), offset, dump(tid))
                return
        logging.debug("%s is up for partition %s (tid=%s)",
                      uuid_str(uuid), offset, dump(tid))
        self.app.broadcastPartitionChanges(cell_list)

    def answerPack(self, conn, status):
        app = self.app
        if app.packing is not None:
            client, msg_id, uid_set = app.packing
            uid_set.remove(conn.getUUID())
            if not uid_set:
                app.packing = None
                if not client.isClosed():
                    client.send(Packets.AnswerPack(True), msg_id)

