#
# Copyright (C) 2006-2019  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from neo import *
from . import BaseMasterHandler
from neo.lib.exception import ProtocolError
from neo.lib.protocol import Packets

class InitializationHandler(BaseMasterHandler):

    def sendPartitionTable(self, conn, ptid, num_replicas, row_list):
        app = self.app
        pt = app.pt
        pt.load(ptid, num_replicas, row_list, app.nm)
        if not pt.filled():
            raise ProtocolError('Partial partition table received')
        cell_list = [(offset, cell.getUUID(), cell.getState())
            for offset in range(pt.getPartitions())
            for cell in pt.getCellList(offset)]
        dm = app.dm
        dm.changePartitionTable(app, ptid, num_replicas, cell_list, reset=True)
        dm.commit()

    def truncate(self, conn, tid):
        dm = self.app.dm
        dm._setBackupTID(None)
        dm._setTruncateTID(tid)
        dm.commit()

    def askRecovery(self, conn):
        app = self.app
        conn.answer(Packets.AnswerRecovery(
            app.pt.getID(),
            app.dm.getBackupTID(),
            app.dm.getTruncateTID()))

    def askLastIDs(self, conn):
        app = self.app
        dm = app.dm
        dm.truncate()
        if not app.disable_pack:
            packed = dm.getPackedIDs()
            if packed:
                self.app.completed_pack_id = pack_id = min(six.itervalues(packed))
                conn.send(Packets.NotifyPackCompleted(pack_id))
        last_tid, last_oid = dm.getLastIDs() # PY3
        conn.answer(Packets.AnswerLastIDs(last_tid, last_oid, dm.getFirstTID()))

    def askPartitionTable(self, conn):
        pt = self.app.pt
        conn.answer(Packets.AnswerPartitionTable(
            pt.getID(), pt.getReplicas(), pt.getRowList()))

    def askLockedTransactions(self, conn):
        conn.answer(Packets.AnswerLockedTransactions(
            self.app.dm.getUnfinishedTIDDict()))

    def validateTransaction(self, conn, ttid, tid):
        dm = self.app.dm
        dm.lockTransaction(tid, ttid, True)
        dm.unlockTransaction(tid, ttid, True, True, True)
        dm.commit()

    def startOperation(self, conn, backup):
        # XXX: see comment in protocol
        self.app.operational = True
        self.app.replicator.startOperation(backup)
