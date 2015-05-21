#
# Copyright (C) 2006-2015  Nexedi SA
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

from neo.lib import logging
from neo.lib.util import dump
from neo.lib.protocol import Packets, ProtocolError
from . import BaseMasterHandler


class MasterOperationHandler(BaseMasterHandler):
    """ This handler is used for the primary master """

    def notifyTransactionFinished(self, conn, *args, **kw):
        self.app.replicator.transactionFinished(*args, **kw)

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
       the information is only about changes from the previous."""
        app = self.app
        if ptid <= app.pt.getID():
            # Ignore this packet.
            logging.debug('ignoring older partition changes')
            return

        # update partition table in memory and the database
        app.pt.update(ptid, cell_list, app.nm)
        app.dm.changePartitionTable(ptid, cell_list)

        # Check changes for replications
        app.replicator.notifyPartitionChanges(cell_list)

    def askLockInformation(self, conn, ttid, tid, oid_list):
        if not ttid in self.app.tm:
            raise ProtocolError('Unknown transaction')
        self.app.tm.lock(ttid, tid, oid_list)
        if not conn.isClosed():
            conn.answer(Packets.AnswerInformationLocked(ttid))

    def notifyUnlockInformation(self, conn, ttid):
        if not ttid in self.app.tm:
            raise ProtocolError('Unknown transaction')
        # TODO: send an answer
        self.app.tm.unlock(ttid)

    def askPack(self, conn, tid):
        app = self.app
        logging.info('Pack started, up to %s...', dump(tid))
        app.dm.pack(tid, app.tm.updateObjectDataForPack)
        logging.info('Pack finished.')
        if not conn.isClosed():
            conn.answer(Packets.AnswerPack(True))

    def replicate(self, conn, tid, upstream_name, source_dict):
        self.app.replicator.backup(tid, {p: a and (a, upstream_name)
                                         for p, a in source_dict.iteritems()})

    def truncate(self, conn, tid):
        self.app.replicator.cancel()
        self.app.dm.truncate(tid)
        conn.close()

    def checkPartition(self, conn, *args):
        self.app.checker(*args)
