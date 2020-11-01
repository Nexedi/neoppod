#
# Copyright (C) 2006-2017  Nexedi SA
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
from neo.lib.protocol import Packets, ZERO_TID
from . import BaseMasterHandler


class MasterOperationHandler(BaseMasterHandler):
    """ This handler is used for the primary master """

    def startOperation(self, conn, backup):
        # XXX: see comment in protocol
        assert self.app.operational and backup
        self.app.replicator.startOperation(backup)

    def askLockInformation(self, conn, ttid, tid):
        self.app.tm.lock(ttid, tid)
        conn.answer(Packets.AnswerInformationLocked(ttid))

    def notifyUnlockInformation(self, conn, ttid):
        self.app.tm.unlock(ttid)

    def askPack(self, conn, tid):
        app = self.app
        logging.info('Pack started, up to %s...', dump(tid))
        app.dm.pack(tid, app.tm.updateObjectDataForPack)
        logging.info('Pack finished.')
        conn.answer(Packets.AnswerPack(True))

    def answerUnfinishedTransactions(self, conn, *args, **kw):
        self.app.replicator.setUnfinishedTIDList(*args, **kw)

    def replicate(self, conn, tid, upstream_name, source_dict):
        self.app.replicator.backup(tid, {p: a and (a, upstream_name)
                                         for p, a in source_dict.iteritems()})

    def checkPartition(self, conn, *args):
        self.app.checker(*args)
