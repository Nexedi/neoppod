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

    def askLockInformation(self, conn, ttid, tid, pack):
        self.app.tm.lock(ttid, tid, pack)
        conn.answer(Packets.AnswerInformationLocked(ttid))

    def notifyUnlockInformation(self, conn, ttid):
        self.app.tm.unlock(ttid)

    def answerPackOrders(self, conn, pack_list, pack_id):
        if pack_list:
            self.app.maybePack(pack_list[0], pack_id)

    def answerUnfinishedTransactions(self, conn, *args, **kw):
        self.app.replicator.setUnfinishedTIDList(*args, **kw)

    def replicate(self, conn, tid, upstream_name, source_dict):
        self.app.replicator.backup(tid, {p: a and (a, upstream_name)
                                         for p, a in six.iteritems(source_dict)})

    def checkPartition(self, conn, *args):
        self.app.checker(*args)
