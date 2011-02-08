#
# Copyright (C) 2006-2010  Nexedi SA

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

import neo.lib

from neo.lib.protocol import ProtocolError
from neo.lib.protocol import Packets
from neo.master.handlers import BaseServiceHandler
from neo.lib.exception import OperationFailure
from neo.lib.util import dump
from neo.lib.connector import ConnectorConnectionClosedException
from neo.lib.pt import PartitionTableException


class StorageServiceHandler(BaseServiceHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn):
        # TODO: unit test
        app = self.app
        uuid = conn.getUUID()
        node = app.nm.getByUUID(uuid)
        app.setStorageNotReady(uuid)
        # XXX: what other values could happen ?
        if node.isRunning():
            conn.notify(Packets.StartOperation())

    def nodeLost(self, conn, node):
        neo.lib.logging.info('storage node lost')
        assert not node.isRunning(), node.getState()

        if not self.app.pt.operational():
            raise OperationFailure, 'cannot continue operation'
        # this is intentionaly placed after the raise because the last cell in a
        # partition must not oudated to allows a cluster restart.
        self.app.outdateAndBroadcastPartition()
        self.app.tm.forget(conn.getUUID())
        if self.app.packing is not None:
            self.answerPack(conn, False)

    def askLastIDs(self, conn):
        app = self.app
        loid = app.tm.getLastOID()
        ltid = app.tm.getLastTID()
        conn.answer(Packets.AnswerLastIDs(loid, ltid, app.pt.getID()))

    def askUnfinishedTransactions(self, conn):
        tm = self.app.tm
        pending_list = tm.registerForNotification(conn.getUUID())
        last_tid = tm.getLastTID()
        p = Packets.AnswerUnfinishedTransactions(last_tid, pending_list)
        conn.answer(p)

    def answerInformationLocked(self, conn, ttid):
        tm = self.app.tm
        if ttid not in tm:
            raise ProtocolError('Unknown transaction')
        # transaction locked on this storage node
        self.app.tm.lock(ttid, conn.getUUID())

    def notifyReplicationDone(self, conn, offset):
        node = self.app.nm.getByUUID(conn.getUUID())
        neo.lib.logging.debug("%s is up for offset %s" % (node, offset))
        try:
            cell_list = self.app.pt.setUpToDate(node, offset)
        except PartitionTableException, e:
            raise ProtocolError(str(e))
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

