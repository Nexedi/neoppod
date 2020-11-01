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

from neo.lib.handler import DelayEvent
from neo.lib.protocol import Packets, ProtocolError, MAX_TID, Errors
from ..app import monotonic_time
from . import MasterHandler

class ClientServiceHandler(MasterHandler):
    """ Handler dedicated to client during service state """

    def _connectionLost(self, conn):
        # cancel its transactions and forgot the node
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        assert node is not None, conn
        for x in app.tm.clientLost(node):
            app.notifyTransactionAborted(*x)
        node.setUnknown()
        app.broadcastNodesInformation([node])

    def askBeginTransaction(self, conn, tid):
        """
            A client request a TID, nothing is kept about it until the finish.
        """
        app = self.app
        # Delay new transaction as long as we are waiting for NotifyReady
        # answers, otherwise we can't know if the client is expected to commit
        # the transaction in full to all these storage nodes.
        if app.storage_starting_set:
            raise DelayEvent
        node = app.nm.getByUUID(conn.getUUID())
        tid = app.tm.begin(node, app.storage_readiness, tid)
        conn.answer(Packets.AnswerBeginTransaction(tid))

    def askNewOIDs(self, conn, num_oids):
        conn.answer(Packets.AnswerNewOIDs(self.app.tm.getNextOIDList(num_oids)))

    def getEventQueue(self):
        # for askBeginTransaction & failedVote
        return self.app.tm

    def failedVote(self, conn, *args):
        app = self.app
        conn.answer((Errors.Ack if app.tm.vote(app, *args) else
                     Errors.IncompleteTransaction)())

    def askFinishTransaction(self, conn, ttid, oid_list, checked_list):
        app = self.app
        tid, node_list = app.tm.prepare(
            app,
            ttid,
            oid_list,
            checked_list,
            conn.getPeerId(),
        )
        if tid:
            p = Packets.AskLockInformation(ttid, tid)
            for node in node_list:
                node.ask(p)

            # NOTE continues in onTransactionCommitted

        else:
            conn.answer(Errors.IncompleteTransaction())
            # It's simpler to abort automatically rather than asking the client
            # to send a notification on tpc_abort, since it would have keep the
            # transaction longer in list of transactions.
            # This should happen so rarely that we don't try to minimize the
            # number of abort notifications by looking the modified partitions.
            self.abortTransaction(conn, ttid, app.getStorageReadySet())

    def askFinalTID(self, conn, ttid):
        tm = self.app.tm
        if tm.getLastTID() < ttid:
            # Invalid ttid, or aborted transaction.
            tid = None
        elif ttid in tm:
            # Transaction is being finished.
            # We'll answer when it is unlocked.
            tm[ttid].registerForNotification(conn.getUUID())
            return
        else:
            # Transaction committed ? Tell client to ask storages.
            tid = MAX_TID
        conn.answer(Packets.AnswerFinalTID(tid))

    def askPack(self, conn, tid):
        app = self.app
        if app.packing is None:
            storage_list = app.nm.getStorageList(only_identified=True)
            app.packing = (conn, conn.getPeerId(),
                {x.getUUID() for x in storage_list})
            p = Packets.AskPack(tid)
            for storage in storage_list:
                storage.getConnection().ask(p)
        else:
            conn.answer(Packets.AnswerPack(False))

    def abortTransaction(self, conn, tid, uuid_list):
        # Consider a failure when the connection between the storage and the
        # client breaks while the answer to the first write is sent back.
        # In other words, the client can not know the exact set of nodes that
        # know this transaction, and it sends us all nodes it considered for
        # writing.
        # We must also add those that are waiting for this transaction to be
        # finished (returned by tm.abort), because they may have join the
        # cluster after that the client started to abort.
        app = self.app
        involved = app.tm.abort(tid, conn.getUUID())
        involved.update(uuid_list)
        app.notifyTransactionAborted(tid, involved)


# like ClientServiceHandler but read-only & only for tid <= backup_tid
class ClientReadOnlyServiceHandler(ClientServiceHandler):

    def _readOnly(self, conn, *args, **kw):
        conn.answer(Errors.ReadOnlyAccess(
            'read-only access because cluster is in backuping mode'))

    askBeginTransaction     = _readOnly
    askNewOIDs              = _readOnly
    askFinishTransaction    = _readOnly
    askFinalTID             = _readOnly
    askPack                 = _readOnly
    abortTransaction        = _readOnly

    # XXX LastIDs is not used by client at all, and it requires work to determine
    # last_oid up to backup_tid, so just make it non-functional for client.
    askLastIDs              = _readOnly

    # like in MasterHandler but returns backup_tid instead of last_tid
    def askLastTransaction(self, conn):
        assert self.app.backup_tid is not None   # we are in BACKUPING mode
        backup_tid = self.app.pt.getBackupTid(min)
        conn.answer(Packets.AnswerLastTransaction(backup_tid))
