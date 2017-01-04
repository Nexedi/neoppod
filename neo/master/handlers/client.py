#
# Copyright (C) 2006-2017  Nexedi SA

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

from neo.lib.protocol import NodeStates, Packets, ProtocolError, MAX_TID, Errors
from ..app import monotonic_time
from . import MasterHandler

class ClientServiceHandler(MasterHandler):
    """ Handler dedicated to client during service state """

    def connectionLost(self, conn, new_state):
        # cancel its transactions and forgot the node
        app = self.app
        if app.listening_conn: # if running
            node = app.nm.getByUUID(conn.getUUID())
            assert node is not None
            app.tm.clientLost(node)
            node.setState(NodeStates.DOWN)
            app.broadcastNodesInformation([node])
            app.nm.remove(node)

    def _notifyNodeInformation(self, conn):
        nm = self.app.nm
        node_list = [nm.getByUUID(conn.getUUID()).asTuple()] # for id_timestamp
        node_list.extend(n.asTuple() for n in nm.getMasterList())
        node_list.extend(n.asTuple() for n in nm.getStorageList())
        conn.notify(Packets.NotifyNodeInformation(monotonic_time(), node_list))

    def askBeginTransaction(self, conn, tid):
        """
            A client request a TID, nothing is kept about it until the finish.
        """
        app = self.app
        node = app.nm.getByUUID(conn.getUUID())
        tid = app.tm.begin(node, app.storage_readiness, tid)
        conn.answer(Packets.AnswerBeginTransaction(tid))

    def askNewOIDs(self, conn, num_oids):
        conn.answer(Packets.AnswerNewOIDs(self.app.tm.getNextOIDList(num_oids)))

    def failedVote(self, conn, *args):
        app = self.app
        ok = app.tm.vote(app, *args)
        if ok is None:
            app.tm.queueEvent(self.failedVote, conn, args)
        else:
            conn.answer((Errors.Ack if ok else Errors.IncompleteTransaction)())

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
                node.ask(p, timeout=60)
        else:
            conn.answer(Errors.IncompleteTransaction())

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

    def abortTransaction(self, conn, tid):
        # BUG: The replicator may wait this transaction to be finished.
        self.app.tm.abort(tid, conn.getUUID())


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
        backup_tid = self.app.pt.getBackupTid()
        conn.answer(Packets.AnswerLastTransaction(backup_tid))
