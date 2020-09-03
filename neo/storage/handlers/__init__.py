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

from functools import partial
from neo.lib import logging
from neo.lib.handler import EventHandler
from neo.lib.exception import PrimaryFailure, ProtocolError, StoppedOperation
from neo.lib.protocol import uuid_str, NodeStates, NodeTypes, Packets


class EventHandler(EventHandler):

    def packetReceived(self, *args):
        with self.app.dm.lock:
            self.dispatch(*args)


class BaseHandler(EventHandler):

    def notifyTransactionFinished(self, conn, ttid, max_tid):
        app = self.app
        app.tm.abort(ttid)
        app.replicator.transactionFinished(ttid, max_tid)

    def abortTransaction(self, conn, ttid, _):
        self.notifyTransactionFinished(conn, ttid, None)


class BaseMasterHandler(BaseHandler):

    def connectionLost(self, conn, new_state):
        if self.app.listening_conn: # if running
            self.app.master_node = None
            raise PrimaryFailure('connection lost')

    def stopOperation(self, conn):
        raise StoppedOperation

    def reelectPrimary(self, conn):
        raise PrimaryFailure('re-election occurs')

    def notifyClusterInformation(self, conn, state):
        self.app.changeClusterState(state)

    def notifyNodeInformation(self, conn, timestamp, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        super(BaseMasterHandler, self).notifyNodeInformation(
            conn, timestamp, node_list)
        for node_type, _, uuid, state, _ in node_list:
            if uuid == self.app.uuid:
                # This is me, do what the master tell me
                logging.info("I was told I'm %s", state)
                if state in (NodeStates.UNKNOWN, NodeStates.DOWN):
                    erase = state == NodeStates.UNKNOWN
                    self.app.shutdown(erase=erase)
            elif node_type == NodeTypes.CLIENT and state != NodeStates.RUNNING:
                logging.info('Notified of non-running client, abort (%s)',
                        uuid_str(uuid))
                # See comment in ClientOperationHandler.connectionClosed
                self.app.tm.abortFor(uuid, even_if_voted=True)

    def notifyPackSigned(self, conn, approved, rejected):
        app = self.app
        if not app.disable_pack:
            app.replicator.keepPendingSignedPackOrders(
                *app.dm.signPackOrders(approved, rejected))
        if approved:
            pack_id = max(approved)
            if app.last_pack_id < pack_id:
                app.last_pack_id = pack_id
                if app.operational:
                    if app.disable_pack:
                        app.notifyPackCompleted()
                    else:
                        app.maybePack()

    def notifyPartitionChanges(self, conn, ptid, num_replicas, cell_list):
        """This is very similar to Send Partition Table, except that
       the information is only about changes from the previous."""
        app = self.app
        if ptid != 1 + app.pt.getID():
            raise ProtocolError('wrong partition table id')
        if app.operational:
            getOutdatedOffsetList = partial(
                app.pt.getOutdatedOffsetListFor, app.uuid)
            were_outdated = set(getOutdatedOffsetList())
        app.pt.update(ptid, num_replicas, cell_list, app.nm)
        app.dm.changePartitionTable(app, ptid, num_replicas, cell_list)
        if app.operational:
            app.replicator.notifyPartitionChanges(cell_list)
            # The U -> !U case is already handled by dm.changePartitionTable.
            # XXX: What about CORRUPTED cells?
            were_outdated.difference_update(getOutdatedOffsetList())
            if were_outdated: # O -> !O
                # After a cell is discarded,
                # the smallest pt.pack may be greater.
                app.notifyPackCompleted()
                # And we may start processing the next pack order.
                app.maybePack()
        app.dm.commit()

    def askFinalTID(self, conn, ttid):
        conn.answer(Packets.AnswerFinalTID(self.app.dm.getFinalTID(ttid)))

    def askPackOrders(self, conn, min_completed_id):
        conn.answer(Packets.AnswerPackOrders(
            self.app.dm.getPackOrders(min_completed_id)))

    def notifyRepair(self, conn, *args):
        app = self.app
        app.dm.repair(app, *args)
