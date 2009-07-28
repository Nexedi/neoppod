#
# Copyright (C) 2006-2009  Nexedi SA
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from neo import logging

from neo import protocol
from neo.handler import EventHandler

class MasterHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def _nodeLost(self, conn, node):
        # override this method in sub-handlers to do specific actions when a
        # node is lost
        pass

    def _dropIt(self, conn, node, new_state):
        if node is None or node.getState() == new_state:
            return
        if new_state != protocol.BROKEN_STATE and node.getState() == protocol.PENDING_STATE:
            # was in pending state, so drop it from the node manager to forget
            # it and do not set in running state when it comes back
            logging.info('drop a pending node from the node manager')
            self.app.nm.remove(node)
        node.setState(new_state)
        # clean node related data in specialized handlers
        self.app.broadcastNodeInformation(node)
        self._nodeLost(conn, node)

    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self._dropIt(conn, node, protocol.TEMPORARILY_DOWN_STATE)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self._dropIt(conn, node, protocol.TEMPORARILY_DOWN_STATE)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        self._dropIt(conn, node, protocol.BROKEN_STATE)

    def handleProtocolError(self, conn, packet, message):
        logging.error('Protocol error %s %s' % (message, conn.getAddress()))

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        logging.error('ignoring Notify Node Information in %s', self.__class__.__name__)

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        logging.error('ignoring Answer Last IDs in %s' % self.__class__.__name__)

    def handleAnswerPartitionTable(self, conn, packet, ptid, cell_list):
        logging.error('ignoring Answer Partition Table in %s' % self.__class__.__name__)

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        logging.error('ignoring Answer Unfinished Transactions in %s' % self.__class__.__name__)

    def handleAnswerTransactionInformation(self, conn, packet, tid, user, desc, ext, oid_list):
        logging.error('ignoring Answer Transactin Information in %s' % self.__class__.__name__)

    def handleTidNotFound(self, conn, packet, message):
        logging.error('ignoring Answer OIDs By TID in %s' % self.__class__.__name__)

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        logging.error('ignoring Answer Object Present in %s' % self.__class__.__name__)

    def handleOidNotFound(self, conn, packet, message):
        logging.error('ignoring OID Not Found in %s' % self.__class__.__name__)

    def handleAskBeginTransaction(self, conn, packet, tid):
        logging.error('ignoring Ask New TID in %s' % self.__class__.__name__)

    def handleAskNewOIDs(self, conn, packet, num_oids):
        logging.error('ignoring Ask New OIDs in %s' % self.__class__.__name__)

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        logging.error('ignoring Finish Transaction in %s' % self.__class__.__name__)

    def handleNotifyInformationLocked(self, conn, packet, tid):
        logging.error('ignoring Notify Information Locked in %s' % self.__class__.__name__)

    def handleAbortTransaction(self, conn, packet, tid):
        logging.error('ignoring Abort Transaction in %s' % self.__class__.__name__)

    def handleAskLastIDs(self, conn, packet):
        logging.error('ignoring Ask Last IDs in %s' % self.__class__.__name__)

    def handleAskUnfinishedTransactions(self, conn, packet):
        logging.error('ignoring ask unfinished transactions in %s' % self.__class__.__name__)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        logging.error('ignoring notify partition changes in %s' % self.__class__.__name__)

    def handleNotifyClusterInformation(self, conn, packet, state):
        logging.error('ignoring notify cluster information in %s' % self.__class__.__name__)

    def handleAskPrimaryMaster(self, conn, packet):
        if conn.getConnector() is None:
            # Connection can be closed by peer after he sent AskPrimaryMaster
            # if he finds the primary master before we answer him.
            # The connection gets closed before this message gets processed
            # because this message might have been queued, but connection
            # interruption takes effect as soon as received.
            return
        app = self.app
        if app.primary:
            primary_uuid = app.uuid
        elif app.primary_master_node is not None:
            primary_uuid = app.primary_master_node.getUUID()
        else:
            primary_uuid = None

        known_master_list = [(app.server, app.uuid, )]
        for n in app.nm.getMasterNodeList():
            if n.getState() == protocol.BROKEN_STATE:
                continue
            known_master_list.append((n.getServer(), n.getUUID(), ))
        conn.answer(protocol.answerPrimaryMaster(primary_uuid,
                                                 known_master_list), packet)

    def handleAskClusterState(self, conn, packet):
        assert conn.getUUID() is not None
        state = self.app.getClusterState()
        conn.answer(protocol.answerClusterState(state), packet)

    def handleAskNodeInformation(self, conn, packet):
        self.app.sendNodesInformations(conn)
        conn.answer(protocol.answerNodeInformation([]), packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        assert len(offset_list) == 0
        app = self.app
        app.sendPartitionTable(conn)
        conn.answer(protocol.answerPartitionTable(app.pt.getID(), []), packet)


class BaseServiceHandler(MasterHandler):
    """This class deals with events for a service phase."""

    def handleAskLastIDs(self, conn, packet):
        app = self.app
        conn.answer(protocol.answerLastIDs(app.loid, app.ltid, app.pt.getID()), packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        app = self.app
        p = protocol.answerUnfinishedTransactions(app.finishing_transaction_dict.keys())
        conn.answer(p, packet)

