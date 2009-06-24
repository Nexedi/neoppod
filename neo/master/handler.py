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

import logging

from neo import protocol
from neo.handler import EventHandler
from neo.protocol import INVALID_UUID, BROKEN_STATE, ADMIN_NODE_TYPE
from neo import protocol
from neo import util

class MasterEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

    def acceptNodeIdentification(self, conn, packet, uuid):
        """ Send a packet to accept the node identification """
        app = self.app
        args = (protocol.MASTER_NODE_TYPE, app.uuid, 
                app.server[0], app.server[1], 
                app.pt.getPartitions(), app.pt.getReplicas(),
                uuid)
        p = protocol.acceptNodeIdentification(*args)
        conn.answer(p, packet)
    
    def registerAdminNode(self, conn, packet, uuid, server):
        """ Register the connection's peer as an admin node """
        from neo.master.administration import AdministrationEventHandler
        from neo.node import AdminNode
        node = self.app.nm.getNodeByUUID(uuid)
        if node is None:
            uuid = self.app.getNewUUID(protocol.ADMIN_NODE_TYPE)
            self.app.nm.add(AdminNode(uuid=uuid, server=server))
        conn.setUUID(uuid)
        conn.setHandler(AdministrationEventHandler(self.app))
        logging.info('Register admin node %s' % util.dump(uuid))
        self.acceptNodeIdentification(conn, packet, uuid)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        raise NotImplementedError('this method must be overridden')

    def handleAnnouncePrimaryMaster(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleReelectPrimaryMaster(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

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

    def handleAskNewTID(self, conn, packet):
        logging.error('ignoring Ask New TID in %s' % self.__class__.__name__)

    def handleAskNewOIDs(self, conn, packet):
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

    def handleAskPrimaryMaster(self, conn, packet):
        app = self.app
        if app.primary:
            primary_uuid = app.uuid
        elif app.primary_master_node is not None:
            primary_uuid = app.primary_master_node.getUUID()
        else:
            primary_uuid = INVALID_UUID

        known_master_list = [app.server + (app.uuid, )]
        for n in app.nm.getMasterNodeList():
            if n.getState() == BROKEN_STATE:
                continue
            known_master_list.append(n.getServer() + \
                                     (n.getUUID() or INVALID_UUID, ))
        conn.answer(protocol.answerPrimaryMaster(primary_uuid,
                                                 known_master_list), packet)

    def handleAskNodeInformation(self, conn, packet):
        self.app.sendNodesInformations(conn)
        conn.answer(protocol.answerNodeInformation([]), packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        assert len(offset_list) == 0
        app = self.app
        app.sendPartitionTable(conn)
        conn.answer(protocol.answerPartitionTable(app.pt.getID(), []), packet)

