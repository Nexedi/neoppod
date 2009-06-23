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

from neo.handler import EventHandler

class MasterEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        raise NotImplementedError('this method must be overridden')

    def handleAskPrimaryMaster(self, conn, packet):
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
