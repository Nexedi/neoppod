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

from neo.storage.handlers import BaseMasterHandler
from neo.protocol import TEMPORARILY_DOWN_STATE
from neo import protocol
from neo.node import StorageNode

class InitializationHandler(BaseMasterHandler):

    def handleAnswerNodeInformation(self, conn, packet, node_list):
        assert not node_list
        self.app.has_node_information = True

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        # XXX: This message should be replaced by a SendNodeInformation to be
        # consistent with SendPartitionTable.
        BaseMasterHandler.handleNotifyNodeInformation(self, conn, packet, node_list)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        """A primary master node sends this packet to synchronize a partition
        table. Note that the message can be split into multiple packets."""
        self.app.pt.load(ptid, row_list, self.app.nm)

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        app = self.app
        pt = app.pt
        assert not row_list
        if not pt.filled():
            raise protocol.ProtocolError('Partial partition table received')
        logging.debug('Got the partition table :')
        self.app.pt.log()
        # Install the partition table into the database for persistency.
        cell_list = []
        for offset in xrange(app.pt.getPartitions()):
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
        app.dm.setPartitionTable(ptid, cell_list)
        self.app.has_partition_table = True

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        # XXX: Currently it safe to ignore those packets because the master is
        # single threaded, it send the partition table without any changes at
        # the same time. Latter it should be needed to put in queue any changes
        # and apply them when the initial partition is filled.
        logging.debug('ignoring notifyPartitionChanges during initialization')
