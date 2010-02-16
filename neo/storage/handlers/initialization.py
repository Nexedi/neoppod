#
# Copyright (C) 2006-2010  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

from neo import logging

from neo.storage.handlers import BaseMasterHandler
from neo import protocol

class InitializationHandler(BaseMasterHandler):

    def answerNodeInformation(self, conn):
        self.app.has_node_information = True

    def notifyNodeInformation(self, conn, node_list):
        # the whole node list is received here
        BaseMasterHandler.notifyNodeInformation(self, conn, node_list)

    def sendPartitionTable(self, conn, ptid, row_list):
        """A primary master node sends this packet to synchronize a partition
        table. Note that the message can be split into multiple packets."""
        self.app.pt.load(ptid, row_list, self.app.nm)

    def answerPartitionTable(self, conn, ptid, row_list):
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
            assigned_to_me = False
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
                if cell.getUUID() == app.uuid:
                    assigned_to_me = True
            if not assigned_to_me:
                logging.debug('drop data for partition %d' % offset)
                # not for me, delete objects database
                app.dm.dropPartition(app.pt.getPartitions(), offset)

        app.dm.setPartitionTable(ptid, cell_list)
        self.app.has_partition_table = True

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        # XXX: This is safe to ignore those notifications because all of the
        # following applies:
        # - master is monothreaded (notifyPartitionChanges cannot happen
        #   between sendPartitionTable/answerPartitionTable packets), so
        #   receiving the whole partition table is atomic
        # - we first ask for node information, and *then* partition
        #   table content, so it is possible to get notifyPartitionChanges
        #   packets in between (or even before asking for node information).
        # - this handler will be changed after receiving answerPartitionTable
        #   and before handling the next packet
        logging.debug('ignoring notifyPartitionChanges during initialization')
