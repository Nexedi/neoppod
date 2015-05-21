#
# Copyright (C) 2006-2015  Nexedi SA
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

from . import BaseMasterHandler
from neo.lib import logging, protocol

class InitializationHandler(BaseMasterHandler):

    def answerNodeInformation(self, conn):
        pass

    def answerPartitionTable(self, conn, ptid, row_list):
        app = self.app
        pt = app.pt
        pt.load(ptid, row_list, self.app.nm)
        if not pt.filled():
            raise protocol.ProtocolError('Partial partition table received')
        logging.debug('Got the partition table:')
        self.app.pt.log()
        # Install the partition table into the database for persistency.
        cell_list = []
        num_partitions = app.pt.getPartitions()
        unassigned_set = set(xrange(num_partitions))
        for offset in xrange(num_partitions):
            for cell in pt.getCellList(offset):
                cell_list.append((offset, cell.getUUID(), cell.getState()))
                if cell.getUUID() == app.uuid:
                    unassigned_set.remove(offset)
        # delete objects database
        if unassigned_set:
            logging.debug('drop data for partitions %r', unassigned_set)
            app.dm.dropPartitions(unassigned_set)

        app.dm.changePartitionTable(ptid, cell_list, reset=True)

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        # XXX: This is safe to ignore those notifications because all of the
        # following applies:
        # - we first ask for node information, and *then* partition
        #   table content, so it is possible to get notifyPartitionChanges
        #   packets in between (or even before asking for node information).
        # - this handler will be changed after receiving answerPartitionTable
        #   and before handling the next packet
        logging.debug('ignoring notifyPartitionChanges during initialization')
