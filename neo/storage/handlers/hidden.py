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
from neo.protocol import NodeTypes, NodeStates, CellStates

class HiddenHandler(BaseMasterHandler):
    """This class implements a generic part of the event handlers."""

    def notifyNodeInformation(self, conn, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        app = self.app
        self.app.nm.update(node_list)
        for node_type, addr, uuid, state in node_list:
            if node_type == NodeTypes.STORAGE:
                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    if state in (NodeStates.DOWN, NodeStates.TEMPORARILY_DOWN,
                            NodeStates.BROKEN):
                        conn.close()
                        erase_db = state == NodeStates.DOWN
                        self.app.shutdown(erase=erase_db)

    def notifyPartitionChanges(self, conn, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        app = self.app
        if ptid <= app.pt.getID():
            # Ignore this packet.
            logging.debug('ignoring older partition changes')
            return

        # update partition table in memory and the database
        app.pt.update(ptid, cell_list, app.nm)
        app.dm.changePartitionTable(ptid, cell_list)

        # Check changes for replications
        for offset, uuid, state in cell_list:
            if uuid == app.uuid and app.replicator is not None:
                # If this is for myself, this can affect replications.
                if state == CellStates.DISCARDED:
                    app.replicator.removePartition(offset)
                elif state == CellStates.OUT_OF_DATE:
                    app.replicator.addPartition(offset)

    def startOperation(self, conn):
        self.app.operational = True

