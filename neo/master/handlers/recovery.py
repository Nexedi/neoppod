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

from neo.protocol import Packets, ProtocolError
from neo.master.handlers import MasterHandler
from neo.util import dump

class RecoveryHandler(MasterHandler):
    """This class deals with events for a recovery phase."""

    def connectionLost(self, conn, new_state):
        node = self.app.nm.getByUUID(conn.getUUID())
        assert node is not None
        if node.getState() == new_state:
            return
        node.setState(new_state)

    def connectionCompleted(self, conn):
        # XXX: handler split review needed to remove this hack
        if not self.app._startup_allowed:
            # ask the last IDs to perform the recovery
            conn.ask(Packets.AskLastIDs())

    def answerLastIDs(self, conn, loid, ltid, lptid):
        app = self.app
        pt = app.pt

        # Get max values.
        app.loid = max(loid, app.loid)
        self.app.tm.setLastTID(ltid)
        if lptid > pt.getID():
            # something newer
            app.target_uuid = conn.getUUID()
            app.pt.setID(lptid)
            conn.ask(Packets.AskPartitionTable([]))

    def answerPartitionTable(self, conn, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        if uuid != app.target_uuid:
            # If this is not from a target node, ignore it.
            logging.warn('got answer partition table from %s while waiting ' \
                    'for %s', dump(uuid), dump(app.target_uuid))
            return
        # load unknown storage nodes
        for offset, row in row_list:
            for uuid, state in row:
                node = app.nm.getByUUID(uuid)
                if node is None:
                    app.nm.createStorage(uuid=uuid)
        # load partition in memory
        try:
            self.app.pt.load(ptid, row_list, self.app.nm)
        except IndexError:
            raise ProtocolError('Invalid offset')

