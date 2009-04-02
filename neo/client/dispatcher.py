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

class Dispatcher:
    """Dispatcher class use to redirect request to thread."""

    def __init__(self):
        # This dict is used to associate conn/message id to client thread queue
        # and thus redispatch answer to the original thread
        self.message_table = {}

    def getQueue(self, conn, packet):
        key = (id(conn), packet.getId())
        return self.message_table.pop(key, None)

    def register(self, conn, msg_id, queue):
        """Register an expectation for a reply. Thanks to GIL, it is
        safe not to use a lock here."""
        key = (id(conn), msg_id)
        self.message_table[key] = queue

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        searched_id = id(conn)
        for conn_id, msg_id in self.message_table.iterkeys():
            if searched_id == conn_id:
                return True
        return False

