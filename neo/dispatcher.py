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

MARKER = []

class Dispatcher:
    """Register a packet, connection pair as expecting a response packet."""

    def __init__(self):
        self.message_table = {}

    def pop(self, conn, msg_id, default=MARKER):
        """Retrieve register-time provided payload."""
        key = (id(conn), msg_id)
        if default is MARKER:
            result = self.message_table.pop(key)
        else:
            result = self.message_table.pop(key, default)
        return result

    def register(self, conn, msg_id, payload):
        """Register an expectation for a reply. Thanks to GIL, it is
        safe not to use a lock here."""
        key = (id(conn), msg_id)
        self.message_table[key] = payload

    def registered(self, conn):
        """Check if a connection is registered into message table."""
        # XXX: serch algorythm could be improved by improving data structure.
        searched_id = id(conn)
        for conn_id, msg_id in self.message_table.iterkeys():
            if searched_id == conn_id:
                return True
        return False

