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

from neo.handler import EventHandler

class BaseHandler(EventHandler):
    """Base class for client-side EventHandler implementations."""

    def __init__(self, app, dispatcher):
        self.app = app
        self.dispatcher = dispatcher
        super(BaseHandler, self).__init__()

    def dispatch(self, conn, packet):
        # Before calling superclass's dispatch method, lock the connection.
        # This covers the case where handler sends a response to received
        # packet.
        conn.lock()
        try:
            super(BaseHandler, self).dispatch(conn, packet)
        finally:
            conn.release()

    def packetReceived(self, conn, packet):
        """Redirect all received packet to dispatcher thread."""
        queue = self.dispatcher.getQueue(conn, packet)
        if queue is None:
            self.dispatch(conn, packet)
        else:
            queue.put((conn, packet))

