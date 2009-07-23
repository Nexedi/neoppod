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
from neo.protocol import UnexpectedPacketError

class BaseHandler(EventHandler):
    """Base class for client-side EventHandler implementations."""

    def __init__(self, app, dispatcher):
        super(BaseHandler, self).__init__(app)
        self.dispatcher = dispatcher

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
        if packet.isResponse():
            queue = self.dispatcher.getQueue(conn, packet)
            if queue is None:
                raise UnexpectedPacketError('Unexpected response packet')
            queue.put((conn, packet))
        else:
            self.dispatch(conn, packet)

    def _notifyQueues(self, conn):
        """
          Put fake packets to task queues so that threads waiting for an
          answer get notified of the disconnection.
        """
        # XXX: not thread-safe !
        queue_set = set()
        conn_id = id(conn)
        for key in self.dispatcher.message_table.keys():
            if conn_id == key[0]:
                queue = self.dispatcher.message_table.pop(key)
                queue_set.add(queue)
        for queue in queue_set:
            queue.put((conn, None))

    def connectionClosed(self, conn):
        super(BaseHandler, self).connectionClosed(conn)
        self._notifyQueues(conn)

    def timeoutExpired(self, conn):
        super(BaseHandler, self).timeoutExpired(conn)
        conn.lock()
        try:
            conn.close()
        finally:
            conn.release()
        self._notifyQueues(conn)

    def connectionFailed(self, conn):
        super(BaseHandler, self).connectionFailed(conn)
        self._notifyQueues(conn)

def unexpectedInAnswerHandler(*args, **kw):
    raise Exception('Unexpected event in an answer handler')

class AnswerBaseHandler(EventHandler):

    connectionStarted = unexpectedInAnswerHandler
    connectionCompleted = unexpectedInAnswerHandler
    connectionFailed = unexpectedInAnswerHandler
    connectionAccepted = unexpectedInAnswerHandler
    timeoutExpired = unexpectedInAnswerHandler
    connectionClosed = unexpectedInAnswerHandler
    packetReceived = unexpectedInAnswerHandler
    peerBroken = unexpectedInAnswerHandler

