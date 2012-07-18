#
# Copyright (C) 2006-2012  Nexedi SA
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

from neo.lib.handler import EventHandler
from neo.lib.protocol import ProtocolError, Packets

class BaseHandler(EventHandler):
    """Base class for client-side EventHandler implementations."""

    def __init__(self, app):
        super(BaseHandler, self).__init__(app)
        self.dispatcher = app.dispatcher

    def dispatch(self, conn, packet, kw={}):
        assert conn._lock._is_owned()
        super(BaseHandler, self).dispatch(conn, packet, kw)

    def packetReceived(self, conn, packet, kw={}):
        """Redirect all received packet to dispatcher thread."""
        if packet.isResponse() and type(packet) is not Packets.Pong:
            if not self.dispatcher.dispatch(conn, packet.getId(), packet, kw):
                raise ProtocolError('Unexpected response packet from %r: %r'
                                    % (conn, packet))
        else:
            self.dispatch(conn, packet, kw)


    def connectionLost(self, conn, new_state):
        self.app.dispatcher.unregister(conn)

    def connectionFailed(self, conn):
        self.app.dispatcher.unregister(conn)


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

