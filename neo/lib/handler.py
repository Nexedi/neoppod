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

import neo.lib
from neo.lib.protocol import NodeStates, ErrorCodes, Packets, Errors
from neo.lib.protocol import PacketMalformedError, UnexpectedPacketError, \
        BrokenNodeDisallowedError, NotReadyError, ProtocolError


class EventHandler(object):
    """This class handles events."""

    def __init__(self, app):
        self.app = app

    def __repr__(self):
        return self.__class__.__name__

    def __unexpectedPacket(self, conn, packet, message=None):
        """Handle an unexpected packet."""
        if message is None:
            message = 'unexpected packet type %s in %s' % (type(packet),
                    self.__class__.__name__)
        else:
            message = 'unexpected packet: %s in %s' % (message,
                    self.__class__.__name__)
        neo.lib.logging.error(message)
        conn.answer(Errors.ProtocolError(message))
        conn.abort()
        # self.peerBroken(conn)

    def dispatch(self, conn, packet):
        """This is a helper method to handle various packet types."""
        try:
            try:
                method = getattr(self, packet.handler_method_name)
            except AttributeError:
                raise UnexpectedPacketError('no handler found')
            args = packet.decode() or ()
            conn.setPeerId(packet.getId())
            method(conn, *args)
        except UnexpectedPacketError, e:
            self.__unexpectedPacket(conn, packet, *e.args)
        except PacketMalformedError:
            neo.lib.logging.error('malformed packet from %r', conn)
            conn.notify(Packets.Notify('Malformed packet: %r' % (packet, )))
            conn.abort()
            # self.peerBroken(conn)
        except BrokenNodeDisallowedError:
            conn.answer(Errors.BrokenNode('go away'))
            conn.abort()
        except NotReadyError, message:
            if not message.args:
                message = 'Retry Later'
            message = str(message)
            conn.answer(Errors.NotReady(message))
            conn.abort()
        except ProtocolError, message:
            message = str(message)
            conn.answer(Errors.ProtocolError(message))
            conn.abort()

    def checkClusterName(self, name):
        # raise an exception if the given name mismatch the current cluster name
        if self.app.name != name:
            neo.lib.logging.error('reject an alien cluster')
            raise ProtocolError('invalid cluster name')


    # Network level handlers

    def packetReceived(self, conn, packet):
        """Called when a packet is received."""
        self.dispatch(conn, packet)

    def connectionStarted(self, conn):
        """Called when a connection is started."""
        neo.lib.logging.debug('connection started for %r', conn)

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        neo.lib.logging.debug('connection completed for %r (from %s:%u)',
                              conn, *conn.getConnector().getAddress())

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        neo.lib.logging.debug('connection failed for %r', conn)

    def connectionAccepted(self, conn):
        """Called when a connection is accepted."""

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        neo.lib.logging.debug('connection closed for %r', conn)
        self.connectionLost(conn, NodeStates.TEMPORARILY_DOWN)

    #def peerBroken(self, conn):
    #    """Called when a peer is broken."""
    #    neo.lib.logging.error('%r is broken', conn)
    #    # NodeStates.BROKEN

    def connectionLost(self, conn, new_state):
        """ this is a method to override in sub-handlers when there is no need
        to make distinction from the kind event that closed the connection  """
        pass


    # Packet handlers.

    def ping(self, conn):
        if not conn.isAborted():
            conn.answer(Packets.Pong())

    def pong(self, conn):
        # Ignore PONG packets. The only purpose of ping/pong packets is
        # to test/maintain underlying connection.
        pass

    def notify(self, conn, message):
        neo.lib.logging.info('notification from %r: %s', conn, message)

    # Error packet handlers.

    def error(self, conn, code, message):
        try:
            getattr(self, Errors[code])(conn, message)
        except (AttributeError, ValueError):
            raise UnexpectedPacketError(message)

    def protocolError(self, conn, message):
        # the connection should have been closed by the remote peer
        neo.lib.logging.error('protocol error: %s' % (message,))

    def timeoutError(self, conn, message):
        neo.lib.logging.error('timeout error: %s' % (message,))

    def brokenNodeDisallowedError(self, conn, message):
        raise RuntimeError, 'broken node disallowed error: %s' % (message,)

    def alreadyPendingError(self, conn, message):
        neo.lib.logging.error('already pending error: %s' % (message, ))

    def ack(self, conn, message):
        neo.lib.logging.debug("no error message : %s" % (message))
