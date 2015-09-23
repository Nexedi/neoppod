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

from . import logging
from .protocol import (
    NodeStates, Packets, Errors, BackendNotImplemented,
    BrokenNodeDisallowedError, NotReadyError, PacketMalformedError,
    ProtocolError, UnexpectedPacketError)
from .util import cached_property


class EventHandler(object):
    """This class handles events."""

    def __new__(cls, app, *args, **kw):
        try:
            return app._handlers[cls]
        except AttributeError: # for BackupApplication
            self = object.__new__(cls)
        except KeyError:
            self = object.__new__(cls)
            if cls.__init__ is object.__init__:
                app._handlers[cls] = self
        self.app = app
        return self

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
        logging.error(message)
        conn.answer(Errors.ProtocolError(message))
        conn.abort()
        # self.peerBroken(conn)

    def dispatch(self, conn, packet, kw={}):
        """This is a helper method to handle various packet types."""
        try:
            conn.setPeerId(packet.getId())
            try:
                method = getattr(self, packet.handler_method_name)
            except AttributeError:
                raise UnexpectedPacketError('no handler found')
            args = packet.decode() or ()
            method(conn, *args, **kw)
        except UnexpectedPacketError, e:
            if not conn.isClosed():
                self.__unexpectedPacket(conn, packet, *e.args)
        except PacketMalformedError, e:
            logging.error('malformed packet from %r: %s', conn, e)
            conn.close()
            # self.peerBroken(conn)
        except BrokenNodeDisallowedError:
            if not conn.isClosed():
                conn.answer(Errors.BrokenNode('go away'))
                conn.abort()
        except NotReadyError, message:
            if not conn.isClosed():
                if not message.args:
                    message = 'Retry Later'
                message = str(message)
                conn.answer(Errors.NotReady(message))
                conn.abort()
        except ProtocolError, message:
            if not conn.isClosed():
                message = str(message)
                conn.answer(Errors.ProtocolError(message))
                conn.abort()
        except BackendNotImplemented, message:
            m = message[0]
            conn.answer(Errors.BackendNotImplemented(
                "%s.%s does not implement %s"
                % (m.im_class.__module__, m.im_class.__name__, m.__name__)))
        except AssertionError:
            conn.close()
            raise

    def checkClusterName(self, name):
        # raise an exception if the given name mismatch the current cluster name
        if self.app.name != name:
            logging.error('reject an alien cluster')
            raise ProtocolError('invalid cluster name')


    # Network level handlers

    def packetReceived(self, *args):
        """Called when a packet is received."""
        self.dispatch(*args)

    def connectionStarted(self, conn):
        """Called when a connection is started."""
        logging.debug('connection started for %r', conn)

    def connectionCompleted(self, conn):
        """Called when a connection is completed."""
        logging.debug('connection completed for %r (from %s:%u)',
                      conn, *conn.getConnector().getAddress())

    def connectionFailed(self, conn):
        """Called when a connection failed."""
        logging.debug('connection failed for %r', conn)

    def connectionAccepted(self, conn):
        """Called when a connection is accepted."""

    def connectionClosed(self, conn):
        """Called when a connection is closed by the peer."""
        logging.debug('connection closed for %r', conn)
        self.connectionLost(conn, NodeStates.TEMPORARILY_DOWN)

    #def peerBroken(self, conn):
    #    """Called when a peer is broken."""
    #    logging.error('%r is broken', conn)
    #    # NodeStates.BROKEN

    def connectionLost(self, conn, new_state):
        """ this is a method to override in sub-handlers when there is no need
        to make distinction from the kind event that closed the connection  """
        pass


    # Packet handlers.

    def acceptIdentification(self, conn, node_type, *args):
        try:
            acceptIdentification = self._acceptIdentification
        except AttributeError:
            raise UnexpectedPacketError('no handler found')
        if conn.isClosed():
            # acceptIdentification received on a closed (probably aborted,
            # actually) connection. Reject any further packet as unexpected.
            conn.setHandler(EventHandler(self.app))
            return
        node = self.app.nm.getByAddress(conn.getAddress())
        assert node.getConnection() is conn, (node.getConnection(), conn)
        if node.getType() == node_type:
            node.setIdentified()
            acceptIdentification(node, *args)
            return
        conn.close()

    def ping(self, conn):
        conn.answer(Packets.Pong())

    def pong(self, conn):
        # Ignore PONG packets. The only purpose of ping/pong packets is
        # to test/maintain underlying connection.
        pass

    def notify(self, conn, message):
        logging.warning('notification from %r: %s', conn, message)

    def closeClient(self, conn):
        conn.server = False
        if not conn.client:
            conn.close()

    # Error packet handlers.

    def error(self, conn, code, message, **kw):
        try:
            getattr(self, Errors[code])(conn, message)
        except (AttributeError, ValueError):
            raise UnexpectedPacketError(message)

    # XXX: For some errors, the connection should have been closed by the remote
    #      peer. But what happens if it's not the case because of some bug ?
    def protocolError(self, conn, message):
        logging.error('protocol error: %s', message)

    def notReadyError(self, conn, message):
        logging.error('not ready: %s', message)

    def timeoutError(self, conn, message):
        logging.error('timeout error: %s', message)

    def brokenNodeDisallowedError(self, conn, message):
        raise RuntimeError, 'broken node disallowed error: %s' % (message,)

    def alreadyPendingError(self, conn, message):
        logging.error('already pending error: %s', message)

    def ack(self, conn, message):
        logging.debug("no error message: %s", message)

    def backendNotImplemented(self, conn, message):
        raise NotImplementedError(message)


class MTEventHandler(EventHandler):
    """Base class of handler implementations for MTClientConnection"""

    @cached_property
    def dispatcher(self):
        return self.app.dispatcher

    def dispatch(self, conn, packet, kw={}):
        assert conn.lock._is_owned() # XXX: see also lockCheckWrapper
        super(MTEventHandler, self).dispatch(conn, packet, kw)

    def packetReceived(self, conn, packet, kw={}):
        """Redirect all received packet to dispatcher thread."""
        if packet.isResponse() and type(packet) is not Packets.Pong:
            if not self.dispatcher.dispatch(conn, packet.getId(), packet, kw):
                raise ProtocolError('Unexpected response packet from %r: %r'
                                    % (conn, packet))
        else:
            self.dispatch(conn, packet, kw)

    def connectionLost(self, conn, new_state):
        self.dispatcher.unregister(conn)

    def connectionFailed(self, conn):
        self.dispatcher.unregister(conn)


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
    protocolError = unexpectedInAnswerHandler
