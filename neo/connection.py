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

from Queue import deque

from neo import logging
from neo.locking import RLock

from neo.protocol import PacketMalformedError, Packets
from neo.event import IdleEvent
from neo.connector import ConnectorException, ConnectorTryAgainException, \
        ConnectorInProgressException, ConnectorConnectionRefusedException, \
        ConnectorConnectionClosedException
from neo.util import dump
from neo.logger import PACKET_LOGGER

from neo import attributeTracker

def not_closed(func):
    def decorator(self, *args, **kw):
        if self.connector is None:
            raise ConnectorConnectionClosedException
        return func(self, *args, **kw)
    return decorator


def lockCheckWrapper(func):
    """
    This function is to be used as a wrapper around
    MT(Client|Server)Connection class methods.

    It uses a "_" method on RLock class, so it might stop working without
    notice (sadly, RLock does not offer any "acquired" method, but that one
    will do as it checks that current thread holds this lock).

    It requires moniroted class to have an RLock instance in self._lock
    property.
    """
    def wrapper(self, *args, **kw):
        if not self._lock._is_owned():
            import traceback
            logging.warning('%s called on %s instance without being locked.' \
                ' Stack:\n%s', func.func_code.co_name, self.__class__.__name__,
                ''.join(traceback.format_stack()))
        # Call anyway
        return func(self, *args, **kw)
    return wrapper


class BaseConnection(object):
    """A base connection."""

    def __init__(self, event_manager, handler, connector = None,
                 addr = None, connector_handler = None):
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self.handler = handler
        if connector is not None:
            self.connector_handler = connector.__class__
            event_manager.register(self)
        else:
            self.connector_handler = connector_handler

    def lock(self):
        return 1

    def unlock(self):
        return None

    def getConnector(self):
        return self.connector

    def setConnector(self, connector):
        if self.connector is not None:
            raise RuntimeError, 'cannot overwrite a connector in a connection'
        if connector is not None:
            self.connector = connector
            self.em.register(self)

    def getAddress(self):
        return self.addr

    def readable(self):
        raise NotImplementedError

    def writable(self):
        raise NotImplementedError

    def close(self):
        """Close the connection."""
        em = self.em
        if self.connector is not None:
            em.removeReader(self)
            em.removeWriter(self)
            em.unregister(self)
            self.connector.shutdown()
            self.connector.close()
            self.connector = None

    __del__ = close

    def getHandler(self):
        return self.handler

    def setHandler(self, handler):
        self.handler = handler

    def getEventManager(self):
        return self.em

    def getUUID(self):
        return None

    def isAborted(self):
        return False

    def isListening(self):
        return False

    def isServer(self):
        return False

    def isClient(self):
        return False

    def hasPendingMessages(self):
        return False

    def hasPendingRequests(self):
        return False

    def whoSetConnector(self):
        """
          Debugging method: call this method to know who set the current
          connector value.
        """
        return attributeTracker.whoSet(self, 'connector')

attributeTracker.track(BaseConnection)

class ListeningConnection(BaseConnection):
    """A listen connection."""

    def __init__(self, event_manager, handler, addr, connector_handler, **kw):
        logging.debug('listening to %s:%d', *addr)
        BaseConnection.__init__(self, event_manager, handler,
                                addr = addr,
                                connector_handler = connector_handler)
        connector = self.connector_handler()
        connector.makeListeningConnection(addr)
        self.setConnector(connector)
        self.em.addReader(self)

    def readable(self):
        try:
            new_s, addr = self.connector.getNewConnection()
            logging.debug('accepted a connection from %s:%d', *addr)
            handler = self.getHandler()
            new_conn = ServerConnection(self.getEventManager(), handler,
                                        connector=new_s, addr=addr)
            handler.connectionAccepted(new_conn)
        except ConnectorTryAgainException:
            pass

    def writable(self):
        return False

    def isListening(self):
        return True


class Connection(BaseConnection):
    """A connection."""

    def __init__(self, event_manager, handler,
                 connector = None, addr = None,
                 connector_handler = None):
        self.read_buf = ""
        self.write_buf = ""
        self.cur_id = 0
        self.peer_id = 0
        self.event_dict = {}
        self.aborted = False
        self.uuid = None
        self._queue = []
        self._expected = deque()
        BaseConnection.__init__(self, event_manager, handler,
                                connector = connector, addr = addr,
                                connector_handler = connector_handler)
        if connector is not None:
            event_manager.addReader(self)

    def isAborted(self):
        return self.aborted

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        self.uuid = uuid

    def setPeerId(self, peer_id):
        self.peer_id = peer_id

    def getPeerId(self):
        return self.peer_id

    def _getNextId(self):
        next_id = self.cur_id
        # Deal with an overflow.
        if self.cur_id == 0xffffffff:
            self.cur_id = 0
        else:
            self.cur_id += 1
        return next_id

    def close(self):
        logging.debug('closing a connector for %s (%s:%d)',
                dump(self.uuid), *(self.addr))
        BaseConnection.close(self)
        for event in self.event_dict.itervalues():
            self.em.removeIdleEvent(event)
        self.event_dict.clear()
        self.write_buf = ""
        self.read_buf = ""
        self._expected.clear()

    def abort(self):
        """Abort dealing with this connection."""
        logging.debug('aborting a connector for %s (%s:%d)',
                dump(self.uuid), *(self.addr))
        self.aborted = True

    def writable(self):
        """Called when self is writable."""
        self._send()
        if not self.write_buf and self.connector is not None:
            if self.aborted:
                self.close()
            else:
                self.em.removeWriter(self)

    def readable(self):
        """Called when self is readable."""
        self._recv()
        self.analyse()

        if self.aborted:
            self.em.removeReader(self)

    def analyse(self):
        """Analyse received data."""
        while True:
            # parse a packet
            try:
                packet = Packets.parse(self.read_buf)
                if packet is None:
                    break
            except PacketMalformedError, msg:
                self.handler._packetMalformed(self, msg)
                return
            self.read_buf = self.read_buf[len(packet):]

            # Remove idle events, if appropriate packets were received.
            for msg_id in (None, packet.getId()):
                event = self.event_dict.pop(msg_id, None)
                if event is not None:
                    self.em.removeIdleEvent(event)

            packet_type = packet.getType()
            if packet_type == Packets.Ping:
                # Send a pong notification
                self.answer(Packets.Pong(), packet.getId())
            elif packet_type != Packets.Pong:
                # Skip PONG packets, its only purpose is to drop IdleEvent
                # generated upong ping.
                self._queue.append(packet)

    def hasPendingMessages(self):
        """
          Returns True if there are messages queued and awaiting processing.
        """
        return len(self._queue) != 0

    def hasPendingRequests(self):
        """
            Returns True if there are pending expected answer packets
        """
        return bool(self._expected)

    def process(self):
        """
          Process a pending packet.
        """
        packet = self._queue.pop(0)
        if packet.isResponse():
            request = None
            if self._expected:
                request = self._expected.popleft()
            if not request or not request.answerMatch(packet):
                req_info = ('', '')
                if request is not None:
                    req_info = (request.getId(), request.__class__)
                rep_info = (packet.getId(), packet.__class__)
                logging.warning('Unexpected answer: %s:%s %s:%s' %
                        (rep_info + req_info))
        PACKET_LOGGER.dispatch(self, packet, 'from')
        self.handler.packetReceived(self, packet)

    def pending(self):
        return self.connector is not None and self.write_buf

    def _closure(self):
        assert self.connector is not None, self.whoSetConnector()
        self.close()
        self.handler.connectionClosed(self)

    def _recv(self):
        """Receive data from a connector."""
        try:
            data = self.connector.receive()
            if not data:
                logging.debug('Connection %r closed in recv', self.connector)
                self._closure()
                return
            self.read_buf += data
        except ConnectorTryAgainException:
            pass
        except ConnectorConnectionRefusedException:
            # should only occur while connecting
            self.close()
            self.handler.connectionFailed(self)
        except ConnectorConnectionClosedException:
            # connection resetted by peer, according to the man, this error
            # should not occurs but it seems it's false
            logging.debug('Connection reset by peer: %r', self.connector)
            self._closure()
        except ConnectorException:
            logging.debug('Unknown connection error: %r', self.connector)
            self._closure()
            # unhandled connector exception
            raise

    def _send(self):
        """Send data to a connector."""
        if not self.write_buf:
            return
        try:
            n = self.connector.send(self.write_buf)
            if not n:
                logging.debug('Connection %r closed in send', self.connector)
                self._closure()
                return
            self.write_buf = self.write_buf[n:]
        except ConnectorTryAgainException:
            pass
        except ConnectorConnectionClosedException:
            # connection resetted by peer
            logging.debug('Connection reset by peer: %r', self.connector)
            self._closure()
        except ConnectorException:
            logging.debug('Unknown connection error: %r', self.connector)
            # unhandled connector exception
            self._closure()
            raise

    def _addPacket(self, packet):
        """Add a packet into the write buffer."""
        if self.connector is None:
            return

        was_empty = not bool(self.write_buf)

        PACKET_LOGGER.dispatch(self, packet, ' to ')
        self.write_buf += packet()

        if was_empty:
            # enable polling for writing.
            self.em.addWriter(self)

    def expectMessage(self, msg_id=None, timeout=5, additional_timeout=30):
        """Expect a message for a reply to a given message ID or any message.

        The purpose of this method is to define how much amount of time is
        acceptable to wait for a message, thus to detect a down or broken
        peer. This is important, because one error may halt a whole cluster
        otherwise. Although TCP defines a keep-alive feature, the timeout
        is too long generally, and it does not detect a certain type of reply,
        thus it is better to probe problems at the application level.

        The message ID specifies what ID is expected. Usually, this should
        be identical with an ID for a request message. If it is None, any
        message is acceptable, so it can be used to check idle time.

        The timeout is the amount of time to wait until keep-alive messages start.
        Once the timeout is expired, the connection starts to ping the peer.

        The additional timeout defines the amount of time after the timeout
        to invoke a timeoutExpired callback. If it is zero, no ping is sent, and
        the callback is executed immediately."""
        if self.connector is None:
            return

        event = IdleEvent(self, msg_id, timeout, additional_timeout)
        self.event_dict[msg_id] = event
        self.em.addIdleEvent(event)

    @not_closed
    def notify(self, packet):
        """ Then a packet with a new ID """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        return msg_id

    @not_closed
    def ask(self, packet, timeout=5, additional_timeout=30):
        """
        Send a packet with a new ID and register the expectation of an answer
        """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self.expectMessage(msg_id, timeout=timeout,
                additional_timeout=additional_timeout)
        self._addPacket(packet)
        assert packet.getAnswer() is not None, packet
        self._expected.append(packet)
        return msg_id

    @not_closed
    def answer(self, packet, msg_id=None):
        """ Answer to a packet by re-using its ID for the packet answer """
        if msg_id is None:
            msg_id = self.getPeerId()
        packet.setId(msg_id)
        assert packet.isResponse(), packet
        self._addPacket(packet)

    def ping(self, timeout=5):
        """ Send a ping and expect to receive a pong notification """
        packet = Packets.Ping()
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self.expectMessage(msg_id, timeout, 0)
        self._addPacket(packet)


class ClientConnection(Connection):
    """A connection from this node to a remote node."""

    def __init__(self, event_manager, handler, addr, connector_handler, **kw):
        self.connecting = True
        Connection.__init__(self, event_manager, handler, addr = addr,
                            connector_handler = connector_handler)
        handler.connectionStarted(self)
        try:
            connector = self.connector_handler()
            self.setConnector(connector)
            try:
                connector.makeClientConnection(addr)
            except ConnectorInProgressException:
                event_manager.addWriter(self)
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                event_manager.addReader(self)
        except ConnectorConnectionRefusedException:
            handler.connectionFailed(self)
            self.close()
        except ConnectorException:
            # unhandled connector exception
            handler.connectionFailed(self)
            self.close()
            raise

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.connector.getError()
            if err:
                self.handler.connectionFailed(self)
                self.close()
                return
            else:
                self.connecting = False
                self.handler.connectionCompleted(self)
                self.em.addReader(self)
        else:
            Connection.writable(self)

    def isClient(self):
        return True


class ServerConnection(Connection):
    """A connection from a remote node to this node."""

    def isServer(self):
        return True


class MTClientConnection(ClientConnection):
    """A Multithread-safe version of ClientConnection."""

    def __init__(self, *args, **kwargs):
        # _lock is only here for lock debugging purposes. Do not use.
        self._lock = lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        self.dispatcher = kwargs.pop('dispatcher')
        self.lock()
        try:
            super(MTClientConnection, self).__init__(*args, **kwargs)
        finally:
            self.unlock()

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

    @lockCheckWrapper
    def writable(self, *args, **kw):
        return super(MTClientConnection, self).writable(*args, **kw)

    @lockCheckWrapper
    def readable(self, *args, **kw):
        return super(MTClientConnection, self).readable(*args, **kw)

    @lockCheckWrapper
    def analyse(self, *args, **kw):
        return super(MTClientConnection, self).analyse(*args, **kw)

    @lockCheckWrapper
    def expectMessage(self, *args, **kw):
        return super(MTClientConnection, self).expectMessage(*args, **kw)

    @lockCheckWrapper
    def notify(self, *args, **kw):
        return super(MTClientConnection, self).notify(*args, **kw)

    @lockCheckWrapper
    def ask(self, queue, packet, timeout=5, additional_timeout=30):
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self.dispatcher.register(self, msg_id, queue)
        self.expectMessage(msg_id)
        self._addPacket(packet)
        assert packet.getAnswer() is not None, packet
        self._expected.append(packet)
        return msg_id

    @lockCheckWrapper
    def answer(self, *args, **kw):
        return super(MTClientConnection, self).answer(*args, **kw)

    def close(self):
        self.lock()
        try:
            super(MTClientConnection, self).close()
        finally:
            self.release()


class MTServerConnection(ServerConnection):
    """A Multithread-safe version of ServerConnection."""

    def __init__(self, *args, **kwargs):
        # _lock is only here for lock debugging purposes. Do not use.
        self._lock = lock = RLock()
        self.acquire = lock.acquire
        self.release = lock.release
        self.lock()
        try:
            super(MTServerConnection, self).__init__(*args, **kwargs)
        finally:
            self.unlock()

    def lock(self, blocking = 1):
        return self.acquire(blocking = blocking)

    def unlock(self):
        self.release()

    @lockCheckWrapper
    def writable(self, *args, **kw):
        return super(MTServerConnection, self).writable(*args, **kw)

    @lockCheckWrapper
    def readable(self, *args, **kw):
        return super(MTServerConnection, self).readable(*args, **kw)

    @lockCheckWrapper
    def analyse(self, *args, **kw):
        return super(MTServerConnection, self).analyse(*args, **kw)

    @lockCheckWrapper
    def expectMessage(self, *args, **kw):
        return super(MTServerConnection, self).expectMessage(*args, **kw)

    @lockCheckWrapper
    def notify(self, *args, **kw):
        return super(MTServerConnection, self).notify(*args, **kw)

    @lockCheckWrapper
    def ask(self, *args, **kw):
        return super(MTServerConnection, self).ask(*args, **kw)

    @lockCheckWrapper
    def answer(self, *args, **kw):
        return super(MTServerConnection, self).answer(*args, **kw)

