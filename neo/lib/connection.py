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

from functools import wraps
from time import time

import neo.lib
from neo.lib.locking import RLock

from neo.lib.protocol import PacketMalformedError, Packets, ParserState
from neo.lib.connector import ConnectorException, ConnectorTryAgainException, \
        ConnectorInProgressException, ConnectorConnectionRefusedException, \
        ConnectorConnectionClosedException
from neo.lib.util import dump
from neo.lib.logger import PACKET_LOGGER

from neo.lib import attributeTracker
from neo.lib.util import ReadBuffer
from neo.lib.profiling import profiler_decorator

CRITICAL_TIMEOUT = 30

class ConnectionClosed(Exception):
    pass

def not_closed(func):
    def decorator(self, *args, **kw):
        if self.connector is None:
            raise ConnectorConnectionClosedException
        return func(self, *args, **kw)
    return wraps(func)(decorator)


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
            neo.lib.logging.warning('%s called on %s instance without being ' \
                'locked. Stack:\n%s', func.func_code.co_name,
                self.__class__.__name__, ''.join(traceback.format_stack()))
        # Call anyway
        return func(self, *args, **kw)
    return wraps(func)(wrapper)

class OnTimeout(object):
    """
      Simple helper class for on_timeout parameter used in HandlerSwitcher
      class.
    """
    def __init__(self, func, *args, **kw):
        self.func = func
        self.args = args
        self.kw = kw

    def __call__(self, conn, msg_id):
        return self.func(conn, msg_id, *self.args, **self.kw)

class HandlerSwitcher(object):
    _next_timeout = None
    _next_timeout_msg_id = None
    _next_on_timeout = None

    def __init__(self, handler):
        # pending handlers and related requests
        self._pending = [[{}, handler]]
        self._is_handling = False

    def clear(self):
        handler = self._pending[0][1]
        self._pending = [[{}, handler]]

    def isPending(self):
        return bool(self._pending[0][0])

    def getHandler(self):
        return self._pending[0][1]

    def getLastHandler(self):
        """ Return the last (may be unapplied) handler registered """
        return self._pending[-1][1]

    @profiler_decorator
    def emit(self, request, timeout, on_timeout):
        # register the request in the current handler
        _pending = self._pending
        if self._is_handling:
            # If this is called while handling a packet, the response is to
            # be excpected for the current handler...
            (request_dict, _) = _pending[0]
        else:
            # ...otherwise, queue for for the latest handler
            assert len(_pending) == 1 or _pending[0][0]
            (request_dict, _) = _pending[-1]
        msg_id = request.getId()
        answer_class = request.getAnswerClass()
        assert answer_class is not None, "Not a request"
        assert msg_id not in request_dict, "Packet id already expected"
        next_timeout = self._next_timeout
        if next_timeout is None or timeout < next_timeout:
            self._next_timeout = timeout
            self._next_timeout_msg_id = msg_id
            self._next_on_timeout = on_timeout
        request_dict[msg_id] = (answer_class, timeout, on_timeout)

    def getNextTimeout(self):
        return self._next_timeout

    def timeout(self, connection):
        msg_id = self._next_timeout_msg_id
        if self._next_on_timeout is not None:
            self._next_on_timeout(connection, msg_id)
            if self._next_timeout_msg_id != msg_id:
                # on_timeout sent a packet with a smaller timeout
                # so keep the connection open
                return
        # Notify that a timeout occured
        return msg_id

    def handle(self, connection, packet):
        assert not self._is_handling
        self._is_handling = True
        try:
            self._handle(connection, packet)
        finally:
            self._is_handling = False

    @profiler_decorator
    def _handle(self, connection, packet):
        assert len(self._pending) == 1 or self._pending[0][0]
        PACKET_LOGGER.dispatch(connection, packet, False)
        if connection.isClosed() and packet.ignoreOnClosedConnection():
            neo.lib.logging.debug('Ignoring packet %r on closed connection %r',
                packet, connection)
            return
        msg_id = packet.getId()
        (request_dict, handler) = self._pending[0]
        # notifications are not expected
        if not packet.isResponse():
            handler.packetReceived(connection, packet)
            return
        # checkout the expected answer class
        (klass, timeout, _) = request_dict.pop(msg_id, (None, None, None))
        if klass and isinstance(packet, klass) or packet.isError():
            handler.packetReceived(connection, packet)
        else:
            neo.lib.logging.error(
                            'Unexpected answer %r in %r', packet, connection)
            if not connection.isClosed():
                notification = Packets.Notify('Unexpected answer: %r' % packet)
                connection.notify(notification)
                connection.abort()
            # handler.peerBroken(connection)
        # apply a pending handler if no more answers are pending
        while len(self._pending) > 1 and not self._pending[0][0]:
            del self._pending[0]
            neo.lib.logging.debug(
                            'Apply handler %r on %r', self._pending[0][1],
                    connection)
        if msg_id == self._next_timeout_msg_id:
            self._updateNextTimeout()

    def _updateNextTimeout(self):
        # Find next timeout and its msg_id
        next_timeout = None
        for pending in self._pending:
            for msg_id, (_, timeout, on_timeout) in pending[0].iteritems():
                if not next_timeout or timeout < next_timeout[0]:
                    next_timeout = timeout, msg_id, on_timeout
        self._next_timeout, self._next_timeout_msg_id, self._next_on_timeout = \
            next_timeout or (None, None, None)

    @profiler_decorator
    def setHandler(self, handler):
        can_apply = len(self._pending) == 1 and not self._pending[0][0]
        if can_apply:
            # nothing is pending, change immediately
            self._pending[0][1] = handler
        else:
            # put the next handler in queue
            self._pending.append([{}, handler])
        return can_apply


class BaseConnection(object):
    """A base connection

    About timeouts:

        Timeout are mainly per-connection instead of per-packet.
        The idea is that most of time, packets are received and processed
        sequentially, so if it takes a long for a peer to process a packet,
        following packets would just be enqueued.
        What really matters is that the peer makes progress in its work.
        As long as we receive an answer, we consider it's still alive and
        it may just have started to process the following request. So we reset
        timeouts.
        There is anyway nothing more we could do, because processing of a packet
        may be delayed in a very unpredictable way depending of previously
        received packets on peer side.
        Even ourself may be slow to receive a packet. We must not timeout for
        an answer that is already in our incoming buffer (read_buf or _queue).
        Timeouts in HandlerSwitcher are only there to prioritize some packets.
    """

    KEEP_ALIVE = 60
    _base_timeout = None

    def __init__(self, event_manager, handler, connector, addr=None):
        assert connector is not None, "Need a low-level connector"
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self._handlers = HandlerSwitcher(handler)
        event_manager.register(self)

    def isPending(self):
        return self._handlers.isPending()

    def updateTimeout(self, t=None):
        if not self._queue:
            if t:
                self._base_timeout = t
            self._timeout = self._handlers.getNextTimeout() or self.KEEP_ALIVE

    def checkTimeout(self, t):
        # first make sure we don't timeout on answers we already received
        if self._base_timeout and not self._queue:
            timeout = t - self._base_timeout
            if self._timeout <= timeout:
                handlers = self._handlers
                if handlers.isPending():
                    msg_id = handlers.timeout(self)
                    if msg_id is None:
                        self._base_timeout = t
                    else:
                        neo.lib.logging.info('timeout for #0x%08x with %r',
                                             msg_id, self)
                        self.close()
                else:
                    self.idle()

    def lock(self):
        return 1

    def unlock(self):
        return None

    def getConnector(self):
        return self.connector

    def getAddress(self):
        return self.addr

    def readable(self):
        raise NotImplementedError

    def writable(self):
        raise NotImplementedError

    def close(self):
        """Close the connection."""
        if self.connector is not None:
            em = self.em
            em.removeReader(self)
            em.removeWriter(self)
            em.unregister(self)
            self.connector.shutdown()
            self.connector.close()
            self.connector = None

    def __repr__(self):
        address = self.addr and '%s:%d' % self.addr or '?'
        return '<%s(uuid=%s, address=%s, closed=%s, handler=%s) at %x>' % (
            self.__class__.__name__,
            dump(self.getUUID()),
            address,
            int(self.isClosed()),
            self.getHandler(),
            id(self),
        )

    __del__ = close

    def getHandler(self):
        return self._handlers.getHandler()

    def setHandler(self, handler):
        if self._handlers.setHandler(handler):
            neo.lib.logging.debug('Set handler %r on %r', handler, self)
        else:
            neo.lib.logging.debug('Delay handler %r on %r', handler, self)

    def getEventManager(self):
        return self.em

    def getUUID(self):
        return None

    def isClosed(self):
        return self.connector is None

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

    def whoSetConnector(self):
        """
          Debugging method: call this method to know who set the current
          connector value.
        """
        return attributeTracker.whoSet(self, 'connector')

    def idle(self):
        pass


attributeTracker.track(BaseConnection)

class ListeningConnection(BaseConnection):
    """A listen connection."""

    def __init__(self, event_manager, handler, addr, connector, **kw):
        neo.lib.logging.debug('listening to %s:%d', *addr)
        BaseConnection.__init__(self, event_manager, handler,
                                addr=addr, connector=connector)
        self.connector.makeListeningConnection(addr)
        self.em.addReader(self)

    def readable(self):
        try:
            new_s, addr = self.connector.getNewConnection()
            neo.lib.logging.debug('accepted a connection from %s:%d', *addr)
            handler = self.getHandler()
            new_conn = ServerConnection(self.getEventManager(), handler,
                connector=new_s, addr=addr)
            handler.connectionAccepted(new_conn)
        except ConnectorTryAgainException:
            pass

    def getAddress(self):
        return self.connector.getAddress()

    def writable(self):
        return False

    def isListening(self):
        return True


class Connection(BaseConnection):
    """A connection."""

    connecting = False

    def __init__(self, event_manager, *args, **kw):
        BaseConnection.__init__(self, event_manager, *args, **kw)
        self.read_buf = ReadBuffer()
        self.write_buf = []
        self.cur_id = 0
        self.peer_id = 0
        self.aborted = False
        self.uuid = None
        self._queue = []
        self._on_close = None
        self._parser_state = ParserState()
        event_manager.addReader(self)

    def setOnClose(self, callback):
        assert self._on_close is None
        self._on_close = callback

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

    @profiler_decorator
    def _getNextId(self):
        next_id = self.cur_id
        self.cur_id = (next_id + 1) & 0xffffffff
        return next_id

    def abort(self):
        """Abort dealing with this connection."""
        neo.lib.logging.debug('aborting a connector for %r', self)
        self.aborted = True
        assert self.write_buf

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
                packet = Packets.parse(self.read_buf, self._parser_state)
                if packet is None:
                    break
            except PacketMalformedError, msg:
                self.getHandler()._packetMalformed(self, msg)
                return
            self._queue.append(packet)

    def hasPendingMessages(self):
        """
          Returns True if there are messages queued and awaiting processing.
        """
        return len(self._queue) != 0

    def process(self):
        """
          Process a pending packet.
        """
        # check out packet and process it with current handler
        packet = self._queue.pop(0)
        self._handlers.handle(self, packet)
        self.updateTimeout()

    def pending(self):
        return self.connector is not None and self.write_buf

    def close(self):
        if self.connector is None:
            assert self._on_close is None
            assert not self.read_buf
            assert not self.write_buf
            assert not self.isPending()
            return
        # process the network events with the last registered handler to
        # solve issues where a node is lost with pending handlers and
        # create unexpected side effects.
        neo.lib.logging.debug('closing a connector for %r', self)
        handler = self._handlers.getLastHandler()
        super(Connection, self).close()
        if self._on_close is not None:
            self._on_close()
            self._on_close = None
        del self.write_buf[:]
        self.read_buf.clear()
        self._handlers.clear()
        if self.connecting:
            handler.connectionFailed(self)
        else:
            handler.connectionClosed(self)

    def _closure(self):
        assert self.connector is not None, self.whoSetConnector()
        self.close()

    @profiler_decorator
    def _recv(self):
        """Receive data from a connector."""
        try:
            data = self.connector.receive()
        except ConnectorTryAgainException:
            pass
        except ConnectorConnectionRefusedException:
            assert self.connecting
            self._closure()
        except ConnectorConnectionClosedException:
            # connection resetted by peer, according to the man, this error
            # should not occurs but it seems it's false
            neo.lib.logging.debug(
                            'Connection reset by peer: %r', self.connector)
            self._closure()
        except:
            neo.lib.logging.debug(
                            'Unknown connection error: %r', self.connector)
            self._closure()
            # unhandled connector exception
            raise
        else:
            if not data:
                neo.lib.logging.debug(
                    'Connection %r closed in recv', self.connector)
                self._closure()
                return
            self._base_timeout = time() # last known remote activity
            self.read_buf.append(data)

    @profiler_decorator
    def _send(self):
        """Send data to a connector."""
        if not self.write_buf:
            return
        msg = ''.join(self.write_buf)
        try:
            n = self.connector.send(msg)
        except ConnectorTryAgainException:
            pass
        except ConnectorConnectionClosedException:
            # connection resetted by peer
            neo.lib.logging.debug(
                            'Connection reset by peer: %r', self.connector)
            self._closure()
        except:
            neo.lib.logging.debug(
                            'Unknown connection error: %r', self.connector)
            # unhandled connector exception
            self._closure()
            raise
        else:
            if not n:
                neo.lib.logging.debug('Connection %r closed in send',
                    self.connector)
                self._closure()
                return
            if n == len(msg):
                del self.write_buf[:]
            else:
                self.write_buf = [msg[n:]]

    @profiler_decorator
    def _addPacket(self, packet):
        """Add a packet into the write buffer."""
        if self.connector is None:
            return

        was_empty = not self.write_buf

        self.write_buf.extend(packet.encode())

        if was_empty:
            # enable polling for writing.
            self.em.addWriter(self)
        PACKET_LOGGER.dispatch(self, packet, True)

    @not_closed
    def notify(self, packet):
        """ Then a packet with a new ID """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        return msg_id

    @profiler_decorator
    @not_closed
    def ask(self, packet, timeout=CRITICAL_TIMEOUT, on_timeout=None):
        """
        Send a packet with a new ID and register the expectation of an answer
        """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        handlers = self._handlers
        t = not handlers.isPending() and time() or None
        handlers.emit(packet, timeout, on_timeout)
        self.updateTimeout(t)
        return msg_id

    @not_closed
    def answer(self, packet, msg_id=None):
        """ Answer to a packet by re-using its ID for the packet answer """
        if msg_id is None:
            msg_id = self.getPeerId()
        packet.setId(msg_id)
        assert packet.isResponse(), packet
        self._addPacket(packet)

    def idle(self):
        self.ask(Packets.Ping())


class ClientConnection(Connection):
    """A connection from this node to a remote node."""

    connecting = True

    def __init__(self, event_manager, handler, addr, connector):
        Connection.__init__(self, event_manager, handler, connector, addr)
        handler.connectionStarted(self)
        try:
            try:
                self.connector.makeClientConnection(addr)
            except ConnectorInProgressException:
                event_manager.addWriter(self)
            else:
                self.connecting = False
                self.updateTimeout(time())
                self.getHandler().connectionCompleted(self)
        except ConnectorConnectionRefusedException:
            self._closure()
        except ConnectorException:
            # unhandled connector exception
            self._closure()
            raise

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.connector.getError()
            if err:
                self._closure()
                return
            else:
                self.connecting = False
                self.updateTimeout(time())
                self.getHandler().connectionCompleted(self)
                self.em.addReader(self)
        else:
            Connection.writable(self)

    def isClient(self):
        return True


class ServerConnection(Connection):
    """A connection from a remote node to this node."""

    # Both server and client must check the connection, in case:
    # - the remote crashed brutally (i.e. without closing TCP connections)
    # - or packets sent by the remote are dropped (network failure)
    # Use different timeout so that in normal condition, server never has to
    # ping the client. Otherwise, it would do it about half of the time.
    KEEP_ALIVE = Connection.KEEP_ALIVE + 5

    def __init__(self, *args, **kw):
        Connection.__init__(self, *args, **kw)
        self.updateTimeout(time())

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
        self.dispatcher.needPollThread()
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

    def notify(self, *args, **kw):
        self.lock()
        try:
            return super(MTClientConnection, self).notify(*args, **kw)
        finally:
            self.unlock()

    @profiler_decorator
    def ask(self, packet, timeout=CRITICAL_TIMEOUT, on_timeout=None,
            queue=None):
        self.lock()
        try:
            if self.isClosed():
                raise ConnectionClosed
            # XXX: Here, we duplicate Connection.ask because we need to call
            # self.dispatcher.register after setId is called and before
            # _addPacket is called.
            msg_id = self._getNextId()
            packet.setId(msg_id)
            if queue is None:
                if type(packet) is not Packets.Ping:
                    raise TypeError, 'Only Ping packet can be asked ' \
                        'without a queue, got a %r.' % (packet, )
            else:
                self.dispatcher.register(self, msg_id, queue)
            self._addPacket(packet)
            handlers = self._handlers
            t = not handlers.isPending() and time() or None
            handlers.emit(packet, timeout, on_timeout)
            self.updateTimeout(t)
            return msg_id
        finally:
            self.unlock()

    @lockCheckWrapper
    def answer(self, *args, **kw):
        return super(MTClientConnection, self).answer(*args, **kw)

    @lockCheckWrapper
    def checkTimeout(self, *args, **kw):
        return super(MTClientConnection, self).checkTimeout(*args, **kw)

    def close(self):
        self.lock()
        try:
            super(MTClientConnection, self).close()
        finally:
            self.release()

    @lockCheckWrapper
    def process(self, *args, **kw):
        return super(MTClientConnection, self).process(*args, **kw)

