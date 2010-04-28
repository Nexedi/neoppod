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

from time import time

from neo import logging
from neo.locking import RLock

from neo.protocol import PacketMalformedError, Packets, ParserState
from neo.connector import ConnectorException, ConnectorTryAgainException, \
        ConnectorInProgressException, ConnectorConnectionRefusedException, \
        ConnectorConnectionClosedException
from neo.util import dump
from neo.logger import PACKET_LOGGER

from neo import attributeTracker
from neo.util import ReadBuffer
from neo.profiling import profiler_decorator

PING_DELAY = 6
PING_TIMEOUT = 5
INCOMING_TIMEOUT = 10
CRITICAL_TIMEOUT = 30

APPLY_HANDLER = object()

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


class HandlerSwitcher(object):
    _next_timeout = None
    _next_timeout_msg_id = None

    def __init__(self, connection, handler):
        self._connection = connection
        # pending handlers and related requests
        self._pending = [[{}, handler]]
        self._is_handling = False

    def clear(self):
        handler = self._pending[0][1]
        self._pending = [[{}, handler]]

    def isPending(self):
        return self._pending[0][0]

    def getHandler(self):
        return self._pending[0][1]

    @profiler_decorator
    def emit(self, request, timeout):
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
        request_dict[msg_id] = (answer_class, timeout)

    def checkTimeout(self, t):
        next_timeout = self._next_timeout
        if next_timeout is not None and next_timeout < t:
            result = self._next_timeout_msg_id
        else:
            result = None
        return result

    def handle(self, packet):
        assert not self._is_handling
        self._is_handling = True
        try:
            self._handle(packet)
        finally:
            self._is_handling = False

    @profiler_decorator
    def _handle(self, packet):
        assert len(self._pending) == 1 or self._pending[0][0]
        PACKET_LOGGER.dispatch(self._connection, packet, 'from')
        msg_id = packet.getId()
        (request_dict, handler) = self._pending[0]
        # notifications are not expected
        if not packet.isResponse():
            handler.packetReceived(self._connection, packet)
            return
        # checkout the expected answer class
        (klass, timeout) = request_dict.pop(msg_id, (None, None))
        if klass and isinstance(packet, klass) or packet.isError():
            handler.packetReceived(self._connection, packet)
        else:
            logging.error('Unexpected answer: %r', packet)
            notification = Packets.Notify('Unexpected answer: %r' % packet)
            self._connection.notify(notification)
            self._connection.abort()
            handler.peerBroken(self._connection)
        # apply a pending handler if no more answers are pending
        while len(self._pending) > 1 and not self._pending[0][0]:
            del self._pending[0]
            logging.debug('Apply handler %r', self._pending[0][1])
        if timeout == self._next_timeout:
            # Find next timeout and its msg_id
            timeout_list = []
            extend = timeout_list.extend
            for (request_dict, handler) in self._pending:
                extend(((timeout, msg_id) \
                    for msg_id, (_, timeout) in request_dict.iteritems()))
            if timeout_list:
                timeout_list.sort(key=lambda x: x[0])
                self._next_timeout, self._next_timeout_msg_id = timeout_list[0]
            else:
                self._next_timeout, self._next_timeout_msg_id = None, None

    @profiler_decorator
    def setHandler(self, handler):
        if len(self._pending) == 1 and not self._pending[0][0]:
            # nothing is pending, change immediately
            logging.debug('Set handler %r', handler)
            self._pending[0][1] = handler
        else:
            # put the next handler in queue
            logging.debug('Delay handler %r', handler)
            self._pending.append([{}, handler])


class Timeout(object):
    """ Keep track of connection-level timeouts """

    def __init__(self):
        self._ping_time = None
        self._critical_time = None

    def update(self, t, force=False):
        """
        Send occurred:
        - set ping time if earlier than existing one
        """
        ping_time = self._ping_time
        t += PING_DELAY
        if force or ping_time is None or t < ping_time:
            self._ping_time = t

    def refresh(self, t):
        """
        Recv occured:
        - reschedule next ping time
        - as this is an evidence that node is alive, remove pong expectation
        """
        self._ping_time = t + PING_DELAY
        self._critical_time = None

    def ping(self, t):
        """
        Ping send occured:
        - reschedule next ping time
        - set pong expectation
        """
        self._ping_time = t + PING_DELAY
        self._critical_time = t + PING_TIMEOUT

    def softExpired(self, t):
        """ Do we need to ping ? """
        return self._ping_time < t

    def hardExpired(self, t):
        """ Have we reached pong latest arrival time, if set ? """
        critical_time = self._critical_time
        return critical_time is not None and critical_time < t


class BaseConnection(object):
    """A base connection."""

    def __init__(self, event_manager, handler, connector, addr=None):
        assert connector is not None, "Need a low-level connector"
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self._handlers = HandlerSwitcher(self, handler)
        self._timeout = Timeout()
        event_manager.register(self)

    def checkTimeout(self, t):
        handlers = self._handlers
        if handlers.isPending():
            msg_id = handlers.checkTimeout(t)
            if msg_id is not None:
                logging.info('timeout for %r with %s:%d', msg_id,
                    *self.getAddress())
                self.close()
                self.getHandler().timeoutExpired(self)
            elif self._timeout.hardExpired(t):
                # critical time reach or pong not received, abort
                logging.info('timeout with %s:%d', *(self.getAddress()))
                self.notify(Packets.Notify('Timeout'))
                self.abort()
                self.getHandler().timeoutExpired(self)
            elif self._timeout.softExpired(t):
                self._timeout.ping(t)
                self.ping()

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

    __del__ = close

    def getHandler(self):
        return self._handlers.getHandler()

    def setHandler(self, handler):
        self._handlers.setHandler(handler)

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

attributeTracker.track(BaseConnection)

class ListeningConnection(BaseConnection):
    """A listen connection."""

    def __init__(self, event_manager, handler, addr, connector, **kw):
        logging.debug('listening to %s:%d', *addr)
        BaseConnection.__init__(self, event_manager, handler,
                                addr=addr, connector=connector)
        self.connector.makeListeningConnection(addr)
        self.em.addReader(self)

    def readable(self):
        try:
            new_s, addr = self.connector.getNewConnection()
            logging.debug('accepted a connection from %s:%d', *addr)
            handler = self.getHandler()
            new_conn = ServerConnection(self.getEventManager(), handler,
                connector=new_s, addr=addr)
            # A request for a node identification should arrive.
            self._timeout.update(time())
            handler.connectionAccepted(new_conn)
        except ConnectorTryAgainException:
            pass

    def writable(self):
        return False

    def isListening(self):
        return True


class Connection(BaseConnection):
    """A connection."""

    def __init__(self, event_manager, handler, connector, addr=None):
        BaseConnection.__init__(self, event_manager, handler,
                                connector=connector, addr=addr)
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

    def close(self):
        logging.debug('closing a connector for %s (%s:%d)',
                dump(self.uuid), *(self.addr))
        BaseConnection.close(self)
        if self._on_close is not None:
            self._on_close()
            self._on_close = None
        del self.write_buf[:]
        self.read_buf.clear()
        self._handlers.clear()

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
                packet = Packets.parse(self.read_buf, self._parser_state)
                if packet is None:
                    break
            except PacketMalformedError, msg:
                self.getHandler()._packetMalformed(self, msg)
                return
            self._timeout.refresh(time())
            packet_type = packet.getType()
            if packet_type == Packets.Ping:
                # Send a pong notification
                self.answer(Packets.Pong(), packet.getId())
            elif packet_type != Packets.Pong:
                # Skip PONG packets, its only purpose is refresh the timeout
                # generated upong ping.
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
        self._handlers.handle(packet)

    def pending(self):
        return self.connector is not None and self.write_buf

    def _closure(self, was_connected=True):
        assert self.connector is not None, self.whoSetConnector()
        handler = self.getHandler()
        self.close()
        if was_connected:
            handler.connectionClosed(self)
        else:
            handler.connectionFailed(self)

    @profiler_decorator
    def _recv(self):
        """Receive data from a connector."""
        try:
            data = self.connector.receive()
        except ConnectorTryAgainException:
            pass
        except ConnectorConnectionRefusedException:
            # should only occur while connecting
            self._closure(was_connected=False)
        except ConnectorConnectionClosedException:
            # connection resetted by peer, according to the man, this error
            # should not occurs but it seems it's false
            logging.debug('Connection reset by peer: %r', self.connector)
            self._closure()
        except:
            logging.debug('Unknown connection error: %r', self.connector)
            self._closure()
            # unhandled connector exception
            raise
        else:
            if not data:
                logging.debug('Connection %r closed in recv', self.connector)
                self._closure()
                return
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
            logging.debug('Connection reset by peer: %r', self.connector)
            self._closure()
        except:
            logging.debug('Unknown connection error: %r', self.connector)
            # unhandled connector exception
            self._closure()
            raise
        else:
            if not n:
                logging.debug('Connection %r closed in send', self.connector)
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

        was_empty = not bool(self.write_buf)

        PACKET_LOGGER.dispatch(self, packet, ' to ')
        self.write_buf.extend(packet.encode())

        if was_empty:
            # enable polling for writing.
            self.em.addWriter(self)

    @not_closed
    def notify(self, packet):
        """ Then a packet with a new ID """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        return msg_id

    @profiler_decorator
    @not_closed
    def ask(self, packet, timeout=CRITICAL_TIMEOUT):
        """
        Send a packet with a new ID and register the expectation of an answer
        """
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        t = time()
        # If there is no pending request, initialise timeout values.
        if not self._handlers.isPending():
            self._timeout.update(t, force=True)
        self._handlers.emit(packet, t + timeout)
        return msg_id

    @not_closed
    def answer(self, packet, msg_id=None):
        """ Answer to a packet by re-using its ID for the packet answer """
        if msg_id is None:
            msg_id = self.getPeerId()
        packet.setId(msg_id)
        assert packet.isResponse(), packet
        self._addPacket(packet)

    @not_closed
    def ping(self):
        packet = Packets.Ping()
        packet.setId(self._getNextId())
        self._addPacket(packet)


class ClientConnection(Connection):
    """A connection from this node to a remote node."""

    def __init__(self, event_manager, handler, addr, connector, **kw):
        self.connecting = True
        Connection.__init__(self, event_manager, handler, addr=addr,
                            connector=connector)
        handler.connectionStarted(self)
        try:
            try:
                self.connector.makeClientConnection(addr)
            except ConnectorInProgressException:
                event_manager.addWriter(self)
            else:
                self.connecting = False
                self.getHandler().connectionCompleted(self)
        except ConnectorConnectionRefusedException:
            self._closure(was_connected=False)
        except ConnectorException:
            # unhandled connector exception
            self._closure(was_connected=False)
            raise

    def writable(self):
        """Called when self is writable."""
        if self.connecting:
            err = self.connector.getError()
            if err:
                self._closure(was_connected=False)
                return
            else:
                self.connecting = False
                self.getHandler().connectionCompleted(self)
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

    def __init__(self, local_var, *args, **kwargs):
        # _lock is only here for lock debugging purposes. Do not use.
        self._lock = lock = RLock()
        self._local_var = local_var
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

    def notify(self, *args, **kw):
        self.lock()
        try:
            return super(MTClientConnection, self).notify(*args, **kw)
        finally:
            self.unlock()

    @profiler_decorator
    def ask(self, packet, timeout=CRITICAL_TIMEOUT):
        self.lock()
        try:
            # XXX: Here, we duplicate Connection.ask because we need to call
            # self.dispatcher.register after setId is called and before
            # _addPacket is called.
            msg_id = self._getNextId()
            packet.setId(msg_id)
            self.dispatcher.register(self, msg_id, self._local_var.queue)
            self._addPacket(packet)
            t = time()
            # If there is no pending request, initialise timeout values.
            if not self._handlers.isPending():
                self._timeout.update(t)
            self._handlers.emit(packet, t + timeout)
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

