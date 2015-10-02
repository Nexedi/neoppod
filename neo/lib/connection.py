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

from functools import wraps
from time import time

from . import attributeTracker, logging
from .connector import ConnectorException, ConnectorDelayedConnection
from .locking import RLock
from .protocol import uuid_str, Errors, \
        PacketMalformedError, Packets, ParserState
from .util import ReadBuffer

CRITICAL_TIMEOUT = 30

class ConnectionClosed(Exception):
    pass

class HandlerSwitcher(object):
    _is_handling = False
    _next_timeout = None
    _next_timeout_msg_id = None
    _next_on_timeout = None
    _pending = ({}, None),

    def __init__(self, handler):
        # pending handlers and related requests
        self._pending = []
        self.setHandler(handler)

    def close(self):
        self.__dict__.clear()

    def isPending(self):
        return bool(self._pending[0][0])

    def cancelRequests(self, conn, message):
        if self.isPending():
            p = Errors.ProtocolError(message)
            while True:
                request_dict, handler = self._pending[0]
                while request_dict:
                    msg_id, request = request_dict.popitem()
                    p.setId(msg_id)
                    handler.packetReceived(conn, p, request[3])
                if len(self._pending) == 1:
                    break
                del self._pending[0]

    def getHandler(self):
        return self._pending[0][1]

    def getLastHandler(self):
        """ Return the last (may be unapplied) handler registered """
        return self._pending[-1][1]

    def emit(self, request, timeout, on_timeout, kw={}):
        # register the request in the current handler
        _pending = self._pending
        if self._is_handling:
            # If this is called while handling a packet, the response is to
            # be excpected for the current handler...
            (request_dict, _) = _pending[0]
        else:
            # ...otherwise, queue for the latest handler
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
        request_dict[msg_id] = answer_class, timeout, on_timeout, kw

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

    def _handle(self, connection, packet):
        assert len(self._pending) == 1 or self._pending[0][0]
        logging.packet(connection, packet, False)
        if connection.isClosed() and packet.ignoreOnClosedConnection():
            logging.debug('Ignoring packet %r on closed connection %r',
                packet, connection)
            return
        msg_id = packet.getId()
        (request_dict, handler) = self._pending[0]
        # notifications are not expected
        if not packet.isResponse():
            handler.packetReceived(connection, packet)
            return
        # checkout the expected answer class
        try:
            klass, _, _, kw = request_dict.pop(msg_id)
        except KeyError:
            klass = None
            kw = {}
        if klass and isinstance(packet, klass) or packet.isError():
            handler.packetReceived(connection, packet, kw)
        else:
            logging.error('Unexpected answer %r in %r', packet, connection)
            if not connection.isClosed():
                notification = Packets.Notify('Unexpected answer: %r' % packet)
                connection.notify(notification)
                connection.abort()
            # handler.peerBroken(connection)
        # apply a pending handler if no more answers are pending
        while len(self._pending) > 1 and not self._pending[0][0]:
            del self._pending[0]
            logging.debug('Apply handler %r on %r', self._pending[0][1],
                    connection)
        if msg_id == self._next_timeout_msg_id:
            self._updateNextTimeout()

    def _updateNextTimeout(self):
        # Find next timeout and its msg_id
        next_timeout = None
        for pending in self._pending:
            for msg_id, (_, timeout, on_timeout, _) in pending[0].iteritems():
                if not next_timeout or timeout < next_timeout[0]:
                    next_timeout = timeout, msg_id, on_timeout
        self._next_timeout, self._next_timeout_msg_id, self._next_on_timeout = \
            next_timeout or (None, None, None)

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

    from .connector import SocketConnector as ConnectorClass
    KEEP_ALIVE = 60

    def __init__(self, event_manager, handler, connector, addr=None):
        assert connector is not None, "Need a low-level connector"
        self.em = event_manager
        self.connector = connector
        self.addr = addr
        self._handlers = HandlerSwitcher(handler)

    # XXX: do not use getHandler
    getHandler      = property(lambda self: self._handlers.getHandler)
    getLastHandler  = property(lambda self: self._handlers.getLastHandler)
    isPending       = property(lambda self: self._handlers.isPending)

    def cancelRequests(self, *args, **kw):
        return self._handlers.cancelRequests(self, *args, **kw)

    def getTimeout(self):
        pass

    def lockWrapper(self, func):
        return func

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
            self.em.unregister(self)
            self.connector.close()
            self.connector = None
            self.aborted = False

    def _getReprInfo(self):
        r = [
            ('uuid', uuid_str(self.getUUID())),
            ('address', '%s:%u' % self.addr if self.addr else '?'),
            ('handler', self.getHandler()),
        ]
        connector = self.connector
        if connector is None:
            return r, ['closed']
        r.append(('fd', connector.getDescriptor()))
        return r, ['aborted'] if self.isAborted() else []

    def __repr__(self):
        r, flags = self._getReprInfo()
        r = map('%s=%s'.__mod__, r)
        r += flags
        return '<%s(%s) at %x>' % (
            self.__class__.__name__,
            ', '.join(r),
            id(self),
        )

    def setHandler(self, handler):
        if self._handlers.setHandler(handler):
            logging.debug('Set handler %r on %r', handler, self)
        else:
            logging.debug('Delay handler %r on %r', handler, self)

    def getUUID(self):
        return None

    def isClosed(self):
        return self.connector is None or self.isAborted()

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

    def __init__(self, app, handler, addr):
        self._ssl = app.ssl
        logging.debug('listening to %s:%d', *addr)
        connector = self.ConnectorClass(addr)
        BaseConnection.__init__(self, app.em, handler, connector, addr)
        connector.makeListeningConnection()
        self.em.register(self)

    def readable(self):
        connector, addr = self.connector.accept()
        logging.debug('accepted a connection from %s:%d', *addr)
        handler = self.getHandler()
        new_conn = ServerConnection(self.em, handler, connector, addr)
        if self._ssl:
            connector.ssl(self._ssl)
            self.em.addWriter(new_conn)
        handler.connectionAccepted(new_conn)

    def getAddress(self):
        return self.connector.getAddress()

    def isListening(self):
        return True


class Connection(BaseConnection):
    """A connection."""

    # XXX: rename isPending, hasPendingMessages & pending methods

    connecting = False
    client = False
    server = False
    peer_id = None
    _next_timeout = None
    _timeout = 0

    def __init__(self, event_manager, *args, **kw):
        BaseConnection.__init__(self, event_manager, *args, **kw)
        self.read_buf = ReadBuffer()
        self.cur_id = 0
        self.aborted = False
        self.uuid = None
        self._queue = []
        self._on_close = None
        self._parser_state = ParserState()

    def _getReprInfo(self):
        r, flags = super(Connection, self)._getReprInfo()
        if self._queue:
            r.append(('len(queue)', len(self._queue)))
        if self._on_close is not None:
            r.append(('on_close', getattr(self._on_close, '__name__', '?')))
        flags.extend(x for x in ('aborted', 'connecting', 'client', 'server')
                       if getattr(self, x))
        return r, flags

    def setOnClose(self, callback):
        self._on_close = callback

    def isClient(self):
        return self.client

    def isServer(self):
        return self.server

    def asClient(self):
        try:
            del self.idle
            assert self.client
        except AttributeError:
            self.client = True

    def asServer(self):
        self.server = True

    def _closeClient(self):
        if self.server:
            del self.idle
            self.client = False
            self.notify(Packets.CloseClient())
        else:
            self.close()

    def closeClient(self):
        if self.connector is not None and self.client:
            self.idle = self._closeClient

    def isAborted(self):
        return self.aborted

    def getUUID(self):
        return self.uuid

    def setUUID(self, uuid):
        self.uuid = uuid

    def setPeerId(self, peer_id):
        assert peer_id is not None
        self.peer_id = peer_id

    def getPeerId(self):
        return self.peer_id

    def _getNextId(self):
        next_id = self.cur_id
        self.cur_id = (next_id + 1) & 0xffffffff
        return next_id

    def updateTimeout(self, t=None):
        if not self._queue:
            if not t:
                t = self._next_timeout - self._timeout
            self._timeout = self._handlers.getNextTimeout() or self.KEEP_ALIVE
            self._next_timeout = t + self._timeout

    def getTimeout(self):
        if not self._queue:
            return self._next_timeout

    def onTimeout(self):
        handlers = self._handlers
        if handlers.isPending():
            msg_id = handlers.timeout(self)
            if msg_id is None:
                self._next_timeout = time() + self._timeout
            else:
                logging.info('timeout for #0x%08x with %r', msg_id, self)
                self.close()
        else:
            self.idle()

    def abort(self):
        """Abort dealing with this connection."""
        logging.debug('aborting a connector for %r', self)
        self.aborted = True
        assert self.pending()
        if self._on_close is not None:
            self._on_close()
            self._on_close = None

    def writable(self):
        """Called when self is writable."""
        try:
            if self.connector.send():
                if self.aborted:
                    self.close()
                else:
                    self.em.removeWriter(self)
        except ConnectorException:
            self._closure()

    def readable(self):
        """Called when self is readable."""
        # last known remote activity
        self._next_timeout = time() + self._timeout
        read_buf = self.read_buf
        try:
            try:
                if self.connector.receive(read_buf):
                    self.em.addWriter(self)
            finally:
                # A connector may read some data
                # before raising ConnectorException
                while 1:
                    packet = Packets.parse(read_buf, self._parser_state)
                    if packet is None:
                        break
                    self._queue.append(packet)
        except ConnectorException:
            self._closure()
        except PacketMalformedError, e:
            logging.error('malformed packet from %r: %s', self, e)
            self._closure()
        if self.aborted:
            self.em.removeReader(self)
        return not not self._queue

    def hasPendingMessages(self):
        """
          Returns True if there are messages queued and awaiting processing.
        """
        return not not self._queue

    def process(self):
        """
          Process a pending packet.
        """
        # check out packet and process it with current handler
        self._handlers.handle(self, self._queue.pop(0))
        self.updateTimeout()

    def pending(self):
        connector = self.connector
        return connector is not None and connector.queued

    @property
    def setReconnectionNoDelay(self):
        return self.connector.setReconnectionNoDelay

    def close(self):
        if self.connector is None:
            assert self._on_close is None
            assert not self.read_buf
            assert not self.isPending()
            return
        # process the network events with the last registered handler to
        # solve issues where a node is lost with pending handlers and
        # create unexpected side effects.
        handler = self._handlers.getLastHandler()
        super(Connection, self).close()
        if self._on_close is not None:
            self._on_close()
            self._on_close = None
        self.read_buf.clear()
        try:
            if self.connecting:
                handler.connectionFailed(self)
                self.connecting = False
            else:
                handler.connectionClosed(self)
        finally:
            self._handlers.close()

    def _closure(self):
        assert self.connector is not None, self.whoSetConnector()
        while self._queue:
            self._handlers.handle(self, self._queue.pop(0))
        self.close()

    def _addPacket(self, packet):
        """Add a packet into the write buffer."""
        if self.connector.queue(packet.encode()):
            # enable polling for writing.
            self.em.addWriter(self)
        logging.packet(self, packet, True)

    def notify(self, packet):
        """ Then a packet with a new ID """
        if self.isClosed():
            raise ConnectionClosed
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        return msg_id

    def ask(self, packet, timeout=CRITICAL_TIMEOUT, on_timeout=None, **kw):
        """
        Send a packet with a new ID and register the expectation of an answer
        """
        if self.isClosed():
            raise ConnectionClosed
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        handlers = self._handlers
        t = None if handlers.isPending() else time()
        handlers.emit(packet, timeout, on_timeout, kw)
        if not self._queue:
            next_timeout = self._next_timeout
            self.updateTimeout(t)
            if self._next_timeout < next_timeout:
                self.em.wakeup()
        return msg_id

    def answer(self, packet, msg_id=None):
        """ Answer to a packet by re-using its ID for the packet answer """
        if self.isClosed():
            raise ConnectionClosed
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
    client = True

    def __init__(self, app, handler, node):
        self._ssl = app.ssl
        addr = node.getAddress()
        connector = self.ConnectorClass(addr)
        Connection.__init__(self, app.em, handler, connector, addr)
        node.setConnection(self)
        handler.connectionStarted(self)
        self._connect()

    def _connect(self):
        try:
            connected = self.connector.makeClientConnection()
        except ConnectorDelayedConnection, c:
            connect_limit, = c.args
            self.getTimeout = lambda: connect_limit
            self.onTimeout = self._delayedConnect
            self.em.register(self, timeout_only=True)
        except ConnectorException:
            self._closure()
        else:
            self.em.register(self)
            if connected:
                self._connectionCompleted()
                # A client connection usually has a pending packet to send
                # from the beginning. It would be too smart to detect when
                # it's not required to poll for writing.
            self.em.addWriter(self)

    def _delayedConnect(self):
        del self.getTimeout, self.onTimeout
        self._connect()

    def writable(self):
        """Called when self is writable."""
        if self.connector.getError():
            self._closure()
        else:
            self._connectionCompleted()
            self.writable()

    def _connectionCompleted(self):
        if self._ssl:
            self.connector.ssl(self._ssl)
        self.writable = self.lockWrapper(super(ClientConnection, self).writable)
        self.connecting = False
        self.updateTimeout(time())
        self.getHandler().connectionCompleted(self)


class ServerConnection(Connection):
    """A connection from a remote node to this node."""

    # Both server and client must check the connection, in case:
    # - the remote crashed brutally (i.e. without closing TCP connections)
    # - or packets sent by the remote are dropped (network failure)
    # Use different timeout so that in normal condition, server never has to
    # ping the client. Otherwise, it would do it about half of the time.
    KEEP_ALIVE = Connection.KEEP_ALIVE + 5

    server = True

    def __init__(self, *args, **kw):
        Connection.__init__(self, *args, **kw)
        self.em.register(self)
        self.updateTimeout(time())


class MTConnectionType(type):

    def __init__(cls, *args):
        if __debug__:
            for name in 'answer',:
                setattr(cls, name, cls.lockCheckWrapper(name))
        for name in ('_delayedConnect', 'close', 'notify', 'onTimeout',
                     'process', 'readable', 'writable'):
            setattr(cls, name, cls.__class__.lockWrapper(cls, name))

    def lockCheckWrapper(cls, name):
        def wrapper(self, *args, **kw):
            # XXX: Unfortunately, RLock does not has any public method
            #      to test whether we own the lock or not.
            assert self.lock._is_owned(), (self, args, kw)
            return getattr(super(cls, self), name)(*args, **kw)
        return wraps(getattr(cls, name).im_func)(wrapper)

    def lockWrapper(cls, name):
        def wrapper(self, *args, **kw):
            with self.lock:
                return getattr(super(cls, self), name)(*args, **kw)
        return wraps(getattr(cls, name).im_func)(wrapper)


class MTClientConnection(ClientConnection):
    """A Multithread-safe version of ClientConnection."""

    __metaclass__ = MTConnectionType

    def lockWrapper(self, func):
        lock = self.lock
        def wrapper(*args, **kw):
            with lock:
                return func(*args, **kw)
        return wrapper

    def __init__(self, *args, **kwargs):
        self.lock = lock = RLock()
        self.dispatcher = kwargs.pop('dispatcher')
        with lock:
            super(MTClientConnection, self).__init__(*args, **kwargs)

    def ask(self, packet, timeout=CRITICAL_TIMEOUT, on_timeout=None,
            queue=None, **kw):
        with self.lock:
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
            t = None if handlers.isPending() else time()
            handlers.emit(packet, timeout, on_timeout, kw)
            if not self._queue:
                next_timeout = self._next_timeout
                self.updateTimeout(t)
                if self._next_timeout < next_timeout:
                    self.em.wakeup()
            return msg_id
