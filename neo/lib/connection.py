#
# Copyright (C) 2006-2019  Nexedi SA
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
import msgpack
from msgpack.exceptions import UnpackValueError

from . import attributeTracker, logging
from .connector import ConnectorException, ConnectorDelayedConnection
from .locking import RLock
from .protocol import uuid_str, Errors, PacketMalformedError, Packets, \
    Unpacker

@apply
class dummy_read_buffer(msgpack.Unpacker):
    def feed(self, _):
        pass

class ConnectionClosed(Exception):
    pass

class HandlerSwitcher(object):
    _is_handling = False
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
                    handler.packetReceived(conn, p, request[1])
                if len(self._pending) == 1:
                    break
                del self._pending[0]

    def getHandler(self):
        return self._pending[0][1]

    def getLastHandler(self):
        """ Return the last (may be unapplied) handler registered """
        return self._pending[-1][1]

    def emit(self, request, kw={}):
        # register the request in the current handler
        _pending = self._pending
        if self._is_handling:
            # If this is called while handling a packet, the response is to
            # be expected for the current handler...
            (request_dict, _) = _pending[0]
        else:
            # ...otherwise, queue for the latest handler
            assert len(_pending) == 1 or _pending[0][0]
            (request_dict, _) = _pending[-1]
        msg_id = request.getId()
        answer_class = request.getAnswerClass()
        assert answer_class is not None, "Not a request"
        assert msg_id not in request_dict, "Packet id already expected"
        request_dict[msg_id] = answer_class, kw

    def handle(self, connection, packet):
        assert not self._is_handling
        self._is_handling = True
        try:
            self._handle(connection, packet)
        finally:
            self._is_handling = False

    def _handle(self, connection, packet):
        pending = self._pending
        assert len(pending) == 1 or pending[0][0], pending
        logging.packet(connection, packet, False)
        if connection.isClosed() and (connection.isAborted() or
                                      packet.ignoreOnClosedConnection()):
            logging.debug('Ignoring packet %r on closed connection %r',
                packet, connection)
            return
        if not packet.isResponse(): # notification
            # XXX: If there are several handlers, which one to use ?
            pending[0][1].packetReceived(connection, packet)
            return
        msg_id = packet.getId()
        request_dict, handler = pending[0]
        # checkout the expected answer class
        try:
            klass, kw = request_dict.pop(msg_id)
        except KeyError:
            klass = None
            kw = {}
        try:
            if klass and isinstance(packet, klass) or packet.isError():
                handler.packetReceived(connection, packet, kw)
            else:
                logging.error('Unexpected answer %r in %r', packet, connection)
                if not connection.isClosed():
                    connection.answer(Errors.ProtocolError(
                        'Unexpected answer: %r' % packet))
                    connection.abort()
        finally:
            # apply a pending handler if no more answers are pending
            while len(pending) > 1 and not pending[0][0]:
                del pending[0]
                logging.debug('Apply handler %r on %r', pending[0][1],
                    connection)

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

        In the past, ask() took a timeout parameter as a way to close the
        connection if the remote node was too long to reply, with the idea
        that something went wrong. There was no known bug but this feature was
        actually a bad idea.

        It is impossible to test whether the remote node is in good state or
        not. The experience shows that timeouts were always triggered because
        the remote nodes were simply too slow. Waiting remains the best option
        and anything else would only make things worse.

        The only case where it could make sense to react on a slow request is
        when there is redundancy, more exactly for read requests to storage
        nodes when there are replicas. A client node could resend its request
        to another node, _without_ breaking the first connection (then wait for
        the first reply and ignore the other).

        The previous timeout implementation (before May 2017) was not well
        suited to support the above use case so most of the code has been
        removed, but it may contain some interesting parts.

        Currently, since applicative pings have been replaced by TCP
        keepalives, timeouts are only used for 2 things:
        - to avoid reconnecting too fast
        - to close idle client connections
    """

    from .connector import SocketConnector as ConnectorClass

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
            self.em.unregister(self, True)
            self.connector = None
            self.aborted = False

    def _getReprInfo(self):
        r = [
            ('nid', uuid_str(self.getUUID())),
            ('address', ('[%s]:%s' if ':' in self.addr[0] else '%s:%s')
                        % self.addr if self.addr else '?'),
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
        changed = self._handlers.setHandler(handler)
        if changed:
            logging.debug('Handler changed on %r', self)
        else:
            logging.debug('Delay handler %r on %r', handler, self)
        return changed

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
        conn = ServerConnection(self.em, self.getHandler(), connector, addr)
        if self._ssl:
            conn.connecting = True
            connector.ssl(self._ssl, conn._connected)
            # Nothing to send as long as we haven't received a ClientHello
            # message.
        else:
            conn._connected()
            self.em.addWriter(conn) # for HANDSHAKE_PACKET

    def getAddress(self):
        return self.connector.getAddress()

    def isListening(self):
        return True


class Connection(BaseConnection):
    """A connection."""

    # XXX: rename isPending, hasPendingMessages & pending methods

    buffering = False
    connecting = True
    client = False
    server = False
    peer_id = None
    _total_unpacked = 0
    _timeout = None

    def __init__(self, event_manager, *args, **kw):
        BaseConnection.__init__(self, event_manager, *args, **kw)
        self.read_buf = Unpacker()
        self.cur_id = 0
        self.aborted = False
        self.uuid = None
        self._queue = []
        self._on_close = None

    def _getReprInfo(self):
        r, flags = super(Connection, self)._getReprInfo()
        if self._queue:
            r.append(('len(queue)', len(self._queue)))
        if self._on_close is not None:
            r.append(('on_close', getattr(self._on_close, '__name__', '?')))
        flags.extend(x for x in ('connecting', 'client', 'server')
                       if getattr(self, x))
        return r, flags

    def setOnClose(self, callback):
        assert not self.isClosed(), self
        self._on_close = callback

    def isClient(self):
        return self.client

    def isServer(self):
        return self.server

    def asClient(self):
        try:
            del self._timeout
        except AttributeError:
            self.client = True
        else:
            assert self.client

    def asServer(self):
        self.server = True

    def _closeClient(self):
        if self.server:
            del self._timeout
            self.client = False
            self.send(Packets.CloseClient())
        else:
            self.close()

    def closeClient(self):
        # Currently, the only usage that is really useful is between a backup
        # storage node and an upstream one, to avoid:
        # - maintaining many connections for nothing when there's no write
        #   activity for a long time (and waste resources with keepalives)
        # - reconnecting too often (i.e. be reactive) when there's moderate
        #   activity (think of a timer with a period of 1 minute)
        if self.connector is not None and self.client:
            self._timeout = time() + 100

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

    def getTimeout(self):
        if not self._queue:
            return self._timeout

    def onTimeout(self):
        assert self._timeout
        self._closeClient()

    def abort(self):
        """Abort dealing with this connection."""
        assert self.pending()
        if self.connecting:
            self.close()
            return
        logging.debug('aborting a connector for %r', self)
        self.aborted = True
        self.read_buf = dummy_read_buffer
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

    def _parse(self):
        from .protocol import HANDSHAKE_PACKET, Packets
        read_buf = self.read_buf
        handshake = read_buf.read_bytes(len(HANDSHAKE_PACKET))
        if handshake != HANDSHAKE_PACKET:
            if HANDSHAKE_PACKET.startswith(handshake): # unlikely so tested last
                # Not enough data and there's no API to know it in advance.
                # Put it back.
                read_buf.feed(handshake)
                return
            if HANDSHAKE_PACKET.startswith(handshake[:5]):
                logging.warning('Protocol version mismatch with %r', self)
            else:
                logging.debug('Rejecting non-NEO %r', self)
            raise ConnectorException
        read_next = read_buf.next
        read_pos = read_buf.tell
        def parse():
            try:
                msg_id, msg_type, args = read_next()
            except StopIteration:
                return
            except UnpackValueError as e:
                raise PacketMalformedError(str(e))
            try:
                packet_klass = Packets[msg_type]
            except KeyError:
                raise PacketMalformedError('Unknown packet type')
            pos = read_pos()
            packet = packet_klass(*args)
            packet.setId(msg_id)
            packet.size = pos - self._total_unpacked
            self._total_unpacked = pos
            return packet
        self._parse = parse
        return parse()

    def readable(self):
        """Called when self is readable."""
        # last known remote activity
        try:
            try:
                if self.connector.receive(self.read_buf):
                    self.em.addWriter(self)
            finally:
                # A connector may read some data
                # before raising ConnectorException
                while 1:
                    packet = self._parse()
                    if packet is None:
                        break
                    self._queue.append(packet)
        except ConnectorException:
            self._closure()
        except PacketMalformedError, e:
            logging.error('malformed packet from %r: %s', self, e)
            self._closure()
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

    def pending(self):
        connector = self.connector
        return connector is not None and connector.queued

    @property
    def setReconnectionNoDelay(self):
        try:
            return self.connector.setReconnectionNoDelay
        except AttributeError:
            raise ConnectionClosed

    def close(self):
        if self.connector is None:
            assert self._on_close is None
            assert not self.read_buf.read_bytes(1)
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
        self.read_buf = dummy_read_buffer
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
            if packet.nodelay or 65536 < self.connector.queue_size:
                assert not self.buffering
                # enable polling for writing.
                self.em.addWriter(self)
            else:
                self.buffering = True
        elif self.buffering and (65536 < self.connector.queue_size
                                 or packet.nodelay):
            self.buffering = False
            self.em.addWriter(self)
        logging.packet(self, packet, True)

    def send(self, packet, msg_id=None):
        """ Then a packet with a new ID """
        if self.isClosed():
            raise ConnectionClosed
        packet.setId(self._getNextId() if msg_id is None else msg_id)
        self._addPacket(packet)

    def ask(self, packet, **kw):
        """
        Send a packet with a new ID and register the expectation of an answer
        """
        if self.isClosed():
            raise ConnectionClosed
        msg_id = self._getNextId()
        packet.setId(msg_id)
        self._addPacket(packet)
        self._handlers.emit(packet, kw)
        return msg_id

    def answer(self, packet):
        """ Answer to a packet by re-using its ID for the packet answer """
        assert packet.isResponse(), packet
        if self.isClosed():
            if packet.ignoreOnClosedConnection() and not packet.isError():
                raise ConnectionClosed
            return
        packet.setId(self.peer_id)
        self._addPacket(packet)

    def _connected(self):
        self.connecting = False
        self.getHandler().connectionCompleted(self)


class ClientConnection(Connection):
    """A connection from this node to a remote node."""

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
                self._maybeConnected()
                # There's always the protocol version to send.
            self.em.addWriter(self)

    def _delayedConnect(self):
        del self.getTimeout, self.onTimeout
        self._connect()

    def writable(self):
        """Called when self is writable."""
        if self.connector.getError():
            self._closure()
        else:
            self._maybeConnected()
            self.writable()

    def _maybeConnected(self):
        self.writable = self.lockWrapper(super(ClientConnection, self).writable)
        if self._ssl:
            self.connector.ssl(self._ssl, self._connected)
        else:
            self._connected()


class ServerConnection(Connection):
    """A connection from a remote node to this node."""

    server = True

    def __init__(self, *args, **kw):
        Connection.__init__(self, *args, **kw)
        self.em.register(self)


class MTConnectionType(type):

    def __init__(cls, *args):
        if __debug__:
            for name in 'answer',:
                setattr(cls, name, cls.lockCheckWrapper(name))
        for name in 'close', 'send':
            setattr(cls, name, cls.__class__.lockWrapper(cls, name))
        for name in ('_delayedConnect', 'onTimeout',
                     'process', 'readable', 'writable'):
            setattr(cls, name, cls.__class__.lockWrapper(cls, name, True))

    def lockCheckWrapper(cls, name):
        def wrapper(self, *args, **kw):
            # XXX: Unfortunately, RLock does not has any public method
            #      to test whether we own the lock or not.
            assert self.lock._is_owned(), (self, args, kw)
            return getattr(super(cls, self), name)(*args, **kw)
        return wraps(getattr(cls, name).im_func)(wrapper)

    def lockWrapper(cls, name, maybe_closed=False):
        if maybe_closed:
            def wrapper(self):
                with self.lock:
                    if self.isClosed():
                        logging.info("%r.%s()", self, name)
                    else:
                        return getattr(super(cls, self), name)()
        else:
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

    # Alias without lock (cheaper than super())
    _ask = ClientConnection.ask.__func__

    def ask(self, packet, queue=None, **kw):
        with self.lock:
            if queue is None:
                if type(packet) is Packets.Ping:
                    return self._ask(packet, **kw)
                raise TypeError('Only Ping packet can be asked'
                    ' without a queue, got a %r.' % packet)
            msg_id = self._ask(packet, **kw)
            self.dispatcher.register(self, msg_id, queue)
        return msg_id

    # Currently, on connected connections, we only use timeouts for
    # closeClient, which is never used for MTClientConnection.
    # So we disable the logic completely as a precaution, and for performance.
    # What is specific to MTClientConnection is that the poll thread must be
    # woken up whenever the timeout is changed to a smaller value.

    def closeClient(self):
        # For example here, in addition to what the super method does,
        # we may have to call `self.em.wakeup()`
        raise NotImplementedError

    def getTimeout(self):
        pass

    def onTimeout(self):
        # It is possible that another thread manipulated the connection while
        # getting a timeout from epoll. Only the poll thread fills _queue
        # so we know that it is empty, but we may have to check timeout values
        # again (i.e. compare time() with the result of getTimeout()).
        raise NotImplementedError

    ###
