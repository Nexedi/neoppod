#
# Copyright (C) 2009-2019  Nexedi SA
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

import socket
import ssl
import errno
from time import time
from . import logging
from .protocol import HANDSHAKE_PACKET

# Global connector registry.
# Fill by calling registerConnectorHandler.
# Read by calling SocketConnector.__new__
connector_registry = {}
def registerConnectorHandler(connector_handler):
    connector_registry[connector_handler.af_type] = connector_handler

class SocketConnector(object):
    """ This class is a wrapper for a socket """

    is_closed = is_server = None
    connect_limit = {}
    CONNECT_LIMIT = 1
    KEEPALIVE = 60, 3, 10
    SOMAXCONN = 5 # for threaded tests

    def __new__(cls, addr, s=None):
        if s is None:
            host, port = addr
            for af_type, cls in connector_registry.iteritems():
                try :
                    socket.inet_pton(af_type, host)
                    break
                except socket.error:
                    pass
            else:
                raise ValueError("Unknown type of host", host)
        self = object.__new__(cls)
        self.addr = cls._normAddress(addr)
        if s is None:
            s = socket.socket(af_type, socket.SOCK_STREAM)
        else:
            self.is_server = True
            self.is_closed = False
        self.socket = s
        self.socket_fd = s.fileno()
        # always use non-blocking sockets
        s.setblocking(0)
        # TCP keepalive, enabled on both sides to detect:
        # - remote host crash
        # - network failure
        # They're more efficient than applicative pings and we don't want
        # to consider the connection dead if the remote node is busy.
        # The following 3 lines are specific to Linux. It seems that OSX
        # has similar options (TCP_KEEPALIVE/TCP_KEEPINTVL/TCP_KEEPCNT),
        # and Windows has SIO_KEEPALIVE_VALS (fixed count of 10).
        idle, cnt, intvl = self.KEEPALIVE
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # disable Nagle algorithm to reduce latency
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.queued = [HANDSHAKE_PACKET]
        self.queue_size = len(HANDSHAKE_PACKET)
        return self

    def queue(self, data):
        was_empty = not self.queued
        self.queued.append(data)
        self.queue_size += len(data)
        return was_empty

    def _error(self, op, exc=None):
        if exc is None:
            logging.debug('%r closed in %s', self, op)
        else:
            logging.debug("%s failed for %s: %s (%s)",
                op, self, errno.errorcode[exc.errno], exc.strerror)
        raise ConnectorException

    # Threaded tests monkey-patch the following 2 operations.
    _connect = lambda self, addr: self.socket.connect(addr)
    _bind = lambda self, addr: self.socket.bind(addr)

    def makeClientConnection(self):
        assert self.is_closed is None
        addr = self.addr
        try:
            connect_limit = self.connect_limit[addr]
            if time() < connect_limit:
                # Next call to queue() must return False
                # in order not to enable polling for writing.
                self.queued or self.queued.append('')
                raise ConnectorDelayedConnection(connect_limit)
            if self.queued and not self.queued[0]:
                del self.queued[0]
        except KeyError:
            pass
        self.connect_limit[addr] = time() + self.CONNECT_LIMIT
        self.is_server = self.is_closed = False
        try:
            self._connect(addr)
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                return False
            self._error('connect', e)
        return True

    def makeListeningConnection(self):
        assert self.is_closed is None
        self.is_closed = False
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._bind(self.addr)
            self.socket.listen(self.SOMAXCONN)
        except socket.error as e:
            self.is_closed = True
            self.socket.close()
            self._error('listen', e)

    def ssl(self, context, on_handshake_done=None):
        self.socket = context.wrap_socket(self.socket,
            server_side=self.is_server,
            do_handshake_on_connect=False)
        self.__class__ = self.SSLHandshakeConnectorClass
        self.on_handshake_done = on_handshake_done
        self.queued or self.queued.append('')

    def getError(self):
        return self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    def getDescriptor(self):
        # this descriptor must only be used by the event manager, where it
        # guarantee uniqueness only while the connector is opened and
        # registered in epoll
        return self.socket_fd

    @staticmethod
    def _normAddress(addr):
        return addr

    def getAddress(self):
        return self._normAddress(self.socket.getsockname())

    def accept(self):
        try:
            s, addr = self.socket.accept()
            s = self.__class__(addr, s)
            return s, s.addr
        except socket.error as e:
            self._error('accept', e)

    def receive(self, read_buf):
        try:
            data = self.socket.recv(65536)
        except socket.error as e:
            self._error('recv', e)
        if data:
            read_buf.feed(data)
            return
        self._error('recv')

    def send(self):
        # XXX: Inefficient for big packets. In any case, we should make sure
        #      that 'msg' does not exceed 2GB with SSL (OverflowError).
        #      Before commit 1a064725b81a702a124d672dba2bcae498980c76,
        #      this happened when many big AddObject packets were sent
        #      for a single replication chunk.
        msg = ''.join(self.queued)
        if msg:
            try:
                n = self.socket.send(msg)
            except socket.error as e:
                self._error('send', e)
            # Do nothing special if n == 0:
            # - it never happens for simple sockets;
            # - for SSL sockets, this is always the case unless everything
            #   could be sent.
            if n != len(msg):
                self.queued[:] = msg[n:],
                self.queue_size -= n
                return False
            del self.queued[:]
            self.queue_size = 0
        else:
            assert not self.queued
        return True


    def shutdown(self):
        self.is_closed = True
        try:
            if self.connect_limit[self.addr] < time():
                del self.connect_limit[self.addr]
        except KeyError:
            pass
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            if e.errno != errno.ENOTCONN:
                raise
        return self.socket.close

    def setReconnectionNoDelay(self):
        """Mark as successful so that we can reconnect without delay"""
        self.connect_limit.pop(self.addr, None)

    def __repr__(self):
        if self.is_closed is None:
            state = ', never opened'
        else:
            if self.is_closed:
                state = ', closed '
            else:
                state = ' fileno %s %s, opened ' % (
                    self.socket_fd, self.getAddress())
            if self.is_server is None:
                state += 'listening'
            else:
                if self.is_server:
                    state += 'from '
                else:
                    state += 'to '
                state += str(self.addr)
        return '<%s at 0x%x%s>' % (self.__class__.__name__, id(self), state)

class SocketConnectorIPv4(SocketConnector):
    " Wrapper for IPv4 sockets"
    af_type = socket.AF_INET

class SocketConnectorIPv6(SocketConnector):
    " Wrapper for IPv6 sockets"
    af_type = socket.AF_INET6

    @staticmethod
    def _normAddress(addr):
        return addr[:2]

registerConnectorHandler(SocketConnectorIPv4)
registerConnectorHandler(SocketConnectorIPv6)


def overlay_connector_class(cls):
    name = cls.__name__[1:]
    alias = name + 'ConnectorClass'
    for base in connector_registry.itervalues():
        setattr(base, alias, type(name + base.__name__,
            cls.__bases__ + (base,), cls.__dict__))
    return cls

@overlay_connector_class
class _SSL:

    def _error(self, op, exc=None):
        if isinstance(exc, ssl.SSLError):
            if not isinstance(exc, ssl.SSLEOFError):
                logging.debug("%s failed for %s: %s", op, self, exc)
                raise ConnectorException
            exc = None
        SocketConnector._error(self, op, exc)

    def receive(self, read_buf):
        try:
            while 1:
                data = self.socket.recv(4096)
                if not data:
                    self._error('recv', None)
                    return
                read_buf.feed(data)
        except ssl.SSLWantReadError:
            pass
        except socket.error as e:
            self._error('recv', e)

@overlay_connector_class
class _SSLHandshake(_SSL):

    # WKRD: Unfortunately, SSL_do_handshake(3SSL) does not try to reject
    #       non-SSL connections as soon as possible, by checking the first
    #       byte. It even does nothing before receiving a full TLSPlaintext
    #       frame (5 bytes).
    #       The NEO protocol is such that a client connection is always the
    #       first to send a packet, as soon as the connection is established,
    #       and without waiting that the protocol versions are checked.
    #       So in practice, non-SSL connection to SSL would never hang, but
    #       there's another issue: such case results in WRONG_VERSION_NUMBER
    #       instead of something like UNEXPECTED_RECORD, because the SSL
    #       version is checked first.
    #       For better logging, we try to detect non-SSL connections with
    #       MSG_PEEK. This only works reliably on server side.
    #       For SSL client connections, 2 things may prevent the workaround to
    #       log that the remote node has not enabled SSL:
    #       - non-SSL data received (or connection closed) before the first
    #         call to 'recv' in 'do_handshake'
    #       - the server connection detects a wrong protocol version before it
    #         sent its one

    def _handshake(self, read_buf=None):
        # ???Writer  |  send  | receive
        # -----------+--------+--------
        # want read  | remove |   -
        # want write |   -    |  add
        try:
            self.socket.do_handshake()
        except ssl.SSLWantReadError:
            return read_buf is None
        except ssl.SSLWantWriteError:
            return read_buf is not None
        except socket.error as e:
            # OpenSSL 1.1 may raise socket.error(0)
            # where previous versions raised SSLEOFError.
            self._error('send' if read_buf is None else 'recv',
                        e if e.errno else None)
        if not self.queued[0]:
            del self.queued[0]
        del self.receive, self.send
        self.__class__ = self.SSLConnectorClass
        cipher, proto, bits = self.socket.cipher()
        logging.debug("SSL handshake done for %s: %s %s", self, cipher, bits)
        if self.on_handshake_done:
            self.on_handshake_done()
        del self.on_handshake_done
        if read_buf is None:
            return self.send()
        self.receive(read_buf)
        return self.queued

    def send(self, read_buf=None):
        handshake = self.receive = self.send = self._handshake
        return handshake(read_buf)

    def receive(self, read_buf):
        try:
            content_type = self.socket._sock.recv(1, socket.MSG_PEEK)
        except socket.error as e:
            self._error('recv', e)
        if content_type == '\26': # handshake
            return self.send(read_buf)
        if content_type:
            logging.debug('Rejecting non-SSL %r', self)
            raise ConnectorException
        self._error('recv')


class ConnectorException(Exception):
    pass

class ConnectorDelayedConnection(ConnectorException):
    pass
