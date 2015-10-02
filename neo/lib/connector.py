#
# Copyright (C) 2009-2015  Nexedi SA
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
        # disable Nagle algorithm to reduce latency
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.queued = []
        return self

    def queue(self, data):
        was_empty = not self.queued
        self.queued += data
        return was_empty

    def _error(self, op, exc):
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
        except socket.error, e:
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
            self.socket.listen(5)
        except socket.error, e:
            self.socket.close()
            self._error('listen', e)

    def ssl(self, ssl):
        self.socket = ssl.wrap_socket(self.socket,
            server_side=self.is_server,
            do_handshake_on_connect=False,
            suppress_ragged_eofs=False)
        self.__class__ = self.SSLHandshakeConnectorClass
        self.queued or self.queued.append('')

    def getError(self):
        return self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    def getDescriptor(self):
        # this descriptor must only be used by the event manager, where it
        # guarantee unicity only while the connector is opened and registered
        # in epoll
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
        except socket.error, e:
            self._error('accept', e)

    def receive(self, read_buf):
        try:
            data = self.socket.recv(4096)
        except socket.error, e:
            self._error('recv', e)
        if data:
            read_buf.append(data)
            return
        logging.debug('%r closed in recv', self)
        raise ConnectorException

    def send(self):
        msg = ''.join(self.queued)
        if msg:
            try:
                n = self.socket.send(msg)
            except socket.error, e:
                self._error('send', e)
            # Do nothing special if n == 0:
            # - it never happens for simple sockets;
            # - for SSL sockets, this is always the case unless everything
            #   could be sent.
            if n != len(msg):
                self.queued[:] = msg[n:],
                return False
            del self.queued[:]
        else:
            assert not self.queued
        return True


    def close(self):
        self.is_closed = True
        try:
            if self.connect_limit[self.addr] < time():
                del self.connect_limit[self.addr]
        except KeyError:
            pass
        return self.socket.close()

    def setReconnectionNoDelay(self):
        """Mark as successful so that we can reconnect without delay"""
        self.connect_limit.pop(self.addr, None)

    def __repr__(self):
        if self.is_closed is None:
            state = 'never opened'
        else:
            if self.is_closed:
                state = 'closed '
            else:
                state = 'opened '
            if self.is_server is None:
                state += 'listening'
            else:
                if self.is_server:
                    state += 'from '
                else:
                    state += 'to '
                state += str(self.addr)
        return '<%s at 0x%x fileno %s %s, %s>' % (self.__class__.__name__,
            id(self), '?' if self.is_closed else self.socket_fd,
            self.getAddress(), state)

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

    def _error(self, op, exc):
        if isinstance(exc, ssl.SSLError):
            logging.debug("%s failed for %s: %s", op, self, exc)
            raise ConnectorException
        SocketConnector._error(self, op, exc)

    def receive(self, read_buf):
        try:
            while 1:
                read_buf.append(self.socket.recv(4096))
        except ssl.SSLWantReadError:
            pass
        except socket.error, e:
            self._error('recv', e)

@overlay_connector_class
class _SSLHandshake(_SSL):

    def receive(self, read_buf=None):
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
        except socket.error, e:
            self._error('SSL handshake', e)
        if not self.queued[0]:
            del self.queued[0]
        self.__class__ = self.SSLConnectorClass
        cipher, proto, bits = self.socket.cipher()
        logging.debug("SSL handshake done for %s: %s %s", self, cipher, bits)
        if read_buf is None:
            return self.send()
        self.receive(read_buf)
        return self.queued

    send = receive


class ConnectorException(Exception):
    pass

class ConnectorDelayedConnection(ConnectorException):
    pass
