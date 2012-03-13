#
# Copyright (C) 2009-2011  Nexedi SA
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
import errno

# Global connector registry.
# Fill by calling registerConnectorHandler.
# Read by calling getConnectorHandler.
connector_registry = {}
DEFAULT_CONNECTOR = 'SocketConnectorIPv4'

def registerConnectorHandler(connector_handler):
    connector_registry[connector_handler.__name__] = connector_handler

def getConnectorHandler(connector=None):
    if connector is None:
        connector = DEFAULT_CONNECTOR
    if isinstance(connector, basestring):
        connector_handler = connector_registry.get(connector)
    else:
        # Allow to directly provide a handler class without requiring to
        # register it first.
        connector_handler = connector
    return connector_handler

class SocketConnector:
    """ This class is a wrapper for a socket """

    is_listening = False
    remote_addr = None
    is_closed = None

    def __init__(self, s=None, accepted_from=None):
        self.accepted_from = accepted_from
        if accepted_from is not None:
            self.remote_addr = accepted_from
            self.is_listening = False
            self.is_closed = False
        if s is None:
            self.socket = socket.socket(self.af_type, socket.SOCK_STREAM)
        else:
            self.socket = s
        self.socket_fd = self.socket.fileno()
        # always use non-blocking sockets
        self.socket.setblocking(0)
        # disable Nagle algorithm to reduce latency
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def makeClientConnection(self, addr):
        self.is_closed = False
        self.remote_addr = addr
        try:
            self.socket.connect(addr)
        except socket.error, (err, errmsg):
            if err == errno.EINPROGRESS:
                raise ConnectorInProgressException
            if err == errno.ECONNREFUSED:
                raise ConnectorConnectionRefusedException
            raise ConnectorException, 'makeClientConnection to %s failed:' \
                ' %s:%s' % (addr, err, errmsg)

    def makeListeningConnection(self, addr):
        self.is_closed = False
        self.is_listening = True
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(addr)
            self.socket.listen(5)
        except socket.error, (err, errmsg):
            self.socket.close()
            raise ConnectorException, 'makeListeningConnection on %s failed:' \
                    ' %s:%s' % (addr, err, errmsg)

    def getError(self):
        return self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    def getAddress(self):
        raise NotImplementedError

    def getDescriptor(self):
        # this descriptor must only be used by the event manager, where it
        # guarantee unicity only while the connector is opened and registered
        # in epoll
        return self.socket_fd

    def getNewConnection(self):
        try:
            (new_s, addr) = self._accept()
            new_s = self.__class__(new_s, accepted_from=addr)
            return (new_s, addr)
        except socket.error, (err, errmsg):
            if err == errno.EAGAIN:
                raise ConnectorTryAgainException
            raise ConnectorException, 'getNewConnection failed: %s:%s' % \
                (err, errmsg)

    def shutdown(self):
        # This may fail if the socket is not connected.
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass

    def receive(self):
        try:
            return self.socket.recv(4096)
        except socket.error, (err, errmsg):
            if err == errno.EAGAIN:
                raise ConnectorTryAgainException
            if err in (errno.ECONNREFUSED, errno.EHOSTUNREACH):
                raise ConnectorConnectionRefusedException
            if err in (errno.ECONNRESET, errno.ETIMEDOUT):
                raise ConnectorConnectionClosedException
            raise ConnectorException, 'receive failed: %s:%s' % (err, errmsg)

    def send(self, msg):
        try:
            return self.socket.send(msg)
        except socket.error, (err, errmsg):
            if err == errno.EAGAIN:
                raise ConnectorTryAgainException
            if err in (errno.ECONNRESET, errno.ETIMEDOUT, errno.EPIPE):
                raise ConnectorConnectionClosedException
            raise ConnectorException, 'send failed: %s:%s' % (err, errmsg)

    def close(self):
        self.is_closed = True
        return self.socket.close()

    def __repr__(self):
        if self.is_closed:
            fileno = '?'
        else:
            fileno = self.socket_fd
        result = '<%s at 0x%x fileno %s %s, ' % (self.__class__.__name__,
                 id(self), fileno, self.socket.getsockname())
        if self.is_closed is None:
            result += 'never opened'
        else:
            if self.is_closed:
                result += 'closed '
            else:
                result += 'opened '
            if self.is_listening:
                result += 'listening'
            else:
                if self.accepted_from is None:
                    result += 'to'
                else:
                    result += 'from'
                result += ' %s' % (self.remote_addr, )
        return result + '>'

    def _accept(self):
        raise NotImplementedError

class SocketConnectorIPv4(SocketConnector):
    " Wrapper for IPv4 sockets"
    af_type = socket.AF_INET

    def _accept(self):
        return self.socket.accept()

    def getAddress(self):
        return self.socket.getsockname()

class SocketConnectorIPv6(SocketConnector):
    " Wrapper for IPv6 sockets"
    af_type = socket.AF_INET6

    def _accept(self):
        new_s, addr =  self.socket.accept()
        return new_s, addr[:2]

    def getAddress(self):
        return self.socket.getsockname()[:2]

registerConnectorHandler(SocketConnectorIPv4)
registerConnectorHandler(SocketConnectorIPv6)

class ConnectorException(Exception):
    pass

class ConnectorTryAgainException(ConnectorException):
    pass

class ConnectorInProgressException(ConnectorException):
    pass

class ConnectorConnectionClosedException(ConnectorException):
    pass

class ConnectorConnectionRefusedException(ConnectorException):
    pass

