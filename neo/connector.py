#
# Copyright (C) 2009  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import socket
import errno
import logging

# Global connector registry.
# Fill by calling registerConnectorHandler.
# Read by calling getConnectorHandler.
connector_registry = {}

def registerConnectorHandler(connector_handler):
  connector_registry[connector_handler.__name__] = connector_handler

def getConnectorHandler(connector):
  if isinstance(connector, basestring):
    connector_handler = connector_registry.get(connector)
  else:
    # Allow to directly provide a handler class without requiring to register
    # it first.
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
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
      self.socket = s
  
  def makeClientConnection(self, addr):
    self.is_closed = False
    self.remote_addr = addr
    try:
      try:
        self.socket.setblocking(0)
        self.socket.connect(addr)
      except socket.error, (err, errmsg):
        if err == errno.EINPROGRESS:
          raise ConnectorInProgressException
        if err == errno.ECONNREFUSED:
          raise ConnectorConnectionRefusedException
        raise ConnectorException, 'makeClientConnection failed: %s:%s' %  (err, errmsg)
    finally:
      logging.debug('%r connecting to %r', self.socket.getsockname(), addr)

  def makeListeningConnection(self, addr):
    self.is_closed = False
    self.is_listening = True
    try:
      self.socket.setblocking(0)
      self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.socket.bind(addr)
      self.socket.listen(5)
    except socket.error, (err, errmsg):
      self.socket.close()
      raise ConnectorException, 'makeListeningConnection failed: %s:%s' % (err, errmsg)

  def getError(self):
    return self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

  def getDescriptor(self):
    return self.socket.fileno()

  def getNewConnection(self):
    try:
      new_s, addr =  self.socket.accept()
      new_s = SocketConnector(new_s, accepted_from=addr)
      return new_s, addr
    except socket.error, (err, errmsg):
      if err == errno.EAGAIN:
        raise ConnectorTryAgainException
      raise ConnectorException, 'getNewConnection failed: %s:%s' % (err, errmsg)

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
      if err == errno.ECONNREFUSED:
        raise ConnectorConnectionRefusedException
      if err == errno.ECONNRESET:
          raise ConnectorConnectionClosedException
      raise ConnectorException, 'receive failed: %s:%s' % (err, errmsg)

  def send(self, msg):
    try:
      return self.socket.send(msg)
    except socket.error, (err, errmsg):
      if err == errno.EAGAIN:
        raise ConnectorTryAgainException
      if err == errno.ECONNRESET:
          raise ConnectorConnectionClosedException
      raise ConnectorException, 'send failed: %s:%s' % (err, errmsg) 

  def close(self):
    self.is_closed = True
    return self.socket.close()

  def __repr__(self):
    try:
      fileno = str(self.socket.fileno())
    except socket.error, (err, errmsg):
      fileno = '?'
    result = '<%s at 0x%x fileno %s %s>' % (self.__class__.__name__, id(self),
      fileno, self.socket.getsockname())
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

registerConnectorHandler(SocketConnector)

class ConnectorException(Exception): pass
class ConnectorTryAgainException(ConnectorException): pass
class ConnectorInProgressException(ConnectorException): pass  
class ConnectorConnectionClosedException(ConnectorException): pass
class ConnectorConnectionRefusedException(ConnectorException): pass

