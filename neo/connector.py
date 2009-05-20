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

  def __init__(self, s=None):
    if s is None:      
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
      self.socket = s
  
  def makeClientConnection(self, addr):
    try:
      self.socket.setblocking(0)
      self.socket.connect(addr)
    except socket.error, (err, errmsg):
      if err == errno.EINPROGRESS:
        raise ConnectorInProgressException
      if err == errno.ECONNREFUSED:
        raise ConnectorConnectionRefusedException
      raise ConnectorException, 'makeClientConnection failed: %s:%s' %  (err, errmsg)

  def makeListeningConnection(self, addr):
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
      new_s = SocketConnector(new_s)
      return new_s, addr
    except socket.error, m:
      if m[0] == errno.EAGAIN:
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
      raise ConnectorException, 'receive failed: %s:%s' % (err, errmsg)

  def send(self, msg):
    try:
      return self.socket.send(msg)
    except socket.error, (err, errmsg):
      if err == errno.EAGAIN:
        raise ConnectorTryAgainException
      raise ConnectorException, 'send failed: %s:%s' % (err, errmsg) 

  def close(self):
    return self.socket.close()

registerConnectorHandler(SocketConnector)

class ConnectorException(Exception): pass
class ConnectorTryAgainException(ConnectorException): pass
class ConnectorInProgressException(ConnectorException): pass  
class ConnectorConnectionRefusedException(ConnectorException): pass

