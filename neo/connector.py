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
from neo.master.tests.connector import *

class SocketConnector:
  """ This class is a wrapper for a socket """

  def __init__(self, s=None):
    if s is None:      
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
      self.socket = s

  def __getattr__(self, name):
    """ fallback to default socket methods """
    return getattr(self.socket, name)
  
  def makeClientConnection(self, addr):
    try:
      self.socket.setblocking(0)
      self.socket.connect(addr)
    except socket.error, m:
      if m[0] == errno.EINPROGRESS:
        raise ConnectorInProgressException
      else:
        logging.error('makeClientConnection: %s', m[1])        
        raise

  def makeListeningConnection(self, addr):
    try:
      self.socket.setblocking(0)
      self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.socket.bind(addr)
      self.socket.listen(5)
    except:
      self.socket.close()
      raise

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
      else:
        logging.error('getNewConnection: %s', m[1])        
        raise

  def shutdown(self):
    # This may fail if the socket is not connected.
    try:
      self.socket.shutdown(socket.SHUT_RDWR)
    except socket.error:
      pass

  def receive(self):
    try:
      return self.socket.recv(4096)
    except socket.error, m:
      if m[0] == errno.EAGAIN:
        raise ConnectorTryAgainException
      else:
        logging.error('receive: %s', m[1])                    
        raise

  def send(self, msg):
    try:
      return self.socket.send(msg)
    except socket.error, m:
      if m[0] == errno.EAGAIN:
        raise ConnectorTryAgainException
      else:
        logging.error('send: %s', m[1])        
        raise


class ConnectorTryAgainException(Exception): pass
class ConnectorInProgressException(Exception): pass  














