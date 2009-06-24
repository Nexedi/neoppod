#
# Copyright (C) 2006-2009  Nexedi SA
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

import logging

from neo import protocol

# Some decorators useful to avoid duplication of patterns in handlers

def identification_required(handler):
    """ Raise UnexpectedPacketError if the identification has not succeed """
    def wrapper(self, conn, packet, *args, **kwargs):
        # check if node identification succeed
        if conn.getUUID() is None:
            raise protocol.UnexpectedPacketError
        # identified, call the handler
        handler(self, conn, packet, *args, **kwargs)
    return wrapper

def restrict_node_types(*node_types):
    """ Raise UnexpectedPacketError if the node type is node in the supplied
    list, if the uuid is None or if the node is not known. This decorator
    should be applied after identification_required """
    def inner(handler):
        def wrapper(self, conn, packet, *args, **kwargs):
            # check if node type is allowed
            uuid = conn.getUUID()
            if uuid is None:
                raise protocol.UnexpectedPacketError
            node = self.app.nm.getNodeByUUID(uuid)
            if node is None:
                raise protocol.UnexpectedPacketError
            if node.getNodeType() not in node_types:
                raise protocol.UnexpectedPacketError
            # all is ok, call the handler
            handler(self, conn, packet, *args, **kwargs)
        return wrapper
    return inner

def client_connection_required(handler):
    """ Raise UnexpectedPacketError if the packet comes from a client connection """
    def wrapper(self, conn, packet, *args, **kwargs):
        if conn.isServerConnection():
            raise protocol.UnexpectedPacketError
        # it's a client connection, call the handler
        handler(self, conn, packet, *args, **kwargs)
    return wrapper

def server_connection_required(handler):
    """ Raise UnexpectedPacketError if the packet comes from a server connection """
    def wrapper(self, conn, packet, *args, **kwargs):
        if not conn.isServerConnection():
            raise protocol.UnexpectedPacketError
        # it's a server connection, call the handler
        handler(self, conn, packet, *args, **kwargs)
    return wrapper

