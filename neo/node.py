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

from time import time
import logging

from neo.protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, VALID_NODE_STATE_LIST
from neo.util import dump

class Node(object):
    """This class represents a node."""

    def __init__(self, server = None, uuid = None):
        self.state = RUNNING_STATE
        self.server = server
        self.uuid = uuid
        self.manager = None
        self.last_state_change = time()

    def setManager(self, manager):
        self.manager = manager

    def getLastStateChange(self):
        return self.last_state_change

    def getState(self):
        return self.state

    def setState(self, new_state):
        assert new_state in VALID_NODE_STATE_LIST
        if self.state != new_state:
            self.state = new_state
            self.last_state_change = time()

    def setServer(self, server):
        if self.server != server:
            if self.server is not None and self.manager is not None:
                self.manager.unregisterServer(self)

            self.server = server
            if server is not None and self.manager is not None:
                self.manager.registerServer(self)

    def getServer(self):
        return self.server

    def setUUID(self, uuid):
        if self.uuid != uuid:
            if self.uuid is not None and self.manager is not None:
                self.manager.unregisterUUID(self)

            self.uuid = uuid
            if uuid is not None and self.manager is not None:
                self.manager.registerUUID(self)

    def getUUID(self):
        return self.uuid

    def getNodeType(self):
        raise NotImplementedError

    def __str__(self):
        server = self.getServer()
        if server is None:
            address, port = None, None
        else:
            address, port = server
        uuid = self.getUUID()
        return '%r:%r (%s)' % (address, port, dump(uuid))

class MasterNode(Node):
    """This class represents a master node."""
    def getNodeType(self):
        return MASTER_NODE_TYPE

class StorageNode(Node):
    """This class represents a storage node."""
    def getNodeType(self):
        return STORAGE_NODE_TYPE

class ClientNode(Node):
    """This class represents a client node."""
    def getNodeType(self):
        return CLIENT_NODE_TYPE

class NodeManager(object):
    """This class manages node status."""

    def __init__(self):
        self.node_list = []
        self.server_dict = {}
        self.uuid_dict = {}

    def add(self, node):
        node.setManager(self)
        self.node_list.append(node)   
        if node.getServer() is not None:
            self.registerServer(node)
        if node.getUUID() is not None:
            self.registerUUID(node)

    def remove(self, node):
        self.node_list.remove(node)
        self.unregisterServer(node)
        self.unregisterUUID(node)

    def registerServer(self, node):
        self.server_dict[node.getServer()] = node

    def unregisterServer(self, node):
        try:
            del self.server_dict[node.getServer()]
        except KeyError:
            pass

    def registerUUID(self, node):
        self.uuid_dict[node.getUUID()] = node

    def unregisterUUID(self, node):
        try:
            del self.uuid_dict[node.getUUID()]
        except KeyError:
            pass

    def getNodeList(self, filter = None):
        if filter is None:
            return list(self.node_list)
        return [n for n in self.node_list if filter(n)]

    def getMasterNodeList(self):
        return self.getNodeList(filter = lambda node: node.getNodeType() == MASTER_NODE_TYPE)

    def getStorageNodeList(self):
        return self.getNodeList(filter = lambda node: node.getNodeType() == STORAGE_NODE_TYPE)

    def getClientNodeList(self):
        return self.getNodeList(filter = lambda node: node.getNodeType() == CLIENT_NODE_TYPE)

    def getNodeByServer(self, server):
        return self.server_dict.get(server)

    def getNodeByUUID(self, uuid):
        return self.uuid_dict.get(uuid)
    
