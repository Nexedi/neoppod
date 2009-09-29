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

from neo import logging
from neo import protocol
from neo.util import dump

# TODO: 
# Node requires a manager
# Index nodes per node type, server, uuid and state 
# A subclass should not tell it's own type

class Node(object):
    """This class represents a node."""

    def __init__(self, manager, server=None, uuid=None, state=protocol.UNKNOWN_STATE):
        self.state = state
        self.server = server
        self.uuid = uuid
        self.manager = manager
        self.last_state_change = time()

    def getLastStateChange(self):
        return self.last_state_change

    def getState(self):
        return self.state

    def setState(self, new_state):
        if self.state != new_state:
            self.state = new_state
            self.last_state_change = time()

    def setServer(self, server):
        self.manager.unregisterServer(self)
        self.server = server
        self.manager.registerServer(self)

    def getServer(self):
        return self.server

    def setUUID(self, uuid):
        self.manager.unregisterUUID(self)
        self.uuid = uuid
        self.manager.registerUUID(self)

    def getUUID(self):
        return self.uuid

    def getType(self):
        raise NotImplementedError

    def __str__(self):
        server = self.getServer()
        if server is None:
            address, port = None, None
        else:
            address, port = server
        uuid = self.getUUID()
        return '%s (%s:%s)' % (dump(uuid), address, port)

    def isMaster(self):
        return isinstance(self, MasterNode)

    def isStorage(self):
        return isinstance(self, StorageNode)

    def isClient(self):
        return isinstance(self, ClientNode)

    def isAdmin(self):
        return isinstance(self, AdminNode)


class MasterNode(Node):
    """This class represents a master node."""

    def getType(self):
        return protocol.MASTER_NODE_TYPE

class StorageNode(Node):
    """This class represents a storage node."""

    def getType(self):
        return protocol.STORAGE_NODE_TYPE

class ClientNode(Node):
    """This class represents a client node."""

    def getType(self):
        return protocol.CLIENT_NODE_TYPE

class AdminNode(Node):
    """This class represents an admin node."""

    def getType(self):
        return protocol.ADMIN_NODE_TYPE


NODE_TYPE_MAPPING = {
    protocol.MASTER_NODE_TYPE: MasterNode,
    protocol.STORAGE_NODE_TYPE: StorageNode,
    protocol.CLIENT_NODE_TYPE: ClientNode,
    protocol.ADMIN_NODE_TYPE: AdminNode,
}

class NodeManager(object):
    """This class manages node status."""

    def __init__(self):
        self.node_list = []
        self.server_dict = {}
        self.uuid_dict = {}

    def add(self, node):
        self.node_list.append(node)   
        if node.getServer() is not None:
            self.registerServer(node)
        if node.getUUID() is not None:
            self.registerUUID(node)

    def remove(self, node):
        if node is None:
            return
        self.node_list.remove(node)
        self.unregisterServer(node)
        self.unregisterUUID(node)

    def registerServer(self, node):
        if node.getServer() is None:
            return
        self.server_dict[node.getServer()] = node

    def unregisterServer(self, node):
        if node.getServer() is None:
            return
        try:
            del self.server_dict[node.getServer()]
        except KeyError:
            pass

    def registerUUID(self, node):
        if node.getUUID() is None:
            return
        self.uuid_dict[node.getUUID()] = node

    def unregisterUUID(self, node):
        if node.getUUID() is None:
            return
        try:
            del self.uuid_dict[node.getUUID()]
        except KeyError:
            pass

    def getNodeList(self, node_filter=None):
        if node_filter is None:
            return list(self.node_list)
        return filter(node_filter, self.node_list)

    def getMasterNodeList(self):
        node_filter = lambda node: node.isMaster()
        return self.getNodeList(node_filter=node_filter)

    def getStorageNodeList(self):
        node_filter = lambda node: node.isStorage()
        return self.getNodeList(node_filter=node_filter)

    def getClientNodeList(self):
        node_filter = lambda node: node.isClient()
        return self.getNodeList(node_filter=node_filter)

    def getNodeByServer(self, server):
        return self.server_dict.get(server)

    def getNodeByUUID(self, uuid):
        if uuid is None:
            return None
        return self.uuid_dict.get(uuid)

    def _createNode(self, klass, **kw):
        node = klass(self, **kw)
        self.add(node)
        return node

    def createMaster(self, **kw):
        """ Create and register a new master """
        return self._createNode(MasterNode, **kw)

    def createStorage(self, *args, **kw):
        """ Create and register a new storage """
        return self._createNode(StorageNode, **kw)

    def createClient(self, *args, **kw):
        """ Create and register a new client """
        return self._createNode(ClientNode, **kw)
    
    def createAdmin(self, *args, **kw):
        """ Create and register a new admin """
        return self._createNode(AdminNode, **kw)

    def createFromNodeType(self, node_type, **kw):
        # XXX: use a static dict or drop this
        klass = {
            protocol.MASTER_NODE_TYPE: MasterNode,
            protocol.STORAGE_NODE_TYPE: StorageNode,
            protocol.CLIENT_NODE_TYPE: ClientNode,
            protocol.ADMIN_NODE_TYPE: AdminNode,
        }.get(node_type)
        if klass is None:
            raise RuntimeError('Unknown node type : %s' % node_type)
        return self._createNode(klass, **kw)
    
    def clear(self, filter=None):
        for node in self.getNodeList():
            if filter is None or filter(node):
                self.remove(node)

    def update(self, node_list):
        for node_type, addr, uuid, state in node_list:
            # lookup in current table
            node_by_uuid = self.getNodeByUUID(uuid)
            node_by_addr = self.getNodeByServer(addr)
            node = node_by_uuid or node_by_addr

            log_args = (node_type, dump(uuid), addr, state)
            if state == protocol.DOWN_STATE:
                # drop down nodes
                logging.debug('drop node %s %s %s %s' % log_args)
                self.remove(node)
            elif node_by_uuid is not None:
                if node.getServer() != addr:
                    # address changed, update it
                    node.setServer(addr)
                logging.debug('update node %s %s %s %s' % log_args)
                node.setState(state)
            else:
                if node_by_addr is not None:
                    # exists only by address,
                    self.remove(node)
                # don't exists, add it
                klass = NODE_TYPE_MAPPING.get(node_type, None)
                if klass is None:
                    raise RuntimeError('Unknown node type')
                node = klass(self, server=addr, uuid=uuid)
                node.setState(state)
                self.add(node)
                logging.info('create node %s %s %s %s' % log_args)
            self.log()

    def log(self):
        logging.debug('Node manager : %d nodes' % len(self.node_list))
        node_with_uuid = set(sorted(self.uuid_dict.values()))
        node_without_uuid = set(self.node_list) - node_with_uuid
        for node in node_with_uuid | node_without_uuid:
            if node.getUUID() is not None:
                uuid = dump(node.getUUID())
            else:
                uuid = '-' * 32
            args = (
                    uuid,
                    node.getType(),
                    node.getState()
            )
            logging.debug('nm: %s : %s/%s' % args)
        for address, node in sorted(self.server_dict.items()):
            args = (
                    address,
                    node.getType(),
                    node.getState()
            )
            logging.debug('nm: %s : %s/%s' % args)

