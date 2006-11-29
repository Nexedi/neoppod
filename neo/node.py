from time import time

from protocol import RUNNING_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE

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
        if self.state != new_state:
            self.state = new_state
            self.last_state_change = time()

    def setServer(self, server):
        if self.server is not None:
            self.manager.unregisterServer(self)

        self.server = server
        self.manager.registerServer(self)

    def getServer(self):
        return self.server

    def setUUID(self, uuid):
        if self.uuid is not None:
            self.manager.unregisterUUID(self)

        self.uuid = uuid
        self.manager.registerUUID(self)

    def getUUID(self):
        return self.uuid

    def getNodeType(self):
        raise NotImplementedError

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
        self.server_dict[node.getUUID()] = node

    def unregisterUUID(self, node):
        try:
            del self.server_dict[node.getUUID()]
        except KeyError:
            pass

    def getNodeList(self, filter = None):
        if filter is None:
            return list(self.node_list)
        return [n for n in self.node_list if filter(n)]

    def getMasterNodeList(self):
        return self.getNodeList(filter = lambda node: isinstance(node, MasterNode))

    def getStorageNodeList(self):
        return self.getNodeList(filter = lambda node: isinstance(node, StorageNode))

    def getClientNodeList(self):
        return self.getNodeList(filter = lambda node: isinstance(node, ClientNode))

    def getNodeByServer(self, server):
        return self.server_dict.get(server)

    def getNodeByUUID(self, uuid):
        return self.uuid_dict.get(uuid)
