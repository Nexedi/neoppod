from time import time

RUNNING_STATE = 0
TEMPORARILY_DOWN_STATE = 2
DOWN_STATE = 3
BROKEN_STATE = 4

class Node(object):
    """This class represents a node."""

    def __init__(self, ip_address = None, port = None, uuid = None):
        self.state = RUNNING_STATE
        self.ip_address = ip_address
        self.port = port
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

    def setServer(self, ip_address, port):
        if self.ip_address is not None:
            self.manager.unregisterServer(self)

        self.ip_address = ip_address
        self.port = port
        self.manager.registerServer(self)

    def getServer(self):
        return self.ip_address, self.port

    def setUUID(self, uuid):
        if self.uuid is not None:
            self.manager.unregisterUUID(self)

        self.uuid = uuid
        self.manager.registerUUID(self)

    def getUUID(self):
        return self.uuid

class MasterNode(Node):
    """This class represents a master node."""
    pass

class StorageNode(Node):
    """This class represents a storage node."""
    pass

class ClientNode(Node):
    """This class represents a client node."""
    pass

class NodeManager(object):
    """This class manages node status."""

    def __init__(self):
        self.node_list = []
        self.server_dict = {}
        self.uuid_dict = {}

    def add(self, node):
        node.setManager(self)
        self.node_list.append(node)   
        if node.getServer()[0] is not None:
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

    def getNodeByServer(self, ip_address, port):
        return self.server_dict.get((ip_address, port))

    def getNodeByUUID(self, uuid):
        return self.uuid_dict.get(uuid)
