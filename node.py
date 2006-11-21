WORKING_STATE = 0
TEMPORARILY_DOWN_STATE = 1
DOWN_STATE = 2
BROKEN_STATE = 3

class Node(object):
    """This class represents a node."""

    def __init__(self, ip_address = None, port = None, uuid = None):
        self.state = WORKING_STATE
        self.ip_address = ip_address
        self.port = port
        self.uuid = uuid

    def getState(self):
        return self.state

    def changeState(self, new_state):
        self.state = new_state

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
        self.node_dict = {}

    def add(self, *args):
        
