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

class Node(object):
    """This class represents a node."""

    def __init__(self, manager, address=None, uuid=None, 
            state=protocol.UNKNOWN_STATE):
        self._state = state
        self._address = address
        self._uuid = uuid
        self._manager = manager
        self._last_state_change = time()

    def getLastStateChange(self):
        return self._last_state_change

    def getState(self):
        return self._state

    def setState(self, new_state):
        if self._state == new_state:
            return
        old_state = self._state
        self._state = new_state
        self._last_state_change = time()
        self._manager._updateState(self, old_state)

    def setAddress(self, address):
        old_address = self._address
        self._address = address
        self._manager._updateAddress(self, old_address)

    def getAddress(self):
        return self._address

    def setUUID(self, uuid):
        old_uuid = self._uuid
        self._uuid = uuid
        self._manager._updateUUID(self, old_uuid)

    def getUUID(self):
        return self._uuid

    def getType(self):
        raise NotImplementedError

    def __repr__(self):
        return '<%s(uuid=%s, address=%s, state=%s)>' % (
            self.__class__.__name__, 
            self._address,
            dump(self._uuid),
            self._state,
        )

    def isMaster(self):
        return isinstance(self, MasterNode)

    def isStorage(self):
        return isinstance(self, StorageNode)

    def isClient(self):
        return isinstance(self, ClientNode)

    def isAdmin(self):
        return isinstance(self, AdminNode)

    def isIdentified(self):
        # XXX: knowing the node's UUID is sufficient ?
        return self._uuid is not Node

    def isRunning(self):
        # FIXME: is it like 'connected' ?
        return self._state == protocol.RUNNING_STATE

    def isTemporarilyDown(self):
        # FIXME: is it like 'unconnected' or UNKNOWN_STATE ?
        return self._state == protocol.TEMPORARILY_DOWN_STATE

    def isDown(self):
        # FIXME: is it like 'unconnected' or 'forgotten' ?
        return self._state == protocol.DOWN_STATE
        
    def isBroken(self):
        return self._state == protocol.BROKEN_STATE

    def isHidden(self):
        return self._state == protocol.HIDDEN_STATE

    def isPending(self):
        return self._state == protocol.PENDING_STATE

    def setRunning(self):
        self.setState(protocol.RUNNING_STATE)

    def setTemporarilyDown(self):
        self.setState(protocol.TEMPORARILY_DOWN_STATE)

    def setDown(self):
        self.setState(protocol.DOWN_STATE)

    def setBroken(self):
        self.setState(protocol.BROKEN_STATE)

    def setHidden(self):
        self.setState(protocol.HIDDEN_STATE)

    def setPending(self):
        self.setState(protocol.PENDING_STATE)

    # XXX: for comptatibility, to be removed
    def getType(self):
        try:
            return NODE_CLASS_MAPPING[self.__class__]
        except KeyError:
            raise NotImplementedError


class MasterNode(Node):
    """This class represents a master node."""
    pass

class StorageNode(Node):
    """This class represents a storage node."""
    pass

class ClientNode(Node):
    """This class represents a client node."""
    pass

class AdminNode(Node):
    """This class represents an admin node."""
    pass


NODE_TYPE_MAPPING = {
    protocol.MASTER_NODE_TYPE: MasterNode,
    protocol.STORAGE_NODE_TYPE: StorageNode,
    protocol.CLIENT_NODE_TYPE: ClientNode,
    protocol.ADMIN_NODE_TYPE: AdminNode,
}
NODE_CLASS_MAPPING = {
    StorageNode: protocol.STORAGE_NODE_TYPE,
    MasterNode: protocol.MASTER_NODE_TYPE,
    ClientNode: protocol.CLIENT_NODE_TYPE,
    AdminNode: protocol.ADMIN_NODE_TYPE,
}

class NodeManager(object):
    """This class manages node status."""

    def __init__(self):
        self._node_set = set()
        self._address_dict = {}
        self._uuid_dict = {}
        self._type_dict = {}
        self._state_dict = {}

    def add(self, node):
        if node in self._node_set:
            return
        self._node_set.add(node)   
        self._updateAddress(node, None)
        self._updateUUID(node, None)
        self.__updateSet(self._type_dict, None, node.__class__, node)
        self.__updateSet(self._state_dict, None, node.getState(), node)

    def remove(self, node):
        if node is None or node not in self._node_set:
            return
        self._node_set.remove(node)
        self.__drop(self._address_dict, node.getAddress())
        self.__drop(self._uuid_dict, node.getUUID())
        self.__dropSet(self._state_dict, node.getState(), node)
        self.__dropSet(self._type_dict, node.__class__, node)

    def __drop(self, index_dict, key):
        try:
            del index_dict[key]
        except KeyError:
            pass

    def __update(self, index_dict, old_key, new_key, node):
        """ Update an index from old to new key """
        # FIXME: should the old_key always be indexed ?
        if old_key is not None:
            del index_dict[old_key]
        if new_key is not None:
            index_dict[new_key] = node

    def _updateAddress(self, node, old_address):
        self.__update(self._address_dict, old_address, node.getAddress(), node)

    def _updateUUID(self, node, old_uuid):
        self.__update(self._uuid_dict, old_uuid, node.getUUID(), node)

    def __dropSet(self, set_dict, key, node):
        if key in set_dict and node in set_dict[key]:
            set_dict[key].remove(node)

    def __updateSet(self, set_dict, old_key, new_key, node):
        """ Update a set index from old to new key """
        # FIXME: should the old_key always be indexed ?
        if old_key in set_dict and node in set_dict[old_key]:
            set_dict[old_key].remove(node)
        if new_key is not None:
            set_dict.setdefault(new_key, set()).add(node)

    def _updateState(self, node, old_state):
        self.__updateSet(self._state_dict, old_state, node.getState(), node)

    def getList(self, node_filter=None):
        if filter is None:
            return list(self._node_set)
        return filter(node_filter, self._node_set)

    def __getList(self, index_dict, key):
        return list(index_dict.setdefault(key, set()))

    def getByStateList(self, state):
        """ Get a node list filtered per the node state """
        return self.__getList(self._state_dict, state)

    def __getTypeList(self, type_klass):
        return self.__getList(self._type_dict, type_klass)

    def getMasterList(self):
        """ Return a list with master nodes """
        return self.__getTypeList(MasterNode)

    def getStorageList(self):
        """ Return a list with storage nodes """
        return self.__getTypeList(StorageNode)

    def getClientList(self):
        """ Return a list with client nodes """
        return self.__getTypeList(ClientNode)

    def getAdminList(self):
        """ Return a list with admin nodes """
        return self.__getTypeList(AdminNode)

    def getByAddress(self, address):
        """ Return the node that match with a given address """
        return self._address_dict.get(address, None)

    def getByUUID(self, uuid):
        """ Return the node that match with a given UUID """
        return self._uuid_dict.get(uuid, None)

    def hasAddress(self, address):
        return self._address_dict.get(address, None) is not None

    def hasUUID(self, uuid):
        return self._uuid_dict.get(uuid, None) is not None

    def _createNode(self, klass, **kw):
        node = klass(self, **kw)
        self.add(node)
        return node

    def createMaster(self, **kw):
        """ Create and register a new master """
        return self._createNode(MasterNode, **kw)

    def createStorage(self, **kw):
        """ Create and register a new storage """
        return self._createNode(StorageNode, **kw)

    def createClient(self, **kw):
        """ Create and register a new client """
        return self._createNode(ClientNode, **kw)
    
    def createAdmin(self, **kw):
        """ Create and register a new admin """
        return self._createNode(AdminNode, **kw)

    def createFromNodeType(self, node_type, **kw):
        klass = NODE_TYPE_MAPPING.get(node_type)
        if klass is None:
            raise RuntimeError('Unknown node type : %s' % node_type)
        return self._createNode(klass, **kw)
    
    def clear(self, filter=None):
        self._node_set.clear()
        self._type_dict.clear()
        self._state_dict.clear()
        self._uuid_dict.clear()
        self._address_dict.clear()

    def update(self, node_list):
        for node_type, addr, uuid, state in node_list:
            # lookup in current table
            node_by_uuid = self.getByUUID(uuid)
            node_by_addr = self.getByAddress(addr)
            node = node_by_uuid or node_by_addr

            log_args = (node_type, dump(uuid), addr, state)
            if state == protocol.DOWN_STATE:
                # drop down nodes
                logging.debug('drop node %s %s %s %s' % log_args)
                self.remove(node)
            elif node_by_uuid is not None:
                if node.getAddress() != addr:
                    # address changed, update it
                    node.setAddress(addr)
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
                node = klass(self, address=addr, uuid=uuid)
                node.setState(state)
                self.add(node)
                logging.info('create node %s %s %s %s' % log_args)
        self.log()

    def log(self):
        logging.debug('Node manager : %d nodes' % len(self._node_set))
        for node in sorted(list(self._node_set)):
            uuid = dump(node.getUUID()) or '-' * 32
            address = node.getAddress() or ''
            if address:
                address = '%s:%d' % address
            logging.debug(' * %32s | %17s | %22s | %s' % (
                uuid, node.getType(), address, node.getState()))

    def __gt__(self, node):
        # sort per UUID if defined
        if self._uuid is not None:
            return self._uuid > node.uuid
        return self._address > node._address

