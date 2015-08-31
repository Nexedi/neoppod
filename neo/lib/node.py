#
# Copyright (C) 2006-2015  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from time import time
from os.path import exists, getsize
import json

from . import attributeTracker, logging
from .protocol import uuid_str, NodeTypes, NodeStates, ProtocolError


class Node(object):
    """This class represents a node."""

    _connection = None

    def __init__(self, manager, address=None, uuid=None,
            state=NodeStates.UNKNOWN):
        self._state = state
        self._address = address
        self._uuid = uuid
        self._manager = manager
        self._last_state_change = time()
        self._identified = False
        manager.add(self)

    def notify(self, packet):
        assert self.isConnected(), 'Not connected'
        self._connection.notify(packet)

    def ask(self, packet, *args, **kw):
        assert self.isConnected(), 'Not connected'
        self._connection.ask(packet, *args, **kw)

    def answer(self, packet, msg_id=None):
        assert self.isConnected(), 'Not connected'
        self._connection.answer(packet, msg_id)

    def getLastStateChange(self):
        return self._last_state_change

    def getState(self):
        return self._state

    def setState(self, new_state):
        if self._state == new_state:
            return
        if new_state == NodeStates.DOWN:
            self._manager.remove(self)
            self._state = new_state
        else:
            old_state = self._state
            self._state = new_state
            self._manager._updateState(self, old_state)
        self._last_state_change = time()

    def setAddress(self, address):
        if self._address == address:
            return
        old_address = self._address
        self._address = address
        self._manager._updateAddress(self, old_address)

    def getAddress(self):
        return self._address

    def setUUID(self, uuid):
        if self._uuid == uuid:
            return
        old_uuid = self._uuid
        self._uuid = uuid
        self._manager._updateUUID(self, old_uuid)
        self._manager._updateIdentified(self)
        if self._connection is not None:
            self._connection.setUUID(uuid)

    def getUUID(self):
        return self._uuid

    def onConnectionClosed(self):
        """
            Callback from node's connection when closed
        """
        assert self._connection is not None
        del self._connection
        self._identified = False
        self._manager._updateIdentified(self)

    def setConnection(self, connection, force=None):
        """
            Define the connection that is currently available to this node.
            If there is already a connection set, 'force' must be given:
            the new connection replaces the old one if it is true. In any case,
            the node must be managed by the same handler for the client and
            server parts.
        """
        assert connection.getUUID() in (None, self._uuid), connection
        connection.setUUID(self._uuid)
        conn = self._connection
        if conn is None:
            self._connection = connection
            if connection.isServer():
                self.setIdentified()
        else:
            assert force is not None, \
                attributeTracker.whoSet(self, '_connection')
            # The test on peer_id is there to protect against buggy nodes.
            # XXX: handler comparison does not cover all cases: there may
            # be a pending handler change, which won't be detected, or a future
            # handler change which is not prevented. Complete implementation
            # should allow different handlers for each connection direction,
            # with in-packets client/server indicators to decide which handler
            # (server-ish or client-ish) to use. There is currently no need for
            # the full-fledged functionality, and it is simpler this way.
            if not force or conn.getPeerId() is not None or \
               type(conn.getHandler()) is not type(connection.getHandler()):
                raise ProtocolError("already connected")
            def on_closed():
                self._connection = connection
                assert connection.isServer()
                self.setIdentified()
            conn.setOnClose(on_closed)
            conn.close()
        assert not connection.isClosed(), connection
        connection.setOnClose(self.onConnectionClosed)
        self._manager._updateIdentified(self)

    def getConnection(self):
        """
            Returns the connection to the node if available
        """
        assert self._connection is not None
        return self._connection

    def isConnected(self, connecting=False):
        """
            Returns True is a connection is established with the node
        """
        return self._connection is not None and (connecting or
            not self._connection.connecting)

    def setIdentified(self):
        assert self._connection is not None
        self._identified = True

    def isIdentified(self):
        """
            Returns True if identification packets have been exchanged
        """
        return self._identified

    def __repr__(self):
        return '<%s(uuid=%s, address=%s, state=%s, connection=%r) at %x>' % (
            self.__class__.__name__,
            uuid_str(self._uuid),
            self._address,
            self._state,
            self._connection,
            id(self),
        )

    def isMaster(self):
        return False

    def isStorage(self):
        return False

    def isClient(self):
        return False

    def isAdmin(self):
        return False

    def isRunning(self):
        return self._state == NodeStates.RUNNING

    def isUnknown(self):
        return self._state == NodeStates.UNKNOWN

    def isTemporarilyDown(self):
        return self._state == NodeStates.TEMPORARILY_DOWN

    def isDown(self):
        return self._state == NodeStates.DOWN

    def isBroken(self):
        return self._state == NodeStates.BROKEN

    def isHidden(self):
        return self._state == NodeStates.HIDDEN

    def isPending(self):
        return self._state == NodeStates.PENDING

    def setRunning(self):
        self.setState(NodeStates.RUNNING)

    def setUnknown(self):
        self.setState(NodeStates.UNKNOWN)

    def setTemporarilyDown(self):
        self.setState(NodeStates.TEMPORARILY_DOWN)

    def setDown(self):
        self.setState(NodeStates.DOWN)

    def setBroken(self):
        self.setState(NodeStates.BROKEN)

    def setHidden(self):
        self.setState(NodeStates.HIDDEN)

    def setPending(self):
        self.setState(NodeStates.PENDING)

    def asTuple(self):
        """ Returned tuple is intented to be used in procotol encoders """
        return (self.getType(), self._address, self._uuid, self._state)

    def __gt__(self, node):
        # sort per UUID if defined
        if self._uuid is not None:
            return self._uuid > node._uuid
        return self._address > node._address

    def getType(self):
        try:
            return NODE_CLASS_MAPPING[self.__class__]
        except KeyError:
            raise NotImplementedError

    def whoSetState(self):
        """
          Debugging method: call this method to know who set the current
          state value.
        """
        return attributeTracker.whoSet(self, '_state')

attributeTracker.track(Node)

class MasterNode(Node):
    """This class represents a master node."""

    def isMaster(self):
        return True

class StorageNode(Node):
    """This class represents a storage node."""

    def isStorage(self):
        return True

class ClientNode(Node):
    """This class represents a client node."""

    def isClient(self):
        return True

class AdminNode(Node):
    """This class represents an admin node."""

    def isAdmin(self):
        return True


NODE_TYPE_MAPPING = {
    NodeTypes.MASTER: MasterNode,
    NodeTypes.STORAGE: StorageNode,
    NodeTypes.CLIENT: ClientNode,
    NodeTypes.ADMIN: AdminNode,
}
NODE_CLASS_MAPPING = {
    StorageNode: NodeTypes.STORAGE,
    MasterNode: NodeTypes.MASTER,
    ClientNode: NodeTypes.CLIENT,
    AdminNode: NodeTypes.ADMIN,
}

class MasterDB(object):
    """
    Manages accesses to master's address database.
    """
    def __init__(self, path):
        self._path = path
        try_load = exists(path) and getsize(path)
        if try_load:
            db = open(path, 'r')
            init_set = map(tuple, json.load(db))
        else:
            db = open(path, 'w+')
            init_set = []
        self._set = set(init_set)
        db.close()

    def _save(self):
        try:
            db = open(self._path, 'w')
        except IOError:
            logging.warning('failed opening master database at %r '
                'for writing, update skipped', self._path)
        else:
            json.dump(list(self._set), db)
            db.close()

    def add(self, addr):
        self._set.add(addr)
        self._save()

    def discard(self, addr):
        self._set.discard(addr)
        self._save()

    def __iter__(self):
        return iter(self._set)

class NodeManager(object):
    """This class manages node status."""
    _master_db = None

    # TODO: rework getXXXList() methods, filter first by node type
    # - getStorageList(identified=True, connected=True, )
    # - getList(...)

    def __init__(self, master_db=None):
        """
        master_db (string)
        Path to a file containing master nodes's addresses. Used to automate
        master list updates. If not provided, no automation will happen.
        """
        self._node_set = set()
        self._address_dict = {}
        self._uuid_dict = {}
        self._type_dict = {}
        self._state_dict = {}
        self._identified_dict = {}
        if master_db is not None:
            self._master_db = db = MasterDB(master_db)
            for addr in db:
                self.createMaster(address=addr)

    close = __init__

    def add(self, node):
        if node in self._node_set:
            logging.warning('adding a known node %r, ignoring', node)
            return
        assert not node.isDown(), node
        self._node_set.add(node)
        self._updateAddress(node, None)
        self._updateUUID(node, None)
        self.__updateSet(self._type_dict, None, node.__class__, node)
        self.__updateSet(self._state_dict, None, node.getState(), node)
        self._updateIdentified(node)
        if node.isMaster() and self._master_db is not None:
            self._master_db.add(node.getAddress())

    def remove(self, node):
        if node not in self._node_set:
            logging.warning('removing unknown node %r, ignoring', node)
            return
        self._node_set.remove(node)
        self.__drop(self._address_dict, node.getAddress())
        self.__drop(self._uuid_dict, node.getUUID())
        self.__dropSet(self._state_dict, node.getState(), node)
        self.__dropSet(self._type_dict, node.__class__, node)
        uuid = node.getUUID()
        if uuid in self._identified_dict:
            del self._identified_dict[uuid]
        if node.isMaster() and self._master_db is not None:
            self._master_db.discard(node.getAddress())

    def __drop(self, index_dict, key):
        try:
            del index_dict[key]
        except KeyError:
            # a node may have not be indexed by uuid or address, eg.:
            # - a master known by address but without UUID
            # - a client or admin node that don't have listening address
            pass

    def __update(self, index_dict, old_key, new_key, node):
        """ Update an index from old to new key """
        if old_key is not None:
            assert index_dict[old_key] is node, '%r is stored as %s, ' \
                'moving %r to %s' % (index_dict[old_key], old_key, node,
                new_key)
            del index_dict[old_key]
        if new_key is not None:
            assert index_dict.get(new_key, node) is node, 'Adding %r at %r ' \
                'would overwrite %r' % (node, new_key, index_dict[new_key])
            index_dict[new_key] = node

    def _updateIdentified(self, node):
        uuid = node.getUUID()
        if uuid:
            # XXX: It's probably a bug to include connecting nodes but there's
            #      no API yet to update manager when connection is established.
            if node.isConnected(connecting=True):
                assert node in self._node_set, node
                self._identified_dict[uuid] = node
            else:
                self._identified_dict.pop(uuid, None)

    def _updateAddress(self, node, old_address):
        self.__update(self._address_dict, old_address, node.getAddress(), node)

    def _updateUUID(self, node, old_uuid):
        self.__update(self._uuid_dict, old_uuid, node.getUUID(), node)

    def __dropSet(self, set_dict, key, node):
        if key in set_dict and node in set_dict[key]:
            set_dict[key].remove(node)

    def __updateSet(self, set_dict, old_key, new_key, node):
        """ Update a set index from old to new key """
        if old_key in set_dict:
            set_dict[old_key].remove(node)
        if new_key is not None:
            set_dict.setdefault(new_key, set()).add(node)

    def _updateState(self, node, old_state):
        assert not node.isDown(), node
        self.__updateSet(self._state_dict, old_state, node.getState(), node)

    def getList(self, node_filter=None):
        if node_filter is None:
            return list(self._node_set)
        return filter(node_filter, self._node_set)

    def getIdentifiedList(self, pool_set=None):
        """
            Returns a generator to iterate over identified nodes
            pool_set is an iterable of UUIDs allowed
        """
        if pool_set is not None:
            identified_nodes = self._identified_dict.items()
            return [v for k, v in identified_nodes if k in pool_set]
        return self._identified_dict.values()

    def getConnectedList(self):
        """
            Returns a generator to iterate over connected nodes
        """
        # TODO: use an index
        return [x for x in self._node_set if x.isConnected()]

    def __getList(self, index_dict, key):
        return index_dict.setdefault(key, set())

    def getByStateList(self, state):
        """ Get a node list filtered per the node state """
        return list(self.__getList(self._state_dict, state))

    def __getTypeList(self, type_klass, only_identified=False):
        node_set = self.__getList(self._type_dict, type_klass)
        if only_identified:
            return [x for x in node_set if x.getUUID() in self._identified_dict]
        return list(node_set)

    def getMasterList(self, only_identified=False):
        """ Return a list with master nodes """
        return self.__getTypeList(MasterNode, only_identified)

    def getStorageList(self, only_identified=False):
        """ Return a list with storage nodes """
        return self.__getTypeList(StorageNode, only_identified)

    def getClientList(self, only_identified=False):
        """ Return a list with client nodes """
        return self.__getTypeList(ClientNode, only_identified)

    def getAdminList(self, only_identified=False):
        """ Return a list with admin nodes """
        return self.__getTypeList(AdminNode, only_identified)

    def getByAddress(self, address):
        """ Return the node that match with a given address """
        return self._address_dict.get(address, None)

    def getByUUID(self, uuid):
        """ Return the node that match with a given UUID """
        return self._uuid_dict.get(uuid, None)

    def hasAddress(self, address):
        return address in self._address_dict

    def hasUUID(self, uuid):
        return uuid in self._uuid_dict

    def _createNode(self, klass, address=None, uuid=None, **kw):
        by_address = self.getByAddress(address)
        by_uuid = self.getByUUID(uuid)
        if by_address is None and by_uuid is None:
            node = klass(self, address=address, uuid=uuid, **kw)
        else:
            if by_uuid is None or by_address is by_uuid:
                node = by_address
            elif by_address is None:
                node = by_uuid
            else:
                raise ValueError('Got different nodes for uuid %s: %r and '
                    'address %r: %r.' % (uuid_str(uuid), by_uuid, address,
                    by_address))
            if uuid is not None:
                node_uuid = node.getUUID()
                if node_uuid is None:
                    node.setUUID(uuid)
                elif node_uuid != uuid:
                    raise ValueError('Expected uuid %s on node %r' % (
                        uuid_str(uuid), node))
            if address is not None:
                node_address = node.getAddress()
                if node_address is None:
                    node.setAddress(address)
                elif node_address != address:
                    raise ValueError('Expected address %r on node %r' % (
                        address, node))
            assert node.__class__ is klass, (node.__class__, klass)
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

    def _getClassFromNodeType(self, node_type):
        klass = NODE_TYPE_MAPPING.get(node_type)
        if klass is None:
            raise ValueError('Unknown node type : %s' % node_type)
        return klass

    def createFromNodeType(self, node_type, **kw):
        return self._createNode(self._getClassFromNodeType(node_type), **kw)

    def update(self, node_list):
        for node_type, addr, uuid, state in node_list:
            # This should be done here (although klass might not be used in this
            # iteration), as it raises if type is not valid.
            klass = self._getClassFromNodeType(node_type)

            # lookup in current table
            node_by_uuid = self.getByUUID(uuid)
            node_by_addr = self.getByAddress(addr)
            node = node_by_uuid or node_by_addr

            log_args = node_type, uuid_str(uuid), addr, state
            if node is None:
                if state == NodeStates.DOWN:
                    logging.debug('NOT creating node %s %s %s %s', *log_args)
                else:
                    node = self._createNode(klass, address=addr, uuid=uuid,
                            state=state)
                    logging.debug('creating node %r', node)
            else:
                assert isinstance(node, klass), 'node %r is not ' \
                    'of expected type: %r' % (node, klass)
                assert None in (node_by_uuid, node_by_addr) or \
                    node_by_uuid is node_by_addr, \
                    'Discrepancy between node_by_uuid (%r) and ' \
                    'node_by_addr (%r)' % (node_by_uuid, node_by_addr)
                if state == NodeStates.DOWN:
                    logging.debug('droping node %r (%r), found with %s '
                        '%s %s %s', node, node.isConnected(), *log_args)
                    if node.isConnected():
                        # cut this connection, node removed by handler
                        node.getConnection().close()
                    self.remove(node)
                else:
                    logging.debug('updating node %r to %s %s %s %s',
                        node, *log_args)
                    node.setUUID(uuid)
                    node.setAddress(addr)
                    node.setState(state)
        self.log()

    def log(self):
        logging.info('Node manager : %u nodes', len(self._node_set))
        if self._node_set:
            node_list = [(node, uuid_str(node.getUUID()))
                         for node in sorted(self._node_set)]
            max_len = max(len(x[1]) for x in node_list)
            for node, uuid in node_list:
                address = node.getAddress() or ''
                if address:
                    address = '%s:%d' % address
                logging.info(' * %*s | %8s | %22s | %s',
                    max_len, uuid, node.getType(), address, node.getState())
