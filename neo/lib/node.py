#
# Copyright (C) 2006-2016  Nexedi SA
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
from .protocol import formatNodeList, uuid_str, \
    NodeTypes, NodeStates, ProtocolError


class Node(object):
    """This class represents a node."""

    _connection = None
    _identified = False
    id_timestamp = None

    def __init__(self, manager, address=None, uuid=None,
            state=NodeStates.UNKNOWN):
        self._state = state
        self._address = address
        self._uuid = uuid
        self._manager = manager
        self._last_state_change = time()
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
        addr = self._address
        return '<%s(uuid=%s%s, state=%s, connection=%r%s) at %x>' % (
            self.__class__.__name__,
            uuid_str(self._uuid),
            ', address=' + ('[%s]:%s' if ':' in addr[0] else '%s:%s') % addr
            if addr else '',
            self._state,
            self._connection,
            '' if self._identified else ', not identified',
            id(self),
        )

    def asTuple(self):
        """ Returned tuple is intended to be used in protocol encoders """
        return (self.getType(), self._address, self._uuid, self._state,
                self.id_timestamp)

    def __gt__(self, node):
        # sort per UUID if defined
        if self._uuid is not None:
            return self._uuid > node._uuid
        return self._address > node._address

    def whoSetState(self):
        """
          Debugging method: call this method to know who set the current
          state value.
        """
        return attributeTracker.whoSet(self, '_state')

attributeTracker.track(Node)


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
        Path to a file containing master nodes' addresses. Used to automate
        master list updates. If not provided, no automation will happen.
        """
        self._node_set = set()
        self._address_dict = {}
        self._uuid_dict = {}
        self._type_dict = {}
        self._state_dict = {}
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
        self.__updateSet(self._type_dict, None, node.getType(), node)
        self.__updateSet(self._state_dict, None, node.getState(), node)
        if node.isMaster() and self._master_db is not None:
            self._master_db.add(node.getAddress())

    def remove(self, node):
        if node not in self._node_set:
            logging.warning('removing unknown node %r, ignoring', node)
            return
        self._node_set.remove(node)
        # a node may have not be indexed by uuid or address, eg.:
        # - a client or admin node that don't have listening address
        self._address_dict.pop(node.getAddress(), None)
        # - a master known by address but without UUID
        self._uuid_dict.pop(node.getUUID(), None)
        self._state_dict[node.getState()].remove(node)
        self._type_dict[node.getType()].remove(node)
        uuid = node.getUUID()
        if node.isMaster() and self._master_db is not None:
            self._master_db.discard(node.getAddress())

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

    def _updateAddress(self, node, old_address):
        self.__update(self._address_dict, old_address, node.getAddress(), node)

    def _updateUUID(self, node, old_uuid):
        self.__update(self._uuid_dict, old_uuid, node.getUUID(), node)

    def __updateSet(self, set_dict, old_key, new_key, node):
        """ Update a set index from old to new key """
        if old_key in set_dict:
            set_dict[old_key].remove(node)
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
        return [x for x in self._node_set if x.isIdentified() and (
            pool_set is None or x.getUUID() in pool_set)]

    def getConnectedList(self):
        """
            Returns a generator to iterate over connected nodes
        """
        # TODO: use an index
        return [x for x in self._node_set if x.isConnected()]

    def getByStateList(self, state):
        """ Get a node list filtered per the node state """
        return list(self._state_dict.get(state, ()))

    def _getTypeList(self, node_type, only_identified=False):
        node_set = self._type_dict.get(node_type, ())
        if only_identified:
            return [x for x in node_set if x.isIdentified()]
        return list(node_set)

    def getByAddress(self, address):
        """ Return the node that match with a given address """
        return self._address_dict.get(address, None)

    def getByUUID(self, uuid, *id_timestamp):
        """ Return the node that match with a given UUID """
        node = self._uuid_dict.get(uuid)
        if not id_timestamp or node and (node.id_timestamp,) == id_timestamp:
            return node

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

    def createFromNodeType(self, node_type, **kw):
        return self._createNode(NODE_TYPE_MAPPING[node_type], **kw)

    def update(self, app, node_list):
        node_set = self._node_set.copy() if app.id_timestamp is None else None
        for node_type, addr, uuid, state, id_timestamp in node_list:
            # This should be done here (although klass might not be used in this
            # iteration), as it raises if type is not valid.
            klass = NODE_TYPE_MAPPING[node_type]

            # lookup in current table
            node_by_uuid = self.getByUUID(uuid)
            node_by_addr = self.getByAddress(addr)
            node = node_by_uuid or node_by_addr

            log_args = node_type, uuid_str(uuid), addr, state, id_timestamp
            if node is None:
                if state == NodeStates.DOWN:
                    logging.debug('NOT creating node %s %s %s %s %s', *log_args)
                    continue
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
                    logging.debug('dropping node %r (%r), found with %s '
                        '%s %s %s %s', node, node.isConnected(), *log_args)
                    if node.isConnected():
                        # Cut this connection, node removed by handler.
                        # It's important for a storage to disconnect nodes that
                        # aren't connected to the primary master, in order to
                        # avoid conflict of node id. The clients will first
                        # reconnect to the master because they cleared their
                        # partition table upon disconnection.
                        node.getConnection().close()
                    self.remove(node)
                    continue
                logging.debug('updating node %r to %s %s %s %s %s',
                    node, *log_args)
                node.setUUID(uuid)
                node.setAddress(addr)
                node.setState(state)
            node.id_timestamp = id_timestamp
            if app.uuid == uuid:
                app.id_timestamp = id_timestamp
        if node_set:
            # For the first notification, we receive a full list of nodes from
            # the master. Remove all unknown nodes from a previous connection.
            for node in node_set - self._node_set:
                self.remove(node)
        self.log()

    def log(self):
        logging.info('Node manager : %u nodes', len(self._node_set))
        if self._node_set:
            logging.info('\n'.join(formatNodeList(
                map(Node.asTuple, self._node_set), ' * ')))

@apply
def NODE_TYPE_MAPPING():
    def setmethod(cls, attr, value):
        assert not hasattr(cls, attr), (cls, attr)
        setattr(cls, attr, value)
    def setfullmethod(cls, attr, value):
        value.__name__ = attr
        setmethod(cls, attr, value)
    def camel_case(enum):
        return str(enum).replace('_', ' ').title().replace(' ', '')
    def setStateAccessors(state):
        name = camel_case(state)
        setfullmethod(Node, 'set' + name, lambda self: self.setState(state))
        setfullmethod(Node, 'is' + name, lambda self: self._state == state)
    map(setStateAccessors, NodeStates)

    node_type_dict = {}
    getType = lambda node_type: staticmethod(lambda: node_type)
    true = staticmethod(lambda: True)
    createNode = lambda cls: lambda self, **kw: self._createNode(cls, **kw)
    getList = lambda node_type: lambda self, only_identified=False: \
        self._getTypeList(node_type, only_identified)
    bases = Node,
    for node_type in NodeTypes:
        name = camel_case(node_type)
        is_name = 'is' + name
        setmethod(Node, is_name, bool)
        node_type_dict[node_type] = cls = type(name + 'Node', bases, {
            'getType': getType(node_type),
            is_name: true,
            })
        setfullmethod(NodeManager, 'create' + name, createNode(cls))
        setfullmethod(NodeManager, 'get%sList' % name, getList(node_type))

    return node_type_dict
