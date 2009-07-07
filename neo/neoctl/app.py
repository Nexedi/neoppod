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

from neo.protocol import node_types, node_states
from neo.protocol import INVALID_UUID, INVALID_PTID
from neo.event import EventManager
from neo.connection import ClientConnection
from neo.exception import OperationFailure
from neo.neoctl.handler import CommandEventHandler
from neo.connector import getConnectorHandler
from neo.util import bin
from neo import protocol

class ActionError(Exception): pass

def addAction(options):
    """
      Add storage nodes pending addition into cluster.
      Parameters:
      node uuid
        UUID of node to add, or "all".
    """
    if len(options) == 1 and options[0] == 'all':
        uuid_list = []
    else:
        uuid_list = [bin(opt) for opt in options]
    return protocol.addPendingNodes(uuid_list)

def setClusterAction(options):
    """
      Set cluster of given name to given state.
      Parameters:
      cluster state
        State to put the cluster in.
    """
    # XXX: why do we ask for a cluster name ?
    # We connect to only one cluster that we get from configuration file,
    # anyway.
    name, state = options
    state = protocol.cluster_states.getFromStr(state)
    if state is None:
        raise ActionError('unknown cluster state')
    return protocol.setClusterState(name, state)

def setNodeAction(options):
    """
      Put given node into given state.
      Parameters:
      node uuid
        UUID of target node
      node state
        Node state to set.
      change partition table (optional)
        If given with a 1 value, allow partition table to be changed.
    """
    uuid = bin(options.pop(0))
    state = node_states.getFromStr(options.pop(0))
    if state is None:
        raise ActionError('unknown state type')
    if len(options):
        modify = int(options.pop(0))
    else:
        modify = 0
    return protocol.setNodeState(uuid, state, modify)

def printClusterAction(options):
    """
      Print cluster state.
    """
    return protocol.askClusterState()

def printNodeAction(options):
    """
      Print nodes of a given type.
      Parameters:
      node type
        Print known nodes of given type.
    """
    node_type = node_types.getFromStr(options.pop(0))
    if node_type is None:
        raise ActionError('unknown node type')
    return protocol.askNodeList(node_type)

def printPTAction(options):
    """
      Print the partition table.
      Parameters:
      range
        all   Prints the entire partition table.
        1 10  Prints rows 1 to 10 of partition table.
        10 0  Prints rows from 10 to the end of partition table.
      node uuid (optional)
        If given, prints only the rows in which given node is used.
    """
    offset = options.pop(0)
    if offset == "all":
        min_offset = 0
        max_offset = 0
    else:
        min_offset = int(offset)
        max_offset = int(options.pop(0))
    if len(options):
        uuid = bin(options.pop(0))
    else:
        uuid = INVALID_UUID
    return protocol.askPartitionList(min_offset, max_offset, uuid)

action_dict = {
    'print': {
        'pt': printPTAction,
        'node': printNodeAction,
        'cluster': printClusterAction,
    },
    'set': {
        'node': setNodeAction,
        'cluster': setClusterAction,
    },
    'add': addAction,
}

class Application(object):
    """The storage node application."""

    conn = None

    def __init__(self, ip, port, handler):

        self.connector_handler = getConnectorHandler(handler)
        self.server = (ip, port)
        self.em = EventManager()
        self.ptid = INVALID_PTID

    def getConnection(self):
        if self.conn is None:
            handler = CommandEventHandler(self)
            # connect to admin node
            self.trying_admin_node = False
            conn = None
            while 1:
                self.em.poll(1)
                if conn is None:
                    self.trying_admin_node = True
                    logging.info('connecting to address %s:%d', *(self.server))
                    conn = ClientConnection(self.em, handler, \
                                            addr = self.server,
                                            connector_handler = self.connector_handler)
                if self.trying_admin_node is False:
                    break
            self.conn = conn
        return self.conn

    def doAction(self, packet):
        conn = self.getConnection()

        conn.ask(packet)
        self.result = ""
        while 1:
            self.em.poll(1)
            if len(self.result):
                break

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def execute(self, args):
        """Execute the command given."""
        # print node type : print list of node of the given type (STORAGE_NODE_TYPE, MASTER_NODE_TYPE...)
        # set node uuid state [1|0] : set the node for the given uuid to the state (RUNNING_STATE, DOWN_STATE...)
        #                             and modify the partition if asked
        # set cluster name [shutdown|operational] : either shutdown the cluster or mark it as operational
        current_action = action_dict
        level = 1
        while current_action is not None and \
              level < len(args) and \
              isinstance(current_action, dict):
            current_action = current_action.get(args[level])
            level += 1
        if callable(current_action):
            try:
                p = current_action(args[level:])
            except ActionError, message:
                self.result = message
            else:
                self.doAction(p)
        else:
            self.result = usage('unknown command')

        return self.result

def _usage(action_dict, level=0):
    result = []
    append = result.append
    sub_level = level + 1
    for name, action in action_dict.iteritems():
        append('%s%s' % ('  ' * level, name))
        if isinstance(action, dict):
            append(_usage(action, level=sub_level))
        else:
            docstring_line_list = getattr(action, '__doc__',
                                          '(no docstring)').split('\n')
            # Strip empty lines at begining & end of line list
            for end in (0, -1):
                while len(docstring_line_list) \
                      and docstring_line_list[end] == '':
                    docstring_line_list.pop(end)
            # Get the indentation of first line, to preserve other lines
            # relative indentation.
            first_line = docstring_line_list[0]
            base_indentation = len(first_line) - len(first_line.lstrip())
            result.extend([('  ' * sub_level) + x[base_indentation:] \
                           for x in docstring_line_list])
    return '\n'.join(result)

def usage(message):
    output_list = [message, 'Available commands:', _usage(action_dict)]
    return '\n'.join(output_list)

