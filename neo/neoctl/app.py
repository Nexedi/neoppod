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
import os
from time import time
from struct import unpack
from collections import deque

from neo.config import ConfigurationManager
from neo.protocol import Packet, ProtocolError, node_types, node_states
from neo.protocol import TEMPORARILY_DOWN_STATE, DOWN_STATE, BROKEN_STATE, \
        INVALID_UUID, INVALID_PTID, partition_cell_states, MASTER_NODE_TYPE
from neo.event import EventManager
from neo.node import NodeManager, MasterNode, StorageNode, ClientNode, AdminNode
from neo.connection import ClientConnection 
from neo.exception import OperationFailure, PrimaryFailure
from neo.neoctl.handler import CommandEventHandler
from neo.connector import getConnectorHandler
from neo.util import bin, dump
from neo import protocol

class Application(object):
    """The storage node application."""

    def __init__(self, ip, port, handler):

        self.connector_handler = getConnectorHandler(handler)
        self.server = (ip, port)
        self.em = EventManager()
        self.ptid = INVALID_PTID


    def execute(self, args):        
        """Execute the command given."""
        handler = CommandEventHandler(self)
        # connect to admin node
        conn = None
        self.trying_admin_node = False
        try:
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
                
        except OperationFailure, msg:
            return "FAIL : %s" %(msg,)


        # here are the possible commands
        # print pt 1-10 [uuid] : print the partition table for row from 1 to 10 [containing node with uuid]
        # print pt all [uuid] : print the partition table for all rows [containing node with uuid]
        # print pt 10-0 [uuid] : print the partition table for row 10 to the end [containing node with uuid]
        # print node type : print list of node of the given type (STORAGE_NODE_TYPE, MASTER_NODE_TYPE...)
        # set node uuid state : set the node for the given uuid to the state (RUNNING_STATE, DOWN_STATE...)
        command = args[0]
        options = args[1:]
        if command == "print":
            print_type = options.pop(0)
            if print_type == "pt":
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
                p = protocol.askPartitionList(min_offset, max_offset, uuid)
            elif print_type == "node":
                node_type = options.pop(0)
                node_type = node_types.getFromStr(node_type)
                if node_type is None:
                    return 'unknown node type'
                p = protocol.askNodeList(node_type)
            else:
                return "unknown command options"
        elif command == "set":
            set_type = options.pop(0)
            if set_type == "node":
                uuid = bin(options.pop(0))
                state = options.pop(0)
                state = node_states.getFromStr(state)
                if state is None:
                    return "unknown state type"
                p = protocol.setNodeState(uuid, state)
            else:
                return "unknown command options"                                
        else:
            return "unknown command"
        
        conn.ask(p)
        self.result = ""
        while 1:
            self.em.poll(1)
            if len(self.result):
                break
        # close connection
        conn.close()
        return self.result
