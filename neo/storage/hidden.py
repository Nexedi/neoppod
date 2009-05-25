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

from neo.handler import EventHandler
from neo.protocol import Packet, \
        INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        DOWN_STATE, TEMPORARILY_DOWN_STATE, HIDDEN_STATE

from neo.util import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection
from neo.exception import PrimaryFailure

class HiddenEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)

    def handleAnnouncePrimaryMaster(self, conn, packet):
        """Theoretically speaking, I should not get this message,
        because the primary master election must happen when I am
        not connected to any master node."""
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            raise RuntimeError('I do not know the uuid %r' % dump(uuid))

        if node.getNodeType() != MASTER_NODE_TYPE:
            self.handleUnexpectedPacket(conn, packet)
            return

        if app.primary_master_node is None:
            # Hmm... I am somehow connected to the primary master already.
            app.primary_master_node = node
            if not isinstance(conn, ClientConnection):
                # I do not want a connection from any master node. I rather
                # want to connect from myself.
                conn.close()
        elif app.primary_master_node.getUUID() == uuid:
            # Yes, I know you are the primary master node.
            pass
        else:
            # It seems that someone else claims taking over the primary
            # master node...
            app.primary_master_node = None
            raise PrimaryFailure('another master node wants to take over')

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() != MASTER_NODE_TYPE \
                or app.primary_master_node is None \
                or app.primary_master_node.getUUID() != uuid:
            return

        for node_type, ip_address, port, uuid, state in node_list:
            addr = (ip_address, port)

            if node_type == STORAGE_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue

                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, BROKEN_STATE):
                        conn.close()
                        self.app.shutdown()
                    elif state == HIDDEN_STATE:
                        # I know I'm hidden
                        continue
                    else:
                        # I must be working again                
                        n = app.nm.getNodeByUUID(uuid)
                        n.setState(state)

            else:
                # Do not care of other node
                pass
