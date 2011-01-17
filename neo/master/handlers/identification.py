#
# Copyright (C) 2006-2010  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import neo

from neo.lib.protocol import NodeTypes, Packets
from neo.lib.protocol import BrokenNodeDisallowedError, ProtocolError
from neo.master.handlers import MasterHandler

class IdentificationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def nodeLost(self, conn, node):
        neo.lib.logging.warning('
                        lost a node in IdentificationHandler : %s' % node)

    def requestIdentification(self, conn, node_type, uuid, address, name):

        self.checkClusterName(name)
        app, nm = self.app, self.app.nm
        node_by_uuid = nm.getByUUID(uuid)
        node_by_addr = nm.getByAddress(address)

        # handle conflicts and broken nodes
        node = node_by_uuid or node_by_addr
        if node_by_uuid is not None:
            if node.getAddress() == address:
                # the node is still alive
                if node.isBroken():
                    raise BrokenNodeDisallowedError
            if node.getAddress() != address:
                # this node has changed its address
                if node.isRunning():
                   # still running, reject this new node
                    raise ProtocolError('invalid server address')
        if node_by_uuid is None and node_by_addr is not None:
            if node.isRunning():
                # still running, reject this new node
                raise ProtocolError('invalid server address')
        if node is not None:
            if node.isConnected():
                # more than one connection from this node
                raise ProtocolError('already connected')
            node.setAddress(address)
            node.setRunning()

        # ask the app the node identification, if refused, an exception is
        # raised
        result = self.app.identifyNode(node_type, uuid, node)
        (uuid, node, state, handler, node_ctor) = result
        if uuid is None:
            # no valid uuid, give it one
            uuid = app.getNewUUID(node_type)
        if node is None:
            # new node
            node = node_ctor(uuid=uuid, address=address)
        # set up the node
        node.setUUID(uuid)
        node.setState(state)
        node.setConnection(conn)
        # set up the connection
        conn.setUUID(uuid)
        conn.setHandler(handler)
        # answer
        args = (NodeTypes.MASTER, app.uuid, app.pt.getPartitions(),
            app.pt.getReplicas(), uuid)
        conn.answer(Packets.AcceptIdentification(*args))
        # trigger the event
        handler.connectionCompleted(conn)
        app.broadcastNodesInformation([node])

