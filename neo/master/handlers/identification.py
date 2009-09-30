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

from neo import logging

from neo import protocol
from neo.master.handlers import MasterHandler

class IdentificationHandler(MasterHandler):
    """This class deals with messages from the admin node only"""

    def handleNodeLost(self, conn, node):
        logging.warning('lost a node in IdentificationHandler : %s' % node)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
            uuid, address, name):

        self.checkClusterName(name)
        app, nm = self.app, self.app.nm
        node_by_uuid = nm.getByUUID(uuid)
        node_by_addr = nm.getByAddress(address)

        # handle conflicts and broken nodes
        node = node_by_uuid or node_by_addr
        if node_by_uuid is not None:
            if node.getAddress() == address:
                if node.getState() == protocol.BROKEN_STATE:
                    raise protocol.BrokenNodeDisallowedError
                # the node is still alive
                node.setState(protocol.RUNNING_STATE)
            if node.getAddress() != address:
                if node.getState() == protocol.RUNNING_STATE:
                    # still running, reject this new node
                    raise protocol.ProtocolError('invalid server address')
                # this node has changed its address
                node.setAddress(address)
                node.setState(protocol.RUNNING_STATE)
        if node_by_uuid is None and node_by_addr is not None:
            if node.getState() == protocol.RUNNING_STATE:
                # still running, reject this new node
                raise protocol.ProtocolError('invalid server address')
            node.setAddress(address)
            node.setState(protocol.RUNNING_STATE)

        # ask the app the node identification, if refused, an exception is raised
        result = self.app.identifyNode(node_type, uuid, node) 
        (uuid, node, state, handler, node_ctor) = result
        if uuid is None:
            # no valid uuid, give it one
            uuid = app.getNewUUID(node_type)
        if node is None:
            # new node
            node = node_ctor(uuid=uuid, address=address)
        handler = handler(self.app)
        # set up the node
        node.setUUID(uuid)
        node.setState(state)
        # set up the connection
        conn.setUUID(uuid)
        conn.setHandler(handler)
        # answer
        args = (protocol.MASTER_NODE_TYPE, app.uuid, app.server, 
                app.pt.getPartitions(), app.pt.getReplicas(), uuid)
        conn.answer(protocol.acceptNodeIdentification(*args), packet.getId())
        # trigger the event
        handler.connectionCompleted(conn)
        app.broadcastNodeInformation(node)

