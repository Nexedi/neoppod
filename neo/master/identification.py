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

from neo import protocol
from neo.master.handler import MasterEventHandler

class IdentificationEventHandler(MasterEventHandler):
    """This class deals with messages from the admin node only"""

    def _nodeLost(self, conn, node):
        logging.warning('lost a node in IdentificationEventHandler : %s' % node)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
            uuid, ip_address, port, name):

        self.checkClusterName(name)
        app, nm = self.app, self.app.nm
        server = (ip_address, port)
        node_by_uuid = nm.getNodeByUUID(uuid)
        node_by_addr = nm.getNodeByServer(server)

        # handle conflicts and broken nodes
        node = node_by_uuid or node_by_addr
        if node_by_uuid is not None:
            if node.getServer() == server:
                if node.getState() == protocol.BROKEN_STATE:
                    raise protocol.BrokenNodeDisallowedError
                # the node is still alive
                node.setState(protocol.RUNNING_STATE)
            if node.getServer() != server:
                if node.getState() == protocol.RUNNING_STATE:
                    # still running, reject this new node
                    raise protocol.ProtocolError('invalid server address')
                # this node has changed its address
                node.setServer(server)
                node.setState(protocol.RUNNING_STATE)
        if node_by_uuid is None and node_by_addr is not None:
            if node.getState() == protocol.RUNNING_STATE:
                # still running, reject this new node
                raise protocol.ProtocolError('invalid server address')
            # FIXME: here the node was known with a different uuid but with the
            # same address, is it safe to forgot the old, even if he's not
            # running ?
            node.setServer(server)
            node.setState(protocol.RUNNING_STATE)

        # ask the app the node identification, if refused, an exception is raised
        result = self.app.identifyNode(node_type, uuid, node) 
        (uuid, node, state, handler, klass) = result
        if uuid == protocol.INVALID_UUID:
            # no valid uuid, give it one
            uuid = app.getNewUUID(node_type)
        if node is None:
            # new node
            node = klass(uuid=uuid, server=(ip_address, port))
            app.nm.add(node)
        handler = handler(self.app)
        # set up the node
        node.setUUID(uuid)
        node.setState(state)
        # set up the connection
        conn.setUUID(uuid)
        conn.setHandler(handler)
        # answer
        args = (protocol.MASTER_NODE_TYPE, app.uuid, app.server[0], app.server[1], 
                app.pt.getPartitions(), app.pt.getReplicas(), uuid)
        conn.answer(protocol.acceptNodeIdentification(*args), packet)
        # trigger the event
        handler.connectionCompleted(conn)
        app.broadcastNodeInformation(node)

