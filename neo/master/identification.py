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
from neo.node import AdminNode, MasterNode, ClientNode, StorageNode
from neo.master.handler import MasterEventHandler
from neo import decorators

class IdentificationEventHandler(MasterEventHandler):
    """This class deals with messages from the admin node only"""

    def connectionClosed(self, conn):
        logging.warning('lost a node in IdentificationEventHandler')

    def timeoutExpired(self, conn):
        logging.warning('lost a node in IdentificationEventHandler')

    def peerBroken(self, conn):
        logging.warning('lost a node in IdentificationEventHandler')

    # TODO: move this into a new handler
    @decorators.identification_required
    def handleAnnouncePrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        # I am also the primary... So restart the election.
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        # XXX: Secondary master can send this packet 
        logging.error('ignoring NotifyNodeInformation packet')

    def handleRequestNodeIdentification(self, conn, packet, node_type,
            uuid, ip_address, port, name):

        self.checkClusterName(name)
        app, nm = self.app, self.app.nm
        server = (ip_address, port)
        node_by_uuid = nm.getNodeByUUID(uuid)
        node_by_addr = nm.getNodeByServer(server)

        def changeNodeAddress(node, server):
            from copy import copy
            if node_type == protocol.STORAGE_NODE_TYPE:
                args = (node.getServer(), server)
                # remove storage from partition table
                cell_list = app.pt.dropNode(node)
                if cell_list:
                    ptid = app.pt.setNextID()
                    app.broadcastPartitionChanges(ptid, cell_list)
            # TODO: check this, avoid copy()
            # set it to down state
            node.setState(protocol.DOWN_STATE)
            app.broadcastNodeInformation(node)
            nm.remove(node)
            # then update the address and set it to running state
            node = copy(node)
            node.setServer(server)
            node.setState(protocol.RUNNING_STATE)
            nm.add(node)
            return node

        # handle conflicts and broken nodes
        node = node_by_uuid or node_by_addr
        if node_by_uuid is not None:
            if node.getServer() == server and node.getState() == protocol.BROKEN_STATE:
                raise protocol.BrokenNodeDisallowedError
            if node.getServer() != server:
                if node.getState() == protocol.RUNNING_STATE:
                    raise protocol.ProtocolError('invalid server address')
                node = changeNodeAddress(node, server)
        if node_by_uuid is None and node_by_addr is not None:
            if node.getState() == protocol.RUNNING_STATE:
                raise protocol.ProtocolError('invalid server address')
            node = changeNodeAddress(node, server)

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

