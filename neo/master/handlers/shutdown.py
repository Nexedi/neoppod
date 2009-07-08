#
# Copyright (C) 2006-2009  Nexedi SA

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
from neo.protocol import CLIENT_NODE_TYPE, ADMIN_NODE_TYPE, INVALID_UUID, \
        RUNNING_STATE, STORAGE_NODE_TYPE, TEMPORARILY_DOWN_STATE, STOPPING
from neo.master.handlers import BaseServiceHandler
from neo import decorators
from neo.util import dump

class ShutdownHandler(BaseServiceHandler):
    """This class deals with events for a shutting down phase."""


    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        logging.error('reject any new connection')
        raise protocol.ProtocolError('cluster is shutting down')


    def handleAskPrimaryMaster(self, conn, packet):
        logging.error('reject any new demand for primary master')
        raise protocol.ProtocolError('cluster is shutting down')

    @decorators.identification_required
    @decorators.restrict_node_types(CLIENT_NODE_TYPE)
    def handleAskNewTID(self, conn, packet):
        logging.error('reject any new demand for new tid')
        raise protocol.ProtocolError('cluster is shutting down')

    @decorators.identification_required
    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        uuid = conn.getUUID()
        conn_node = app.nm.getNodeByUUID(uuid)
        if conn_node is None:
            raise RuntimeError('I do not know the uuid %r' % dump(uuid))

        if app.cluster_state == STOPPING and len(app.finishing_transaction_dict) == 0:
            # do not care about these messages as we are shutting down all nodes
            return

        for node_type, ip_address, port, uuid, state in node_list:
            if node_type in (CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                # No interest.
                continue

            if uuid == INVALID_UUID:
                # No interest.
                continue

            if app.uuid == uuid:
                # This looks like me...
                if state == RUNNING_STATE:
                    # Yes, I know it.
                    continue
                else:
                    # What?! What happened to me?
                    raise RuntimeError, 'I was told that I am bad'

            addr = (ip_address, port)
            node = app.nm.getNodeByUUID(uuid)
            if node is None:
                node = app.nm.getNodeByServer(addr)
                if node is None:
                    # I really don't know such a node. What is this?
                    continue
            else:
                if node.getServer() != addr:
                    # This is different from what I know.
                    continue

            if node.getState() == state:
                # No change. Don't care.
                continue

            if state == node.getState():
                # No problem.
                continue


            node.setState(state)
            # Something wrong happened possibly. Cut the connection to
            # this node, if any, and notify the information to others.
            # XXX this can be very slow.
            # XXX does this need to be closed in all cases ?
            for c in app.em.getConnectionList():
                if c.getUUID() == uuid:
                    c.close()

            logging.debug('broadcasting node information')
            app.broadcastNodeInformation(node)
            if node.getNodeType() == STORAGE_NODE_TYPE:
                if state == TEMPORARILY_DOWN_STATE:
                    cell_list = app.pt.outdate()
                    if len(cell_list) != 0:
                        ptid = app.pt.setNextID()
                        app.broadcastPartitionChanges(ptid, cell_list)
