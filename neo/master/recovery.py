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
from neo.protocol import MASTER_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, ADMIN_NODE_TYPE
from neo.master.handler import MasterEventHandler
from neo.exception import ElectionFailure
from neo.protocol import Packet, UnexpectedPacketError, INVALID_UUID, INVALID_PTID
from neo.node import ClientNode, StorageNode, MasterNode, AdminNode
from neo.util import dump

class RecoveryEventHandler(MasterEventHandler):
    """This class deals with events for a recovery phase."""

    def connectionCompleted(self, conn):
        # ask the last IDs to perform the recovery
        conn.ask(protocol.askLastIDs())

    def connectionClosed(self, conn):
        app = self.app
        uuid = conn.getUUID()
        node = app.nm.getNodeByUUID(uuid)
        if node.getState() == RUNNING_STATE:
            node.setState(TEMPORARILY_DOWN_STATE)
            app.broadcastNodeInformation(node)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        app = self.app
        uuid = conn.getUUID()
        node = app.nm.getNodeByUUID(uuid)
        if node.getState() == RUNNING_STATE:
            node.setState(TEMPORARILY_DOWN_STATE)
            app.broadcastNodeInformation(node)
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        app = self.app
        uuid = conn.getUUID()
        node = app.nm.getNodeByUUID(uuid)
        if node.getState() != BROKEN_STATE:
            node.setState(BROKEN_STATE)
            app.broadcastNodeInformation(node)
        MasterEventHandler.peerBroken(self, conn)

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
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

            if state == RUNNING_STATE:
                # No problem.
                continue

            # Something wrong happened possibly. Cut the connection to this node,
            # if any, and notify the information to others.
            # XXX this can be very slow.
            c = app.em.getConnectionByUUID(uuid)
            if c is not None:
                c.close()
            node.setState(state)
            app.broadcastNodeInformation(node)

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # If the target is still unknown, set it to this node for now.
        if app.target_uuid is None:
            app.target_uuid = uuid

        # Get max values.
        if app.loid < loid:
            app.loid = loid
        if app.ltid < ltid:
            app.ltid = ltid
        if app.pt.getID() == INVALID_PTID or app.pt.getID() < lptid:
            app.pt.setID(lptid)
            # I need to use the node which has the max Partition Table ID.
            app.target_uuid = uuid
        elif app.pt.getID() == lptid and app.target_uuid is None:
            app.target_uuid = uuid

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if uuid != app.target_uuid:
            # If this is not from a target node, ignore it.
            logging.warn('got answer partition table from %s while waiting for %s',
                         dump(uuid), dump(app.target_uuid))
            return

        for offset, cell_list in row_list:
            if offset >= app.pt.getPartitions() or app.pt.hasOffset(offset):
                # There must be something wrong.
                raise UnexpectedPacketError

            for uuid, state in cell_list:
                n = app.nm.getNodeByUUID(uuid)
                if n is None:
                    n = StorageNode(uuid = uuid)
                    n.setState(TEMPORARILY_DOWN_STATE)
                    app.nm.add(n)
                app.pt.setCell(offset, n, state)

