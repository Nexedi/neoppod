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
from neo.protocol import Packet, INVALID_UUID, INVALID_PTID
from neo.node import ClientNode, StorageNode, MasterNode, AdminNode
from neo.util import dump

class RecoveryEventHandler(MasterEventHandler):
    """This class deals with events for a recovery phase."""

    def connectionClosed(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                app.broadcastNodeInformation(node)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                app.broadcastNodeInformation(node)
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() != BROKEN_STATE:
                node.setState(BROKEN_STATE)
                app.broadcastNodeInformation(node)
        MasterEventHandler.peerBroken(self, conn)

    def packetReceived(self, conn, packet):
        MasterEventHandler.packetReceived(self, conn, packet)

    def handleRequestNodeIdentification(self, conn, packet, node_type, uuid, 
                                        ip_address, port, name):
        app = self.app
        if node_type not in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
            logging.info('reject a connection from a client')
            conn.answer(protocol.notReady('retry later'), packet)
            conn.abort()
            return
        if name != app.name:
            logging.error('reject an alien cluster')
            conn.answer(protocol.protocolError('invalid cluster name'), packet)
            conn.abort()
            return

        # Here are many situations. In principle, a node should be identified by
        # an UUID, since an UUID never change when moving a storage node to a different
        # server, and an UUID always changes for a master node and a client node whenever
        # it restarts, so more reliable than a server address.
        # 
        # However, master nodes can be known only as the server addresses. And, a node
        # may claim a server address used by another node.

        addr = (ip_address, port)
        if not app.isValidUUID(uuid, addr):
            # Here we have an UUID conflict, assume that's a new node
            node = None
        else:
            # First, get the node by the UUID.
            node = app.nm.getNodeByUUID(uuid)
        if node is None:
            # generate an uuid for this node
            while not app.isValidUUID(uuid, addr):
                uuid = app.getNewUUID(node_type)
            # If nothing is present, try with the server address.
            node = app.nm.getNodeByServer(addr)
            if node is None:
                # Nothing is found. So this must be the first time that this node
                # connected to me.
                if node_type == MASTER_NODE_TYPE:
                    node = MasterNode(server = addr, uuid = uuid)
                elif node_type == ADMIN_NODE_TYPE:
                    node = AdminNode(uuid = uuid)
                else:
                    node = StorageNode(server = addr, uuid = uuid)
                app.nm.add(node)
                app.broadcastNodeInformation(node)
            else:
                # Otherwise, I know it only by the server address or the same server
                # address but with a different UUID.
                if node.getUUID() is None:
                    # This must be a master node.
                    if node.getNodeType() != MASTER_NODE_TYPE or node_type != MASTER_NODE_TYPE:
                        # Error. This node uses the same server address as a master
                        # node.
                        p = protocol.protocolError('invalid server address') 
                        conn.answer(p, packet)
                        conn.abort()
                        return

                    node.setUUID(uuid)
                    if node.getState() != RUNNING_STATE:
                        node.setState(RUNNING_STATE)
                    app.broadcastNodeInformation(node)
                else:
                    # This node has a different UUID.
                    if node.getState() == RUNNING_STATE:
                        # If it is still running, reject this node.
                        p = protocol.protocolError('invalid server address')
                        conn.answer(p, packet)
                        conn.abort()
                        return
                    else:
                        # Otherwise, forget the old one.
                        node.setState(BROKEN_STATE)
                        app.broadcastNodeInformation(node)
                        # And insert a new one.
                        node.setUUID(uuid)
                        node.setState(RUNNING_STATE)
                        app.broadcastNodeInformation(node)
        else:
            # I know this node by the UUID.
            if node.getServer() != addr:
                # This node has a different server address.
                if node.getState() == RUNNING_STATE:

                    # If it is still running, reject this node.
                    p = protocol.protocolError('invalid server address')
                    conn.answer(p, packet)
                    conn.abort()
                    return
                else:
                    # Otherwise, forget the old one.
                    node.setState(BROKEN_STATE)
                    app.broadcastNodeInformation(node)
                    # And insert a new one.
                    node.setServer(addr)
                    node.setState(RUNNING_STATE)
                    app.broadcastNodeInformation(node)
            else:
                # If this node is broken, reject it. Otherwise, assume that it is
                # working again.
                if node.getState() == BROKEN_STATE:
                    p = protocol.brokenNodeDisallowedError('go away')
                    conn.answer(p, packet)
                    conn.abort()
                    return
                else:
                    node.setUUID(uuid)
                    node.setState(RUNNING_STATE)
                    app.broadcastNodeInformation(node)

        conn.setUUID(uuid)

        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE,
                                   app.uuid, app.server[0], app.server[1],
                                   app.num_partitions, app.num_replicas, uuid)
        # Next, the peer should ask a primary master node.
        conn.answer(p, packet)

    def handleAskPrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        # Merely tell the peer that I am the primary master node.
        # It is not necessary to send known master nodes, because
        # I must send all node information immediately.
        p = protocol.answerPrimaryMaster(app.uuid, [])
        conn.answer(p, packet)

        # Send the information.
        node_list = []
        for n in app.nm.getNodeList():
            try:
                ip_address, port = n.getServer()
            except TypeError:
                ip_address, port = '0.0.0.0', 0
            node_list.append((n.getNodeType(), ip_address, port, 
                              n.getUUID() or INVALID_UUID, n.getState()))
            if len(node_list) == 10000:
                # Ugly, but it is necessary to split a packet, if it is too big.
                conn.notify(protocol.notifyNodeInformation(node_list))
                del node_list[:]
        conn.notify(protocol.notifyNodeInformation(node_list))

        # If this is a storage node, ask the last IDs.
        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() == STORAGE_NODE_TYPE:
            conn.ask(protocol.askLastIDs())
        elif node.getNodeType() == ADMIN_NODE_TYPE and app.lptid != INVALID_PTID:
            # send partition table if exists
            logging.info('sending partition table %s to %s' % (dump(app.lptid),
                                                              conn.getAddress()))
            # Split the packet if too huge.
            row_list = []
            for offset in xrange(app.num_partitions):
                row_list.append((offset, app.pt.getRow(offset)))
                if len(row_list) == 1000:
                    conn.notify(protocol.sendPartitionTable(app.lptid, row_list))
                    del row_list[:]
            if len(row_list) != 0:
                conn.notify(protocol.sendPartitionTable(app.lptid, row_list))

    def handleAnnouncePrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        # I am also the primary... So restart the election.
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

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
            for c in app.em.getConnectionList():
                if c.getUUID() == uuid:
                    c.close()
            node.setState(state)
            app.broadcastNodeInformation(node)

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() != STORAGE_NODE_TYPE:
            self.handleUnexpectedPacket(conn, packet)
            return

        # If the target is still unknown, set it to this node for now.
        if app.target_uuid is None:
            app.target_uuid = uuid

        # Get max values.
        if app.loid < loid:
            app.loid = loid
        if app.ltid < ltid:
            app.ltid = ltid
        if app.lptid == INVALID_PTID or app.lptid < lptid:
            app.lptid = lptid
            # I need to use the node which has the max Partition Table ID.
            app.target_uuid = uuid
        elif app.lptid == lptid and app.target_uuid is None:
            app.target_uuid = uuid

    def handleAnswerPartitionTable(self, conn, packet, ptid, row_list):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() != STORAGE_NODE_TYPE:
            self.handleUnexpectedPacket(conn, packet)
            return
        if uuid != app.target_uuid:
            # If this is not from a target node, ignore it.
            logging.warn('got answer partition table from %s while waiting for %s',
                         dump(uuid), dump(app.target_uuid))
            return

        for offset, cell_list in row_list:
            if offset >= app.num_partitions or app.pt.hasOffset(offset):
                # There must be something wrong.
                self.handleUnexpectedPacket(conn, packet)
                return

            for uuid, state in cell_list:
                n = app.nm.getNodeByUUID(uuid)
                if n is None:
                    n = StorageNode(uuid = uuid)
                    n.setState(TEMPORARILY_DOWN_STATE)
                    app.nm.add(n)
                app.pt.setCell(offset, n, state)

