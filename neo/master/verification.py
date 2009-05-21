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
from neo.protocol import MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        ADMIN_NODE_TYPE
from neo.master.handler import MasterEventHandler
from neo.exception import VerificationFailure, ElectionFailure
from neo.protocol import Packet, UnexpectedPacketError, INVALID_UUID
from neo.util import dump
from neo.node import ClientNode, StorageNode, MasterNode, AdminNode
from neo.handler import identification_required, restrict_node_types

class VerificationEventHandler(MasterEventHandler):
    """This class deals with events for a verification phase."""

    def connectionClosed(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                app.broadcastNodeInformation(node)
                if node.getNodeType() in (CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif node.getNodeType() == STORAGE_NODE_TYPE:
                    if not app.pt.operational():
                        # Catastrophic.
                        raise VerificationFailure, 'cannot continue verification'
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                app.broadcastNodeInformation(node)
                if node.getNodeType() in (CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif node.getNodeType() == STORAGE_NODE_TYPE:
                    if not app.pt.operational():
                        # Catastrophic.
                        raise VerificationFailure, 'cannot continue verification'
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node.getState() != BROKEN_STATE:
                node.setState(BROKEN_STATE)
                app.broadcastNodeInformation(node)
                if node.getNodeType() in (CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif node.getNodeType() == STORAGE_NODE_TYPE:
                    cell_list = app.pt.dropNode(node)
                    ptid = app.getNextPartitionTableID()
                    app.broadcastPartitionChanges(ptid, cell_list)
                    if not app.pt.operational():
                        # Catastrophic.
                        raise VerificationFailure, 'cannot continue verification'
        MasterEventHandler.peerBroken(self, conn)

    def packetReceived(self, conn, packet):
        MasterEventHandler.packetReceived(self, conn, packet)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        app = self.app
        if node_type not in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
            logging.info('reject a connection from a client')
            raise protocol.NotReadyError
        if name != app.name:
            logging.error('reject an alien cluster')
            raise protocol.ProtocolError('invalid cluster name')

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
            # generate a new uuid for this node
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
                        raise protocol.ProtocolError('invalid server address')
                    node.setUUID(uuid)
                    if node.getState() != RUNNING_STATE:
                        node.setState(RUNNING_STATE)
                    app.broadcastNodeInformation(node)
                else:
                    # This node has a different UUID.
                    if node.getState() == RUNNING_STATE:
                        # If it is still running, reject this node.
                        raise protocol.ProtocolError('invalid server address')
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
                    raise protocol.ProtocolError('invalid server address')
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
                    raise protocol.BrokenNotDisallowedError
                node.setUUID(uuid)
                node.setState(RUNNING_STATE)
                app.broadcastNodeInformation(node)

        conn.setUUID(uuid)

        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE,
                                   app.uuid, app.server[0], app.server[1],
                                   app.num_partitions, app.num_replicas, uuid)
        # Next, the peer should ask a primary master node.
        conn.answer(p, packet)

    @identification_required
    def handleAskPrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        app = self.app

        # Merely tell the peer that I am the primary master node.
        # It is not necessary to send known master nodes, because
        # I must send all node information immediately.
        conn.answer(protocol.answerPrimaryMaster(app.uuid, []), packet)

        # Send the information.
        app.sendNodesInformations(conn)

        # If this is a storage node or an admin node, send the partition table.
        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() in (STORAGE_NODE_TYPE, ADMIN_NODE_TYPE):
            app.sendPartitionTable(conn)

    @identification_required
    def handleAnnouncePrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        # I am also the primary... So restart the election.
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    @identification_required
    def handleNotifyNodeInformation(self, conn, packet, node_list):
        uuid = conn.getUUID()
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

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.lptid < lptid:
            logging.critical('got later information in verification')
            raise VerificationFailure

    def handleAnswerPartitionTable(self, conn, packet, ptid, cell_list):
        # Ignore this packet.
        pass

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        uuid = conn.getUUID()
        logging.info('got unfinished transactions %s from %s:%d', 
                tid_list, *(conn.getAddress()))
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_tid_set.update(tid_list)
        app.asking_uuid_dict[uuid] = True

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleAnswerTransactionInformation(self, conn, packet, tid,
                                           user, desc, ext, oid_list):
        uuid = conn.getUUID()
        logging.info('got OIDs %s for %s from %s:%d', 
                oid_list, tid, *(conn.getAddress()))
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        oid_set = set(oid_list)
        if app.unfinished_oid_set is None:
            # Someone does not agree.
            pass
        elif len(app.unfinished_oid_set) == 0:
            # This is the first answer.
            app.unfinished_oid_set.update(oid_set)
        elif app.unfinished_oid_set != oid_set:
            app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleTidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('TID not found: %s', message)
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        uuid = conn.getUUID()
        logging.info('object %s:%s found', dump(oid), dump(tid))
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.asking_uuid_dict[uuid] = True

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleOidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        logging.info('OID not found: %s', message)
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return
        app.object_present = False
        app.asking_uuid_dict[uuid] = True
