import logging

from neo.protocol import MASTER_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE
from neo.master.handler import MasterEventHandler
from neo.exception import VerificationFailure
from neo.protocol import Packet, INVALID_UUID
from neo.util import dump

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
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif isinstance(node, StorageNode):
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
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif isinstance(node, StorageNode):
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
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                elif isinstance(node, StorageNode):
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
        if node_type not in (MASTER_NODE_TYPE, STORAGE_NODE_TYPE):
            logging.info('reject a connection from a client')
            conn.addPacket(Packet().notReady(packet.getId(), 'retry later'))
            conn.abort()
            return
        if name != app.name:
            logging.error('reject an alien cluster')
            conn.addPacket(Packet().protocolError(packet.getId(),
                                                  'invalid cluster name'))
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
        # First, get the node by the UUID.
        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            # If nothing is present, try with the server address.
            node = app.nm.getNodeByServer(addr)
            if node is None:
                # Nothing is found. So this must be the first time that this node
                # connected to me.
                if node_type == MASTER_NODE_TYPE:
                    node = MasterNode(server = addr, uuid = uuid)
                else:
                    node = StorageNode(server = address, uuid = uuid)
                app.nm.add(node)
                app.broadcastNodeInformation(node)
            else:
                # Otherwise, I know it only by the server address or the same server
                # address but with a different UUID.
                if node.getUUID() is None:
                    # This must be a master node.
                    if not isinstance(node, MasterNode) or node_type != MASTER_NODE_TYPE:
                        # Error. This node uses the same server address as a master
                        # node.
                        conn.addPacket(Packet().protocolError(packet.getId(),
                                                              'invalid server address'))
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
                        conn.addPacket(Packet().protocolError(packet.getId(),
                                                              'invalid server address'))
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
                    conn.addPacket(Packet().protocolError(packet.getId(),
                                                          'invalid server address'))
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
                    p = Packet()
                    p.brokenNodeDisallowedError(packet.getId(), 'go away')
                    conn.addPacket(p)
                    conn.abort()
                    return
                else:
                    node.setUUID(uuid)
                    node.setState(RUNNING_STATE)
                    app.broadcastNodeInformation(node)

        conn.setUUID(uuid)

        p = Packet()
        p.acceptNodeIdentification(packet.getId(), MASTER_NODE_TYPE,
                                   app.uuid, app.server[0], app.server[1],
                                   app.num_partitions, app.num_replicas)
        conn.addPacket(p)
        # Next, the peer should ask a primary master node.
        conn.expectMessage()

    def handleAskPrimaryMaster(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        # Merely tell the peer that I am the primary master node.
        # It is not necessary to send known master nodes, because
        # I must send all node information immediately.
        p = Packet()
        p.answerPrimaryMaster(packet.getId(), app.uuid, [])
        conn.addPacket(p)

        # Send the information.
        node_list = []
        for n in app.nm.getNodeList():
            ip_address, port = n.getServer()
            node_list.append((n.getNodeType(), ip_address, port, 
                              n.getUUID() or INVALID_UUID, n.getState()))
            if len(node_list) == 10000:
                # Ugly, but it is necessary to split a packet, if it is too big.
                p = Packet()
                p.notifyNodeInformation(conn.getNextId(), node_list)
                conn.addPacket(p)
                del node_list[:]
        p = Packet()
        p.notifyNodeInformation(conn.getNextId(), node_list)
        conn.addPacket(p)

        # If this is a storage node, send the partition table.
        node = app.nm.getNodeByUUID(uuid)
        if isinstance(node, StorageNode):
            # Split the packet if too huge.
            p = Packet()
            row_list = []
            for offset in xrange(app.num_partitions):
                row_list.append((offset, app.pt.getRow(offset)))
                if len(row_list) == 1000:
                    p.sendPartitionTable(app.lptid, row_list)
                    conn.addPacket(p)
                    del row_list[:]
            if len(row_list) != 0:
                p.sendPartitionTable(conn.getNextId(), app.lptid, row_list)
                conn.addPacket(p)

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
            if node_type == CLIENT_NODE:
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
        if not isinstance(node, StorageNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.lptid < lptid:
            logging.critical('got later information in verification')
            raise VerificationFailure

    def handleAnswerPartitionTable(self, conn, packet, cell_list):
        # Ignore this packet.
        pass

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        logging.info('got unfinished transactions %s from %s:%d', 
                tid_list, *(conn.getAddress()))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return

        app.unfinished_tid_set.update(tid_list)
        app.asking_uuid_dict[uuid] = True

    def handleAnswerOIDsByTID(self, conn, packet, oid_list, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        logging.info('got OIDs %s for %s from %s:%d', 
                oid_list, tid, *(conn.getAddress()))
        app = self.app
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

    def handleTidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        logging.info('TID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return

        app.unfinished_oid_set = None
        app.asking_uuid_dict[uuid] = True

    def handleAnswerObjectPresent(self, conn, packet, oid, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        logging.info('object %s:%s found', dump(oid), dump(tid))
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return

        app.asking_uuid_dict[uuid] = True

    def handleOidNotFound(self, conn, packet, message):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        logging.info('OID not found: %s', message)
        app = self.app
        if app.asking_uuid_dict.get(uuid, True):
            # No interest.
            return

        app.object_present = False
        app.asking_uuid_dict[uuid] = True
