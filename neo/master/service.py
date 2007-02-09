import logging
from copy import copy

from neo.protocol import MASTER_NODE_TYPE, CLIENT_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE
from neo.master.handler import MasterEventHandler
from neo.protocol import Packet, INVALID_UUID
from neo.exception import OperationFailure, ElectionFailure
from neo.node import ClientNode, StorageNode, MasterNode
from neo.util import dump

class FinishingTransaction(object):
    """This class describes a finishing transaction."""

    def __init__(self, conn):
        self._conn = conn
        self._msg_id = None
        self._oid_list = None
        self._uuid_set = None
        self._locked_uuid_set = set()

    def getConnection(self):
        return self._conn

    def setMessageId(self, msg_id):
        self._msg_id = msg_id

    def getMessageId(self):
        return self._msg_id

    def setOIDList(self, oid_list):
        self._oid_list = oid_list

    def getOIDList(self):
        return self._oid_list

    def setUUIDSet(self, uuid_set):
        self._uuid_set = uuid_set

    def getUUIDSet(self):
        return self._uuid_set

    def addLockedUUID(self, uuid):
        if uuid in self._uuid_set:
            self._locked_uuid_set.add(uuid)

    def allLocked(self):
        return self._uuid_set == self._locked_uuid_set

class ServiceEventHandler(MasterEventHandler):
    """This class deals with events for a service phase."""

    def connectionClosed(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node is not None and node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                logging.debug('broadcasting node information')
                app.broadcastNodeInformation(node)
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                    for tid, t in app.finishing_transaction_dict.items():
                        if t.getConnection() is conn:
                            del app.finishing_transaction_dict[tid]
                elif isinstance(node, StorageNode):
                    if not app.pt.operational():
                        # Catastrophic.
                        raise OperationFailure, 'cannot continue operation'
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node is not None and node.getState() == RUNNING_STATE:
                node.setState(TEMPORARILY_DOWN_STATE)
                logging.debug('broadcasting node information')
                app.broadcastNodeInformation(node)
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                    for tid, t in app.finishing_transaction_dict.items():
                        if t.getConnection() is conn:
                            del app.finishing_transaction_dict[tid]
                elif isinstance(node, StorageNode):
                    if not app.pt.operational():
                        # Catastrophic.
                        raise OperationFailure, 'cannot continue operation'
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        uuid = conn.getUUID()
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if node is not None and node.getState() != BROKEN_STATE:
                node.setState(BROKEN_STATE)
                logging.debug('broadcasting node information')
                app.broadcastNodeInformation(node)
                if isinstance(node, ClientNode):
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                    for tid, t in app.finishing_transaction_dict.items():
                        if t.getConnection() is conn:
                            del app.finishing_transaction_dict[tid]
                elif isinstance(node, StorageNode):
                    cell_list = app.pt.dropNode(node)
                    ptid = app.getNextPartitionTableID()
                    app.broadcastPartitionChanges(ptid, cell_list)
                    if not app.pt.operational():
                        # Catastrophic.
                        raise OperationFailure, 'cannot continue operation'
        MasterEventHandler.peerBroken(self, conn)

    def packetReceived(self, conn, packet):
        MasterEventHandler.packetReceived(self, conn, packet)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        app = self.app
        if name != app.name:
            logging.error('reject an alien cluster')
            conn.addPacket(Packet().protocolError(packet.getId(),
                                                  'invalid cluster name'))
            conn.abort()
            return

        # Here are many situations. In principle, a node should be identified
        # by an UUID, since an UUID never change when moving a storage node
        # to a different server, and an UUID always changes for a master node
        # and a client node whenever it restarts, so more reliable than a
        # server address.
        #
        # However, master nodes can be known only as the server addresses.
        # And, a node may claim a server address used by another node.
        addr = (ip_address, port)
        # First, get the node by the UUID.
        node = app.nm.getNodeByUUID(uuid)
        old_node = None
        if node is None:
            # If nothing is present, try with the server address.
            node = app.nm.getNodeByServer(addr)
            if node is None:
                # Nothing is found. So this must be the first time that
                # this node connected to me.
                if node_type == MASTER_NODE_TYPE:
                    node = MasterNode(server = addr, uuid = uuid)
                elif node_type == CLIENT_NODE_TYPE:
                    node = ClientNode(uuid = uuid)
                else:
                    node = StorageNode(server = addr, uuid = uuid)
                app.nm.add(node)
                logging.debug('broadcasting node information')
                app.broadcastNodeInformation(node)
            else:
                # Otherwise, I know it only by the server address or the same
                # server address but with a different UUID.
                if node.getUUID() is None:
                    # This must be a master node.
                    if not isinstance(node, MasterNode) \
                            or node_type != MASTER_NODE_TYPE:
                        # Error. This node uses the same server address as
                        # a master node.
                        p = Packet()
                        p.protocolError(packet.getId(), 
                                        'invalid server address') 
                        conn.addPacket(p)
                        conn.abort()
                        return

                    node.setUUID(uuid)
                    if node.getState() != RUNNING_STATE:
                        node.setState(RUNNING_STATE)
                    logging.debug('broadcasting node information')
                    app.broadcastNodeInformation(node)
                else:
                    # This node has a different UUID.
                    if node.getState() == RUNNING_STATE:
                        # If it is still running, reject this node.
                        p = Packet()
                        p.protocolError(packet.getId(), 
                                        'invalid server address') 
                        conn.addPacket(p)
                        conn.abort()
                        return
                    else:
                        # Otherwise, forget the old one.
                        node.setState(DOWN_STATE)
                        logging.debug('broadcasting node information')
                        app.broadcastNodeInformation(node)
                        app.nm.remove(node)
                        old_node = node
                        node = copy(node)
                        # And insert a new one.
                        node.setUUID(uuid)
                        node.setState(RUNNING_STATE)
                        logging.debug('broadcasting node information')
                        app.broadcastNodeInformation(node)
                        app.nm.add(node)
        else:
            # I know this node by the UUID.
            try:
                ip_address, port = node.getServer()
            except TypeError:
                ip_address, port = '0.0.0.0', 0
            if (ip_address, port) != addr:
                # This node has a different server address.
                if node.getState() == RUNNING_STATE:
                    # If it is still running, reject this node.
                    p = Packet()
                    p.protocolError(packet.getId(), 
                                    'invalid server address') 
                    conn.addPacket(p)
                    conn.abort()
                    return
                else:
                    # Otherwise, forget the old one.
                    node.setState(DOWN_STATE)
                    logging.debug('broadcasting node information')
                    app.broadcastNodeInformation(node)
                    app.nm.remove(node)
                    old_node = node
                    node = copy(node)
                    # And insert a new one.
                    node.setServer(addr)
                    node.setState(RUNNING_STATE)
                    logging.debug('broadcasting node information')
                    app.broadcastNodeInformation(node)
                    app.nm.add(node)
            else:
                # If this node is broken, reject it. Otherwise, assume that
                # it is working again.
                if node.getState() == BROKEN_STATE:
                    p = Packet()
                    p.brokenNodeDisallowedError(packet.getId(), 'go away')
                    conn.addPacket(p)
                    conn.abort()
                    return
                else:
                    node.setUUID(uuid)
                    node.setState(RUNNING_STATE)
                    logging.debug('broadcasting node information')
                    app.broadcastNodeInformation(node)

        conn.setUUID(uuid)

        if isinstance(node, StorageNode):
            # If this is a storage node, add it into the partition table.
            # Note that this does no harm, even if the node is not new.
            if old_node is not None:
                logging.info('dropping %s from a partition table', 
                             dump(old_node.getUUID()))
                cell_list = app.pt.dropNode(old_node)
            else:
                cell_list = []
            logging.info('adding %s into a partition table',
                         dump(node.getUUID()))
            cell_list.extend(app.pt.addNode(node))
            if len(cell_list) != 0:
                ptid = app.getNextPartitionTableID()
                app.broadcastPartitionChanges(ptid, cell_list)

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
        logging.debug('sending notify node information to %s:%d',
                      *(conn.getAddress()))
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
                p = Packet()
                p.notifyNodeInformation(conn.getNextId(), node_list)
                conn.addPacket(p)
                del node_list[:]
        p = Packet()
        p.notifyNodeInformation(conn.getNextId(), node_list)
        conn.addPacket(p)

        # If this is a storage node or a client node, send the partition table.
        node = app.nm.getNodeByUUID(uuid)
        if isinstance(node, (StorageNode, ClientNode)):
            logging.debug('sending send partition table to %s:%d',
                          *(conn.getAddress()))
            # Split the packet if too huge.
            p = Packet()
            row_list = []
            for offset in xrange(app.num_partitions):
                row_list.append((offset, app.pt.getRow(offset)))
                if len(row_list) == 1000:
                    p.sendPartitionTable(conn.getNextId(), app.lptid, row_list)
                    conn.addPacket(p)
                    del row_list[:]
            if len(row_list) != 0:
                p.sendPartitionTable(conn.getNextId(), app.lptid, row_list)
                conn.addPacket(p)

        # If this is a storage node, ask it to start.
        if isinstance(node, StorageNode):
            conn.addPacket(Packet().startOperation(conn.getNextId()))

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
            if node_type == CLIENT_NODE_TYPE:
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

            # Something wrong happened possibly. Cut the connection to
            # this node, if any, and notify the information to others.
            # XXX this can be very slow.
            for c in app.em.getConnectionList():
                if c.getUUID() == uuid:
                    c.close()
            node.setState(state)
            logging.debug('broadcasting node information')
            app.broadcastNodeInformation(node)

            if isinstance(node, StorageNode) \
                    and state in (DOWN_STATE, BROKEN_STATE):
                cell_list = app.pt.dropNode(node)
                if len(cell_list) != 0:
                    ptid = app.getNextPartitionTableID()
                    app.broadcastPartitionChanges(ptid, cell_list)

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
            logging.critical('got later information in service')
            raise OperationFailure

    def handleAskNewTID(self, conn, packet):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, ClientNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        tid = app.getNextTID()
        app.finishing_transaction_dict[tid] = FinishingTransaction(conn)
        conn.addPacket(Packet().answerNewTID(packet.getId(), tid))

    def handleAskNewOIDs(self, conn, packet, num_oids):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, ClientNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        oid_list = app.getNewOIDList(num_oids)
        conn.addPacket(Packet().answerNewOIDs(packet.getId(), oid_list))

    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, ClientNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            self.handleUnexpectedPacket(conn, packet)
            return

        # Collect partitions related to this transaction.
        getPartition = app.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        for part in partition_set:
            uuid_set.update((cell.getUUID() for cell \
                                in app.pt.getCellList(part)))

        # Request locking data.
        for c in app.em.getConnectionList():
            if c.getUUID() in uuid_set:
                msg_id = c.getNextId()
                c.addPacket(Packet().lockInformation(msg_id, tid))
                c.expectMessage(msg_id, timeout = 60)

        try:
            t = app.finishing_transaction_dict[tid]
            t.setOIDList(oid_list)
            t.setUUIDSet(uuid_set)
            t.setMessageId(packet.getId())
        except KeyError:
            logging.warn('finishing transaction %s does not exist', dump(tid))
            pass

    def handleNotifyInformationLocked(self, conn, packet, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, StorageNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            self.handleUnexpectedPacket(conn, packet)
            return

        try:
            t = app.finishing_transaction_dict[tid]
            t.addLockedUUID(uuid)
            if t.allLocked():
                # I have received all the answers now. So send a Notify
                # Transaction Finished to the initiated client node,
                # Invalidate Objects to the other client nodes, and Unlock
                # Information to relevant storage nodes.
                p = Packet()
                for c in app.em.getConnectionList():
                    uuid = c.getUUID()
                    if uuid is not None:
                        node = app.nm.getNodeByUUID(uuid)
                        if isinstance(node, ClientNode):
                            if c is t.getConnection():
                                p.notifyTransactionFinished(t.getMessageId(), 
                                                            tid)
                                c.addPacket(p)
                            else:
                                p.invalidateObjects(c.getNextId(), 
                                                    t.getOIDList(), tid)
                                c.addPacket(p)
                        elif isinstance(node, StorageNode):
                            if uuid in t.getUUIDSet():
                                p.unlockInformation(c.getNextId(), tid)
                                c.addPacket(p)
                del app.finishing_transaction_dict[tid]
        except KeyError:
            # What is this?
            pass

    def handleAbortTransaction(self, conn, packet, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        node = app.nm.getNodeByUUID(uuid)
        if not isinstance(node, ClientNode):
            self.handleUnexpectedPacket(conn, packet)
            return

        try:
            app.finishing_transaction_dict[tid]
        except KeyError:
            logging.warn('aborting transaction %s does not exist', dump(tid))
            pass
