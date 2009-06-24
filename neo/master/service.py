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
from copy import copy

from neo import protocol
from neo.protocol import MASTER_NODE_TYPE, CLIENT_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        UP_TO_DATE_STATE, FEEDING_STATE, DISCARDED_STATE, \
        STORAGE_NODE_TYPE, ADMIN_NODE_TYPE, OUT_OF_DATE_STATE, \
        HIDDEN_STATE, PENDING_STATE
from neo.master.handler import MasterEventHandler
from neo.protocol import Packet, UnexpectedPacketError, INVALID_UUID
from neo.exception import OperationFailure, ElectionFailure
from neo.node import ClientNode, StorageNode, MasterNode, AdminNode
from neo.handler import identification_required, restrict_node_types
from neo.util import dump
from neo.master import ENABLE_PENDING_NODES

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

    def _dealWithNodeFailure(self, conn, new_state):
        uuid = conn.getUUID()
        if uuid is None:
            return
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        if node is not None and node.getState() == RUNNING_STATE:
            node.setState(new_state)
            logging.debug('broadcasting node information')
            app.broadcastNodeInformation(node)
            if node.getNodeType() == CLIENT_NODE_TYPE:
                # If this node is a client, just forget it.
                app.nm.remove(node)
                for tid, t in app.finishing_transaction_dict.items():
                    if t.getConnection() is conn:
                        del app.finishing_transaction_dict[tid]
            elif node.getNodeType() == ADMIN_NODE_TYPE:
                # If this node is an admin , just forget it.
                app.nm.remove(node)
            elif node.getNodeType() == STORAGE_NODE_TYPE:
                if not app.pt.operational():
                    # Catastrophic.
                    raise OperationFailure, 'cannot continue operation'
                

    def connectionClosed(self, conn):
        self._dealWithNodeFailure(conn, TEMPORARILY_DOWN_STATE)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        self._dealWithNodeFailure(conn, TEMPORARILY_DOWN_STATE)
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
                if node.getNodeType() == CLIENT_NODE_TYPE:
                    # If this node is a client, just forget it.
                    app.nm.remove(node)
                    for tid, t in app.finishing_transaction_dict.items():
                        if t.getConnection() is conn:
                            del app.finishing_transaction_dict[tid]
                elif node.getNodeType() == ADMIN_NODE_TYPE:
                    # If this node is an admin , just forget it.
                    app.nm.remove(node)
                elif node.getNodeType() == STORAGE_NODE_TYPE:
                    cell_list = app.pt.dropNode(node)
                    ptid = app.getNextPartitionTableID()
                    app.broadcastPartitionChanges(ptid, cell_list)
                    if not app.pt.operational():
                        # Catastrophic.
                        raise OperationFailure, 'cannot continue operation'
        MasterEventHandler.peerBroken(self, conn)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        app = self.app
        if name != app.name:
            logging.error('reject an alien cluster')
            raise protocol.ProtocolError('invalid cluster name')

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
        if node is not None and node.getServer() != addr:
            # Here we have an UUID conflict, assume that's a new node
            # XXX what about a storage node wich has changed of address ?
            # it still must be used with its old data if marked out of date
            # into the partition table
            node = None
        old_node = None
        if node is None:
            # generate a new uuid for this node
            while not app.isValidUUID(uuid, addr):
                uuid = app.getNewUUID(node_type)
            # If nothing is present, try with the server address.
            node = app.nm.getNodeByServer(addr)
            if node is None:
                # Nothing is found. So this must be the first time that
                # this node connected to me.
                if node_type == MASTER_NODE_TYPE:
                    node = MasterNode(server = addr, uuid = uuid)
                elif node_type == CLIENT_NODE_TYPE:
                    node = ClientNode(uuid = uuid)
                elif node_type == ADMIN_NODE_TYPE:
                    node = AdminNode(uuid = uuid)
                else:
                    node = StorageNode(server = addr, uuid = uuid)
                    if ENABLE_PENDING_NODES:
                        node.setState(PENDING_STATE)
                app.nm.add(node)
                logging.debug('broadcasting node information')
                app.broadcastNodeInformation(node)
            else:
                # Otherwise, I know it only by the server address or the same
                # server address but with a different UUID.
                if node.getUUID() is None:
                    # This must be a master node loaded from configuration
                    if node.getNodeType() != MASTER_NODE_TYPE \
                            or node_type != MASTER_NODE_TYPE:
                        # Error. This node uses the same server address as
                        # a master node.
                        raise protocol.ProtocolError('invalid server address')

                    node.setUUID(uuid)
                    if node.getState() != RUNNING_STATE:
                        node.setState(RUNNING_STATE)
                    logging.debug('broadcasting node information')
                    app.broadcastNodeInformation(node)
                else:
                    # This node has a different UUID.
                    if node.getState() == RUNNING_STATE:
                        # If it is still running, reject this node.
                        raise protocol.ProtocolError('invalid uuid')
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
                    raise protocol.ProtocolError('invalid server address')
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
                    raise protocol.BrokenNodeDisallowedError
                else:
                    node.setUUID(uuid)
                    node.setState(RUNNING_STATE)
                    logging.info('broadcasting node information as running %s' %(node.getState(),))
                    app.broadcastNodeInformation(node)

        conn.setUUID(uuid)

        if not ENABLE_PENDING_NODES and node.getNodeType() == STORAGE_NODE_TYPE:
            # If this is a storage node, add it into the partition table.
            # Note that this does no harm, even if the node is not new.
            if old_node is not None:
                logging.info('dropping %s from a partition table', 
                             dump(old_node.getUUID()))
                cell_list = app.pt.dropNode(old_node)
            else:
                cell_list = []
            cell_list.extend(app.pt.addNode(node))
            logging.info('added %s into a partition table (%d modifications)',
                         dump(node.getUUID()), len(cell_list))
            if len(cell_list) != 0:
                ptid = app.getNextPartitionTableID()
                app.broadcastPartitionChanges(ptid, cell_list)

        p = protocol.acceptNodeIdentification(MASTER_NODE_TYPE,
                                   app.uuid, app.server[0], app.server[1],
                                   app.pt.getPartitions(), app.pt.getReplicas(), uuid)
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
        logging.info('sending notify node information to %s:%d', *(conn.getAddress()))
        app.sendNodesInformations(conn)

        # If this is a storage node or a client node or an admin node, send the partition table.
        node = app.nm.getNodeByUUID(uuid)
        if node.getNodeType() in (STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, ADMIN_NODE_TYPE):
            logging.info('sending partition table to %s:%d', *(conn.getAddress()))
            app.sendPartitionTable(conn)

        # If this is a non-pending storage node, ask it to start.
        if node.getNodeType() == STORAGE_NODE_TYPE and node.getState() != PENDING_STATE:
            conn.notify(protocol.startOperation())

    @identification_required
    def handleAnnouncePrimaryMaster(self, conn, packet):
        # I am also the primary... So restart the election.
        raise ElectionFailure, 'another primary arises'

    def handleReelectPrimaryMaster(self, conn, packet):
        raise ElectionFailure, 'reelection requested'

    @identification_required
    def handleNotifyNodeInformation(self, conn, packet, node_list):
        app = self.app
        uuid = conn.getUUID()
        conn_node = app.nm.getNodeByUUID(uuid)
        if conn_node is None:
            raise RuntimeError('I do not know the uuid %r' % dump(uuid))

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
                if state in (DOWN_STATE, BROKEN_STATE):
                    # XXX still required to change here ??? who can send
                    # this kind of message with these status except admin node
                    cell_list = app.pt.dropNode(node)
                    if len(cell_list) != 0:
                        ptid = app.getNextPartitionTableID()
                        app.broadcastPartitionChanges(ptid, cell_list)
                elif state == TEMPORARILY_DOWN_STATE:
                    cell_list = app.pt.outdate()
                    if len(cell_list) != 0:
                        ptid = app.getNextPartitionTableID()
                        app.broadcastPartitionChanges(ptid, cell_list)

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.pt.getID() < lptid:
            logging.critical('got later information in service')
            raise OperationFailure

    @identification_required
    @restrict_node_types(CLIENT_NODE_TYPE)
    def handleAskNewTID(self, conn, packet):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        tid = app.getNextTID()
        app.finishing_transaction_dict[tid] = FinishingTransaction(conn)
        conn.answer(protocol.answerNewTID(tid), packet)

    @identification_required
    @restrict_node_types(CLIENT_NODE_TYPE)
    def handleAskNewOIDs(self, conn, packet, num_oids):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        oid_list = app.getNewOIDList(num_oids)
        conn.answer(protocol.answerNewOIDs(oid_list), packet)

    @identification_required
    @restrict_node_types(CLIENT_NODE_TYPE)
    def handleFinishTransaction(self, conn, packet, oid_list, tid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            raise UnexpectedPacketError

        # Collect partitions related to this transaction.
        getPartition = app.getPartition
        partition_set = set()
        partition_set.add(getPartition(tid))
        partition_set.update((getPartition(oid) for oid in oid_list))

        # Collect the UUIDs of nodes related to this transaction.
        uuid_set = set()
        for part in partition_set:
            uuid_set.update((cell.getUUID() for cell in app.pt.getCellList(part) \
                             if cell.getNodeState() != HIDDEN_STATE))

        # Request locking data.
        # build a new set as we may not send the message to all nodes as some
        # might be not reachable at that time
        used_uuid_set = set()
        for c in app.em.getConnectionList():
            if c.getUUID() in uuid_set:
                c.ask(protocol.lockInformation(tid), timeout=60)
                used_uuid_set.add(c.getUUID())

        try:
            t = app.finishing_transaction_dict[tid]
            t.setOIDList(oid_list)
            t.setUUIDSet(used_uuid_set)
            t.setMessageId(packet.getId())
        except KeyError:
            logging.warn('finishing transaction %s does not exist', dump(tid))
            pass

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleNotifyInformationLocked(self, conn, packet, tid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)

        # If the given transaction ID is later than the last TID, the peer
        # is crazy.
        if app.ltid < tid:
            raise UnexpectedPacketError

        try:
            t = app.finishing_transaction_dict[tid]
            t.addLockedUUID(uuid)
            if t.allLocked():
                # I have received all the answers now. So send a Notify
                # Transaction Finished to the initiated client node,
                # Invalidate Objects to the other client nodes, and Unlock
                # Information to relevant storage nodes.
                for c in app.em.getConnectionList():
                    uuid = c.getUUID()
                    if uuid is not None:
                        node = app.nm.getNodeByUUID(uuid)
                        if node.getNodeType() == CLIENT_NODE_TYPE:
                            if c is t.getConnection():
                                p = protocol.notifyTransactionFinished(tid)
                                c.notify(p, t.getMessageId())
                            else:
                                p = protocol.invalidateObjects(t.getOIDList(), tid)
                                c.notify(p)
                        elif node.getNodeType() == STORAGE_NODE_TYPE:
                            if uuid in t.getUUIDSet():
                                p = protocol.unlockInformation(tid)
                                c.notify(p)
                del app.finishing_transaction_dict[tid]
        except KeyError:
            # What is this?
            pass

    @identification_required
    @restrict_node_types(CLIENT_NODE_TYPE)
    def handleAbortTransaction(self, conn, packet, tid):
        uuid = conn.getUUID()
        node = self.app.nm.getNodeByUUID(uuid)
        try:
            del self.app.finishing_transaction_dict[tid]
        except KeyError:
            logging.warn('aborting transaction %s does not exist', dump(tid))
            pass

    @identification_required
    def handleAskLastIDs(self, conn, packet):
        app = self.app
        conn.answer(protocol.answerLastIDs(app.loid, app.ltid, app.pt.getID()), packet)

    @identification_required
    def handleAskUnfinishedTransactions(self, conn, packet):
        app = self.app
        p = protocol.answerUnfinishedTransactions(app.finishing_transaction_dict.keys())
        conn.answer(p, packet)

    @identification_required
    @restrict_node_types(STORAGE_NODE_TYPE)
    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        # This should be sent when a cell becomes up-to-date because
        # a replication has finished.
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)

        new_cell_list = []
        for cell in cell_list:
            if cell[2] != UP_TO_DATE_STATE:
                logging.warn('only up-to-date state should be sent')
                continue

            if uuid != cell[1]:
                logging.warn('only a cell itself should send this packet')
                continue

            offset = cell[0]
            logging.debug("node %s is up for offset %s" %(dump(node.getUUID()), offset))

            # check the storage said it is up to date for a partition it was assigne to
            for xcell in app.pt.getCellList(offset):
                if xcell.getNode().getUUID() == node.getUUID() and \
                       xcell.getState() not in (OUT_OF_DATE_STATE, UP_TO_DATE_STATE):
                    msg = "node %s telling that it is UP TO DATE for offset \
                    %s but where %s for that offset" %(dump(node.getUUID()), offset, xcell.getState())
                    logging.warning(msg)
                    self.handleError(conn, packet, INTERNAL_ERROR_CODE, msg)
                    return
                    

            app.pt.setCell(offset, node, UP_TO_DATE_STATE)
            new_cell_list.append(cell)

            # If the partition contains a feeding cell, drop it now.
            for feeding_cell in app.pt.getCellList(offset):
                if feeding_cell.getState() == FEEDING_STATE:
                    app.pt.removeCell(offset, feeding_cell.getNode())
                    new_cell_list.append((offset, feeding_cell.getUUID(), 
                                          DISCARDED_STATE))
                    break

        if new_cell_list:
            ptid = app.pt.setNextID()
            app.broadcastPartitionChanges(ptid, new_cell_list)


    @identification_required
    @restrict_node_types(ADMIN_NODE_TYPE)
    def handleSetNodeState(self, conn, packet, uuid, state, modify_partition_table):
        logging.info("set node state for %s-%s : %s" %(dump(uuid), state, modify_partition_table))
        app = self.app
        if uuid == app.uuid:
            # get message for self
            if state == RUNNING_STATE:
                # yes I know
                p = protocol.answerNodeState(app.uuid, state)
                conn.answer(p, packet)
                return
            else:
                # I was asked to shutdown
                node.setState(state)
                ip, port = node.getServer()
                node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
                conn.answer(protocol.notifyNodeInformation(node_list), packet)
                app.shutdown()

        node = app.nm.getNodeByUUID(uuid)
        if node is None:
            p = protocol.protocolError('invalid uuid')
            conn.notify(p)
            return
        if node.getState() == state:
            # no change, just notify admin node
            node.setState(state)
            ip, port = node.getServer()
            node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
            conn.answer(protocol.notifyNodeInformation(node_list), packet)

        # forward information to all nodes
        if node.getState() != state:
            node.setState(state)
            ip, port = node.getServer()
            node_list = [(node.getNodeType(), ip, port, node.getUUID(), node.getState()),]
            conn.answer(protocol.notifyNodeInformation(node_list), packet)
            app.broadcastNodeInformation(node)
            # If this is a storage node, ask it to start.
            if node.getNodeType() == STORAGE_NODE_TYPE and state == RUNNING_STATE:
                for sn_conn in app.em.getConnectionList():
                    if sn_conn.getUUID() == node.getUUID():
                        logging.info("asking sn to start operation")
                        sn_conn.notify(protocol.startOperation())

        # modify the partition table if required
        if modify_partition_table and node.getNodeType() == STORAGE_NODE_TYPE: 
            if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, HIDDEN_STATE):
                # remove from pt
                cell_list = app.pt.dropNode(node)
            else:
                # add to pt
                cell_list = app.pt.addNode(node)
            if len(cell_list) != 0:
                ptid = app.getNextPartitionTableID()
                app.broadcastPartitionChanges(ptid, cell_list)
        else:
            # outdate node in partition table
            cell_list = app.pt.outdate()
            if len(cell_list) != 0:
                ptid = app.getNextPartitionTableID()
                app.broadcastPartitionChanges(ptid, cell_list)
            
    def handleAddPendingNodes(self, conn, packet, uuid_list):
        uuids = ', '.join([dump(uuid) for uuid in uuid_list])
        logging.debug('Add nodes %s' % uuids)
        app, nm, em, pt = self.app, self.app.nm, self.app.em, self.app.pt
        cell_list = []
        uuid_set = set()
        # take all pending nodes
        for node in nm.getStorageNodeList():
            if node.getState() == PENDING_STATE:
                uuid_set.add(node.getUUID())
        # keep only selected nodes
        if uuid_list:
            uuid_set = uuid_set.intersection(set(uuid_list))
        # nothing to do
        if not uuid_set:
            logging.warning('No nodes added')
            conn.answer(protocol.answerNewNodes(()), packet)
            return
        uuids = ', '.join([dump(uuid) for uuid in uuid_set])
        logging.info('Adding nodes %s' % uuids)
        # switch nodes to running state
        for uuid in uuid_set:
            node = nm.getNodeByUUID(uuid)
            new_cells = pt.addNode(node)
            cell_list.extend(new_cells)
            node.setState(RUNNING_STATE)
            app.broadcastNodeInformation(node)
        # start nodes
        for s_conn in em.getConnectionList():
            if s_conn.getUUID() in uuid_set:
                s_conn.notify(protocol.startOperation())
        # broadcast the new partition table
        app.broadcastPartitionChanges(app.getNextPartitionTableID(), cell_list)
        conn.answer(protocol.answerNewNodes(list(uuid_set)), packet)

