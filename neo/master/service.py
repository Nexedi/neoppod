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
from neo.protocol import CLIENT_NODE_TYPE, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, DOWN_STATE, \
        UP_TO_DATE_STATE, FEEDING_STATE, DISCARDED_STATE, \
        STORAGE_NODE_TYPE, ADMIN_NODE_TYPE, OUT_OF_DATE_STATE, \
        HIDDEN_STATE, PENDING_STATE, INVALID_UUID
from neo.master.handler import MasterEventHandler
from neo.protocol import UnexpectedPacketError
from neo.exception import OperationFailure
from neo.util import dump

class ServiceEventHandler(MasterEventHandler):
    """This class deals with events for a service phase."""

    def _dropIt(self, conn, node, new_state):
        raise RuntimeError('rhis method must be overriden')

    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node is not None and node.getState() == RUNNING_STATE:
            self._dropIt(conn, node, TEMPORARILY_DOWN_STATE)
        MasterEventHandler.connectionClosed(self, conn)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() == RUNNING_STATE:
            self._dropIt(conn, node, TEMPORARILY_DOWN_STATE)
        MasterEventHandler.timeoutExpired(self, conn)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() != BROKEN_STATE:
            self._dropIt(conn, node, BROKEN_STATE)
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

            node.setState(state)
            # Something wrong happened possibly. Cut the connection to
            # this node, if any, and notify the information to others.
            # XXX this can be very slow.
            # XXX does this need to be closed in all cases ?
            c = app.em.getConnectionByUUID(uuid)
            if c is not None:
                c.close()

            app.broadcastNodeInformation(node)
            if node.getNodeType() == STORAGE_NODE_TYPE:
                if state == TEMPORARILY_DOWN_STATE:
                    cell_list = app.pt.outdate()
                    if len(cell_list) != 0:
                        ptid = app.pt.setNextID()
                        app.broadcastPartitionChanges(ptid, cell_list)


    def handleAskLastIDs(self, conn, packet):
        app = self.app
        conn.answer(protocol.answerLastIDs(app.loid, app.ltid, app.pt.getID()), packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        app = self.app
        p = protocol.answerUnfinishedTransactions(app.finishing_transaction_dict.keys())
        conn.answer(p, packet)


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


class ClientServiceEventHandler(ServiceEventHandler):
    """ Handler dedicated to client during service state """

    def connectionCompleted(self, conn):
        pass

    def _dropIt(self, conn, node, new_state):
        app = self.app
        node.setState(new_state)
        app.broadcastNodeInformation(node)
        app.nm.remove(node)
        for tid, t in app.finishing_transaction_dict.items():
            if t.getConnection() is conn:
                del app.finishing_transaction_dict[tid]

    def handleAbortTransaction(self, conn, packet, tid):
        uuid = conn.getUUID()
        node = self.app.nm.getNodeByUUID(uuid)
        try:
            del self.app.finishing_transaction_dict[tid]
        except KeyError:
            logging.warn('aborting transaction %s does not exist', dump(tid))
            pass

    def handleAskNewTID(self, conn, packet):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        tid = app.getNextTID()
        app.finishing_transaction_dict[tid] = FinishingTransaction(conn)
        conn.answer(protocol.answerNewTID(tid), packet)

    def handleAskNewOIDs(self, conn, packet, num_oids):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        oid_list = app.getNewOIDList(num_oids)
        conn.answer(protocol.answerNewOIDs(oid_list), packet)

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


class StorageServiceEventHandler(ServiceEventHandler):
    """ Handler dedicated to storages during service state """

    def connectionCompleted(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() == RUNNING_STATE:
            conn.notify(protocol.startOperation())

    def _dropIt(self, conn, node, new_state):
        app = self.app
        node.setState(new_state)
        app.broadcastNodeInformation(node)
        if not app.pt.operational():
            raise OperationFailure, 'cannot continue operation'

    def connectionClosed(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() == RUNNING_STATE:
            self._dropIt(conn, node, TEMPORARILY_DOWN_STATE)

    def timeoutExpired(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() == RUNNING_STATE:
            self._dropIt(conn, node, TEMPORARILY_DOWN_STATE)

    def peerBroken(self, conn):
        node = self.app.nm.getNodeByUUID(conn.getUUID())
        if node.getState() != BROKEN_STATE:
            self._dropIt(conn, node, BROKEN_STATE)

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

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        uuid = conn.getUUID()
        app = self.app
        node = app.nm.getNodeByUUID(uuid)
        # If I get a bigger value here, it is dangerous.
        if app.loid < loid or app.ltid < ltid or app.pt.getID() < lptid:
            logging.critical('got later information in service')
            raise OperationFailure

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


