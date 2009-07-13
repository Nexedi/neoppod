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

from neo.handler import EventHandler
from neo import protocol
from neo.protocol import Packet, UnexpectedPacketError, \
        INVALID_UUID, RUNNING_STATE, BROKEN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE, \
        DOWN_STATE, TEMPORARILY_DOWN_STATE, HIDDEN_STATE
from neo.util import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.exception import PrimaryFailure, OperationFailure

class BaseStorageHandler(EventHandler):
    """This class implements a generic part of the event handlers."""

    def dealWithClientFailure(self, uuid):
        pass

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        raise NotImplementedError('this method must be overridden')

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas, your_uuid):
        raise NotImplementedError('this method must be overridden')

    def handleAskLastIDs(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAskPartitionTable(self, conn, packet, offset_list):
        raise NotImplementedError('this method must be overridden')

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        raise NotImplementedError('this method must be overridden')

    def handleStopOperation(self, conn, packet):
        raise NotImplementedError('this method must be overridden')

    def handleAskTransactionInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleLockInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleUnlockInformation(self, conn, packet, tid):
        raise NotImplementedError('this method must be overridden')

    def handleNotifyClusterInformation(self, conn, packet, state):
        logging.error('ignoring notify cluster information in %s' % self.__class__.__name__)

    def handleAbortTransaction(self, conn, packet, tid):
        logging.info('ignoring abort transaction')
        pass

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        logging.info('ignoring answer unfinished transactions')
        pass

    def handleAskOIDs(self, conn, packet, first, last, partition):
        logging.info('ignoring ask oids')
        pass


class BaseMasterHandler(BaseStorageHandler):

    def timeoutExpired(self, conn):
        raise PrimaryFailure('times out')

    def connectionClosed(self, conn):
        raise PrimaryFailure('dead')

    def peerBroken(self, conn):
        raise PrimaryFailure('broken')

    def handleReelectPrimaryMaster(self, conn, packet):
        raise PrimaryFailure('re-election occurs')

    def handleNotifyNodeInformation(self, conn, packet, node_list):
        """Store information on nodes, only if this is sent by a primary
        master node."""
        # XXX it might be better to implement this callback in each handler.
        uuid = conn.getUUID()
        app = self.app

        for node_type, ip_address, port, uuid, state in node_list:
            addr = (ip_address, port)
            # Try to retrieve it from nm
            n = None
            if uuid != INVALID_UUID:
                n = app.nm.getNodeByUUID(uuid)
            if n is None:
                n = app.nm.getNodeByServer(addr)
                if n is not None and uuid != INVALID_UUID:
                    # node only exists by address, remove it
                    app.nm.remove(n)
                    n = None
            elif n.getServer() != addr:
                # same uuid but different address, update it
                n.setServer(addr)

            if state == protocol.DOWN_STATE and n is not None:
                # this node is consider as down, remove it
                self.app.nm.remove(n)
                continue

            if node_type == MASTER_NODE_TYPE:
                if n is None:
                    n = MasterNode(server = addr)
                    app.nm.add(n)

                n.setState(state)
                if uuid != INVALID_UUID:
                    if n.getUUID() is None:
                        n.setUUID(uuid)

            elif node_type == STORAGE_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue

                if uuid == self.app.uuid:
                    # This is me, do what the master tell me
                    logging.info("I was told I'm %s" %(state))
                    if state in (DOWN_STATE, TEMPORARILY_DOWN_STATE, BROKEN_STATE):
                        conn.close()
                        self.app.shutdown()
                    elif state == HIDDEN_STATE:
                        n = app.nm.getNodeByUUID(uuid)
                        if n is not None:
                            n.setState(state)                
                        raise OperationFailure

                if n is None:
                    n = StorageNode(server = addr, uuid = uuid)
                    app.nm.add(n)

                n.setState(state)                

            elif node_type == CLIENT_NODE_TYPE:
                if uuid == INVALID_UUID:
                    # No interest.
                    continue

                if state == RUNNING_STATE:
                    if n is None:
                        n = ClientNode(uuid = uuid)
                        app.nm.add(n)
                else:
                    self.dealWithClientFailure(uuid)
                    if n is not None:
                        logging.debug('removing client node %s', dump(uuid))
                        app.nm.remove(n)
            if n is not None:
                logging.info("added %s %s" %(dump(n.getUUID()), n.getServer()))


class BaseClientAndStorageOperationHandler(BaseStorageHandler):
    """ Accept requests common to client and storage nodes """

    def handleAskTIDs(self, conn, packet, first, last, partition):
        # This method is complicated, because I must return TIDs only
        # about usable partitions assigned to me.
        if first >= last:
            raise protocol.ProtocolError('invalid offsets')

        app = self.app

        if partition == protocol.INVALID_PARTITION:
            # Collect all usable partitions for me.
            getCellList = app.pt.getCellList
            partition_list = []
            for offset in xrange(app.pt.getPartitions()):
                for cell in getCellList(offset, readable=True):
                    if cell.getUUID() == app.uuid:
                        partition_list.append(offset)
                        break
        else:
            partition_list = [partition]

        tid_list = app.dm.getTIDList(first, last - first,
                             app.pt.getPartitions(), partition_list)
        conn.answer(protocol.answerTIDs(tid_list), packet)

    def handleAskObjectHistory(self, conn, packet, oid, first, last):
        if first >= last:
            raise protocol.ProtocolError( 'invalid offsets')

        app = self.app
        history_list = app.dm.getObjectHistory(oid, first, last - first)
        if history_list is None:
            history_list = []
        p = protocol.answerObjectHistory(oid, history_list)
        conn.answer(p, packet)

    def handleAskTransactionInformation(self, conn, packet, tid):
        app = self.app
        t = app.dm.getTransaction(tid)
        if t is None:
            p = protocol.tidNotFound('%s does not exist' % dump(tid))
        else:
            p = protocol.answerTransactionInformation(tid, t[1], t[2], t[3], t[0])
        conn.answer(p, packet)

    def handleAskObject(self, conn, packet, oid, serial, tid):
        app = self.app
        if oid in app.load_lock_dict:
            # Delay the response.
            app.queueEvent(self.handleAskObject, conn, packet, oid,
                           serial, tid)
            return

        if serial == protocol.INVALID_SERIAL:
            serial = None
        if tid == protocol.INVALID_TID:
            tid = None
        o = app.dm.getObject(oid, serial, tid)
        if o is not None:
            serial, next_serial, compression, checksum, data = o
            if next_serial is None:
                next_serial = protocol.INVALID_SERIAL
            logging.debug('oid = %s, serial = %s, next_serial = %s',
                          dump(oid), dump(serial), dump(next_serial))
            p = protocol.answerObject(oid, serial, next_serial,
                           compression, checksum, data)
        else:
            logging.debug('oid = %s not found', dump(oid))
            p = protocol.oidNotFound('%s does not exist' % dump(oid))
        conn.answer(p, packet)


# import all handlers in the current namespace
from neo.storage.handlers.identification import IdentificationHandler
from neo.storage.handlers.initialization import InitializationHandler
from neo.storage.handlers.verification import VerificationHandler
from neo.storage.handlers.replication import ReplicationHandler
from neo.storage.handlers.storage import StorageOperationHandler
from neo.storage.handlers.master import MasterOperationHandler
from neo.storage.handlers.client import ClientOperationHandler
from neo.storage.handlers.hidden import HiddenHandler
