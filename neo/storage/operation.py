import logging

from neo.storage.handler import StorageEventHandler
from neo.protocol import INVALID_UUID, INVALID_SERIAL, INVALID_TID, \
        INVALID_PARTITION, \
        RUNNING_STATE, BROKEN_STATE, TEMPORARILY_DOWN_STATE, \
        MASTER_NODE_TYPE, STORAGE_NODE_TYPE, CLIENT_NODE_TYPE
from neo.util import dump
from neo.node import MasterNode, StorageNode, ClientNode
from neo.connection import ClientConnection
from neo.protocol import Packet
from neo.exception import PrimaryFailure, OperationFailure

class TransactionInformation(object):
    """This class represents information on a transaction."""
    def __init__(self, uuid):
        self._uuid = uuid
        self._object_dict = {}
        self._transaction = None

    def getUUID(self):
        return self._uuid

    def addObject(self, oid, compression, checksum, data):
        self._object_dict[oid] = (oid, compression, checksum, data)

    def addTransaction(self, oid_list, user, desc, ext):
        self._transaction = (oid_list, user, desc, ext)

    def getObjectList(self):
        return self._object_dict.values()

    def getTransaction(self):
        return self._transaction

class OperationEventHandler(StorageEventHandler):
    """This class deals with events for a operation phase."""

    def connectionCompleted(self, conn):
        # FIXME this must be implemented for replications.
        raise NotImplementedError

    def connectionFailed(self, conn):
        # FIXME this must be implemented for replications.
        raise NotImplementedError

    def connectionAccepted(self, conn, s, addr):
        """Called when a connection is accepted."""
        # Client nodes and other storage nodes may connect. Also,
        # master nodes may connect, only if they misunderstand that
        # I am a master node.
        StorageEventHandler.connectionAccepted(self, conn, s, addr)

    def dealWithClientFailure(self, uuid):
        if uuid is not None:
            app = self.app
            node = app.nm.getNodeByUUID(uuid)
            if isinstance(node, ClientNode):
                for tid, t in app.transaction_dict.items():
                    if t.getUUID() == uuid:
                        for o in t.getObjectList():
                            oid = o[0]
                            try:
                                del app.store_lock_dict[oid]
                                del app.load_lock_dict[oid]
                            except KeyError:
                                pass
                        del app.transaction_dict[tid]

                # Now it may be possible to execute some events.
                app.executeQueuedEvents()

    def timeoutExpired(self, conn):
        if isinstance(conn, ClientConnection):
            if conn.getUUID() == self.app.primary_master_node.getUUID():
                # If a primary master node timeouts, I cannot continue.
                logging.critical('the primary master node times out')
                raise PrimaryFailure('the primary master node times out')
            else:
                # Otherwise, this connection is to another storage node.
                raise NotImplemented
        else:
            self.dealWithClientFailure(conn.getUUID())

        StorageEventHandler.timeoutExpired(self, conn)

    def connectionClosed(self, conn):
        if isinstance(conn, ClientConnection):
            if conn.getUUID() == self.app.primary_master_node.getUUID():
                # If a primary master node closes, I cannot continue.
                logging.critical('the primary master node is dead')
                raise PrimaryFailure('the primary master node is dead')
            else:
                # Otherwise, this connection is to another storage node.
                raise NotImplemented
        else:
            self.dealWithClientFailure(conn.getUUID())

        StorageEventHandler.connectionClosed(self, conn)

    def peerBroken(self, conn):
        if isinstance(conn, ClientConnection):
            if conn.getUUID() == self.app.primary_master_node.getUUID():
                # If a primary master node gets broken, I cannot continue.
                logging.critical('the primary master node is broken')
                raise PrimaryFailure('the primary master node is broken')
            else:
                # Otherwise, this connection is to another storage node.
                raise NotImplemented
        else:
            self.dealWithClientFailure(conn.getUUID())

        StorageEventHandler.peerBroken(self, conn)

    def handleRequestNodeIdentification(self, conn, packet, node_type,
                                        uuid, ip_address, port, name):
        if isinstance(conn, ClientConnection):
            self.handleUnexpectedPacket(conn, packet)
        else:
            app = self.app
            if name != app.name:
                logging.error('reject an alien cluster')
                conn.addPacket(Packet().protocolError(packet.getId(),
                                                      'invalid cluster name'))
                conn.abort()
                return

            addr = (ip_address, port)
            node = app.nm.getNodeByUUID(uuid)
            if node is None:
                if node_type == MASTER_NODE_TYPE:
                    node = app.nm.getNodeByServer(addr)
                    if node is None:
                        node = MasterNode(server = addr, uuid = uuid)
                        app.nm.add(node)
                else:
                    # If I do not know such a node, and it is not even a master
                    # node, simply reject it.
                    logging.error('reject an unknown node')
                    conn.addPacket(Packet().notReady(packet.getId(),
                                                     'unknown node'))
                    conn.abort()
                    return
            else:
                # If this node is broken, reject it.
                if node.getUUID() == uuid:
                    if node.getState() == BROKEN_STATE:
                        p = Packet()
                        p.brokenNodeDisallowedError(packet.getId(), 'go away')
                        conn.addPacket(p)
                        conn.abort()
                        return

            # Trust the UUID sent by the peer.
            node.setUUID(uuid)
            conn.setUUID(uuid)

            p = Packet()
            p.acceptNodeIdentification(packet.getId(), STORAGE_NODE_TYPE,
                                       app.uuid, app.server[0], app.server[1],
                                       app.num_partitions, app.num_replicas)
            conn.addPacket(p)

            if node_type == MASTER_NODE_TYPE:
                conn.abort()

    def handleAcceptNodeIdentification(self, conn, packet, node_type,
                                       uuid, ip_address, port,
                                       num_partitions, num_replicas):
        if isinstance(conn, ClientConnection):
            raise NotImplementedError
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerPrimaryMaster(self, conn, packet, primary_uuid,
                                  known_master_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskLastIDs(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskPartitionTable(self, conn, packet, offset_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleSendPartitionTable(self, conn, packet, ptid, row_list):
        self.handleUnexpectedPacket(conn, packet)

    def handleNotifyPartitionChanges(self, conn, packet, ptid, cell_list):
        """This is very similar to Send Partition Table, except that
        the information is only about changes from the previous."""
        if isinstance(conn, ClientConnection):
            app = self.app
            nm = app.nm
            pt = app.pt
            if app.ptid >= ptid:
                # Ignore this packet.
                return

            # First, change the table on memory.
            app.ptid = ptid
            for offset, uuid, state in cell_list:
                node = nm.getNodeByUUID(uuid)
                if node is None:
                    node = StorageNode(uuid = uuid)
                    if uuid != self.uuid:
                        node.setState(TEMPORARILY_DOWN_STATE)
                    nm.add(node)

                pt.setCell(offset, node, state)

            # Then, the database.
            app.dm.changePartitionTable(ptid, cell_list)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleStartOperation(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleStopOperation(self, conn, packet):
        if isinstance(conn, ClientConnection):
            raise OperationFailure('operation stopped')
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskUnfinishedTransactions(self, conn, packet):
        self.handleUnexpectedPacket(conn, packet)

    def handleAskTransactionInformation(self, conn, packet, tid):
        app = self.app
        t = app.dm.getTransaction(tid)

        p = Packet()
        if t is None:
            p.tidNotFound(packet.getId(), '%s does not exist' % dump(tid))
        else:
            p.answerTransactionInformation(packet.getId(), tid,
                                           t[1], t[2], t[3], t[0])
        conn.addPacket(p)

    def handleAskObjectPresent(self, conn, packet, oid, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleDeleteTransaction(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleCommitTransaction(self, conn, packet, tid):
        self.handleUnexpectedPacket(conn, packet)

    def handleLockInformation(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            try:
                t = app.transaction_dict[tid]
                object_list = t.getObjectList()
                for o in object_list:
                    app.load_lock_dict[o[0]] = tid

                app.dm.storeTransaction(tid, object_list, t.getTransaction())
            except KeyError:
                pass

            conn.addPacket(Packet().notifyInformationLocked(packet.getId(), tid))
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleUnlockInformation(self, conn, packet, tid):
        if isinstance(conn, ClientConnection):
            app = self.app
            try:
                t = app.transaction_dict[tid]
                object_list = t.getObjectList()
                for o in object_list:
                    oid = o[0]
                    del app.load_lock_dict[oid]
                    del app.store_lock_dict[oid]

                app.dm.finishTransaction(tid)
                del app.transaction_dict[tid]

                # Now it may be possible to execute some events.
                app.executeQueuedEvents()
            except KeyError:
                pass
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskObject(self, conn, packet, oid, serial, tid):
        app = self.app
        if oid in app.load_lock_dict:
            # Delay the response.
            app.queueEvent(self.handleAskObject, conn, packet, oid,
                           serial, tid)
            return

        if serial == INVALID_SERIAL:
            serial = None
        if tid == INVALID_TID:
            tid = None
        o = app.dm.getObject(oid, serial, tid)
        p = Packet()
        if o is not None:
            serial, next_serial, compression, checksum, data = o
            if next_serial is None:
                next_serial = INVALID_SERIAL
            logging.debug('oid = %s, serial = %s, next_serial = %s',
                          dump(oid), dump(serial), dump(next_serial))
            p.answerObject(packet.getId(), oid, serial, next_serial,
                           compression, checksum, data)
        else:
            logging.debug('oid = %s not found', dump(oid))
            p.oidNotFound(packet.getId(), '%s does not exist' % dump(oid))
        conn.addPacket(p)

    def handleAskTIDs(self, conn, packet, first, last, partition):
        # This method is complicated, because I must return TIDs only
        # about usable partitions assigned to me.
        if first >= last:
            conn.addPacket(Packet().protocolError(packet.getId(),
                                                  'invalid offsets'))
            return

        app = self.app

        if partition == INVALID_PARTITION:
            # Collect all usable partitions for me.
            getCellList = app.pt.getCellList
            partition_list = []
            for offset in xrange(app.num_partitions):
                for cell in getCellList(offset, True):
                    if cell.getUUID() == app.uuid:
                        partition_list.append(offset)
                        break
        else:
            partition_list = [partition]

        tid_list = app.dm.getTIDList(first, last - first,
                                     app.num_partitions, partition_list)
        conn.addPacket(Packet().answerTIDs(packet.getId(), tid_list))

    def handleAskObjectHistory(self, conn, packet, oid, first, last):
        if first >= last:
            conn.addPacket(Packet().protocolError(packet.getId(),
                                                  'invalid offsets'))
            return

        app = self.app
        history_list = app.dm.getObjectHistory(oid, first, last - first)
        conn.addPacket(Packet().answerObjectHistory(packet.getId(), oid,
                                                    history_list))

    def handleAskStoreTransaction(self, conn, packet, tid, user, desc,
                                  ext, oid_list):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app

        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        t.addTransaction(oid_list, user, desc, ext)
        conn.addPacket(Packet().answerStoreTransaction(packet.getId(), tid))

    def handleAskStoreObject(self, conn, packet, oid, serial,
                             compression, checksum, data, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return
        # First, check for the locking state.
        app = self.app
        locking_tid = app.store_lock_dict.get(oid)
        if locking_tid is not None:
            if locking_tid < tid:
                # Delay the response.
                app.queueEvent(self.handleAskStoreObject, conn, packet,
                               oid, serial, compression, checksum,
                               data, tid)
            else:
                # If a newer transaction already locks this object,
                # do not try to resolve a conflict, so return immediately.
                logging.info('unresolvable conflict in %s', dump(oid))
                conn.addPacket(Packet().answerStoreObject(packet.getId(), 1,
                                                          oid, locking_tid))
            return

        # Next, check if this is generated from the latest revision.
        history_list = app.dm.getObjectHistory(oid)
        if history_list:
            last_serial = history_list[0][0]
            if last_serial != serial:
                logging.info('resolvable conflict in %s', dump(oid))
                conn.addPacket(Packet().answerStoreObject(packet.getId(), 1,
                                                          oid, last_serial))
                return
        # Now store the object.
        t = app.transaction_dict.setdefault(tid, TransactionInformation(uuid))
        t.addObject(oid, compression, checksum, data)
        conn.addPacket(Packet().answerStoreObject(packet.getId(), 0,
                                                  oid, serial))
        app.store_lock_dict[oid] = tid

    def handleAbortTransaction(self, conn, packet, tid):
        uuid = conn.getUUID()
        if uuid is None:
            self.handleUnexpectedPacket(conn, packet)
            return

        app = self.app
        try:
            t = app.transaction_dict[tid]
            object_list = t.getObjectList()
            for o in object_list:
                oid = o[0]
                try:
                    del app.load_lock_dict[oid]
                except KeyError:
                    pass
                del app.store_lock_dict[oid]

            del app.transaction_dict[tid]

            # Now it may be possible to execute some events.
            app.executeQueuedEvents()
        except KeyError:
            pass

    def handleAnswerLastIDs(self, conn, packet, loid, ltid, lptid):
        if isinstance(conn, ClientConnection):
            self.app.replicator.setCriticalTID(packet, ltid)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAnswerUnfinishedTransactions(self, conn, packet, tid_list):
        if isinstance(conn, ClientConnection):
            self.app.replicator.setUnfinishedTIDList(tid_list)
        else:
            self.handleUnexpectedPacket(conn, packet)

    def handleAskOIDs(self, conn, packet, first, last, partition):
        # This method is complicated, because I must return OIDs only
        # about usable partitions assigned to me.
        if first >= last:
            conn.addPacket(Packet().protocolError(packet.getId(),
                                                  'invalid offsets'))
            return

        app = self.app

        if partition == INVALID_PARTITION:
            # Collect all usable partitions for me.
            getCellList = app.pt.getCellList
            partition_list = []
            for offset in xrange(app.num_partitions):
                for cell in getCellList(offset, True):
                    if cell.getUUID() == app.uuid:
                        partition_list.append(offset)
                        break
        else:
            partition_list = [partition]

        oid_list = app.dm.getOIDList(first, last - first,
                                     app.num_partitions, partition_list)
        conn.addPacket(Packet().answerOIDs(packet.getId(), oid_list))
