#
# Copyright (C) 2009-2010  Nexedi SA
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import new
import unittest
from cPickle import dumps
from mock import Mock, ReturnValues
from ZODB.POSException import StorageTransactionError, UndoError, ConflictError
from neo.tests import NeoUnitTestBase
from neo.client.app import Application, RevisionIndex
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.client.exception import NEOStorageDoesNotExistError
from neo.lib.protocol import Packet, Packets, Errors, INVALID_TID
from neo.lib.util import makeChecksum
import time

def _getMasterConnection(self):
    if self.master_conn is None:
        self.uuid = 'C' * 16
        self.num_partitions = 10
        self.num_replicas = 1
        self.pt = Mock({
            'getCellListForOID': (),
            'getCellListForTID': (),
        })
        self.master_conn = Mock()
    return self.master_conn

def getPartitionTable(self):
    if self.pt is None:
        self.master_conn = _getMasterConnection(self)
    return self.pt

def _waitMessage(self, conn, msg_id, handler=None):
    if handler is None:
        raise NotImplementedError
    else:
        handler.dispatch(conn, conn.fakeReceived())

def resolving_tryToResolveConflict(oid, conflict_serial, serial, data):
    return data

def failing_tryToResolveConflict(oid, conflict_serial, serial, data):
    return None

class ClientApplicationTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # apply monkey patches
        self._getMasterConnection = Application._getMasterConnection
        self._waitMessage = Application._waitMessage
        self.getPartitionTable = Application.getPartitionTable
        Application._getMasterConnection = _getMasterConnection
        Application._waitMessage = _waitMessage
        Application.getPartitionTable = getPartitionTable
        self._to_stop_list = []

    def tearDown(self):
        # stop threads
        for app in self._to_stop_list:
            app.close()
        # restore environnement
        Application._getMasterConnection = self._getMasterConnection
        Application._waitMessage = self._waitMessage
        Application.getPartitionTable = self.getPartitionTable
        NeoUnitTestBase.tearDown(self)

    # some helpers

    def checkAskPacket(self, conn, packet_type, decode=False):
        calls = conn.mockGetNamedCalls('ask')
        self.assertEquals(len(calls), 1)
        # client connection got queue as first parameter
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet.getType(), packet_type)
        if decode:
            return packet.decode()
        return packet

    def getApp(self, master_nodes='127.0.0.1:10010', name='test',
               connector='SocketConnector', **kw):
        app = Application(master_nodes, name, connector, **kw)
        self._to_stop_list.append(app)
        app.dispatcher = Mock({ })
        return app

    def getConnectionPool(self, conn_list):
        return  Mock({
            'iterateForObject': conn_list,
        })

    def makeOID(self, value=None):
        from random import randint
        if value is None:
            value = randint(0, 255)
        return '\00' * 7 + chr(value)
    makeTID = makeOID

    def getNodeCellConn(self, index=1, address=('127.0.0.1', 10000)):
        conn = Mock({
            'getAddress': address,
            '__repr__': 'connection mock'
        })
        node = Mock({
            '__repr__': 'node%s' % index,
            '__hash__': index,
            'getConnection': conn,
        })
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
            'getNode': node,
        })
        return (node, cell, conn)

    def makeTransactionObject(self, user='u', description='d', _extension='e'):
        class Transaction(object):
            pass
        txn = Transaction()
        txn.user = user
        txn.description = description
        txn._extension = _extension
        return txn

    def beginTransaction(self, app, tid):
        packet = Packets.AnswerBeginTransaction(tid=tid)
        packet.setId(0)
        app.master_conn = Mock({ 'fakeReceived': packet, })
        txn = self.makeTransactionObject()
        app.tpc_begin(txn, tid=tid)
        return txn

    def storeObject(self, app, oid=None, data='DATA'):
        tid = app.local_var.tid
        if oid is None:
            oid = self.makeOID()
        obj = (oid, tid, 'DATA', '', app.local_var.txn)
        packet = Packets.AnswerStoreObject(conflicting=0, oid=oid, serial=tid)
        packet.setId(0)
        conn = Mock({ 'getNextId': 1, 'fakeReceived': packet, })
        cell = Mock({ 'getAddress': 'FakeServer', 'getState': 'FakeState', })
        app.cp = Mock({ 'getConnForCell': conn})
        app.pt = Mock({ 'getCellListForOID': (cell, cell, ) })
        return oid

    def voteTransaction(self, app):
        tid = app.local_var.tid
        txn = app.local_var.txn
        packet = Packets.AnswerStoreTransaction(tid=tid)
        packet.setId(0)
        conn = Mock({ 'getNextId': 1, 'fakeReceived': packet, })
        cell = Mock({ 'getAddress': 'FakeServer', 'getState': 'FakeState', })
        app.pt = Mock({ 'getCellListForTID': (cell, cell, ) })
        app.cp = Mock({ 'getConnForCell': ReturnValues(None, conn), })
        app.tpc_vote(txn, resolving_tryToResolveConflict)

    def askFinishTransaction(self, app):
        txn = app.local_var.txn
        tid = app.local_var.tid
        packet = Packets.AnswerTransactionFinished(tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10010),
            'fakeReceived': packet,
        })
        app.local_var.txn_voted = True
        app.tpc_finish(txn, None)

    # common checks

    def checkDispatcherRegisterCalled(self, app, conn):
        calls = app.dispatcher.mockGetNamedCalls('register')
        #self.assertEquals(len(calls), 1)
        #self.assertEquals(calls[0].getParam(0), conn)
        #self.assertTrue(isinstance(calls[0].getParam(2), Queue))

    def test_getQueue(self):
        app = self.getApp()
        # Test sanity check
        self.assertTrue(getattr(app, 'local_var', None) is not None)
        # Test that queue is created
        self.assertTrue(getattr(app.local_var, 'queue', None) is not None)

    def test_registerDB(self):
        app = self.getApp()
        dummy_db = []
        app.registerDB(dummy_db, None)
        self.assertTrue(app.getDB() is dummy_db)

    def test_new_oid(self):
        app = self.getApp()
        test_msg_id = 50
        test_oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        response_packet = Packets.AnswerNewOIDs(test_oid_list[:])
        response_packet.setId(0)
        app.master_conn = Mock({'getNextId': test_msg_id, '_addPacket': None,
                                'expectMessage': None, 'lock': None,
                                'unlock': None,
                                # Test-specific method
                                'fakeReceived': response_packet})
        new_oid = app.new_oid()
        self.assertTrue(new_oid in test_oid_list)
        self.assertEqual(len(app.new_oid_list), 1)
        self.assertTrue(app.new_oid_list[0] in test_oid_list)
        self.assertNotEqual(app.new_oid_list[0], new_oid)

    def test_load(self):
        app = self.getApp()
        app.local_var.barrier_done = True
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        snapshot_tid = self.makeTID(3)
        an_object = (1, oid, tid1, tid2, 0, makeChecksum('OBJ'), 'OBJ', None)
        # connection to SN close
        self.assertTrue((oid, tid1) not in mq)
        self.assertTrue((oid, tid2) not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({'getUUID': '\x10' * 16,
                     'getAddress': ('127.0.0.1', 0),
                     'fakeReceived': packet,
                     })
        app.local_var.queue = Mock({'get' : ReturnValues(
            (conn, None), (conn, packet)
        )})
        app.pt = Mock({ 'getCellListForOID': [cell, ], })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        Application._waitMessage = self._waitMessage
        # XXX: test disabled because of an infinite loop
        # self.assertRaises(NEOStorageError, app.load, snapshot_tid, oid)
        # self.checkAskObject(conn)
        Application._waitMessage = _waitMessage
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue((oid, tid1) not in mq)
        self.assertTrue((oid, tid2) not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': [cell, ], })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        self.assertRaises(NEOStorageNotFoundError, app.load, snapshot_tid, oid)
        self.checkAskObject(conn)
        # object found on storage nodes and put in cache
        packet = Packets.AnswerObject(*an_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        app.local_var.asked_object = an_object[:-1]
        answer_barrier = Packets.AnswerBarrier()
        answer_barrier.setId(1)
        app.master_conn = Mock({
            'getNextId': 1,
            'fakeReceived': answer_barrier,
        })
        result = app.load(snapshot_tid, oid)[:2]
        self.assertEquals(result, ('OBJ', tid1))
        self.checkAskObject(conn)
        self.assertTrue((oid, tid1) in mq)
        # object is now cached, try to reload it
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        result = app.load(snapshot_tid, oid)[:2]
        self.assertEquals(result, ('OBJ', tid1))
        self.checkNoPacketSent(conn)

    def test_loadSerial(self):
        app = self.getApp()
        app.local_var.barrier_done = True
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        snapshot_tid = self.makeTID(3)
        def loadSerial(oid, serial):
            return app.load(snapshot_tid, oid, serial=serial)[0]
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue((oid, tid1) not in mq)
        self.assertTrue((oid, tid2) not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': [Mock()]})
        app.cp = self.getConnectionPool([(Mock(), conn)])
        self.assertRaises(NEOStorageNotFoundError, loadSerial, oid, tid2)
        self.checkAskObject(conn)
        # object should not have been cached
        self.assertFalse((oid, tid2) in mq)
        # now a cached version ewxists but should not be hit
        mq.store((oid, tid2), ('WRONG', None))
        self.assertTrue((oid, tid2) in mq)
        another_object = (1, oid, tid2, INVALID_TID, 0,
            makeChecksum('RIGHT'), 'RIGHT', None)
        packet = Packets.AnswerObject(*another_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        app.local_var.asked_object = another_object[:-1]
        result = loadSerial(oid, tid1)
        self.assertEquals(result, 'RIGHT')
        self.checkAskObject(conn)
        self.assertTrue((oid, tid2) in mq)

    def test_loadBefore(self):
        app = self.getApp()
        app.local_var.barrier_done = True
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        tid3 = self.makeTID(3)
        snapshot_tid = self.makeTID(4)
        def loadBefore(oid, tid):
            return app.load(snapshot_tid, oid, tid=tid)
        # object not found in NEO -> NEOStorageDoesNotExistError
        self.assertTrue((oid, tid1) not in mq)
        self.assertTrue((oid, tid2) not in mq)
        packet = Errors.OidDoesNotExist('')
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': [Mock()]})
        app.cp = self.getConnectionPool([(Mock(), conn)])
        self.assertRaises(NEOStorageDoesNotExistError, loadBefore, oid, tid2)
        self.checkAskObject(conn)
        # no visible version -> NEOStorageNotFoundError
        an_object = (1, oid, INVALID_TID, None, 0, 0, '', None)
        packet = Packets.AnswerObject(*an_object[1:])
        packet.setId(0)
        conn = Mock({
            '__str__': 'FakeConn',
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        app.local_var.asked_object = an_object[:-1]
        self.assertRaises(NEOStorageError, loadBefore, oid, tid1)
        # object should not have been cached
        self.assertFalse((oid, tid1) in mq)
        # as for loadSerial, the object is cached but should be loaded from db
        mq.store((oid, tid1), ('WRONG', tid2))
        self.assertTrue((oid, tid1) in mq)
        app.cache_revision_index.invalidate([oid], tid2)
        another_object = (1, oid, tid2, tid3, 0, makeChecksum('RIGHT'),
            'RIGHT', None)
        packet = Packets.AnswerObject(*another_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        app.local_var.asked_object = another_object
        result = loadBefore(oid, tid3)
        self.assertEquals(result, ('RIGHT', tid2, tid3))
        self.checkAskObject(conn)
        self.assertTrue((oid, tid1) in mq)

    def test_tpc_begin(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = Mock()
        # first, tid is supplied
        self.assertNotEquals(getattr(app, 'tid', None), tid)
        self.assertNotEquals(getattr(app, 'txn', None), txn)
        packet = Packets.AnswerBeginTransaction(tid=tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
        })
        app.tpc_begin(transaction=txn, tid=tid)
        self.assertTrue(app.local_var.txn is txn)
        self.assertEquals(app.local_var.tid, tid)
        # next, the transaction already begin -> raise
        self.assertRaises(StorageTransactionError, app.tpc_begin,
            transaction=txn, tid=None)
        self.assertTrue(app.local_var.txn is txn)
        self.assertEquals(app.local_var.tid, tid)
        # cancel and start a transaction without tid
        app.local_var.txn = None
        app.local_var.tid = None
        # no connection -> NEOStorageError (wait until connected to primary)
        #self.assertRaises(NEOStorageError, app.tpc_begin, transaction=txn, tid=None)
        # ask a tid to pmn
        packet = Packets.AnswerBeginTransaction(tid=tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
        })
        app.dispatcher = Mock({ })
        app.tpc_begin(transaction=txn, tid=None)
        self.checkAskNewTid(app.master_conn)
        self.checkDispatcherRegisterCalled(app, app.master_conn)
        # check attributes
        self.assertTrue(app.local_var.txn is txn)
        self.assertEquals(app.local_var.tid, tid)

    def test_store1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        app.local_var.txn = old_txn = object()
        self.assertTrue(app.local_var.txn is not txn)
        self.assertRaises(StorageTransactionError, app.store, oid, tid, '',
            None, txn)
        self.assertEquals(app.local_var.txn, old_txn)
        # check partition_id and an empty cell list -> NEOStorageError
        app.local_var.txn = txn
        app.local_var.tid = tid
        app.pt = Mock({ 'getCellListForOID': (), })
        app.num_partitions = 2
        self.assertRaises(NEOStorageError, app.store, oid, tid, '',  None,
            txn)
        calls = app.pt.mockGetNamedCalls('getCellListForOID')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), oid) # oid=11

    def test_store2(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # build conflicting state
        app.local_var.txn = txn
        app.local_var.tid = tid
        packet = Packets.AnswerStoreObject(conflicting=1, oid=oid, serial=tid)
        packet.setId(0)
        storage_address = ('127.0.0.1', 10020)
        node, cell, conn = self.getNodeCellConn(address=storage_address)
        app.pt = Mock({ 'getCellListForOID': (cell, cell)})
        app.cp = self.getConnectionPool([(node, conn)])
        class Dispatcher(object):
            def pending(self, queue): 
                return not queue.empty()
        app.dispatcher = Dispatcher()
        app.nm.createStorage(address=storage_address)
        app.local_var.data_dict[oid] = 'BEFORE'
        app.local_var.data_list.append(oid)
        app.store(oid, tid, '', None, txn)
        app.local_var.queue.put((conn, packet))
        self.assertRaises(ConflictError, app.waitStoreResponses,
            failing_tryToResolveConflict)
        self.assertTrue(oid not in app.local_var.data_dict)
        self.assertEquals(app.local_var.object_stored_counter_dict[oid], {})
        self.checkAskStoreObject(conn)

    def test_store3(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # case with no conflict
        app.local_var.txn = txn
        app.local_var.tid = tid
        packet = Packets.AnswerStoreObject(conflicting=0, oid=oid, serial=tid)
        packet.setId(0)
        storage_address = ('127.0.0.1', 10020)
        node, cell, conn = self.getNodeCellConn(address=storage_address)
        app.cp = self.getConnectionPool([(node, conn)])
        app.pt = Mock({ 'getCellListForOID': (cell, cell, ) })
        class Dispatcher(object):
            def pending(self, queue): 
                return not queue.empty()
        app.dispatcher = Dispatcher()
        app.nm.createStorage(address=storage_address)
        app.store(oid, tid, 'DATA', None, txn)
        self.checkAskStoreObject(conn)
        app.local_var.queue.put((conn, packet))
        app.waitStoreResponses(resolving_tryToResolveConflict)
        self.assertEquals(app.local_var.object_stored_counter_dict[oid], {tid: 1})
        self.assertEquals(app.local_var.data_dict.get(oid, None), 'DATA')
        self.assertFalse(oid in app.local_var.conflict_serial_dict)

    def test_tpc_vote1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        app.local_var.txn = old_txn = object()
        self.assertTrue(app.local_var.txn is not txn)
        self.assertRaises(StorageTransactionError, app.tpc_vote, txn,
            resolving_tryToResolveConflict)
        self.assertEquals(app.local_var.txn, old_txn)

    def test_tpc_vote2(self):
        # fake transaction object
        app = self.getApp()
        app.local_var.txn = self.makeTransactionObject()
        app.local_var.tid = self.makeTID()
        # wrong answer -> failure
        packet = Packets.AnswerStoreTransaction(INVALID_TID)
        packet.setId(0)
        conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
            'getAddress': ('127.0.0.1', 0),
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        app.dispatcher = Mock()
        self.assertRaises(NEOStorageError, app.tpc_vote, app.local_var.txn,
            resolving_tryToResolveConflict)
        self.checkAskPacket(conn, Packets.AskStoreTransaction)

    def test_tpc_vote3(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = txn
        app.local_var.tid = tid
        # response -> OK
        packet = Packets.AnswerStoreTransaction(tid=tid)
        packet.setId(0)
        conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
        })
        node = Mock({
            '__hash__': 1,
            '__repr__': 'FakeNode',
        })
        app.cp = self.getConnectionPool([(node, conn)])
        app.dispatcher = Mock()
        app.tpc_vote(txn, resolving_tryToResolveConflict)
        self.checkAskStoreTransaction(conn)
        self.checkDispatcherRegisterCalled(app, conn)

    def test_tpc_abort1(self):
        # ignore mismatch transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        app.local_var.tid = tid
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        cell = Mock()
        app.pt = Mock({'getCellListForTID': (cell, cell)})
        app.cp = Mock({'getConnForCell': ReturnValues(None, cell)})
        app.tpc_abort(txn)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        self.assertEquals(app.local_var.txn, old_txn)
        self.assertEquals(app.local_var.tid, tid)

    def test_tpc_abort2(self):
        # 2 nodes : 1 transaction in the first, 2 objects in the second
        # connections to each node should received only one packet to abort
        # and transaction must also be aborted on the master node
        # for simplicity, just one cell per partition
        oid1, oid2 = self.makeOID(2), self.makeOID(4) # on partition 0
        app, tid = self.getApp(), self.makeTID(1)     # on partition 1
        txn = self.makeTransactionObject()
        app.local_var.txn, app.local_var.tid = txn, tid
        app.master_conn = Mock({'__hash__': 0})
        app.num_partitions = 2
        cell1 = Mock({ 'getNode': 'NODE1', '__hash__': 1 })
        cell2 = Mock({ 'getNode': 'NODE2', '__hash__': 2 })
        conn1, conn2 = Mock({ 'getNextId': 1, }), Mock({ 'getNextId': 2, })
        app.cp = Mock({ 'getConnForNode': ReturnValues(conn1, conn2), })
        # fake data
        app.local_var.data_dict = {oid1: '', oid2: ''}
        app.local_var.involved_nodes = set([cell1, cell2])
        app.tpc_abort(txn)
        # will check if there was just one call/packet :
        self.checkNotifyPacket(conn1, Packets.AbortTransaction)
        self.checkNotifyPacket(conn2, Packets.AbortTransaction)
        self.assertEquals(app.local_var.tid, None)
        self.assertEquals(app.local_var.txn, None)
        self.assertEquals(app.local_var.data_dict, {})
        self.assertEquals(app.local_var.txn_voted, False)

    def test_tpc_abort3(self):
        """ check that abort is sent to all nodes involved in the transaction """
        app = self.getApp()
        # three partitions/storages: one per object/transaction
        app.num_partitions = num_partitions = 3
        app.num_replicas = 0
        tid = self.makeTID(num_partitions)  # on partition 0
        oid1 = self.makeOID(num_partitions + 1) # on partition 1, conflicting
        oid2 = self.makeOID(num_partitions + 2) # on partition 2
        # storage nodes
        uuid1, uuid2, uuid3 = [self.getNewUUID() for _ in range(3)]
        address1 = ('127.0.0.1', 10000)
        address2 = ('127.0.0.1', 10001)
        address3 = ('127.0.0.1', 10002)
        app.nm.createMaster(address=address1, uuid=uuid1)
        app.nm.createStorage(address=address2, uuid=uuid2)
        app.nm.createStorage(address=address3, uuid=uuid3)
        # answer packets
        packet1 = Packets.AnswerStoreTransaction(tid=tid)
        packet2 = Packets.AnswerStoreObject(conflicting=1, oid=oid1, serial=tid)
        packet3 = Packets.AnswerStoreObject(conflicting=0, oid=oid2, serial=tid)
        [p.setId(i) for p, i in zip([packet1, packet2, packet3], range(3))]
        conn1 = Mock({'__repr__': 'conn1', 'getAddress': address1,
                      'fakeReceived': packet1, 'getUUID': uuid1})
        conn2 = Mock({'__repr__': 'conn2', 'getAddress': address2,
                      'fakeReceived': packet2, 'getUUID': uuid2})
        conn3 = Mock({'__repr__': 'conn3', 'getAddress': address3,
                      'fakeReceived': packet3, 'getUUID': uuid3})
        node1 = Mock({'__repr__': 'node1', '__hash__': 1, 'getConnection': conn1})
        node2 = Mock({'__repr__': 'node2', '__hash__': 2, 'getConnection': conn2})
        node3 = Mock({'__repr__': 'node3', '__hash__': 3, 'getConnection': conn3})
        cell1 = Mock({ 'getNode': node1, '__hash__': 1, 'getConnection': conn1})
        cell2 = Mock({ 'getNode': node2, '__hash__': 2, 'getConnection': conn2})
        cell3 = Mock({ 'getNode': node3, '__hash__': 3, 'getConnection': conn3})
        # fake environment
        app.pt = Mock({
            'getCellListForTID': [cell1],
            'getCellListForOID': ReturnValues([cell2], [cell3]),
        })
        app.cp = Mock({'getConnForCell': ReturnValues(conn2, conn3, conn1)})
        app.cp = Mock({
            'getConnForNode': ReturnValues(conn2, conn3, conn1),
            'iterateForObject': [(node2, conn2), (node3, conn3), (node1, conn1)],
        })
        app.dispatcher = Mock()
        app.master_conn = Mock({'__hash__': 0})
        txn = self.makeTransactionObject()
        app.local_var.txn, app.local_var.tid = txn, tid
        class Dispatcher(object):
            def pending(self, queue):
                return not queue.empty()
        app.dispatcher = Dispatcher()
        # conflict occurs on storage 2
        app.store(oid1, tid, 'DATA', None, txn)
        app.store(oid2, tid, 'DATA', None, txn)
        app.local_var.queue.put((conn2, packet2))
        app.local_var.queue.put((conn3, packet3))
        # vote fails as the conflict is not resolved, nothing is sent to storage 3
        self.assertRaises(ConflictError, app.tpc_vote, txn, failing_tryToResolveConflict)
        # abort must be sent to storage 1 and 2
        app.tpc_abort(txn)
        self.checkAbortTransaction(conn2)
        self.checkAbortTransaction(conn3)

    def test_tpc_finish1(self):
        # transaction mismatch: raise
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        self.assertRaises(StorageTransactionError, app.tpc_finish, txn, None)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        self.assertEquals(app.local_var.txn, old_txn)

    def test_tpc_finish2(self):
        # bad answer -> NEOStorageError
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn, app.local_var.tid = txn, tid
        # test callable passed to tpc_finish
        self.f_called = False
        self.f_called_with_tid = None
        def hook(tid):
            self.f_called = True
            self.f_called_with_tid = tid
        packet = Packets.AnswerTransactionFinished(INVALID_TID, INVALID_TID)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10000),
            'fakeReceived': packet,
        })
        self.vote_params = None
        tpc_vote = app.tpc_vote
        def voteDetector(transaction, tryToResolveConflict):
            self.vote_params = (transaction, tryToResolveConflict)
        dummy_tryToResolveConflict = []
        app.tpc_vote = voteDetector
        app.dispatcher = Mock({})
        app.local_var.txn_voted = True
        self.assertRaises(NEOStorageError, app.tpc_finish, txn,
            dummy_tryToResolveConflict, hook)
        self.assertFalse(self.f_called)
        self.assertEqual(self.vote_params, None)
        self.checkAskFinishTransaction(app.master_conn)
        self.checkDispatcherRegisterCalled(app, app.master_conn)
        # Call again, but this time transaction is not voted yet
        app.local_var.txn_voted = False
        self.f_called = False
        self.assertRaises(NEOStorageError, app.tpc_finish, txn,
            dummy_tryToResolveConflict, hook)
        self.assertFalse(self.f_called)
        self.assertTrue(self.vote_params[0] is txn)
        self.assertTrue(self.vote_params[1] is dummy_tryToResolveConflict)
        app.tpc_vote = tpc_vote

    def test_tpc_finish3(self):
        # transaction is finished
        app = self.getApp()
        tid = self.makeTID()
        ttid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn, app.local_var.tid = txn, ttid
        self.f_called = False
        self.f_called_with_tid = None
        def hook(tid):
            self.f_called = True
            self.f_called_with_tid = tid
        packet = Packets.AnswerTransactionFinished(ttid, tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10010),
            'fakeReceived': packet,
        })
        app.dispatcher = Mock({})
        app.local_var.txn_voted = True
        app.local_var.txn_finished = True
        app.tpc_finish(txn, None, hook)
        self.assertTrue(self.f_called)
        self.assertEquals(self.f_called_with_tid, tid)
        self.checkAskFinishTransaction(app.master_conn)
        #self.checkDispatcherRegisterCalled(app, app.master_conn)
        self.assertEquals(app.local_var.tid, None)
        self.assertEquals(app.local_var.txn, None)
        self.assertEquals(app.local_var.data_dict, {})
        self.assertEquals(app.local_var.txn_voted, False)

    def test_undo1(self):
        # invalid transaction
        app = self.getApp()
        tid = self.makeTID()
        snapshot_tid = self.getNextTID()
        txn = self.makeTransactionObject()
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data):
            marker.append(1)
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        self.assertRaises(StorageTransactionError, app.undo, snapshot_tid, tid,
            txn, tryToResolveConflict)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        # nothing done
        self.assertEquals(marker, [])
        self.assertEquals(app.local_var.txn, old_txn)

    def _getAppForUndoTests(self, oid0, tid0, tid1, tid2):
        app = self.getApp()
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({
            'getCellListForTID': [cell, ],
            'getCellListForOID': [cell, ],
            'getCellList': [cell, ],
        })
        transaction_info = Packets.AnswerTransactionInformation(tid1, '', '',
            '', False, (oid0, ))
        transaction_info.setId(1)
        conn = Mock({
            'getNextId': 1,
            'fakeReceived': transaction_info,
            'getAddress': ('127.0.0.1', 10010),
        })
        node = app.nm.createStorage(address=conn.getAddress())
        app.cp = Mock({
            'iterateForObject': [(node, conn)],
            'getConnForCell': conn,
        })
        class Dispatcher(object):
            def pending(self, queue): 
                return not queue.empty()
        app.dispatcher = Dispatcher()
        def load(snapshot_tid, oid, serial):
            self.assertEqual(oid, oid0)
            return ({tid0: 'dummy', tid2: 'cdummy'}[serial], None, None)
        app.load = load
        store_marker = []
        def _store(oid, serial, data, data_serial=None):
            store_marker.append((oid, serial, data, data_serial))
        app._store = _store
        app.local_var.clear()
        return app, conn, store_marker

    def test_undoWithResolutionSuccess(self):
        """
        Try undoing transaction tid1, which contains object oid.
        Object oid previous revision before tid1 is tid0.
        Transaction tid2 modified oid (and contains its data).

        Undo is accepted, because conflict resolution succeeds.
        """
        oid0 = self.makeOID(1)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        snapshot_tid = self.getNextTID()
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid2, tid0, False)})
        undo_serial.setId(2)
        app.local_var.queue.put((conn, undo_serial))
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append((oid, conflict_serial, serial, data, committedData))
            return 'solved'
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        app.undo(snapshot_tid, tid1, txn, tryToResolveConflict)
        # Checking what happened
        moid, mconflict_serial, mserial, mdata, mcommittedData = marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mconflict_serial, tid2)
        self.assertEqual(mserial, tid1)
        self.assertEqual(mdata, 'dummy')
        self.assertEqual(mcommittedData, 'cdummy')
        moid, mserial, mdata, mdata_serial = store_marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mserial, tid2)
        self.assertEqual(mdata, 'solved')
        self.assertEqual(mdata_serial, None)

    def test_undoWithResolutionFailure(self):
        """
        Try undoing transaction tid1, which contains object oid.
        Object oid previous revision before tid1 is tid0.
        Transaction tid2 modified oid (and contains its data).

        Undo is rejeced with a raise, because conflict resolution fails.
        """
        oid0 = self.makeOID(1)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        snapshot_tid = self.getNextTID()
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid2, tid0, False)})
        undo_serial.setId(2)
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        app.local_var.queue.put((conn, undo_serial))
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append((oid, conflict_serial, serial, data, committedData))
            return None
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        self.assertRaises(UndoError, app.undo, snapshot_tid, tid1, txn,
            tryToResolveConflict)
        # Checking what happened
        moid, mconflict_serial, mserial, mdata, mcommittedData = marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mconflict_serial, tid2)
        self.assertEqual(mserial, tid1)
        self.assertEqual(mdata, 'dummy')
        self.assertEqual(mcommittedData, 'cdummy')
        self.assertEqual(len(store_marker), 0)
        # Likewise, but conflict resolver raises a ConflictError.
        # Still, exception raised by undo() must be UndoError.
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append((oid, conflict_serial, serial, data, committedData))
            raise ConflictError
        # The undo
        app.local_var.queue.put((conn, undo_serial))
        self.assertRaises(UndoError, app.undo, snapshot_tid, tid1, txn,
            tryToResolveConflict)
        # Checking what happened
        moid, mconflict_serial, mserial, mdata, mcommittedData = marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mconflict_serial, tid2)
        self.assertEqual(mserial, tid1)
        self.assertEqual(mdata, 'dummy')
        self.assertEqual(mcommittedData, 'cdummy')
        self.assertEqual(len(store_marker), 0)

    def test_undo(self):
        """
        Try undoing transaction tid1, which contains object oid.
        Object oid previous revision before tid1 is tid0.

        Undo is accepted, because tid1 is object's current revision.
        """
        oid0 = self.makeOID(1)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        snapshot_tid = self.getNextTID()
        transaction_info = Packets.AnswerTransactionInformation(tid1, '', '',
            '', False, (oid0, ))
        transaction_info.setId(1)
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid1, tid0, True)})
        undo_serial.setId(2)
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        app.local_var.queue.put((conn, undo_serial))
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            raise Exception, 'Test called conflict resolution, but there ' \
                'is no conflict in this test !'
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        app.undo(snapshot_tid, tid1, txn, tryToResolveConflict)
        # Checking what happened
        moid, mserial, mdata, mdata_serial = store_marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mserial, tid1)
        self.assertEqual(mdata, None)
        self.assertEqual(mdata_serial, tid0)

    def test_undoLog(self):
        app = self.getApp()
        app.num_partitions = 2
        uuid1, uuid2 = '\x00' * 15 + '\x01', '\x00' * 15 + '\x02'
        # two nodes, two partition, two transaction, two objects :
        node1, node2 = Mock({}), Mock({})
        cell1, cell2 = Mock({}), Mock({})
        tid1, tid2 = self.makeTID(1), self.makeTID(2)
        oid1, oid2 = self.makeOID(1), self.makeOID(2)
        # TIDs packets supplied by _waitMessage hook
        # TXN info packets
        extension = dumps({})
        p3 = Packets.AnswerTransactionInformation(tid1, '', '',
                extension, False, (oid1, ))
        p4 = Packets.AnswerTransactionInformation(tid2, '', '',
                extension, False, (oid2, ))
        p3.setId(0)
        p4.setId(1)
        conn = Mock({
            'getNextId': 1,
            'getUUID': ReturnValues(uuid1, uuid2),
            'fakeGetApp': app,
            'fakeReceived': ReturnValues(p3, p4),
            'getAddress': ('127.0.0.1', 10010),
        })
        app.pt = Mock({
            'getNodeList': (node1, node2, ),
            'getCellListForTID': ReturnValues([cell1], [cell2]),
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        def waitResponses(self):
            self.local_var.node_tids = {uuid1: (tid1, ), uuid2: (tid2, )}
        app.waitResponses = new.instancemethod(waitResponses, app, Application)
        def txn_filter(info):
            return info['id'] > '\x00' * 8
        result = app.undoLog(0, 4, filter=txn_filter)
        self.assertEquals(result[0]['id'], tid1)
        self.assertEquals(result[1]['id'], tid2)

    def test_history(self):
        app = self.getApp()
        oid = self.makeOID(1)
        tid1, tid2 = self.makeTID(1), self.makeTID(2)
        object_history = ( (tid1, 42), (tid2, 42),)
        # object history, first is a wrong oid, second is valid
        p2 = Packets.AnswerObjectHistory(oid, object_history)
        extension = dumps({'k': 'v'})
        # transaction history
        p3 = Packets.AnswerTransactionInformation(tid1, 'u', 'd',
                extension, False, (oid, ))
        p4 = Packets.AnswerTransactionInformation(tid2, 'u', 'd',
                extension, False, (oid, ))
        p2.setId(0)
        p3.setId(1)
        p4.setId(2)
        # faked environnement
        conn = Mock({
            'getNextId': 1,
            'fakeGetApp': app,
            'fakeReceived': ReturnValues(p2, p3, p4),
            'getAddress': ('127.0.0.1', 10010),
        })
        object_cells = [ Mock({}), ]
        history_cells = [ Mock({}), Mock({}) ]
        app.pt = Mock({
            'getCellListForOID': object_cells,
            'getCellListForTID': ReturnValues(history_cells, history_cells),
        })
        app.cp = self.getConnectionPool([(Mock(), conn)])
        # start test here
        result = app.history(oid)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0]['tid'], tid1)
        self.assertEquals(result[1]['tid'], tid2)
        self.assertEquals(result[0]['size'], 42)
        self.assertEquals(result[1]['size'], 42)

    def test_connectToPrimaryNode(self):
        # here we have three master nodes :
        # the connection to the first will fail
        # the second will have changed
        # the third will not be ready
        # after the third, the partition table will be operational
        # (as if it was connected to the primary master node)
        from neo.tests import DoNothingConnector
        # will raise IndexError at the third iteration
        app = self.getApp('127.0.0.1:10010 127.0.0.1:10011')
        # TODO: test more connection failure cases
        # Seventh packet : askNodeInformation succeeded
        all_passed = []
        def _waitMessage8(conn, msg_id, handler=None):
            all_passed.append(1)
        # Sixth packet : askPartitionTable succeeded
        def _waitMessage7(conn, msg_id, handler=None):
            app.pt = Mock({'operational': True})
            app._waitMessage = _waitMessage8
        # fifth packet : request node identification succeeded
        def _waitMessage6(conn, msg_id, handler=None):
            conn.setUUID('D' * 16)
            app.uuid = 'C' * 16
            app._waitMessage = _waitMessage7
        # fourth iteration : connection to primary master succeeded
        def _waitMessage5(conn, msg_id, handler=None):
            app.trying_master_node = app.primary_master_node = Mock({
                'getAddress': ('192.168.1.1', 10000),
                '__str__': 'Fake master node',
            })
            app._waitMessage = _waitMessage6
        # third iteration : node not ready
        def _waitMessage4(conn, msg_id, handler=None):
            app.setNodeNotReady()
            app.trying_master_node = None
            app._waitMessage = _waitMessage5
        # second iteration : master node changed
        def _waitMessage3(conn, msg_id, handler=None):
            app.primary_master_node = Mock({
                'getAddress': ('192.168.1.1', 10000),
                '__str__': 'Fake master node',
            })
            app._waitMessage = _waitMessage4
        # first iteration : connection failed
        def _waitMessage2(conn, msg_id, handler=None):
            app.trying_master_node = None
            app._waitMessage = _waitMessage3
        # do nothing for the first call
        def _waitMessage1(conn, msg_id, handler=None):
            app._waitMessage = _waitMessage2
        app._waitMessage = _waitMessage1
        # faked environnement
        app.connector_handler = DoNothingConnector
        app.em = Mock({'getConnectionList': []})
        app.pt = Mock({ 'operational': False})
        app.master_conn = app._connectToPrimaryNode()
        self.assertEqual(len(all_passed), 1)
        self.assertTrue(app.master_conn is not None)
        self.assertTrue(app.pt.operational())

    def test_askStorage(self):
        """ _askStorage is private but test it anyway """
        app = self.getApp('')
        app.dispatcher = Mock()
        conn = Mock()
        self.test_ok = False
        def _waitMessage_hook(app, conn, msg_id, handler=None):
            self.test_ok = True
        packet = Packets.AskBeginTransaction()
        packet.setId(0)
        app._waitMessage = _waitMessage_hook
        app._askStorage(conn, packet)
        # check packet sent, connection unlocked and dispatcher updated
        self.checkAskNewTid(conn)
        self.checkDispatcherRegisterCalled(app, conn)
        # and _waitMessage called
        self.assertTrue(self.test_ok)

    def test_askPrimary(self):
        """ _askPrimary is private but test it anyway """
        app = self.getApp('')
        app.dispatcher = Mock()
        conn = Mock()
        app.master_conn = conn
        app.primary_handler = Mock()
        self.test_ok = False
        def _waitMessage_hook(app, conn, msg_id, handler=None):
            self.assertTrue(handler is app.primary_handler)
            self.test_ok = True
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage_hook
        packet = Packets.AskBeginTransaction()
        packet.setId(0)
        try:
            app._askPrimary(packet)
        finally:
            Application._waitMessage = _waitMessage_old
        # check packet sent, connection locked during process and dispatcher updated
        self.checkAskNewTid(conn)
        self.checkDispatcherRegisterCalled(app, conn)
        # and _waitMessage called
        self.assertTrue(self.test_ok)
        # check NEOStorageError is raised when the primary connection is lost
        app.master_conn = None
        # check disabled since we reonnect to pmn
        #self.assertRaises(NEOStorageError, app._askPrimary, packet)

    def test_threadContextIsolation(self):
        """ Thread context properties must not be visible accross instances
            while remaining in the same thread """
        app1 = self.getApp()
        app1_local = app1.local_var
        app2 = self.getApp()
        app2_local = app2.local_var
        property_id = 'thread_context_test'
        self.assertFalse(hasattr(app1_local, property_id))
        self.assertFalse(hasattr(app2_local, property_id))
        setattr(app1_local, property_id, 'value')
        self.assertTrue(hasattr(app1_local, property_id))
        self.assertFalse(hasattr(app2_local, property_id))

    def test_pack(self):
        app = self.getApp()
        marker = []
        def askPrimary(packet):
            marker.append(packet)
        app._askPrimary = askPrimary
        # XXX: could not identify a value causing TimeStamp to return ZERO_TID
        #self.assertRaises(NEOStorageError, app.pack, )
        self.assertEqual(len(marker), 0)
        now = time.time()
        app.pack(now)
        self.assertEqual(len(marker), 1)
        self.assertEqual(marker[0].getType(), Packets.AskPack)
        # XXX: how to validate packet content ?

    def test_RevisionIndex_1(self):
        # Test add, getLatestSerial, getSerialList and clear
        # without invalidations
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        tid1 = self.getOID(1)
        tid2 = self.getOID(2)
        tid3 = self.getOID(3)
        ri = RevisionIndex()
        # index is empty
        self.assertEqual(ri.getSerialList(oid1), [])
        ri.add((oid1, tid1))
        # now, it knows oid1 at tid1
        self.assertEqual(ri.getLatestSerial(oid1), tid1)
        self.assertEqual(ri.getSerialList(oid1), [tid1])
        self.assertEqual(ri.getSerialList(oid2), [])
        ri.add((oid1, tid2))
        # and at tid2
        self.assertEqual(ri.getLatestSerial(oid1), tid2)
        self.assertEqual(ri.getSerialList(oid1), [tid2, tid1])
        ri.remove((oid1, tid1))
        # oid1 at tid1 was pruned from cache
        self.assertEqual(ri.getLatestSerial(oid1), tid2)
        self.assertEqual(ri.getSerialList(oid1), [tid2])
        ri.remove((oid1, tid2))
        # oid1 is completely priuned from cache
        self.assertEqual(ri.getLatestSerial(oid1), None)
        self.assertEqual(ri.getSerialList(oid1), [])
        ri.add((oid1, tid2))
        ri.add((oid1, tid1))
        # oid1 is populated, but in non-chronological order, check index
        # still answers consistent result.
        self.assertEqual(ri.getLatestSerial(oid1), tid2)
        self.assertEqual(ri.getSerialList(oid1), [tid2, tid1])
        ri.add((oid2, tid3))
        # which is not affected by the addition of oid2 at tid3
        self.assertEqual(ri.getLatestSerial(oid1), tid2)
        self.assertEqual(ri.getSerialList(oid1), [tid2, tid1])
        ri.clear()
        # index is empty again
        self.assertEqual(ri.getSerialList(oid1), [])
        self.assertEqual(ri.getSerialList(oid2), [])

    def test_RevisionIndex_2(self):
        # Test getLatestSerial & getSerialBefore with invalidations
        oid1 = self.getOID(1)
        tid1 = self.getOID(1)
        tid2 = self.getOID(2)
        tid3 = self.getOID(3)
        tid4 = self.getOID(4)
        tid5 = self.getOID(5)
        tid6 = self.getOID(6)
        ri = RevisionIndex()
        ri.add((oid1, tid1))
        ri.add((oid1, tid2))
        self.assertEqual(ri.getLatestSerial(oid1), tid2)
        self.assertEqual(ri.getSerialBefore(oid1, tid2), tid1)
        self.assertEqual(ri.getSerialBefore(oid1, tid3), tid2)
        self.assertEqual(ri.getSerialBefore(oid1, tid4), tid2)
        ri.invalidate([oid1], tid3)
        # We don't have the latest data in cache, return None
        self.assertEqual(ri.getLatestSerial(oid1), None)
        self.assertEqual(ri.getSerialBefore(oid1, tid2), tid1)
        self.assertEqual(ri.getSerialBefore(oid1, tid3), tid2)
        # There is a gap between the last version we have and requested one,
        # return None
        self.assertEqual(ri.getSerialBefore(oid1, tid4), None)
        ri.add((oid1, tid3))
        # No gap anymore, tid3 found.
        self.assertEqual(ri.getLatestSerial(oid1), tid3)
        self.assertEqual(ri.getSerialBefore(oid1, tid4), tid3)
        ri.invalidate([oid1], tid4)
        ri.invalidate([oid1], tid5)
        # A bigger gap...
        self.assertEqual(ri.getLatestSerial(oid1), None)
        self.assertEqual(ri.getSerialBefore(oid1, tid5), None)
        self.assertEqual(ri.getSerialBefore(oid1, tid6), None)
        # not entirely filled.
        ri.add((oid1, tid5))
        # Still, we know the latest and what is before tid6
        self.assertEqual(ri.getLatestSerial(oid1), tid5)
        self.assertEqual(ri.getSerialBefore(oid1, tid5), None)
        self.assertEqual(ri.getSerialBefore(oid1, tid6), tid5)

if __name__ == '__main__':
    unittest.main()

