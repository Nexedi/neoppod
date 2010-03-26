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

import unittest
from cPickle import dumps
from mock import Mock, ReturnValues
from ZODB.POSException import StorageTransactionError, UndoError, ConflictError
from neo.tests import NeoTestBase
from neo.client.app import Application
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.protocol import Packet, Packets, Errors, INVALID_TID, INVALID_SERIAL
from neo.util import makeChecksum

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

def _getPartitionTable(self):
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

class ClientApplicationTests(NeoTestBase):

    def setUp(self):
        # apply monkey patches
        self._getMasterConnection = Application._getMasterConnection
        self._waitMessage = Application._waitMessage
        self._getPartitionTable = Application._getPartitionTable
        Application._getMasterConnection = _getMasterConnection
        Application._waitMessage = _waitMessage
        Application._getPartitionTable = _getPartitionTable

    def tearDown(self):
        # restore environnement
        Application._getMasterConnection = self._getMasterConnection
        Application._waitMessage = self._waitMessage
        Application._getPartitionTable = self._getPartitionTable

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
        app.dispatcher = Mock({ })
        return app

    def makeOID(self, value=None):
        from random import randint
        if value is None:
            value = randint(0, 255)
        return '\00' * 7 + chr(value)
    makeTID = makeOID

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
        app.tpc_finish(txn)

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

    def test_getSerial(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid = self.makeTID()
        # cache cleared
        self.assertTrue(oid not in mq)
        app.pt = Mock({ 'getCellListForOID': (), })
        app.local_var.history = (oid, [(tid, 0)])
        # If object len is 0, this object doesn't exist anymore because its
        # creation has been undone.
        self.assertRaises(KeyError, app.getSerial, oid)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellListForOID')), 1)
        # Otherwise, result from ZODB
        app.pt = Mock({ 'getCellListForOID': (), })
        app.local_var.history = (oid, [(tid, 1)])
        self.assertEquals(app.getSerial(oid), tid)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellListForOID')), 1)
        # fill the cache -> hit
        mq.store(oid, (tid, ' '))
        self.assertTrue(oid in mq)
        app.pt = Mock({ 'getCellListForOID': (), })
        app.getSerial(oid)
        self.assertEquals(app.getSerial(oid), tid)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellListForOID')), 0)

    def test_load(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        an_object = (1, oid, tid1, tid2, 0, makeChecksum('OBJ'), 'OBJ', None)
        # connection to SN close
        self.assertTrue(oid not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({'getUUID': '\x10' * 16,
                     'getAddress': ('127.0.0.1', 0),
                     'fakeReceived': packet,
                     })
        app.local_var.queue = Mock({'get' : (conn, None)})
        app.pt = Mock({ 'getCellListForOID': (cell, ), })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = -1
        Application._waitMessage = self._waitMessage
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkAskObject(conn)
        Application._waitMessage = _waitMessage
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': (cell, ), })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkAskObject(conn)
        # object found on storage nodes and put in cache
        packet = Packets.AnswerObject(*an_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = an_object[:-1]
        result = app.load(oid)
        self.assertEquals(result, ('OBJ', tid1))
        self.checkAskObject(conn)
        self.assertTrue(oid in mq)
        # object is now cached, try to reload it
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        result = app.load(oid)
        self.assertEquals(result, ('OBJ', tid1))
        self.checkNoPacketSent(conn)

    def test_loadSerial(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': (cell, ), })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadSerial, oid, tid2)
        self.checkAskObject(conn)
        # object should not have been cached
        self.assertFalse(oid in mq)
        # now a cached version ewxists but should not be hit
        mq.store(oid, (tid1, 'WRONG'))
        self.assertTrue(oid in mq)
        another_object = (1, oid, tid2, INVALID_SERIAL, 0,
            makeChecksum('RIGHT'), 'RIGHT', None)
        packet = Packets.AnswerObject(*another_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = another_object[:-1]
        result = app.loadSerial(oid, tid1)
        self.assertEquals(result, 'RIGHT')
        self.checkAskObject(conn)
        self.assertTrue(oid in mq)

    def test_loadBefore(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Errors.OidNotFound('')
        packet.setId(0)
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.pt = Mock({ 'getCellListForOID': (cell, ), })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadBefore, oid, tid2)
        self.checkAskObject(conn)
        # no previous versions -> return None
        an_object = (1, oid, tid2, INVALID_SERIAL, 0, makeChecksum(''), '',
            None)
        packet = Packets.AnswerObject(*an_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = an_object[:-1]
        result = app.loadBefore(oid, tid1)
        self.assertEquals(result, None)
        # object should not have been cached
        self.assertFalse(oid in mq)
        # as for loadSerial, the object is cached but should be loaded from db
        mq.store(oid, (tid1, 'WRONG'))
        self.assertTrue(oid in mq)
        another_object = (1, oid, tid1, tid2, 0, makeChecksum('RIGHT'),
            'RIGHT', None)
        packet = Packets.AnswerObject(*another_object[1:])
        packet.setId(0)
        conn = Mock({
            'getAddress': ('127.0.0.1', 0),
            'fakeReceived': packet,
        })
        app.cp = Mock({ 'getConnForCell' : conn})
        app.local_var.asked_object = another_object
        result = app.loadBefore(oid, tid1)
        self.assertEquals(result, ('RIGHT', tid1, tid2))
        self.checkAskObject(conn)
        self.assertTrue(oid in mq)

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
        # next, the transaction already begin -> do nothing
        app.tpc_begin(transaction=txn, tid=None)
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
        conn = Mock({
            'getNextId': 1,
            'getAddress': storage_address,
        })
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellListForOID': (cell, cell, )})
        app.cp = Mock({ 'getConnForCell': ReturnValues(None, conn)})
        class Dispatcher(object):
            def pending(self, queue): 
                return not queue.empty()
        app.dispatcher = Dispatcher()
        app.nm.createStorage(address=storage_address)
        app.local_var.object_stored = (oid, tid)
        app.local_var.data_dict[oid] = 'BEFORE'
        app.store(oid, tid, '', None, txn)
        app.local_var.queue.put((conn, packet))
        self.assertRaises(ConflictError, app.waitStoreResponses,
            failing_tryToResolveConflict)
        self.assertTrue(oid not in app.local_var.data_dict)
        self.assertEquals(app.local_var.conflict_serial_dict[oid], tid)
        self.assertEquals(app.local_var.object_stored_counter_dict[oid], 0)
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
        conn = Mock({
            'getNextId': 1,
            'getAddress': storage_address,
        })
        app.cp = Mock({ 'getConnForCell': ReturnValues(None, conn, ) })
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
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
        self.assertEquals(app.local_var.object_stored_counter_dict[oid], 1)
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
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = txn
        app.local_var.tid = tid
        # wrong answer -> failure
        packet = Packets.AnswerTIDs(())
        packet.setId(0)
        conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
            'getAddress': ('127.0.0.1', 0),
        })
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellListForTID': (cell, cell, ) })
        app.cp = Mock({ 'getConnForCell': ReturnValues(None, conn), })
        app.dispatcher = Mock()
        app.tpc_begin(txn, tid)
        self.assertRaises(NEOStorageError, app.tpc_vote, txn,
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
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellListForTID': (cell, cell, ) })
        app.cp = Mock({ 'getConnForCell': ReturnValues(None, conn), })
        app.dispatcher = Mock()
        app.tpc_begin(txn, tid)
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
        app.pt = Mock({
            'getCellListForOID': ReturnValues((cell1, ), (cell1, )),
            'getCellListForTID': (cell1, cell2),
        })
        app.cp = Mock({ 'getConnForCell': ReturnValues(conn1, conn2), })
        # fake data
        app.local_var.data_dict = {oid1: '', oid2: ''}
        app.tpc_abort(txn)
        # will check if there was just one call/packet :
        self.checkNotifyPacket(conn1, Packets.AbortTransaction)
        self.checkNotifyPacket(conn2, Packets.AbortTransaction)
        self.checkNotifyPacket(app.master_conn, Packets.AbortTransaction)
        self.assertEquals(app.local_var.tid, None)
        self.assertEquals(app.local_var.txn, None)
        self.assertEquals(app.local_var.data_dict, {})
        self.assertEquals(app.local_var.txn_voted, False)
        self.assertEquals(app.local_var.txn_finished, False)

    def test_tpc_finish1(self):
        # ignore mismatch transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        cell = Mock()
        app.pt = Mock({'getCellListForTID': (cell, cell)})
        app.cp = Mock({'getConnForCell': ReturnValues(None, cell)})
        app.tpc_finish(txn)
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
        packet = Packets.AnswerBeginTransaction(INVALID_TID)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10000),
            'fakeReceived': packet,
        })
        app.dispatcher = Mock({})
        app.local_var.txn_finished = False
        self.assertRaises(NEOStorageError, app.tpc_finish, txn, hook)
        self.assertTrue(self.f_called)
        self.assertEquals(self.f_called_with_tid, tid)
        self.checkAskFinishTransaction(app.master_conn)
        self.checkDispatcherRegisterCalled(app, app.master_conn)

    def test_tpc_finish3(self):
        # transaction is finished
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn, app.local_var.tid = txn, tid
        self.f_called = False
        self.f_called_with_tid = None
        def hook(tid):
            self.f_called = True
            self.f_called_with_tid = tid
        packet = Packets.AnswerTransactionFinished(tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10010),
            'fakeReceived': packet,
        })
        app.dispatcher = Mock({})
        app.local_var.txn_finished = True
        app.tpc_finish(txn, hook)
        self.assertTrue(self.f_called)
        self.assertEquals(self.f_called_with_tid, tid)
        self.checkAskFinishTransaction(app.master_conn)
        #self.checkDispatcherRegisterCalled(app, app.master_conn)
        self.assertEquals(app.local_var.tid, None)
        self.assertEquals(app.local_var.txn, None)
        self.assertEquals(app.local_var.data_dict, {})
        self.assertEquals(app.local_var.txn_voted, False)
        self.assertEquals(app.local_var.txn_finished, False)

    def test_undo1(self):
        # invalid transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data):
            marker.append(1)
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        cell = Mock()
        self.assertRaises(StorageTransactionError, app.undo, tid, txn,
            tryToResolveConflict)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        # nothing done
        self.assertEquals(marker, [])
        self.assertEquals(app.local_var.txn, old_txn)

    def test_undo2(self):
        # Three tests here :
        # undo txn2 where obj2 was modified in tid3 -> fail
        # undo txn3 where obj2 was altered from tid2 -> ok
        # txn4 is the transaction where the undo occurs
        app = self.getApp()
        app.num_partitions = 2
        oid1, oid2 = self.makeOID(1), self.makeOID(2)
        oid3 = self.makeOID(3)
        tid1, tid2 = self.makeTID(1), self.makeTID(2)
        tid3, tid4 = self.makeTID(3), self.makeTID(4)
        # commit version 1 of object 2
        txn2 = self.beginTransaction(app, tid=tid2)
        self.storeObject(app, oid=oid2, data='O1V2')
        self.voteTransaction(app)
        self.askFinishTransaction(app)
        # commit version 2 of object 2
        txn3 = self.beginTransaction(app, tid=tid3)
        self.storeObject(app, oid=oid2, data='O2V2')
        self.voteTransaction(app)
        self.askFinishTransaction(app)
        # undo 1 -> undoing non-last TID, and conflict resolution succeeded
        u1p1 = Packets.AnswerTransactionInformation(tid1, '', '', '',
                False, (oid2, ))
        u1p2 = Packets.AnswerUndoTransaction([], [oid2], [])
        # undo 2 -> undoing non-last TID, and conflict resolution failed
        u2p1 = Packets.AnswerTransactionInformation(tid2, '', '', '',
                False, (oid2, ))
        u2p2 = Packets.AnswerUndoTransaction([], [oid2], [])
        # undo 3 -> "live" conflict (another transaction modifying the object
        # we want to undo)
        u3p1 = Packets.AnswerTransactionInformation(tid3, '', '', '',
                False, (oid3, ))
        u3p2 = Packets.AnswerUndoTransaction([], [], [oid3])
        # undo 4 -> undoing last tid
        u4p1 = Packets.AnswerTransactionInformation(tid3, '', '', '',
                False, (oid1, ))
        u4p2 = Packets.AnswerUndoTransaction([oid1], [], [])
        # test logic
        packets = (u1p1, u1p2, u2p1, u2p2, u3p1, u3p2, u4p1, u4p2)
        for i, p in enumerate(packets):
            p.setId(i)
        storage_address = ('127.0.0.1', 10010)
        conn = Mock({
            'getNextId': 1,
            'fakeReceived': ReturnValues(
                u1p1,
                u2p1,
                u4p1,
                u3p1,
            ),
            'getAddress': storage_address,
        })
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({
            'getCellListForTID': (cell, ),
            'getCellListForOID': (cell, ),
        })
        app.cp = Mock({'getConnForCell': conn, 'getConnForNode': conn})
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append(1)
            return resolution_result
        class Dispatcher(object):
            def pending(self, queue): 
                return not queue.empty()
        app.dispatcher = Dispatcher()
        def _load(oid, tid=None, serial=None):
            assert tid is not None
            assert serial is None, serial
            return ('dummy', oid, tid)
        app._load = _load
        app.nm.createStorage(address=storage_address)
        # all start here
        app.local_var.clear()
        txn4 = self.beginTransaction(app, tid=tid4)
        marker = []
        resolution_result = 'solved'
        app.local_var.queue.put((conn, u1p2))
        app.undo(tid1, txn4, tryToResolveConflict)
        self.assertEquals(marker, [1])

        app.local_var.clear()
        txn4 = self.beginTransaction(app, tid=tid4)
        marker = []
        resolution_result = None
        app.local_var.queue.put((conn, u2p2))
        self.assertRaises(UndoError, app.undo, tid2, txn4,
            tryToResolveConflict)
        self.assertEquals(marker, [1])

        app.local_var.clear()
        txn4 = self.beginTransaction(app, tid=tid4)
        marker = []
        resolution_result = None
        app.local_var.queue.put((conn, u4p2))
        self.assertEquals(app.undo(tid3, txn4, tryToResolveConflict),
            (tid4, [oid1, ]))
        self.assertEquals(marker, [])

        app.local_var.clear()
        txn4 = self.beginTransaction(app, tid=tid4)
        marker = []
        resolution_result = None
        app.local_var.queue.put((conn, u3p2))
        self.assertRaises(ConflictError, app.undo, tid3, txn4,
            tryToResolveConflict)
        self.assertEquals(marker, [])

        self.askFinishTransaction(app)

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
        app.cp = Mock({ 'getConnForCell': conn})
        def _waitAnyMessage(self):
            self.local_var.node_tids = {uuid1: (tid1, ), uuid2: (tid2, )}
            Application._waitAnyMessage = _waitAnyMessage_old
        _waitAnyMessage_old = Application._waitAnyMessage
        Application._waitAnyMessage = _waitAnyMessage
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
        app.cp = Mock({ 'getConnForCell': conn})
        # start test here
        result = app.history(oid)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0]['tid'], tid1)
        self.assertEquals(result[1]['tid'], tid2)
        self.assertEquals(result[0]['size'], 42)
        self.assertEquals(result[1]['size'], 42)

    def _test_connectToPrimaryNode(self):
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
        def _waitMessage8(self, conn, msg_id, handler=None):
            all_passed.append(1)
        # Sixth packet : askPartitionTable succeeded
        def _waitMessage7(self, conn, msg_id, handler=None):
            app.pt = Mock({'operational': True})
            Application._waitMessage = _waitMessage8
        # fifth packet : request node identification succeeded
        def _waitMessage6(self, conn, msg_id, handler=None):
            conn.setUUID('D' * 16)
            app.uuid = 'C' * 16
            Application._waitMessage = _waitMessage7
        # fourth iteration : connection to primary master succeeded
        def _waitMessage5(self, conn, msg_id, handler=None):
            app.trying_master_node = app.primary_master_node = Mock({
                'getAddress': ('192.168.1.1', 10000),
                '__str__': 'Fake master node',
            })
            Application._waitMessage = _waitMessage6
        # third iteration : node not ready
        def _waitMessage4(app, conn, msg_id, handler=None):
            app.setNodeNotReady()
            app.trying_master_node = None
            Application._waitMessage = _waitMessage5
        # second iteration : master node changed
        def _waitMessage3(app, conn, msg_id, handler=None):
            app.primary_master_node = Mock({
                'getAddress': ('192.168.1.1', 10000),
                '__str__': 'Fake master node',
            })
            Application._waitMessage = _waitMessage4
        # first iteration : connection failed
        def _waitMessage2(app, conn, msg_id, handler=None):
            app.trying_master_node = None
            Application._waitMessage = _waitMessage3
        # do nothing for the first call
        def _waitMessage1(app, conn, msg_id, handler=None):
            Application._waitMessage = _waitMessage2
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage1
        # faked environnement
        app.connector_handler = DoNothingConnector
        app.em = Mock({})
        app.pt = Mock({ 'operational': False})
        try:
            app.master_conn = app._connectToPrimaryNode()
            self.assertEqual(len(all_passed), 1)
            self.assertTrue(app.master_conn is not None)
            self.assertTrue(app.pt.operational())
        finally:
            Application._waitMessage = _waitMessage_old

    def test_askStorage(self):
        """ _askStorage is private but test it anyway """
        app = self.getApp('')
        app.dispatcher = Mock()
        conn = Mock()
        self.test_ok = False
        def _waitMessage_hook(app, conn, msg_id, handler=None):
            self.test_ok = True
        _waitMessage_old = Application._waitMessage
        packet = Packets.AskBeginTransaction(None)
        packet.setId(0)
        Application._waitMessage = _waitMessage_hook
        try:
            app._askStorage(conn, packet)
        finally:
            Application._waitMessage = _waitMessage_old
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
        packet = Packets.AskBeginTransaction(None)
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

if __name__ == '__main__':
    unittest.main()

