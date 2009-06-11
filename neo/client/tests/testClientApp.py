#
# Copyright (C) 2009  Nexedi SA
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

import unittest
import logging
from mock import Mock, ReturnValues, ReturnIterator
from ZODB.POSException import StorageTransactionError, UndoError, ConflictError
from neo.tests.base import NeoTestBase
from neo.client.app import Application
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError, \
        NEOStorageConflictError
from neo import protocol
from neo.protocol import *
from neo.pt import PartitionTable
import neo.connection
import os

def _getMasterConnection(self):
    self.uuid = 'C' * 16
    self.num_partitions = 10
    self.num_replicas = 1
    self.pt = PartitionTable(self.num_partitions, self.num_replicas)
    return Mock() # master_conn
Application._getMasterConnection_ord = Application._getMasterConnection
Application._getMasterConnection = _getMasterConnection

def _waitMessage(self, conn=None, msg_id=None, handler=None):
    if conn is not None and handler is not None:
        handler.dispatch(conn, conn.fakeReceived())
    else:
        raise NotImplementedError
Application._waitMessage_org = Application._waitMessage
Application._waitMessage = _waitMessage

class TestSocketConnector(object):
    """
      Test socket connector.
      Exports both an API compatible with neo.connector.SocketConnector
      and a test-only API which allows sending packets to created connectors.
    """
    def __init__(self):
        raise NotImplementedError

    def makeClientConnection(self, addr):
        raise NotImplementedError

    def makeListeningConnection(self, addr):
        raise NotImplementedError

    def getError(self):
        raise NotImplementedError

    def getDescriptor(self):
        raise NotImplementedError

    def getNewConnection(self):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError
    
    def receive(self):
        raise NotImplementedError

    def send(self, msg):
        raise NotImplementedError

class ClientApplicationTest(NeoTestBase):

    def setUp(self):
        logging.basicConfig(level = logging.WARNING)

    # some helpers

    def getApp(self, master_nodes='127.0.0.1:10010', name='test',
               connector='SocketConnector', **kw):
        app = Application(master_nodes, name, connector, **kw)
        return app

    def makeOID(self, value=None):
        from random import randint
        if value is None:
            value = randint(0, 255)
        return '\00' * 7 + chr(value)
    makeTID = makeOID

    def makeTransactionObject(self, user='u', description='d', _extension='e'):
        class Transaction(object): pass
        txn = Transaction()
        txn.user = user
        txn.description = description
        txn._extension = _extension
        return txn

    def beginTransaction(self, app, tid):
        txn = self.makeTransactionObject()
        app.tpc_begin(txn, tid=tid)
        return txn

    def storeObject(self, app, oid=None, data='DATA'):
        tid = app.local_var.tid
        if oid is None:
            oid = self.makeOID()
        obj = (oid, tid, 'DATA', '', app.local_var.txn)
        packet = protocol.answerStoreObject(conflicting=0, oid=oid, serial=tid)
        conn = Mock({ 'getNextId': 1, 'fakeReceived': packet, })
        cell = Mock({ 'getServer': 'FakeServer', 'getState': 'FakeState', })
        app.cp = Mock({ 'getConnForNode': conn})
        app.pt = Mock({ 'getCellList': (cell, cell, ) })
        return oid

    def voteTransaction(self, app):
        tid = app.local_var.tid
        txn = app.local_var.txn
        packet = protocol.answerStoreTransaction(tid=tid)
        conn = Mock({ 'getNextId': 1, 'fakeReceived': packet, })
        cell = Mock({ 'getServer': 'FakeServer', 'getState': 'FakeState', })
        app.pt = Mock({ 'getCellList': (cell, cell, ) })
        app.cp = Mock({ 'getConnForNode': ReturnValues(None, conn), })
        app.tpc_vote(txn)

    def finishTransaction(self, app):
        txn = app.local_var.txn
        tid = app.local_var.tid
        packet = protocol.notifyTransactionFinished(tid)
        app.master_conn = Mock({ 
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10010),
            'fakeReceived': packet,    
        })
        app.tpc_finish(txn)

    # common checks

    def checkDispatcherRegisterCalled(self, app, conn):
        from Queue import Queue
        calls = app.dispatcher.mockGetNamedCalls('register')
        self.assertEquals(len(calls), 1)
        #self.assertEquals(calls[0].getParam(0), conn)
        self.assertTrue(isinstance(calls[0].getParam(2), Queue))

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
        response_packet = protocol.answerNewOIDs(test_oid_list[:])
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
        # cache cleared -> result from ZODB
        self.assertTrue(oid not in mq)
        app.pt = Mock({ 'getCellList': (), })
        app.local_var.history = (oid, [(tid, 0)])
        self.assertEquals(app.getSerial(oid), tid)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 1)
        # fill the cache -> hit
        mq.store(oid, (tid, ''))
        self.assertTrue(oid in mq)
        app.pt = Mock({ 'getCellList': (), })
        app.getSerial(oid)
        self.assertEquals(app.getSerial(oid), tid)
        self.assertEquals(len(app.pt.mockGetNamedCalls('getCellList')), 0)
    
    def test_load(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        an_object = (1, oid, tid1, tid2, 0, 0, '')
        # connection to SN close
        self.assertTrue(oid not in mq)
        packet = protocol.oidNotFound('')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({'getUUID': '\x10' * 16,
                     'getServer': ('127.0.0.1', 0),
                     'fakeReceived': packet,    
                     })
        app.local_var.queue = Mock({'get_nowait' : (conn, None)})
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        Application._waitMessage = Application._waitMessage_org
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkAskObject(conn)
        Application._waitMessage = _waitMessage
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = protocol.oidNotFound('')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkAskObject(conn)
        # object found on storage nodes and put in cache
        packet = protocol.answerObject(*an_object[1:])
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = an_object
        result = app.load(oid)
        self.assertEquals(result, ('', tid1))
        self.checkAskObject(conn)
        self.assertTrue(oid in mq)
        # object is now cached, try to reload it 
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        result = app.load(oid)
        self.assertEquals(result, ('', tid1))
        self.checkNoPacketSent(conn)
        
    def test_loadSerial(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = protocol.oidNotFound('')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadSerial, oid, tid2)
        self.checkAskObject(conn)
        # object should not have been cached
        self.assertFalse(oid in mq)
        # now a cached version ewxists but should not be hit 
        mq.store(oid, (tid1, 'WRONG'))
        self.assertTrue(oid in mq)
        another_object = (1, oid, tid2, INVALID_SERIAL, 0, 0, 'RIGHT')
        packet = protocol.answerObject(*another_object[1:])
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = another_object
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
        packet = protocol.oidNotFound('')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadBefore, oid, tid2)
        self.checkAskObject(conn)
        # no previous versions -> return None
        an_object = (1, oid, tid2, INVALID_SERIAL, 0, 0, '')
        packet = protocol.answerObject(*an_object[1:])
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = an_object
        result = app.loadBefore(oid, tid1)
        self.assertEquals(result, None)
        # object should not have been cached
        self.assertFalse(oid in mq)
        # as for loadSerial, the object is cached but should be loaded from db 
        mq.store(oid, (tid1, 'WRONG'))
        self.assertTrue(oid in mq)
        another_object = (1, oid, tid1, tid2, 0, 0, 'RIGHT')
        packet = protocol.answerObject(*another_object[1:])
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
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
        # no connection -> NEOStorageError
        self.assertRaises(NEOStorageError, app.tpc_begin, transaction=txn, tid=None)
        # ask a tid to pmn
        packet = protocol.answerNewTID(tid=tid)
        app.master_conn = Mock({
            'getNextId': 1,
            'expectMessage': None, 
            'lock': None,
            'unlock': None,
            'fakeReceived': packet,
        })
        app.dispatcher = Mock({
        })
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
        self.assertRaises(StorageTransactionError, app.store, oid, tid, '', None, txn)
        self.assertEquals(app.local_var.txn, old_txn)
        # check partition_id and an empty cell list -> NEOStorageError
        app.local_var.txn = txn
        app.local_var.tid = tid
        app.pt = Mock({ 'getCellList': (), })
        app.num_partitions = 2 
        self.assertRaises(NEOStorageError, app.store, oid, tid, '',  None, txn)
        calls = app.pt.mockGetNamedCalls('getCellList')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), 1) # oid=11 

    def test_store2(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # build conflicting state
        app.local_var.txn = txn
        app.local_var.tid = tid
        packet = protocol.answerStoreObject(conflicting=1, oid=oid, serial=tid)
        conn = Mock({ 
            'getNextId': 1,
            'fakeReceived': packet,    
        })
        cell = Mock({
            'getServer': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellList': (cell, cell, )})
        app.cp = Mock({ 'getConnForNode': ReturnValues(None, conn)})
        app.dispatcher = Mock({})
        app.local_var.object_stored = (oid, tid)
        app.local_var.data_dict[oid] = 'BEFORE'
        self.assertRaises(NEOStorageConflictError, app.store, oid, tid, '', None, txn)
        self.assertTrue(oid not in app.local_var.data_dict)
        self.assertEquals(app.conflict_serial, tid)
        self.assertEquals(app.local_var.object_stored, (-1, tid))
        self.checkAskStoreObject(conn)
        self.checkDispatcherRegisterCalled(app, conn)

    def test_store3(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # case with no conflict
        app.local_var.txn = txn
        app.local_var.tid = tid
        packet = protocol.answerStoreObject(conflicting=0, oid=oid, serial=tid)
        conn = Mock({ 
            'getNextId': 1,
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode': ReturnValues(None, conn, ) })
        cell = Mock({
            'getServer': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellList': (cell, cell, ) })
        app.dispatcher = Mock({})
        app.conflict_serial = None # reset by hand
        app.local_var.object_stored = ()
        app.store(oid, tid, 'DATA', None, txn)
        self.assertEquals(app.local_var.object_stored, (oid, tid))
        self.assertEquals(app.local_var.data_dict.get(oid, None), 'DATA')
        self.assertNotEquals(app.conflict_serial, tid)
        self.checkAskStoreObject(conn)
        self.checkDispatcherRegisterCalled(app, conn)

    def test_tpc_vote1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        app.local_var.txn = old_txn = object()
        self.assertTrue(app.local_var.txn is not txn)
        self.assertRaises(StorageTransactionError, app.tpc_vote, txn)
        self.assertEquals(app.local_var.txn, old_txn)

    def test_tpc_vote2(self):
        # fake transaction object
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = txn
        app.local_var.tid = tid
        # wrong answer -> failure
        packet = protocol.answerNewOIDs(())
        conn = Mock({ 
            'getNextId': 1,
            'fakeReceived': packet,    
            'getAddress': ('127.0.0.1', 0),
        })
        cell = Mock({
            'getServer': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellList': (cell, cell, ) })
        app.cp = Mock({ 'getConnForNode': ReturnValues(None, conn), })
        app.dispatcher = Mock()
        app.tpc_begin(txn, tid)
        self.assertRaises(NEOStorageError, app.tpc_vote, txn)
        self.assertEquals(len(conn.mockGetNamedCalls('abort')), 1)
        calls = conn.mockGetNamedCalls('ask')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet._type, ASK_STORE_TRANSACTION)

    def test_tpc_vote3(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.local_var.txn = txn
        app.local_var.tid = tid
        # response -> OK
        packet = protocol.answerStoreTransaction(tid=tid)
        conn = Mock({ 
            'getNextId': 1,
            'fakeReceived': packet,    
        })
        cell = Mock({
            'getServer': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({ 'getCellList': (cell, cell, ) })
        app.cp = Mock({ 'getConnForNode': ReturnValues(None, conn), })
        app.dispatcher = Mock()
        app.tpc_begin(txn, tid)
        app.tpc_vote(txn)
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
        app.pt = Mock({'getCellList': (cell, cell)})
        app.cp = Mock({'getConnForNode': ReturnValues(None, cell)})
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
        app.pt = Mock({ 'getCellList': ReturnValues((cell1, ), (cell1, ), (cell1, cell2)), })
        app.cp = Mock({ 'getConnForNode': ReturnValues(conn1, conn2), })
        # fake data
        app.local_var.data_dict = {oid1: '', oid2: ''}
        app.tpc_abort(txn)
        # will check if there was just one call/packet :
        self.checkNotifyPacket(conn1, ABORT_TRANSACTION)
        self.checkNotifyPacket(conn2, ABORT_TRANSACTION)
        self.checkNotifyPacket(app.master_conn, ABORT_TRANSACTION)
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
        app.pt = Mock({'getCellList': (cell, cell)})
        app.cp = Mock({'getConnForNode': ReturnValues(None, cell)})
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
        packet = protocol.answerNewTID(INVALID_TID) 
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
        self.checkFinishTransaction(app.master_conn)
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
        packet = protocol.notifyTransactionFinished(tid)
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
        self.checkFinishTransaction(app.master_conn)
        self.checkDispatcherRegisterCalled(app, app.master_conn)
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
        wrapper = Mock()
        app.local_var.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.local_var.txn is txn)
        conn = Mock()
        cell = Mock()
        self.assertRaises(StorageTransactionError, app.undo, tid, txn, wrapper)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        # nothing done
        self.assertEquals(len(wrapper.mockGetNamedCalls('tryToResolveConflict')), 0)
        self.assertEquals(app.local_var.txn, old_txn)

    def test_undo2(self):
        # Four tests here :
        # undo txn1 where obj1 was created -> fail
        # undo txn2 where obj2 was modified in tid3 -> fail
        # undo txn3 where there is a conflict on obj2
        # undo txn3 where obj2 was altered from tid2 -> ok
        # txn4 is the transaction where the undo occurs
        app = self.getApp()
        app.num_partitions = 2
        oid1, oid2 = self.makeOID(1), self.makeOID(2)
        tid1, tid2 = self.makeTID(1), self.makeTID(2)
        tid3, tid4 = self.makeTID(3), self.makeTID(4)
        # commit version 1 of object 1
        txn1 = self.beginTransaction(app, tid=tid1)
        self.storeObject(app, oid=oid1, data='O1V1')
        self.voteTransaction(app)
        self.finishTransaction(app)
        # commit version 1 of object 2
        txn2 = self.beginTransaction(app, tid=tid2)
        self.storeObject(app, oid=oid2, data='O1V2')
        self.voteTransaction(app)
        self.finishTransaction(app)
        # commit version 2 of object 2
        txn3 = self.beginTransaction(app, tid=tid3)
        self.storeObject(app, oid=oid2, data='O2V2')
        self.voteTransaction(app)
        self.finishTransaction(app)
        # undo 1 -> no previous revision
        u1p1 = protocol.answerTransactionInformation(tid1, '', '', '', (oid1, ))
        u1p2 = protocol.oidNotFound('oid not found')
        # undo 2 -> not end tid
        u2p1 = protocol.answerTransactionInformation(tid2, '', '', '', (oid2, ))
        u2p2 = protocol.answerObject(oid2, tid2, tid3, 0, 0, 'O2V1')
        # undo 3 -> conflict
        u3p1 = protocol.answerTransactionInformation(tid3, '', '', '', (oid2, ))
        u3p2 = protocol.answerObject(oid2, tid3, tid3, 0, 0, 'O2V2')
        u3p3 = protocol.answerStoreObject(conflicting=1, oid=oid2, serial=tid2)
        # undo 4 -> ok
        u4p1 = protocol.answerTransactionInformation(tid3, '', '', '', (oid2, ))
        u4p2 = protocol.answerObject(oid2, tid3, tid3, 0, 0, 'O2V2')
        u4p3 = protocol.answerStoreObject(conflicting=0, oid=oid2, serial=tid2)
        # test logic
        packets = (u1p1, u1p2, u2p1, u2p2, u3p1, u3p2, u3p3, u3p1, u4p2, u4p3)
        conn = Mock({ 
            'getNextId': 1, 
            'fakeReceived': ReturnValues(*packets),
            'getAddress': ('127.0.0.1', 10010),
        })
        cell = Mock({ 'getServer': 'FakeServer', 'getState': 'FakeState', })
        app.pt = Mock({ 'getCellList': (cell, ) })
        app.cp = Mock({ 'getConnForNode': conn})
        wrapper = Mock({'tryToResolveConflict': None})
        txn4 = self.beginTransaction(app, tid=tid4)
        # all start here
        self.assertRaises(UndoError, app.undo, tid1, txn4, wrapper)
        self.assertRaises(UndoError, app.undo, tid2, txn4, wrapper)
        self.assertRaises(ConflictError, app.undo, tid3, txn4, wrapper)
        self.assertEquals(len(wrapper.mockGetNamedCalls('tryToResolveConflict')), 1)
        self.assertEquals(app.undo(tid3, txn4, wrapper), (tid4, [oid2, ]))
        self.finishTransaction(app)

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
        p3 = protocol.answerTransactionInformation(tid1, '', '', '', (oid1, ))
        p4 = protocol.answerTransactionInformation(tid2, '', '', '', (oid2, ))
        conn = Mock({
            'getNextId': 1,
            'getUUID': ReturnValues(uuid1, uuid2),
            'fakeGetApp': app,
            'fakeReceived': ReturnValues(p3, p4),
            'getAddress': ('127.0.0.1', 10010),
        })
        app.pt = Mock({
            'getNodeList': (node1, node2, ),
            'getCellList': ReturnValues([cell1], [cell2]),
        })
        app.cp = Mock({ 'getConnForNode': conn})
        def _waitMessage(self, conn=None, msg_id=None, handler=None):
            self.local_var.node_tids = {uuid1: (tid1, ), uuid2: (tid2, )}
            Application._waitMessage = _waitMessage_old
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage
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
        p1 = protocol.answerObjectHistory(self.makeOID(2), ())
        p2 = protocol.answerObjectHistory(oid, object_history)
        # transaction history
        p3 = protocol.answerTransactionInformation(tid1, 'u', 'd', 'e', (oid, ))
        p4 = protocol.answerTransactionInformation(tid2, 'u', 'd', 'e', (oid, ))
        # faked environnement
        conn = Mock({
            'getNextId': 1,
            'fakeGetApp': app,
            'fakeReceived': ReturnValues(p1, p2, p3, p4),
            'getAddress': ('127.0.0.1', 10010),
        })
        object_cells = [ Mock({}), Mock({}) ]
        history_cells = [ Mock({}), Mock({}) ]
        app.pt = Mock({
            'getCellList': ReturnValues(object_cells, history_cells,
                history_cells),
        })
        app.cp = Mock({ 'getConnForNode': conn})
        # start test here
        result = app.history(oid)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0]['tid'], tid1)
        self.assertEquals(result[1]['tid'], tid2)
        self.assertEquals(result[0]['size'], 42)
        self.assertEquals(result[1]['size'], 42)

    def test_connectToPrimaryMasterNode(self):
        # here we have three master nodes :
        # the connection to the first will fail
        # the second will have changed
        # the third will not be ready
        # after the third, the partition table will be operational 
        # (as if it was connected to the primary master node)
        from neo.master.tests.connector import DoNothingConnector
        # will raise IndexError at the third iteration
        app = self.getApp('127.0.0.1:10010 127.0.0.1:10011')
        # third iteration : node not ready
        def _waitMessage4(app, conn=None, msg_id=None, handler=None):
            app.local_var.node_ready = False 
            self.all_passed = True
        # second iteration : master node changed
        def _waitMessage3(app, conn=None, msg_id=None, handler=None):
            app.primary_master_node = Mock({
                'getServer': ('192.168.1.1', 10000),
                '__str__': 'Fake master node',
            })
            Application._waitMessage = _waitMessage4
        # first iteration : connection failed
        def _waitMessage2(app, conn=None, msg_id=None, handler=None):
            app.primary_master_node = -1
            Application._waitMessage = _waitMessage3
        # do nothing for the first call
        def _waitMessage1(app, conn=None, msg_id=None, handler=None):
            Application._waitMessage = _waitMessage2
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage1
        # faked environnement
        app.connector_handler = DoNothingConnector
        app.em = Mock({})
        app.pt = Mock({ 'operational': ReturnValues(False, False, True, True)})
        self.all_passed = False
        try:
            app.connectToPrimaryMasterNode_org()
        finally:
            Application._waitMessage = _waitMessage_old
        self.assertEquals(len(app.pt.mockGetNamedCalls('clear')), 1)
        self.assertTrue(self.all_passed)
        self.assertTrue(app.master_conn, neo.connection.MTClientConnection)
        self.assertTrue(app.pt.operational())
        self.assertEquals(len(app.nm.getNodeList()), 3)

    def test_askStorage(self):
        """ _askStorage is private but test it anyway """
        app = self.getApp('')
        app.dispatcher = Mock()
        conn = Mock()
        self.test_ok = False
        def _waitMessage_hook(app, conn=None, msg_id=None, handler=None):
            self.test_ok = True
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage_hook
        packet = protocol.askNewTID()
        try:
            app._askStorage(conn, packet)
        finally:
            Application._waitMessage = _waitMessage_old
        # check packet sent, connection unlocked and dispatcher updated
        self.checkAskNewTid(conn)
        self.assertEquals(len(conn.mockGetNamedCalls('unlock')), 1)
        self.assertEquals(len(app.dispatcher.mockGetNamedCalls('register')), 1)
        # and _waitMessage called
        self.assertTrue(self.test_ok)

    def test_askPrimary(self):
        """ _askPrimary is private but test it anyway """
        app = self.getApp('')
        app.dispatcher = Mock()
        conn = Mock()
        app.master_conn = conn
        app.primary_handler = object()
        self.test_ok = False
        def _waitMessage_hook(app, conn=None, msg_id=None, handler=None):
            self.assertEquals(handler, app.primary_handler)
            self.test_ok = True
        _waitMessage_old = Application._waitMessage
        Application._waitMessage = _waitMessage_hook
        packet = protocol.askNewTID()
        try:
            app._askPrimary(packet)
        finally:
            Application._waitMessage = _waitMessage_old
        # check packet sent, connection locked during process and dispatcher updated
        self.checkAskNewTid(conn)
        self.assertEquals(len(conn.mockGetNamedCalls('lock')), 1)
        self.assertEquals(len(conn.mockGetNamedCalls('unlock')), 1)
        self.assertEquals(len(app.dispatcher.mockGetNamedCalls('register')), 1)
        # and _waitMessage called
        self.assertTrue(self.test_ok)
        # check NEOStorageError is raised when the primary connection is lost
        app.master_conn = None
        self.assertRaises(NEOStorageError, app._askPrimary, packet)


if __name__ == '__main__':
    unittest.main()

