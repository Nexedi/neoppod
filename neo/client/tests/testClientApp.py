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
from mock import Mock, ReturnValues
from ZODB.POSException import StorageTransactionError
from neo.protocol import INVALID_UUID
from neo.client.app import Application
from neo.protocol import Packet
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError, \
        NEOStorageConflictError
from neo.protocol import *
import os

def connectToPrimaryMasterNode(self):
    # TODO: remove monkeypatching and properly simulate master node connection
    pass
Application.connectToPrimaryMasterNode_org = Application.connectToPrimaryMasterNode
Application.connectToPrimaryMasterNode = connectToPrimaryMasterNode

def _waitMessage(self, conn=None, msg_id=None):
    if conn is None:
        raise NotImplementedError
    self.answer_handler.dispatch(conn, conn.fakeReceived())
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

class ConnectionPoolTest(unittest.TestCase):

    def getUUID(self):
        uuid = INVALID_UUID
        while uuid == INVALID_UUID:
            uuid = os.urandom(16)
        self.uuid = uuid
        return uuid

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

    def checkDispatcherRegisterCalled(self, app, conn, msg_id):
        calls = app.dispatcher.mockGetNamedCalls('register')
        self.assertEquals(len(calls), 1)
        self.assertTrue(calls[0].getParam(0) is conn)
        self.assertEquals(calls[0].getParam(1), msg_id)
        self.assertEquals(calls[0].getParam(2), app.local_var.queue)

    def checkPacketSent(self, conn, msg_id, packet_type):
        calls = conn.mockGetNamedCalls('addPacket')
        self.assertEquals(len(calls), 1)
        packet = calls[0].getParam(0)
        self.assertTrue(isinstance(packet, Packet))
        self.assertEquals(packet._type, packet_type)

    def checkMessageExpected(self, conn, msg_id):
        calls = conn.mockGetNamedCalls('expectMessage')
        self.assertEquals(len(calls), 1)
        self.assertEquals(calls[0].getParam(0), msg_id)

    def checkNoPacketSent(self, conn):
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)
        self.assertEquals(len(conn.mockGetNamedCalls('expectMessage')), 0)

    def getApp(self, master_nodes='127.0.0.1:10010', name='test',
               connector='SocketConnector', **kw):
        # TODO: properly simulate master node connection
        app = Application(master_nodes, name, connector, **kw)
        app.num_partitions = 10
        app.num_replicas = 2
        return app

    def test_getQueue(self):
        app = self.getApp()
        # Test sanity check
        self.assertTrue(getattr(app, 'local_var', None) is not None)
        # Test that queue is created if it does not exist in local_var
        self.assertTrue(getattr(app.local_var, 'queue', None) is None)
        queue = app.getQueue()
        # Test sanity check
        self.assertTrue(getattr(app.local_var, 'queue', None) is queue)

    def test_registerDB(self):
        app = self.getApp()
        dummy_db = []
        app.registerDB(dummy_db, None)
        self.assertTrue(app.getDB() is dummy_db)

    def test_new_oid(self):
        app = self.getApp()
        test_msg_id = 50
        test_oid_list = ['\x00\x00\x00\x00\x00\x00\x00\x01', '\x00\x00\x00\x00\x00\x00\x00\x02']
        response_packet = Packet()
        response_packet.answerNewOIDs(test_msg_id, test_oid_list[:])
        app.master_conn = Mock({'getNextId': test_msg_id, 'addPacket': None,
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
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Packet()
        packet.oidNotFound(oid, '')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        # object found on storage nodes and put in cache
        packet = Packet()
        packet.answerObject(*an_object)
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = an_object
        result = app.load(oid)
        self.assertEquals(result, ('', tid1))
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        self.assertTrue(oid in mq)
        # object is now cached, try to reload it 
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        result = app.load(oid)
        self.assertEquals(result, ('', tid1))
        self.assertEquals(len(conn.mockGetNamedCalls('addPacket')), 0)
        
    def test_loadSerial(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Packet()
        packet.oidNotFound(oid, '')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadSerial, oid, tid2)
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        # object should not have been cached
        self.assertFalse(oid in mq)
        # now a cached version ewxists but should not be hit 
        mq.store(oid, (tid1, 'WRONG'))
        self.assertTrue(oid in mq)
        packet = Packet()
        another_object = (1, oid, tid2, INVALID_SERIAL, 0, 0, 'RIGHT')
        packet.answerObject(*another_object)
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = another_object
        result = app.loadSerial(oid, tid1)
        self.assertEquals(result, 'RIGHT')
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        self.assertTrue(oid in mq)

    def test_loadBefore(self):
        app = self.getApp()
        mq = app.mq_cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertTrue(oid not in mq)
        packet = Packet()
        packet.oidNotFound(oid, '')
        cell = Mock({ 'getUUID': '\x00' * 16})
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.pt = Mock({ 'getCellList': (cell, ), })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = -1
        self.assertRaises(NEOStorageNotFoundError, app.loadBefore, oid, tid2)
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        # no previous versions -> return None
        an_object = (1, oid, tid2, INVALID_SERIAL, 0, 0, '')
        packet = Packet()
        packet.answerObject(*an_object)
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
        packet = Packet()
        packet.answerObject(*another_object)
        conn = Mock({ 
            'getServer': ('127.0.0.1', 0),
            'fakeReceived': packet,    
        })
        app.cp = Mock({ 'getConnForNode' : conn})
        app.local_var.asked_object = another_object
        result = app.loadBefore(oid, tid1)
        self.assertEquals(result, ('RIGHT', tid1, tid2))
        self.checkPacketSent(conn, 1, ASK_OBJECT)
        self.assertTrue(oid in mq)

    def test_tpc_begin(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = Mock()
        # first, tid is supplied 
        self.assertNotEquals(getattr(app, 'tid', None), tid)
        self.assertNotEquals(getattr(app, 'txn', None), txn)
        app.tpc_begin(transaction=txn, tid=tid)
        self.assertTrue(app.txn is txn)
        self.assertEquals(app.tid, tid)
        # next, the transaction already begin -> do nothing
        app.tpc_begin(transaction=txn, tid=None)
        self.assertTrue(app.txn is txn)
        self.assertEquals(app.tid, tid)
        # cancel and start a transaction without tid
        app.txn = None
        app.tid = None
        # no connection -> NEOStorageError
        self.assertRaises(NEOStorageError, app.tpc_begin, transaction=txn, tid=None)
        # ask a tid to pmn
        packet = Packet()
        packet.answerNewTID(msg_id=1, tid=tid)
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
        self.checkPacketSent(app.master_conn, 1, ASK_NEW_TID)
        self.checkMessageExpected(app.master_conn, 1)
        self.checkDispatcherRegisterCalled(app, app.master_conn, 1)
        # check attributes
        self.assertTrue(app.txn is txn)
        self.assertEquals(app.tid, tid)

    def test_store1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        app.txn = old_txn = object()
        self.assertTrue(app.txn is not txn)
        self.assertRaises(StorageTransactionError, app.store, oid, tid, '', None, txn)
        self.assertEquals(app.txn, old_txn)
        # check partition_id and an empty cell list -> NEOStorageError
        app.txn = txn
        app.tid = tid
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
        app.txn = txn
        app.tid = tid
        packet = Packet()
        packet.answerStoreObject(msg_id=1, conflicting=1, oid=oid, serial=tid)
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
        app.txn_object_stored = (oid, tid)
        app.txn_data_dict[oid] = 'BEFORE'
        self.assertRaises(NEOStorageConflictError, app.store, oid, tid, '', None, txn)
        self.assertTrue(oid not in app.txn_data_dict)
        self.assertEquals(app.conflict_serial, tid)
        self.assertEquals(app.txn_object_stored, (-1, tid))
        self.checkPacketSent(conn, 1, ASK_STORE_OBJECT)
        self.checkMessageExpected(conn, 1)
        self.checkDispatcherRegisterCalled(app, conn, 1)

    def test_store3(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # case with no conflict
        app.txn = txn
        app.tid = tid
        packet = Packet()
        packet.answerStoreObject(msg_id=1, conflicting=0, oid=oid, serial=tid)
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
        app.txn_object_stored = ()
        app.store(oid, tid, 'DATA', None, txn)
        self.assertEquals(app.txn_object_stored, (oid, tid))
        self.assertEquals(app.txn_data_dict.get(oid, None), 'DATA')
        self.assertNotEquals(app.conflict_serial, tid)
        self.checkPacketSent(conn, 1, ASK_STORE_OBJECT)
        self.checkMessageExpected(conn, 1)
        self.checkDispatcherRegisterCalled(app, conn, 1)

    def test_tpc_vote1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        app.txn = old_txn = object()
        self.assertTrue(app.txn is not txn)
        self.assertRaises(StorageTransactionError, app.tpc_vote, txn)
        self.assertEquals(app.txn, old_txn)

    def test_tpc_vote2(self):
        # fake transaction object
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn = txn
        app.tid = tid
        packet = Packet()
        # wrong answer -> failure
        packet.answerNewOIDs(1, ())
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
        self.assertRaises(NEOStorageError, app.tpc_vote, txn)
        self.checkPacketSent(conn, 1, ASK_STORE_TRANSACTION)
        self.checkMessageExpected(conn, 1)
        self.checkDispatcherRegisterCalled(app, conn, 1)

    def test_tpc_vote3(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn = txn
        app.tid = tid
        packet = Packet()
        # response -> OK
        packet.answerStoreTransaction(msg_id=1, tid=tid)
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
        self.checkPacketSent(conn, 1, ASK_STORE_TRANSACTION)
        self.checkMessageExpected(conn, 1)
        self.checkDispatcherRegisterCalled(app, conn, 1)

    def test_tpc_abort1(self):
        # ignore mismatch transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn = old_txn = object()
        app.master_conn = Mock()
        app.tid = tid
        self.assertFalse(app.txn is txn)
        conn = Mock()
        cell = Mock()
        app.pt = Mock({'getCellList': (cell, cell)})
        app.cp = Mock({'getConnForNode': ReturnValues(None, cell)})
        app.tpc_abort(txn)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        self.assertEquals(app.txn, old_txn)
        self.assertEquals(app.tid, tid)

    def test_tpc_abort2(self):
        # 2 nodes : 1 transaction in the first, 2 objects in the second
        # connections to each node should received only one packet to abort
        # and transaction must also be aborted on the master node
        # for simplicity, just one cell per partition
        oid1, oid2 = self.makeOID(2), self.makeOID(4) # on partition 0
        app, tid = self.getApp(), self.makeTID(1)     # on partition 1
        txn = self.makeTransactionObject()
        app.txn, app.tid = txn, tid
        app.master_conn = Mock({'__hash__': 0})
        app.num_partitions = 2
        cell1 = Mock({ 'getNode': 'NODE1', '__hash__': 1 })
        cell2 = Mock({ 'getNode': 'NODE2', '__hash__': 2 })
        conn1, conn2 = Mock({ 'getNextId': 1, }), Mock({ 'getNextId': 2, })
        app.pt = Mock({ 'getCellList': ReturnValues((cell1, ), (cell1, ), (cell1, cell2)), })
        app.cp = Mock({ 'getConnForNode': ReturnValues(conn1, conn2), })
        # fake data
        app.txn_data_dict = {oid1: '', oid2: ''}
        app.tpc_abort(txn)
        # will check if there was just one call/packet :
        self.checkPacketSent(conn1, 1, ABORT_TRANSACTION)
        self.checkPacketSent(conn2, 2, ABORT_TRANSACTION)
        self.checkPacketSent(app.master_conn, app.master_conn.getNextId(), ABORT_TRANSACTION)
        self.assertEquals(app.tid, None)
        self.assertEquals(app.txn, None)
        self.assertEquals(app.txn_data_dict, {})
        self.assertEquals(app.txn_voted, False)
        self.assertEquals(app.txn_finished, False)

    def test_tpc_finish1(self):
        # ignore mismatch transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn = old_txn = object()
        app.master_conn = Mock()
        self.assertFalse(app.txn is txn)
        conn = Mock()
        cell = Mock()
        app.pt = Mock({'getCellList': (cell, cell)})
        app.cp = Mock({'getConnForNode': ReturnValues(None, cell)})
        app.tpc_finish(txn)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)
        self.assertEquals(app.txn, old_txn)

    def test_tpc_finish2(self):
        # bad answer -> NEOStorageError
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn, app.tid = txn, tid
        # test callable passed to tpc_finish
        self.f_called = False
        self.f_called_with_tid = None
        def hook(tid): 
            self.f_called = True
            self.f_called_with_tid = tid
        packet = Packet()
        packet.answerNewTID(1, INVALID_TID) 
        app.master_conn = Mock({ 
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10000),
            'fakeReceived': packet,    
        })
        app.dispatcher = Mock({})
        app.txn_finished = False
        self.assertRaises(NEOStorageError, app.tpc_finish, txn, hook)
        self.assertTrue(self.f_called)
        self.assertEquals(self.f_called_with_tid, tid)
        self.checkPacketSent(app.master_conn, 1, FINISH_TRANSACTION)
        self.checkDispatcherRegisterCalled(app, app.master_conn, 1)

    def test_tpc_finish3(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.txn, app.tid = txn, tid
        # test callable passed to tpc_finish
        self.f_called = False
        self.f_called_with_tid = None
        def hook(tid): 
            self.f_called = True
            self.f_called_with_tid = tid
        packet = Packet()
        packet.notifyTransactionFinished(1, tid)
        app.master_conn = Mock({ 
            'getNextId': 1,
            'getAddress': ('127.0.0.1', 10010),
            'fakeReceived': packet,    
        })
        app.dispatcher = Mock({})
        app.txn_finished = True
        app.tpc_finish(txn, hook)
        self.assertTrue(self.f_called)
        self.assertEquals(self.f_called_with_tid, tid)
        self.checkPacketSent(app.master_conn, 1, FINISH_TRANSACTION)
        self.checkDispatcherRegisterCalled(app, app.master_conn, 1)

    def test_undo(self):
        raise NotImplementedError

    def test_undoLog(self):
        raise NotImplementedError

    def test_history(self):
        raise NotImplementedError

    def test_sync(self):
        raise NotImplementedError

    def test_connectToPrimaryMasterNode(self):
        raise NotImplementedError
        app = getApp()
        app.connectToPrimaryMasterNode_org()

if __name__ == '__main__':
    unittest.main()

