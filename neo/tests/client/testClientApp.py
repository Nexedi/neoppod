#
# Copyright (C) 2009-2016  Nexedi SA
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import threading
import unittest
from mock import Mock, ReturnValues
from ZODB.POSException import StorageTransactionError, UndoError, ConflictError
from .. import NeoUnitTestBase, buildUrlFromString
from neo.client.app import Application
from neo.client.cache import test as testCache
from neo.client.exception import NEOStorageError, NEOStorageNotFoundError
from neo.lib.protocol import NodeTypes, Packets, Errors, UUID_NAMESPACES
from neo.lib.util import makeChecksum

class Dispatcher(object):

    def pending(self, queue):
        return not queue.empty()

    def forget_queue(self, queue, flush_queue=True):
        pass

def _getMasterConnection(self):
    if self.master_conn is None:
        self.last_tid = None
        self.uuid = 1 + (UUID_NAMESPACES[NodeTypes.CLIENT] << 24)
        self.num_partitions = 10
        self.num_replicas = 1
        self.pt = Mock({'getCellList': ()})
        self.master_conn = Mock()
    return self.master_conn

def getConnection(kw):
    conn = Mock(kw)
    conn.lock = threading.RLock()
    return conn

def _ask(self, conn, packet, handler=None, **kw):
    self.setHandlerData(None)
    conn.ask(packet, **kw)
    if handler is None:
        raise NotImplementedError
    else:
        handler.dispatch(conn, conn.fakeReceived())
    return self.getHandlerData()

def failing_tryToResolveConflict(oid, conflict_serial, serial, data):
    raise ConflictError

class ClientApplicationTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        # apply monkey patches
        self._getMasterConnection = Application._getMasterConnection
        self._ask = Application._ask
        Application._getMasterConnection = _getMasterConnection
        Application._ask = _ask
        self._to_stop_list = []

    def _tearDown(self, success):
        # stop threads
        for app in self._to_stop_list:
            app.close()
        # restore environment
        Application._ask = self._ask
        Application._getMasterConnection = self._getMasterConnection
        NeoUnitTestBase._tearDown(self, success)

    # some helpers

    def _begin(self, app, txn, tid):
        txn_context = app._txn_container.new(txn)
        txn_context['ttid'] = tid
        return txn_context

    def getApp(self, master_nodes=None, name='test', **kw):
        if master_nodes is None:
            master_nodes = '%s:10010' % buildUrlFromString(self.local_ip)
        app = Application(master_nodes, name, **kw)
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
            value = randint(1, 255)
        return '\00' * 7 + chr(value)
    makeTID = makeOID

    def getNodeCellConn(self, index=1, address=('127.0.0.1', 10000), uuid=None):
        conn = getConnection({
            'getAddress': address,
            '__repr__': 'connection mock',
            'getUUID': uuid,
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

    # common checks

    def checkDispatcherRegisterCalled(self, app, conn):
        calls = app.dispatcher.mockGetNamedCalls('register')
        #self.assertEqual(len(calls), 1)
        #self.assertEqual(calls[0].getParam(0), conn)
        #self.assertTrue(isinstance(calls[0].getParam(2), Queue))

    testCache = testCache

    def test_load(self):
        app = self.getApp()
        cache = app._cache
        oid = self.makeOID()
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        tid3 = self.makeTID(3)
        tid4 = self.makeTID(4)
        # connection to SN close
        self.assertFalse(oid in cache._oid_dict)
        conn = Mock({'getAddress': ('', 0)})
        app.cp = Mock({'iterateForObject': [(Mock(), conn)]})
        def fakeReceived(packet):
            packet.setId(0)
            conn.fakeReceived = iter((packet,)).next
        def fakeObject(oid, serial, next_serial, data):
            fakeReceived(Packets.AnswerObject(oid, serial, next_serial, 0,
                                              makeChecksum(data), data, None))
            return data, serial, next_serial

        fakeReceived(Errors.OidNotFound(''))
        #Application._waitMessage = self._waitMessage
        # XXX: test disabled because of an infinite loop
        # self.assertRaises(NEOStorageError, app.load, oid, None, tid2)
        # self.checkAskObject(conn)
        #Application._waitMessage = _waitMessage
        # object not found in NEO -> NEOStorageNotFoundError
        self.assertFalse(oid in cache._oid_dict)

        fakeReceived(Errors.OidNotFound(''))
        self.assertRaises(NEOStorageNotFoundError, app.load, oid)
        self.checkAskObject(conn)

        r1 = fakeObject(oid, tid1, tid3, 'FOO')
        self.assertEqual(r1, app.load(oid, None, tid2))
        self.checkAskObject(conn)
        for t in tid2, tid3:
            self.assertEqual(cache._load(oid, t).tid, tid1)
        self.assertEqual(r1, app.load(oid, tid1))
        self.assertEqual(r1, app.load(oid, None, tid3))
        self.assertRaises(StandardError, app.load, oid, tid2)
        self.assertRaises(StopIteration, app.load, oid)
        self.checkAskObject(conn)

        r2 = fakeObject(oid, tid3, None, 'BAR')
        self.assertEqual(r2, app.load(oid, None, tid4))
        self.checkAskObject(conn)
        self.assertEqual(r2, app.load(oid))
        self.assertEqual(r2, app.load(oid, tid3))

        cache.invalidate(oid, tid4)
        self.assertRaises(StopIteration, app.load, oid)
        self.checkAskObject(conn)
        self.assertEqual(len(cache._oid_dict[oid]), 2)

    def test_tpc_begin(self):
        app = self.getApp()
        tid = self.makeTID()
        txn = Mock()
        # first, tid is supplied
        self.assertRaises(StorageTransactionError, app._txn_container.get, txn)
        packet = Packets.AnswerBeginTransaction(tid=tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
        })
        app.tpc_begin(transaction=txn, tid=tid)
        txn_context = app._txn_container.get(txn)
        self.assertTrue(txn_context['txn'] is txn)
        self.assertEqual(txn_context['ttid'], tid)
        # next, the transaction already begin -> raise
        self.assertRaises(StorageTransactionError, app.tpc_begin,
            transaction=txn, tid=None)
        txn_context = app._txn_container.get(txn)
        self.assertTrue(txn_context['txn'] is txn)
        self.assertEqual(txn_context['ttid'], tid)
        # start a transaction without tid
        txn = Mock()
        # no connection -> NEOStorageError (wait until connected to primary)
        #self.assertRaises(NEOStorageError, app.tpc_begin, transaction=txn, tid=None)
        # ask a tid to pmn
        packet = Packets.AnswerBeginTransaction(tid=tid)
        packet.setId(0)
        app.master_conn = Mock({
            'getNextId': 1,
            'fakeReceived': packet,
        })
        app.tpc_begin(transaction=txn, tid=None)
        self.checkAskNewTid(app.master_conn)
        self.checkDispatcherRegisterCalled(app, app.master_conn)
        # check attributes
        txn_context = app._txn_container.get(txn)
        self.assertTrue(txn_context['txn'] is txn)
        self.assertEqual(txn_context['ttid'], tid)

    def test_store1(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # invalid transaction > StorageTransactionError
        self.assertRaises(StorageTransactionError, app.store, oid, tid, '',
            None, txn)
        # check partition_id and an empty cell list -> NEOStorageError
        self._begin(app, txn, self.makeTID())
        app.pt = Mock({'getCellList': ()})
        app.num_partitions = 2
        self.assertRaises(NEOStorageError, app.store, oid, tid, '',  None,
            txn)
        calls = app.pt.mockGetNamedCalls('getCellList')
        self.assertEqual(len(calls), 1)

    def test_store2(self):
        app = self.getApp()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # build conflicting state
        txn_context = self._begin(app, txn, tid)
        packet = Packets.AnswerStoreObject(conflicting=1, oid=oid, serial=tid)
        packet.setId(0)
        storage_address = ('127.0.0.1', 10020)
        node, cell, conn = self.getNodeCellConn(address=storage_address)
        app.pt = Mock()
        app.cp = self.getConnectionPool([(node, conn)])
        app.dispatcher = Dispatcher()
        app.nm.createStorage(address=storage_address)
        data_dict = txn_context['data_dict']
        data_dict[oid] = 'BEFORE'
        app.store(oid, tid, '', None, txn)
        txn_context['queue'].put((conn, packet, {}))
        self.assertRaises(ConflictError, app.waitStoreResponses, txn_context,
            failing_tryToResolveConflict)
        self.assertTrue(oid not in data_dict)
        self.assertEqual(txn_context['object_stored_counter_dict'][oid], {})
        self.checkAskStoreObject(conn)

    def test_store3(self):
        app = self.getApp()
        uuid = self.getStorageUUID()
        oid = self.makeOID(11)
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        # case with no conflict
        txn_context = self._begin(app, txn, tid)
        packet = Packets.AnswerStoreObject(conflicting=0, oid=oid, serial=tid)
        packet.setId(0)
        storage_address = ('127.0.0.1', 10020)
        node, cell, conn = self.getNodeCellConn(address=storage_address,
            uuid=uuid)
        app.cp = self.getConnectionPool([(node, conn)])
        app.pt = Mock()
        app.dispatcher = Dispatcher()
        app.nm.createStorage(address=storage_address)
        app.store(oid, tid, 'DATA', None, txn)
        self.checkAskStoreObject(conn)
        txn_context['queue'].put((conn, packet, {}))
        app.waitStoreResponses(txn_context, None) # no conflict in this test
        self.assertEqual(txn_context['object_stored_counter_dict'][oid],
            {tid: {uuid}})
        self.assertEqual(txn_context['cache_dict'][oid], 'DATA')
        self.assertFalse(oid in txn_context['data_dict'])
        self.assertFalse(oid in txn_context['conflict_serial_dict'])

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
        address1 = ('127.0.0.1', 10000); uuid1 = self.getMasterUUID()
        address2 = ('127.0.0.1', 10001); uuid2 = self.getStorageUUID()
        address3 = ('127.0.0.1', 10002); uuid3 = self.getStorageUUID()
        app.nm.createMaster(address=address1, uuid=uuid1)
        app.nm.createStorage(address=address2, uuid=uuid2)
        app.nm.createStorage(address=address3, uuid=uuid3)
        # answer packets
        packet1 = Packets.AnswerStoreTransaction(tid=tid)
        packet2 = Packets.AnswerStoreObject(conflicting=1, oid=oid1, serial=tid)
        packet3 = Packets.AnswerStoreObject(conflicting=0, oid=oid2, serial=tid)
        [p.setId(i) for p, i in zip([packet1, packet2, packet3], range(3))]
        conn1 = getConnection({'__repr__': 'conn1', 'getAddress': address1,
                               'fakeReceived': packet1, 'getUUID': uuid1})
        conn2 = getConnection({'__repr__': 'conn2', 'getAddress': address2,
                               'fakeReceived': packet2, 'getUUID': uuid2})
        conn3 = getConnection({'__repr__': 'conn3', 'getAddress': address3,
                              'fakeReceived': packet3, 'getUUID': uuid3})
        node1 = Mock({'__repr__': 'node1', '__hash__': 1, 'getConnection': conn1})
        node2 = Mock({'__repr__': 'node2', '__hash__': 2, 'getConnection': conn2})
        node3 = Mock({'__repr__': 'node3', '__hash__': 3, 'getConnection': conn3})
        # fake environment
        app.cp = Mock({'getConnForCell': ReturnValues(conn2, conn3, conn1)})
        app.cp = Mock({
            'getConnForNode': ReturnValues(conn2, conn3, conn1),
            'iterateForObject': [(node2, conn2), (node3, conn3), (node1, conn1)],
        })
        app.master_conn = Mock({'__hash__': 0})
        txn = self.makeTransactionObject()
        txn_context = self._begin(app, txn, tid)
        app.dispatcher = Dispatcher()
        # conflict occurs on storage 2
        app.store(oid1, tid, 'DATA', None, txn)
        app.store(oid2, tid, 'DATA', None, txn)
        queue = txn_context['queue']
        queue.put((conn2, packet2, {}))
        queue.put((conn3, packet3, {}))
        # vote fails as the conflict is not resolved, nothing is sent to storage 3
        self.assertRaises(ConflictError, app.tpc_vote, txn, failing_tryToResolveConflict)
        # abort must be sent to storage 1 and 2
        app.tpc_abort(txn)
        self.checkAbortTransaction(conn2)
        self.checkAbortTransaction(conn3)

    def test_undo1(self):
        # invalid transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.master_conn = Mock()
        conn = Mock()
        self.assertRaises(StorageTransactionError, app.undo, tid,
            txn, failing_tryToResolveConflict)
        # no packet sent
        self.checkNoPacketSent(conn)
        self.checkNoPacketSent(app.master_conn)

    def _getAppForUndoTests(self, oid0, tid0, tid1, tid2):
        app = self.getApp()
        cell = Mock({
            'getAddress': 'FakeServer',
            'getState': 'FakeState',
        })
        app.pt = Mock({'getCellList': [cell]})
        transaction_info = Packets.AnswerTransactionInformation(tid1, '', '',
            '', False, (oid0, ))
        transaction_info.setId(1)
        conn = getConnection({
            'getNextId': 1,
            'fakeReceived': transaction_info,
            'getAddress': ('127.0.0.1', 10020),
        })
        node = app.nm.createStorage(address=conn.getAddress())
        app.cp = Mock({
            'iterateForObject': [(node, conn)],
            'getConnForCell': conn,
        })
        app.dispatcher = Dispatcher()
        def load(oid, tid=None, before_tid=None):
            self.assertEqual(oid, oid0)
            return ({tid0: 'dummy', tid2: 'cdummy'}[tid], None, None)
        app.load = load
        store_marker = []
        def _store(txn_context, oid, serial, data, data_serial=None,
                unlock=False):
            store_marker.append((oid, serial, data, data_serial))
        app._store = _store
        app.last_tid = self.getNextTID()
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
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid2, tid0, False)})
        conn.ask = lambda p, queue=None, **kw: \
            isinstance(p, Packets.AskObjectUndoSerial) and \
            queue.put((conn, undo_serial, kw))
        undo_serial.setId(2)
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append((oid, conflict_serial, serial, data, committedData))
            return 'solved'
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        app.undo(tid1, txn, tryToResolveConflict)
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

        Undo is rejected with a raise, because conflict resolution fails.
        """
        oid0 = self.makeOID(1)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid2, tid0, False)})
        undo_serial.setId(2)
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        conn.ask = lambda p, queue=None, **kw: \
            type(p) is Packets.AskObjectUndoSerial and \
            queue.put((conn, undo_serial, kw))
        marker = []
        def tryToResolveConflict(oid, conflict_serial, serial, data,
                committedData=''):
            marker.append((oid, conflict_serial, serial, data, committedData))
            raise ConflictError
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        self.assertRaises(UndoError, app.undo, tid1, txn, tryToResolveConflict)
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
        self.assertRaises(UndoError, app.undo, tid1, txn, tryToResolveConflict)
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
        transaction_info = Packets.AnswerTransactionInformation(tid1, '', '',
            '', False, (oid0, ))
        transaction_info.setId(1)
        undo_serial = Packets.AnswerObjectUndoSerial({
            oid0: (tid1, tid0, True)})
        undo_serial.setId(2)
        app, conn, store_marker = self._getAppForUndoTests(oid0, tid0, tid1,
            tid2)
        conn.ask = lambda p, queue=None, **kw: \
            type(p) is Packets.AskObjectUndoSerial and \
            queue.put((conn, undo_serial, kw))
        # The undo
        txn = self.beginTransaction(app, tid=tid3)
        app.undo(tid1, txn, None) # no conflict resolution in this test
        # Checking what happened
        moid, mserial, mdata, mdata_serial = store_marker[0]
        self.assertEqual(moid, oid0)
        self.assertEqual(mserial, tid1)
        self.assertEqual(mdata, None)
        self.assertEqual(mdata_serial, tid0)

    def test_connectToPrimaryNode(self):
        # here we have three master nodes :
        # the connection to the first will fail
        # the second will have changed
        # the third will not be ready
        # after the third, the partition table will be operational
        # (as if it was connected to the primary master node)
        # will raise IndexError at the third iteration
        app = self.getApp('127.0.0.1:10010 127.0.0.1:10011')
        # TODO: test more connection failure cases
        # askLastTransaction
        def _ask8(_):
            pass
        # Sixth packet : askPartitionTable succeeded
        def _ask7(_):
            app.pt = Mock({'operational': True})
        # fifth packet : request node identification succeeded
        def _ask6(conn):
            app.master_conn = conn
            app.uuid = 1 + (UUID_NAMESPACES[NodeTypes.CLIENT] << 24)
            app.trying_master_node = app.primary_master_node = Mock({
                'getAddress': ('127.0.0.1', 10011),
                '__str__': 'Fake master node',
            })
        # third iteration : node not ready
        def _ask4(_):
            app.trying_master_node = None
        # second iteration : master node changed
        def _ask3(_):
            app.primary_master_node = Mock({
                'getAddress': ('127.0.0.1', 10010),
                '__str__': 'Fake master node',
            })
        # first iteration : connection failed
        def _ask2(_):
            app.trying_master_node = None
        # do nothing for the first call
        # Case of an unknown primary_uuid (XXX: handler should probably raise,
        # it's not normal for a node to inform of a primary uuid without
        # telling us what its address is.)
        def _ask1(_):
            pass
        ask_func_list = [_ask1, _ask2, _ask3, _ask4, _ask6, _ask7, _ask8]
        def _ask_base(conn, _, handler=None):
            ask_func_list.pop(0)(conn)
            app.nm.getByAddress(conn.getAddress())._connection = None
        app._ask = _ask_base
        # fake environment
        app.em.close()
        app.em = Mock({'getConnectionList': []})
        app.pt = Mock({ 'operational': False})
        app.start = lambda: None
        app.master_conn = app._connectToPrimaryNode()
        self.assertFalse(ask_func_list)
        self.assertTrue(app.master_conn is not None)
        self.assertTrue(app.pt.operational())

if __name__ == '__main__':
    unittest.main()

