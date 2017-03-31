#
# Copyright (C) 2009-2017  Nexedi SA
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

import unittest
from ..mock import Mock
from ZODB.POSException import StorageTransactionError
from .. import NeoUnitTestBase, buildUrlFromString
from neo.client.app import Application
from neo.client.cache import test as testCache
from neo.client.exception import NEOStorageError
from neo.lib.protocol import NodeTypes, UUID_NAMESPACES

class ClientApplicationTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)
        self._to_stop_list = []

    def _tearDown(self, success):
        # stop threads
        for app in self._to_stop_list:
            app.close()
        NeoUnitTestBase._tearDown(self, success)

    # some helpers

    def _begin(self, app, txn, tid):
        txn_context = app._txn_container.new(txn)
        txn_context.ttid = tid
        return txn_context

    def getApp(self, master_nodes=None, name='test', **kw):
        if master_nodes is None:
            master_nodes = '%s:10010' % buildUrlFromString(self.local_ip)
        app = Application(master_nodes, name, **kw)
        self._to_stop_list.append(app)
        app.dispatcher = Mock({ })
        return app

    def makeOID(self, value=None):
        from random import randint
        if value is None:
            value = randint(1, 255)
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

    # common checks

    testCache = testCache

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

    def test_undo1(self):
        # invalid transaction
        app = self.getApp()
        tid = self.makeTID()
        txn = self.makeTransactionObject()
        app.master_conn = Mock()
        self.assertRaises(StorageTransactionError, app.undo, tid, txn)
        # no packet sent
        self.checkNoPacketSent(app.master_conn)

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

