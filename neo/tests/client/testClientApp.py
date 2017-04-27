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

if __name__ == '__main__':
    unittest.main()

