#
# Copyright (C) 2009-2015  Nexedi SA
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
from mock import Mock
from .. import NeoUnitTestBase
from neo.lib.node import NodeManager
from neo.lib.pt import PartitionTable
from neo.lib.protocol import NodeTypes
from neo.client.handlers.master import PrimaryBootstrapHandler
from neo.client.handlers.master import PrimaryNotificationsHandler, \
       PrimaryAnswersHandler
from neo.client.exception import NEOStorageError

class MasterHandlerTests(NeoUnitTestBase):
    def setUp(self):
        super(MasterHandlerTests, self).setUp()
        self.db = Mock()
        self.app = Mock({'getDB': self.db,
                         'txn_contexts': ()})
        self.app.nm = NodeManager()
        self.app.dispatcher = Mock()
        self._next_port = 3000

    def getKnownMaster(self):
        node = self.app.nm.createMaster(address=(
            self.local_ip, self._next_port),
        )
        self._next_port += 1
        conn = self.getFakeConnection(address=node.getAddress())
        node.setConnection(conn)
        return node, conn

class MasterBootstrapHandlerTests(MasterHandlerTests):

    def setUp(self):
        super(MasterBootstrapHandlerTests, self).setUp()
        self.handler = PrimaryBootstrapHandler(self.app)

    def checkCalledOnApp(self, method, index=0):
        calls = self.app.mockGetNamedCalls(method)
        self.assertTrue(len(calls) > index)
        return calls[index].params

    def test_notReady(self):
        conn = self.getFakeConnection()
        self.handler.notReady(conn, 'message')
        self.assertEqual(self.app.trying_master_node, None)

    def test_acceptIdentification1(self):
        """ Non-master node """
        node, conn = self.getKnownMaster()
        self.handler.acceptIdentification(conn, NodeTypes.CLIENT,
            node.getUUID(), 100, 0, None, None, [])
        self.checkClosed(conn)

    def test_acceptIdentification2(self):
        """ No UUID supplied """
        node, conn = self.getKnownMaster()
        uuid = self.getMasterUUID()
        addr = conn.getAddress()
        self.checkProtocolErrorRaised(self.handler.acceptIdentification,
            conn, NodeTypes.MASTER, uuid, 100, 0, None,
            addr, [(addr, uuid)],
        )

    def test_acceptIdentification3(self):
        """ identification accepted  """
        node, conn = self.getKnownMaster()
        uuid = self.getMasterUUID()
        addr = conn.getAddress()
        your_uuid = self.getClientUUID()
        self.handler.acceptIdentification(conn, NodeTypes.MASTER, uuid,
            100, 2, your_uuid, addr, [(addr, uuid)])
        self.assertEqual(self.app.uuid, your_uuid)
        self.assertEqual(node.getUUID(), uuid)
        self.assertTrue(isinstance(self.app.pt, PartitionTable))

    def _getMasterList(self, uuid_list):
        port = 1000
        master_list = []
        for uuid in uuid_list:
            master_list.append((('127.0.0.1', port), uuid))
            port += 1
        return master_list

    def test_answerPartitionTable(self):
        conn = self.getFakeConnection()
        self.app.pt = Mock()
        ptid = 0
        row_list = ([], [])
        self.handler.answerPartitionTable(conn, ptid, row_list)
        load_calls = self.app.pt.mockGetNamedCalls('load')
        self.assertEqual(len(load_calls), 1)
        # load_calls[0].checkArgs(ptid, row_list, self.app.nm)


class MasterNotificationsHandlerTests(MasterHandlerTests):

    def setUp(self):
        super(MasterNotificationsHandlerTests, self).setUp()
        self.handler = PrimaryNotificationsHandler(self.app)

    def test_connectionClosed(self):
        conn = self.getFakeConnection()
        node = Mock()
        self.app.master_conn = conn
        self.app.primary_master_node = node
        self.handler.connectionClosed(conn)
        self.assertEqual(self.app.master_conn, None)
        self.assertEqual(self.app.primary_master_node, None)

    def test_invalidateObjects(self):
        conn = self.getFakeConnection()
        tid = self.getNextTID()
        oid1, oid2, oid3 = self.getOID(1), self.getOID(2), self.getOID(3)
        self.app._cache = Mock({
            'invalidate': None,
        })
        self.handler.invalidateObjects(conn, tid, [oid1, oid3])
        cache_calls = self.app._cache.mockGetNamedCalls('invalidate')
        self.assertEqual(len(cache_calls), 2)
        cache_calls[0].checkArgs(oid1, tid)
        cache_calls[1].checkArgs(oid3, tid)
        invalidation_calls = self.db.mockGetNamedCalls('invalidate')
        self.assertEqual(len(invalidation_calls), 1)
        invalidation_calls[0].checkArgs(tid, [oid1, oid3])

    def test_notifyPartitionChanges(self):
        conn = self.getFakeConnection()
        self.app.pt = Mock({'filled': True})
        ptid = 0
        cell_list = (Mock(), Mock())
        self.handler.notifyPartitionChanges(conn, ptid, cell_list)
        update_calls = self.app.pt.mockGetNamedCalls('update')
        self.assertEqual(len(update_calls), 1)
        update_calls[0].checkArgs(ptid, cell_list, self.app.nm)


class MasterAnswersHandlerTests(MasterHandlerTests):

    def setUp(self):
        super(MasterAnswersHandlerTests, self).setUp()
        self.handler = PrimaryAnswersHandler(self.app)

    def test_answerBeginTransaction(self):
        tid = self.getNextTID()
        conn = self.getFakeConnection()
        self.handler.answerBeginTransaction(conn, tid)
        calls = self.app.mockGetNamedCalls('setHandlerData')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)

    def test_answerNewOIDs(self):
        conn = self.getFakeConnection()
        oid1, oid2, oid3 = self.getOID(0), self.getOID(1), self.getOID(2)
        self.handler.answerNewOIDs(conn, [oid1, oid2, oid3])
        self.assertEqual(self.app.new_oid_list, [oid3, oid2, oid1])

    def test_answerTransactionFinished(self):
        conn = self.getFakeConnection()
        ttid2 = self.getNextTID()
        tid2 = self.getNextTID()
        self.handler.answerTransactionFinished(conn, ttid2, tid2)
        calls = self.app.mockGetNamedCalls('setHandlerData')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid2)

    def test_answerPack(self):
        self.assertRaises(NEOStorageError, self.handler.answerPack, None, False)
        # Check it doesn't raise
        self.handler.answerPack(None, True)

if __name__ == '__main__':
    unittest.main()

