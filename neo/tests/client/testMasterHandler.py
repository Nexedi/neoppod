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
from mock import Mock, ReturnValues
from neo.tests import NeoUnitTestBase
from neo.pt import PartitionTable
from neo.protocol import NodeTypes, NodeStates
from neo.client.handlers.master import PrimaryBootstrapHandler
from neo.client.handlers.master import PrimaryNotificationsHandler, \
       PrimaryAnswersHandler
from neo.client.exception import NEOStorageError

MARKER = []


class MasterHandlerTests(NeoUnitTestBase):

    def setUp(self):
        pass

    def getConnection(self):
        return self.getFakeConnection()


class MasterBootstrapHandlerTests(MasterHandlerTests):

    def setUp(self):
        self.app = Mock()
        self.handler = PrimaryBootstrapHandler(self.app)

    def checkCalledOnApp(self, method, index=0):
        calls = self.app.mockGetNamedCalls(method)
        self.assertTrue(len(calls) > index)
        return calls[index].params

    def test_notReady(self):
        conn = self.getConnection()
        self.handler.notReady(conn, 'message')
        self.assertEqual(self.app.trying_master_node, None)
        self.checkCalledOnApp('setNodeNotReady')

    def test_acceptIdentification1(self):
        """ Non-master node """
        conn = self.getConnection()
        uuid = self.getNewUUID()
        self.handler.acceptIdentification(conn, NodeTypes.CLIENT, 
            uuid, 100, 0, None)
        self.checkClosed(conn)

    def test_acceptIdentification2(self):
        """ No UUID supplied """
        conn = self.getConnection()
        uuid = self.getNewUUID()
        self.checkProtocolErrorRaised(self.handler.acceptIdentification,
            conn, NodeTypes.MASTER, uuid, 100, 0, None)

    def test_acceptIdentification3(self):
        """ identification accepted  """
        node = Mock()
        conn = self.getConnection()
        uuid = self.getNewUUID()
        your_uuid = self.getNewUUID()
        partitions = 100
        replicas = 2
        self.app.nm = Mock({'getByAddress': node})
        self.handler.acceptIdentification(conn, NodeTypes.MASTER, uuid, 
            partitions, replicas, your_uuid)
        self.assertEqual(self.app.uuid, your_uuid)
        self.checkUUIDSet(conn, uuid)
        self.checkUUIDSet(node, uuid)
        self.assertTrue(isinstance(self.app.pt, PartitionTable))

    def _getMasterList(self, uuid_list):
        port = 1000
        master_list = []
        for uuid in uuid_list:
            master_list.append((('127.0.0.1', port), uuid))
            port += 1
        return master_list

    def test_answerPrimary1(self):
        """ Primary not known, master udpated """
        node, uuid = Mock(), self.getNewUUID()
        conn = self.getConnection()
        master_list = [(('127.0.0.1', 1000), uuid)]
        self.app.primary_master_node = Mock()
        self.app.trying_master_node = Mock()
        self.app.nm = Mock({'getByAddress': node})
        self.handler.answerPrimary(conn, None, master_list)
        self.checkUUIDSet(node, uuid)
        # previously known primary master forgoten
        self.assertEqual(self.app.primary_master_node, None)
        self.assertEqual(self.app.trying_master_node, None)
        self.checkClosed(conn)

    def test_answerPrimary2(self):
        """ Primary known """
        current_node = Mock({'__repr__': '1'})
        node, uuid = Mock({'__repr__': '2'}), self.getNewUUID()
        conn = self.getConnection()
        master_list = [(('127.0.0.1', 1000), uuid)]
        self.app.primary_master_node = None
        self.app.trying_master_node = current_node
        self.app.nm = Mock({
            'getByAddress': node,
            'getByUUID': node,
        })
        self.handler.answerPrimary(conn, uuid, [])
        self.assertEqual(self.app.trying_master_node, None)
        self.assertTrue(self.app.primary_master_node is node)
        self.checkClosed(conn)

    def test_answerPartitionTable(self):
        conn = self.getConnection()
        self.app.pt = Mock()
        ptid = 0
        row_list = ([], [])
        self.handler.answerPartitionTable(conn, ptid, row_list)
        load_calls = self.app.pt.mockGetNamedCalls('load')
        self.assertEqual(len(load_calls), 1)
        # load_calls[0].checkArgs(ptid, row_list, self.app.nm)


class MasterNotificationsHandlerTests(MasterHandlerTests):

    def setUp(self):
        self.db = Mock()
        self.app = Mock({'getDB': self.db})
        self.app.nm = Mock()
        self.app.dispatcher = Mock()
        self.handler = PrimaryNotificationsHandler(self.app)

    def test_connectionClosed(self):
        conn = self.getConnection()
        node = Mock()
        self.app.master_conn = conn
        self.app.primary_master_node = node
        self.handler.connectionClosed(conn)
        self.assertEqual(self.app.master_conn, None)
        self.assertEqual(self.app.primary_master_node, None)

    def test_invalidateObjects(self):
        conn = self.getConnection()
        tid = self.getNextTID()
        oid1, oid2 = self.getOID(1), self.getOID(2)
        self.app.mq_cache = {
            oid1: tid,
            oid2: tid,
        }
        self.handler.invalidateObjects(conn, tid, [oid1])
        self.assertFalse(oid1 in self.app.mq_cache)
        self.assertTrue(oid2 in self.app.mq_cache)
        invalidation_calls = self.db.mockGetNamedCalls('invalidate')
        self.assertEqual(len(invalidation_calls), 1)
        invalidation_calls[0].checkArgs(tid, {oid1:tid})

    def test_notifyPartitionChanges(self):
        conn = self.getConnection()
        self.app.pt = Mock({'filled': True})
        ptid = 0
        cell_list = (Mock(), Mock())
        self.handler.notifyPartitionChanges(conn, ptid, cell_list)
        update_calls = self.app.pt.mockGetNamedCalls('update')
        self.assertEqual(len(update_calls), 1)
        update_calls[0].checkArgs(ptid, cell_list, self.app.nm)

    def test_notifyNodeInformation(self):
        conn = self.getConnection()
        addr = ('127.0.0.1', 1000)
        node_list = [
            (NodeTypes.CLIENT, addr, self.getNewUUID(), NodeStates.UNKNOWN),
            (NodeTypes.STORAGE, addr, self.getNewUUID(), NodeStates.DOWN),
        ]
        # XXX: it might be better to test with real node & node manager
        conn1 = self.getFakeConnection()
        conn2 = self.getFakeConnection()
        node1 = Mock({
            'getConnection': conn1, 
            '__nonzero__': 1,
            'isConnected': True,
            '__repr__': 'Fake Node',
        })
        node2 = Mock({
            'getConnection': conn2, 
            '__nonzero__': 1,
            'isConnected': True,
            '__repr__': 'Fake Node',
        })
        self.app.nm = Mock({'getByUUID': ReturnValues(node1, node2)})
        self.app.cp = Mock()
        self.handler.notifyNodeInformation(conn, node_list)
        # node manager updated
        update_calls = self.app.nm.mockGetNamedCalls('update')
        self.assertEqual(len(update_calls), 1)
        update_calls[0].checkArgs(node_list)
        # connections closed
        self.checkClosed(conn1)
        self.checkClosed(conn2)
        # storage removed from connection pool
        remove_calls = self.app.cp.mockGetNamedCalls('removeConnection')
        self.assertEqual(len(remove_calls), 1)
        remove_calls[0].checkArgs(conn2)
        # storage unregistered
        unregister_calls = self.app.dispatcher.mockGetNamedCalls('unregister')
        self.assertEqual(len(unregister_calls), 1)
        unregister_calls[0].checkArgs(conn2)


class MasterAnswersHandlerTests(MasterHandlerTests):

    def setUp(self):
        self.app = Mock()
        self.handler = PrimaryAnswersHandler(self.app)

    def test_answerBeginTransaction(self):
        tid = self.getNextTID()
        conn = self.getConnection()
        self.handler.answerBeginTransaction(conn, tid)
        calls = self.app.mockGetNamedCalls('setTID')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)

    def test_answerNewOIDs(self):
        conn = self.getConnection()
        oid1, oid2, oid3 = self.getOID(0), self.getOID(1), self.getOID(2)
        self.handler.answerNewOIDs(conn, [oid1, oid2, oid3])
        self.assertEqual(self.app.new_oid_list, [oid1, oid2, oid3])

    def test_answerTransactionFinished(self):
        conn = self.getConnection()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID(tid1)
        # wrong TID
        self.app = Mock({'getTID': tid1})
        self.checkProtocolErrorRaised(self.handler.answerTransactionFinished, 
            conn, tid2)
        # matching TID
        app = Mock({'getTID': tid2})
        handler = PrimaryAnswersHandler(app=app)
        handler.answerTransactionFinished(conn, tid2)
        calls = app.mockGetNamedCalls('setTransactionFinished')
        self.assertEqual(len(calls), 1)
        
    def test_answerPack(self):
        self.assertRaises(NEOStorageError, self.handler.answerPack, None, False)
        # Check it doesn't raise
        self.handler.answerPack(None, True)

if __name__ == '__main__':
    unittest.main()

