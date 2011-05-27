#
# Copyright (C) 2010  Nexedi SA
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
from neo.storage.replicator import Replicator, Partition, Task
from neo.lib.protocol import CellStates, NodeStates, Packets

class StorageReplicatorTests(NeoUnitTestBase):

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_populate(self):
        my_uuid = self.getNewUUID()
        other_uuid = self.getNewUUID()
        app = Mock()
        app.uuid = my_uuid
        app.pt = Mock({
            'getPartitions': 2,
            'getOutdatedOffsetListFor': [0],
        })
        replicator = Replicator(app)
        self.assertEqual(replicator.new_partition_set, set())
        replicator.populate()
        self.assertEqual(replicator.new_partition_set, set([0]))

    def test_reset(self):
        replicator = Replicator(None)
        replicator.task_list = ['foo']
        replicator.task_dict = {'foo': 'bar'}
        replicator.current_partition = 'foo'
        replicator.current_connection = 'foo'
        replicator.replication_done = 'foo'
        replicator.reset()
        self.assertEqual(replicator.task_list, [])
        self.assertEqual(replicator.task_dict, {})
        self.assertEqual(replicator.current_partition, None)
        self.assertEqual(replicator.current_connection, None)
        self.assertTrue(replicator.replication_done)

    def test_setCriticalTID(self):
        critical_tid = self.getNextTID()
        partition = Partition(0, critical_tid, [])
        self.assertEqual(partition.getCriticalTID(), critical_tid)
        self.assertEqual(partition.getOffset(), 0)

    def test_act(self):
        # Also tests "pending"
        uuid = self.getNewUUID()
        master_uuid = self.getNewUUID()
        critical_tid_0 = self.getNextTID()
        critical_tid_1 = self.getNextTID()
        critical_tid_2 = self.getNextTID()
        unfinished_ttid_1 = self.getOID(1)
        unfinished_ttid_2 = self.getOID(2)
        app = Mock()
        app.server = ('127.0.0.1', 10000)
        app.name = 'fake cluster'
        app.em = Mock({
            'register': None,
        })
        def connectorGenerator():
            return Mock()
        app.connector_handler = connectorGenerator
        app.uuid = uuid
        node_addr = ('127.0.0.1', 1234)
        node = Mock({
            'getAddress': node_addr,
        })
        running_cell = Mock({
            'getNodeState': NodeStates.RUNNING,
            'getNode': node,
        })
        unknown_cell = Mock({
            'getNodeState': NodeStates.UNKNOWN,
        })
        app.pt = Mock({
            'getCellList': [running_cell, unknown_cell],
            'getOutdatedOffsetListFor': [0],
            'getPartition': 0,
        })
        node_conn_handler = Mock({
            'startReplication': None,
        })
        node_conn = Mock({
            'getAddress': node_addr,
            'getHandler': node_conn_handler,
        })
        replicator = Replicator(app)
        replicator.populate()
        def act():
            app.master_conn = self.getFakeConnection(uuid=master_uuid)
            self.assertTrue(replicator.pending())
            replicator.act()
        # ask unfinished tids
        act()
        unfinished_tids = app.master_conn.mockGetNamedCalls('ask')[0].getParam(0)
        self.assertTrue(replicator.new_partition_set)
        self.assertEqual(type(unfinished_tids),
            Packets.AskUnfinishedTransactions)
        self.assertTrue(replicator.waiting_for_unfinished_tids)
        # nothing happens until waiting_for_unfinished_tids becomes False
        act()
        self.checkNoPacketSent(app.master_conn)
        self.assertTrue(replicator.waiting_for_unfinished_tids)
        # first time, there is an unfinished tid before critical tid,
        # replication cannot start, and unfinished TIDs are asked again
        replicator.setUnfinishedTIDList(critical_tid_0,
            [unfinished_ttid_1, unfinished_ttid_2])
        self.assertFalse(replicator.waiting_for_unfinished_tids)
        # Note: detection that nothing can be replicated happens on first call
        # and unfinished tids are asked again on second call. This is ok, but
        # might change, so just call twice.
        act()
        replicator.transactionFinished(unfinished_ttid_1, critical_tid_1)
        act()
        replicator.transactionFinished(unfinished_ttid_2, critical_tid_2)
        replicator.current_connection = node_conn
        act()
        self.assertEqual(replicator.current_partition,
            replicator.partition_dict[0])
        self.assertEqual(len(node_conn_handler.mockGetNamedCalls(
            'startReplication')), 1)
        self.assertFalse(replicator.replication_done)
        # Other calls should do nothing
        replicator.current_connection = Mock()
        act()
        self.checkNoPacketSent(app.master_conn)
        self.checkNoPacketSent(replicator.current_connection)
        # Mark replication over for this partition
        replicator.replication_done = True
        # Don't finish while there are pending answers
        replicator.current_connection = Mock({
            'isPending': True,
        })
        act()
        self.assertTrue(replicator.pending())
        replicator.current_connection = Mock({
            'isPending': False,
        })
        act()
        # also, replication is over
        self.assertFalse(replicator.pending())

    def test_removePartition(self):
        replicator = Replicator(None)
        replicator.partition_dict = {0: None, 2: None}
        replicator.new_partition_set = set([1])
        replicator.removePartition(0)
        self.assertEqual(replicator.partition_dict, {2: None})
        self.assertEqual(replicator.new_partition_set, set([1]))
        replicator.removePartition(1)
        replicator.removePartition(2)
        self.assertEqual(replicator.partition_dict, {})
        self.assertEqual(replicator.new_partition_set, set())
        # Must not raise
        replicator.removePartition(3)

    def test_addPartition(self):
        replicator = Replicator(None)
        replicator.partition_dict = {0: None}
        replicator.new_partition_set = set([1])
        replicator.addPartition(0)
        replicator.addPartition(1)
        self.assertEqual(replicator.partition_dict, {0: None})
        self.assertEqual(replicator.new_partition_set, set([1]))
        replicator.addPartition(2)
        self.assertEqual(replicator.partition_dict, {0: None})
        self.assertEqual(len(replicator.new_partition_set), 2)
        self.assertEqual(replicator.new_partition_set, set([1, 2]))

    def test_processDelayedTasks(self):
        replicator = Replicator(None)
        replicator.reset()
        marker = []
        def someCallable(foo, bar=None):
            return (foo, bar)
        replicator._addTask(1, someCallable, args=('foo', ))
        self.assertRaises(ValueError, replicator._addTask, 1, None)
        replicator._addTask(2, someCallable, args=('foo', ), kw={'bar': 'bar'})
        replicator.processDelayedTasks()
        self.assertEqual(replicator._getCheckResult(1), ('foo', None))
        self.assertEqual(replicator._getCheckResult(2), ('foo', 'bar'))
        # Also test Task
        task = Task(someCallable, args=('foo', ))
        self.assertRaises(ValueError, task.getResult)
        task.process()
        self.assertRaises(ValueError, task.process)
        self.assertEqual(task.getResult(), ('foo', None))

if __name__ == "__main__":
    unittest.main()

