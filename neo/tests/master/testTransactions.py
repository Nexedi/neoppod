#
# Copyright (C) 2006-2015  Nexedi SA
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
from struct import pack
from .. import NeoUnitTestBase
from neo.lib.protocol import NodeTypes
from neo.lib.util import packTID, unpackTID, addTID
from neo.master.transactions import Transaction, TransactionManager

class testTransactionManager(NeoUnitTestBase):

    def makeTID(self, i):
        return pack('!Q', i)

    def makeOID(self, i):
        return pack('!Q', i)

    def makeNode(self, node_type):
        uuid = self.getNewUUID(node_type)
        node = Mock({'getUUID': uuid, '__hash__': uuid, '__repr__': 'FakeNode'})
        return uuid, node

    def testTransaction(self):
        # test data
        node = Mock({'__repr__': 'Node'})
        tid = self.makeTID(1)
        ttid = self.makeTID(2)
        oid_list = (oid1, oid2) = [self.makeOID(1), self.makeOID(2)]
        uuid_list = (uuid1, uuid2) = [self.getStorageUUID(),
                                      self.getStorageUUID()]
        msg_id = 1
        # create transaction object
        txn = Transaction(node, ttid)
        txn.prepare(tid, oid_list, uuid_list, msg_id)
        self.assertEqual(txn.getUUIDList(), uuid_list)
        self.assertEqual(txn.getOIDList(), oid_list)
        # lock nodes one by one
        self.assertFalse(txn.lock(uuid1))
        self.assertTrue(txn.lock(uuid2))
        # check that repr() works
        repr(txn)

    def testManager(self):
        # test data
        node = Mock({'__hash__': 1})
        msg_id = 1
        oid_list = (oid1, oid2) = self.makeOID(1), self.makeOID(2)
        uuid_list = uuid1, uuid2 = self.getStorageUUID(), self.getStorageUUID()
        client_uuid = self.getClientUUID()
        # create transaction manager
        callback = Mock()
        txnman = TransactionManager(on_commit=callback)
        self.assertFalse(txnman.hasPending())
        self.assertEqual(txnman.registerForNotification(uuid1), [])
        # begin the transaction
        ttid = txnman.begin(node)
        self.assertTrue(ttid is not None)
        self.assertEqual(len(txnman.registerForNotification(uuid1)), 1)
        self.assertTrue(txnman.hasPending())
        # prepare the transaction
        tid = txnman.prepare(ttid, 1, oid_list, uuid_list, msg_id)
        self.assertTrue(txnman.hasPending())
        self.assertEqual(txnman.registerForNotification(uuid1), [ttid])
        txn = txnman[ttid]
        self.assertEqual(txn.getTID(), tid)
        self.assertEqual(txn.getUUIDList(), list(uuid_list))
        self.assertEqual(txn.getOIDList(), list(oid_list))
        # lock nodes
        txnman.lock(ttid, uuid1)
        self.assertEqual(len(callback.getNamedCalls('__call__')), 0)
        txnman.lock(ttid, uuid2)
        self.assertEqual(len(callback.getNamedCalls('__call__')), 1)
        # transaction finished
        txnman.remove(client_uuid, ttid)
        self.assertEqual(txnman.registerForNotification(uuid1), [])

    def testAbortFor(self):
        oid_list = [self.makeOID(1), ]
        storage_1_uuid, node1 = self.makeNode(NodeTypes.STORAGE)
        storage_2_uuid, node2 = self.makeNode(NodeTypes.STORAGE)
        client_uuid, client = self.makeNode(NodeTypes.CLIENT)
        txnman = TransactionManager(lambda tid, txn: None)
        # register 4 transactions made by two nodes
        self.assertEqual(txnman.registerForNotification(storage_1_uuid), [])
        ttid1 = txnman.begin(client)
        tid1 = txnman.prepare(ttid1, 1, oid_list, [storage_1_uuid], 1)
        self.assertEqual(txnman.registerForNotification(storage_1_uuid), [ttid1])
        # abort transactions of another node, transaction stays
        txnman.abortFor(node2)
        self.assertEqual(txnman.registerForNotification(storage_1_uuid), [ttid1])
        # abort transactions of requesting node, transaction is not removed
        # because the transaction is prepared and must remains until the end of
        # the 2PC
        txnman.abortFor(node1)
        self.assertEqual(txnman.registerForNotification(storage_1_uuid), [ttid1])
        self.assertTrue(txnman.hasPending())
        # ...and the lock is available
        txnman.begin(client, self.getNextTID())

    def test_getNextOIDList(self):
        txnman = TransactionManager(lambda tid, txn: None)
        # must raise as we don"t have one
        self.assertEqual(txnman.getLastOID(), None)
        self.assertRaises(RuntimeError, txnman.getNextOIDList, 1)
        # ask list
        txnman.setLastOID(self.getOID(1))
        oid_list = txnman.getNextOIDList(15)
        self.assertEqual(len(oid_list), 15)
        # begin from 1, so generated oid from 2 to 16
        for i, oid in zip(xrange(len(oid_list)), oid_list):
            self.assertEqual(oid, self.getOID(i+2))

    def test_forget(self):
        client1 = Mock({'__hash__': 1})
        client2 = Mock({'__hash__': 2})
        client3 = Mock({'__hash__': 3})
        storage_1_uuid = self.getStorageUUID()
        storage_2_uuid = self.getStorageUUID()
        oid_list = [self.makeOID(1), ]
        client_uuid = self.getClientUUID()

        tm = TransactionManager(lambda tid, txn: None)
        # Transaction 1: 2 storage nodes involved, one will die and the other
        # already answered node lock
        msg_id_1 = 1
        ttid1 = tm.begin(client1)
        tid1 = tm.prepare(ttid1, 1, oid_list,
            [storage_1_uuid, storage_2_uuid], msg_id_1)
        tm.lock(ttid1, storage_2_uuid)
        t1 = tm[ttid1]
        self.assertFalse(t1.locked())
        # Storage 1 dies:
        # t1 is over
        self.assertTrue(t1.forget(storage_1_uuid))
        self.assertEqual(t1.getUUIDList(), [storage_2_uuid])
        tm.remove(client_uuid, tid1)

        # Transaction 2: 2 storage nodes involved, one will die
        msg_id_2 = 2
        ttid2 = tm.begin(client2)
        tid2 = tm.prepare(ttid2, 1, oid_list,
            [storage_1_uuid, storage_2_uuid], msg_id_2)
        t2 = tm[ttid2]
        self.assertFalse(t2.locked())
        # Storage 1 dies:
        # t2 still waits for storage 2
        self.assertFalse(t2.forget(storage_1_uuid))
        self.assertEqual(t2.getUUIDList(), [storage_2_uuid])
        self.assertTrue(t2.lock(storage_2_uuid))
        tm.remove(client_uuid, tid2)

        # Transaction 3: 1 storage node involved, which won't die
        msg_id_3 = 3
        ttid3 = tm.begin(client3)
        tid3 = tm.prepare(ttid3, 1, oid_list, [storage_2_uuid, ],
            msg_id_3)
        t3 = tm[ttid3]
        self.assertFalse(t3.locked())
        # Storage 1 dies:
        # t3 doesn't care
        self.assertFalse(t3.forget(storage_1_uuid))
        self.assertEqual(t3.getUUIDList(), [storage_2_uuid])
        self.assertTrue(t3.lock(storage_2_uuid))
        tm.remove(client_uuid, tid3)

    def testTIDUtils(self):
        """
        Tests packTID/unpackTID/addTID.
        """
        min_tid = pack('!LL', 0, 0)
        min_unpacked_tid = ((1900, 1, 1, 0, 0), 0)
        max_tid = pack('!LL', 2**32 - 1, 2 ** 32 - 1)
        # ((((9917 - 1900) * 12 + (10 - 1)) * 31 + (14 - 1)) * 24 + 4) * 60 +
        # 15 == 2**32 - 1
        max_unpacked_tid = ((9917, 10, 14, 4, 15), 2**32 - 1)

        self.assertEqual(unpackTID(min_tid), min_unpacked_tid)
        self.assertEqual(unpackTID(max_tid), max_unpacked_tid)
        self.assertEqual(packTID(*min_unpacked_tid), min_tid)
        self.assertEqual(packTID(*max_unpacked_tid), max_tid)

        self.assertEqual(addTID(min_tid, 1), pack('!LL', 0, 1))
        self.assertEqual(addTID(pack('!LL', 0, 2**32 - 1), 1),
            pack('!LL', 1, 0))
        self.assertEqual(addTID(pack('!LL', 0, 2**32 - 1), 2**32 + 1),
            pack('!LL', 2, 0))
        # Check impossible dates are avoided (2010/11/31 doesn't exist)
        self.assertEqual(
            unpackTID(addTID(packTID((2010, 11, 30, 23, 59), 2**32 - 1), 1)),
            ((2010, 12, 1, 0, 0), 0))

    def testTransactionLock(self):
        """
        Transaction lock is present to ensure invalidation TIDs are sent in
        strictly increasing order.
        Note: this implementation might change later, to allow more paralelism.
        """
        client_uuid, client = self.makeNode(NodeTypes.CLIENT)
        tm = TransactionManager(lambda tid, txn: None)
        # With a requested TID, lock spans from begin to remove
        ttid1 = self.getNextTID()
        ttid2 = self.getNextTID()
        tid1 = tm.begin(client, ttid1)
        self.assertEqual(tid1, ttid1)
        tm.remove(client_uuid, tid1)
        # Without a requested TID, lock spans from prepare to remove only
        ttid3 = tm.begin(client)
        ttid4 = tm.begin(client) # Doesn't raise
        node = Mock({'getUUID': client_uuid, '__hash__': 0})
        tid4 = tm.prepare(ttid4, 1, [], [], 0)
        tm.remove(client_uuid, tid4)
        tm.prepare(ttid3, 1, [], [], 0)

    def testClientDisconectsAfterBegin(self):
        client_uuid1, node1 = self.makeNode(NodeTypes.CLIENT)
        tm = TransactionManager(lambda tid, txn: None)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tm.begin(node1, tid1)
        tm.abortFor(node1)
        self.assertTrue(tid1 not in tm)

    def testUnlockPending(self):
        callback = Mock()
        uuid1, node1 = self.makeNode(NodeTypes.CLIENT)
        uuid2, node2 = self.makeNode(NodeTypes.CLIENT)
        storage_uuid = self.getStorageUUID()
        tm = TransactionManager(callback)
        ttid1 = tm.begin(node1)
        ttid2 = tm.begin(node2)
        tid1 = tm.prepare(ttid1, 1, [], [storage_uuid], 0)
        tid2 = tm.prepare(ttid2, 1, [], [storage_uuid], 0)
        tm.lock(ttid2, storage_uuid)
        # txn 2 is still blocked by txn 1
        self.assertEqual(len(callback.getNamedCalls('__call__')), 0)
        tm.lock(ttid1, storage_uuid)
        # both transactions are unlocked when txn 1 is fully locked
        self.assertEqual(len(callback.getNamedCalls('__call__')), 2)

if __name__ == '__main__':
    unittest.main()
