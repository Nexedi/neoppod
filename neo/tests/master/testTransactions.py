#
# Copyright (C) 2006-2010  Nexedi SA
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
from mock import Mock
from struct import pack, unpack
from neo.tests import NeoTestBase

from neo.master.transactions import Transaction, TransactionManager

class testTransactionManager(NeoTestBase):

    def makeTID(self, i):
        return pack('!Q', i)

    def makeOID(self, i):
        return pack('!Q', i)

    def makeUUID(self, i):
        return '\0' * 12 + pack('!Q', i)

    def testTransaction(self):
        # test data
        node = Mock({'__repr__': 'Node'})
        tid = self.makeTID(1)
        oid_list = (oid1, oid2) = (self.makeOID(1), self.makeOID(2))
        uuid_list = (uuid1, uuid2) = (self.makeUUID(1), self.makeUUID(2))
        msg_id = 1
        # create transaction object
        txn = Transaction(node, tid)
        self.assertEqual(txn.getUUIDList(), [])
        txn.prepare(oid_list, uuid_list, msg_id)
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
        uuid_list = (uuid1, uuid2) = self.makeUUID(1), self.makeUUID(2)
        # create transaction manager
        txnman = TransactionManager()
        self.assertFalse(txnman.hasPending())
        self.assertEqual(txnman.getPendingList(), [])
        # begin the transaction
        tid = txnman.begin(node, None)
        self.assertTrue(tid is not None)
        self.assertTrue(txnman.hasPending())
        self.assertEqual(len(txnman.getPendingList()), 1)
        self.assertEqual(txnman.getPendingList()[0], tid)
        self.assertEqual(txnman[tid].getTID(), tid)
        # prepare the transaction
        txnman.prepare(tid, oid_list, uuid_list, msg_id)
        txn = txnman[tid]
        self.assertEqual(txn.getUUIDList(), list(uuid_list))
        self.assertEqual(txn.getOIDList(), list(oid_list))
        # lock nodes
        self.assertFalse(txnman.lock(tid, uuid1))
        self.assertTrue(txnman.lock(tid, uuid2))
        # transaction finished
        txnman.remove(tid)
        self.assertEqual(txnman.getPendingList(), [])

    def testAbortFor(self):
        node1 = Mock({'__hash__': 1})
        node2 = Mock({'__hash__': 2})
        txnman = TransactionManager()
        # register 4 transactions made by two nodes
        tid11 = txnman.begin(node1, None)
        tid12 = txnman.begin(node1, None)
        tid21 = txnman.begin(node2, None)
        tid22 = txnman.begin(node2, None)
        self.assertTrue(tid11 < tid12 < tid21 < tid22)
        self.assertEqual(len(txnman.getPendingList()), 4)
        # abort transactions of one node
        txnman.abortFor(node1)
        tid_list = txnman.getPendingList()
        self.assertEqual(len(tid_list), 2)
        self.assertTrue(tid21 in tid_list)
        self.assertTrue(tid22 in tid_list)
        # then the other
        txnman.abortFor(node2)
        self.assertEqual(txnman.getPendingList(), [])
        self.assertFalse(txnman.hasPending())

    def test_getNextOIDList(self):
        txnman = TransactionManager()
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

    def test_getNextTID(self):
        txnman = TransactionManager()
        # no previous TID
        self.assertEqual(txnman.getLastTID(), None)
        # first transaction
        node1 = Mock({'__hash__': 1})
        tid1 = txnman.begin(node1, None)
        self.assertTrue(tid1 is not None)
        self.assertEqual(txnman.getLastTID(), tid1)
        # set a new last TID
        ntid = pack('!Q', unpack('!Q', tid1)[0] + 10)
        txnman.setLastTID(ntid)
        self.assertEqual(txnman.getLastTID(), ntid)
        self.assertTrue(ntid > tid1)
        # new trancation
        node2 = Mock({'__hash__': 2})
        tid2 = txnman.begin(node2, None)
        self.assertTrue(tid2 is not None)
        self.assertTrue(tid2 > ntid > tid1)

    def test_forget(self):
        client1 = Mock({'__hash__': 1})
        client2 = Mock({'__hash__': 2})
        client3 = Mock({'__hash__': 3})
        storage_1_uuid = self.makeUUID(1)
        storage_2_uuid = self.makeUUID(2)
        tid1 = self.makeTID(1)
        tid2 = self.makeTID(2)
        tid3 = self.makeTID(3)
        oid_list = [self.makeOID(1), ]

        tm = TransactionManager()
        # Transaction 1: 2 storage nodes involved, one will die and the other
        # already answered node lock
        msg_id_1 = 1
        tm.begin(client1, tid1)
        tm.prepare(tid1, oid_list, [storage_1_uuid, storage_2_uuid], msg_id_1)
        tm.lock(tid1, storage_2_uuid)
        # Transaction 2: 2 storage nodes involved, one will die
        msg_id_2 = 2
        tm.begin(client2, tid2)
        tm.prepare(tid2, oid_list, [storage_1_uuid, storage_2_uuid], msg_id_2)
        # Transaction 3: 1 storage node involved, which won't die
        msg_id_3 = 3
        tm.begin(client3, tid3)
        tm.prepare(tid3, oid_list, [storage_2_uuid, ], msg_id_3)

        t1 = tm[tid1]
        t2 = tm[tid2]
        t3 = tm[tid3]

        # Assert initial state
        self.assertFalse(t1.locked())
        self.assertFalse(t2.locked())
        self.assertFalse(t3.locked())

        # Storage 1 dies:
        # t1 is over
        self.assertTrue(t1.forget(storage_1_uuid))
        # t2 still waits for storage 2
        self.assertFalse(t2.forget(storage_1_uuid))
        self.assertTrue(t2.lock(storage_2_uuid))
        # t3 doesn't care
        self.assertFalse(t3.forget(storage_1_uuid))
        self.assertTrue(t3.lock(storage_2_uuid))

if __name__ == '__main__':
    unittest.main()
