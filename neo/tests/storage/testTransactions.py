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
from mock import Mock
from neo.tests import NeoTestBase
from neo.storage.transactions import Transaction, TransactionManager
from neo.storage.transactions import ConflictError, DelayedError


class TransactionTests(NeoTestBase):

    def testInit(self):
        uuid = self.getNewUUID()
        tid = self.getNextTID()
        txn = Transaction(uuid, tid)
        self.assertEqual(txn.getUUID(), uuid)
        self.assertEqual(txn.getTID(), tid)
        self.assertEqual(txn.getObjectList(), [])
        self.assertEqual(txn.getOIDList(), [])

    def testLock(self):
        txn = Transaction(self.getNewUUID(), self.getNextTID())
        self.assertFalse(txn.isLocked())
        txn.lock()
        self.assertTrue(txn.isLocked())
        # disallow lock more than once
        self.assertRaises(AssertionError, txn.lock)

    def testTransaction(self):
        txn = Transaction(self.getNewUUID(), self.getNextTID())
        oid_list = [self.getOID(1), self.getOID(2)]
        txn_info = (oid_list, 'USER', 'DESC', 'EXT', False)
        txn.prepare(*txn_info)
        self.assertEqual(txn.getTransactionInformations(), txn_info)

    def testObjects(self):
        txn = Transaction(self.getNewUUID(), self.getNextTID())
        oid1, oid2 = self.getOID(1), self.getOID(2)
        object1 = (oid1, 1, '1', 'O1', None)
        object2 = (oid2, 1, '2', 'O2', None)
        self.assertEqual(txn.getObjectList(), [])
        self.assertEqual(txn.getOIDList(), [])
        txn.addObject(*object1)
        self.assertEqual(txn.getObjectList(), [object1])
        self.assertEqual(txn.getOIDList(), [oid1])
        txn.addObject(*object2)
        self.assertEqual(txn.getObjectList(), [object1, object2])
        self.assertEqual(txn.getOIDList(), [oid1, oid2])

    def test_getObject(self):
        oid_1 = self.getOID(1)
        oid_2 = self.getOID(2)
        txn = Transaction(self.getNewUUID(), self.getNextTID())
        object_info = (oid_1, None, None, None, None)
        txn.addObject(*object_info)
        self.assertEqual(txn.getObject(oid_2), None)
        self.assertEqual(txn.getObject(oid_1), object_info)

class TransactionManagerTests(NeoTestBase):

    def setUp(self):
        self.app = Mock()
        # no history
        self.app.dm = Mock({'getObjectHistory': []})
        self.manager = TransactionManager(self.app)
        self.ltid = None

    def _getTransaction(self):
        tid = self.getNextTID(self.ltid)
        oid_list = [self.getOID(1), self.getOID(2)]
        return (tid, (oid_list, 'USER', 'DESC', 'EXT', False))

    def _getObject(self, value):
        oid = self.getOID(value)
        serial = self.getNextTID()
        return (serial, (oid, 1, str(value), 'O' + str(value), None))

    def _checkTransactionStored(self, *args):
        calls = self.app.dm.mockGetNamedCalls('storeTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(*args)

    def _checkTransactionFinished(self, tid):
        calls = self.app.dm.mockGetNamedCalls('finishTransaction')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs(tid)

    def _checkQueuedEventExecuted(self, number=1):
        calls = self.app.mockGetNamedCalls('executeQueuedEvents')
        self.assertEqual(len(calls), number)
        
    def testSimpleCase(self):
        """ One node, one transaction, not abort """
        uuid = self.getNewUUID()
        tid, txn = self._getTransaction()
        serial1, object1 = self._getObject(1)
        serial2, object2 = self._getObject(2)
        self.manager.storeTransaction(uuid, tid, *txn)
        self.manager.storeObject(uuid, tid, serial1, *object1)
        self.manager.storeObject(uuid, tid, serial2, *object2)
        self.assertTrue(tid in self.manager)
        self.manager.lock(tid)
        self._checkTransactionStored(tid, [object1, object2], txn)
        self.manager.unlock(tid)
        self.assertFalse(tid in self.manager)
        self._checkTransactionFinished(tid)

    def testDelayed(self):
        """ Two transactions, the first cause delaytion of the second """
        uuid = self.getNewUUID()
        tid1, txn1 = self._getTransaction()
        tid2, txn2 = self._getTransaction()
        serial, obj = self._getObject(1)
        # first transaction lock the object
        self.manager.storeTransaction(uuid, tid1, *txn1)
        self.assertTrue(tid1 in self.manager)
        self.manager.storeObject(uuid, tid1, serial, *obj)
        self.manager.lock(tid1)
        # the second is delayed
        self.manager.storeTransaction(uuid, tid2, *txn2)
        self.assertTrue(tid2 in self.manager)
        self.assertRaises(DelayedError, self.manager.storeObject, 
                uuid, tid2, serial, *obj)

    def testUnresolvableConflict(self):
        """ A newer transaction has already modified an object """
        uuid = self.getNewUUID()
        tid1, txn1 = self._getTransaction()
        tid2, txn2 = self._getTransaction()
        serial, obj = self._getObject(1)
        # the (later) transaction lock (change) the object
        self.manager.storeTransaction(uuid, tid2, *txn2)
        self.manager.storeObject(uuid, tid2, serial, *obj)
        self.assertTrue(tid2 in self.manager)
        self.manager.lock(tid2)
        # the previous it's not using the latest version
        self.manager.storeTransaction(uuid, tid1, *txn1)
        self.assertTrue(tid1 in self.manager)
        self.assertRaises(ConflictError, self.manager.storeObject, 
                uuid, tid1, serial, *obj)

    def testResolvableConflict(self):
        """ Try to store an object with the lastest revision """
        uuid = self.getNewUUID()
        tid, txn = self._getTransaction()
        serial, obj = self._getObject(1)
        next_serial = self.getNextTID(serial)
        # try to store without the last revision
        self.app.dm = Mock({'getObjectHistory': [next_serial]})
        self.manager.storeTransaction(uuid, tid, *txn)
        self.assertRaises(ConflictError, self.manager.storeObject, 
                uuid, tid, serial, *obj)

    def testConflictWithTwoNodes(self):
        """ Ensure conflict/delaytion is working with different nodes"""
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        self.assertNotEqual(uuid1, uuid2)
        tid1, txn1 = self._getTransaction()
        tid2, txn2 = self._getTransaction()
        serial1, obj1 = self._getObject(1)
        serial2, obj2 = self._getObject(2)
        # first transaction lock the object
        self.manager.storeTransaction(uuid1, tid1, *txn1)
        self.assertTrue(tid1 in self.manager)
        self.manager.storeObject(uuid1, tid1, serial1, *obj1)
        self.manager.lock(tid1)
        # second transaction is delayed
        self.manager.storeTransaction(uuid2, tid2, *txn2)
        self.assertTrue(tid2 in self.manager)
        self.assertRaises(DelayedError, self.manager.storeObject, 
                uuid2, tid2, serial1, *obj1)
        # the second transaction lock another object
        self.manager.storeTransaction(uuid2, tid2, *txn2)
        self.manager.storeObject(uuid2, tid2, serial2, *obj2)
        self.assertTrue(tid2 in self.manager)
        self.manager.lock(tid2)
        # the first get a conflict
        self.manager.storeTransaction(uuid1, tid1, *txn1)
        self.assertTrue(tid1 in self.manager)
        self.assertRaises(ConflictError, self.manager.storeObject, 
                uuid1, tid1, serial2, *obj2)

    def testAbortUnlocked(self):
        """ Abort a non-locked transaction """
        uuid = self.getNewUUID()
        tid, txn = self._getTransaction()
        serial, obj = self._getObject(1)
        self.manager.storeTransaction(uuid, tid, *txn)
        self.manager.storeObject(uuid, tid, serial, *obj)
        self.assertTrue(tid in self.manager)
        # transaction is not locked
        self.manager.abort(tid)
        self.assertFalse(tid in self.manager)
        self.assertFalse(self.manager.loadLocked(obj[0]))
        self._checkQueuedEventExecuted()

    def testAbortLockedDoNothing(self):
        """ Try to abort a locked transaction """
        uuid = self.getNewUUID()
        tid, txn = self._getTransaction()
        serial, obj = self._getObject(1)
        self.manager.storeTransaction(uuid, tid, *txn)
        self.manager.storeObject(uuid, tid, serial, *obj)
        # lock transaction
        self.manager.lock(tid)
        self.assertTrue(tid in self.manager)
        self.manager.abort(tid, even_if_locked=False)
        self.assertTrue(tid in self.manager)
        self.assertTrue(self.manager.loadLocked(obj[0]))
        self._checkQueuedEventExecuted(number=0)
        
    def testAbortForNode(self):
        """ Abort transaction for a node """
        uuid1 = self.getNewUUID()
        uuid2 = self.getNewUUID()
        self.assertNotEqual(uuid1, uuid2)
        tid1, txn1 = self._getTransaction()
        tid2, txn2 = self._getTransaction()
        tid3, txn3 = self._getTransaction()
        self.manager.storeTransaction(uuid1, tid1, *txn1)
        # node 2 owns tid2 & tid3 and lock tid2 only
        self.manager.storeTransaction(uuid2, tid2, *txn2)
        self.manager.storeTransaction(uuid2, tid3, *txn3)
        self.manager.lock(tid2)
        self.assertTrue(tid1 in self.manager)
        self.assertTrue(tid2 in self.manager)
        self.assertTrue(tid3 in self.manager)
        self.manager.abortFor(uuid2)
        # only tid3 is aborted
        self.assertTrue(tid1 in self.manager)
        self.assertTrue(tid2 in self.manager)
        self.assertFalse(tid3 in self.manager)
        self._checkQueuedEventExecuted(number=1)
        
    def testReset(self):
        """ Reset the manager """
        uuid = self.getNewUUID()
        tid, txn = self._getTransaction()
        serial, obj = self._getObject(1)
        self.manager.storeTransaction(uuid, tid, *txn)
        self.manager.storeObject(uuid, tid, serial, *obj)
        self.manager.lock(tid)
        self.assertTrue(tid in self.manager)
        self.manager.reset()
        self.assertFalse(tid in self.manager)
        self.assertFalse(self.manager.loadLocked(obj[0]))

    def test_getObjectFromTransaction(self):
        uuid = self.getNewUUID()
        tid1, txn1 = self._getTransaction()
        tid2, txn2 = self._getTransaction()
        serial1, obj1 = self._getObject(1)
        serial2, obj2 = self._getObject(2)
        self.manager.storeObject(uuid, tid1, serial1, *obj1)
        self.assertEqual(self.manager.getObjectFromTransaction(tid2, obj1[0]),
            None)
        self.assertEqual(self.manager.getObjectFromTransaction(tid1, obj2[0]),
            None)
        self.assertEqual(self.manager.getObjectFromTransaction(tid1, obj1[0]),
            obj1)

if __name__ == "__main__":
    unittest.main()
