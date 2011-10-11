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
from mock import Mock
from neo.lib.util import dump, p64, u64
from neo.lib.protocol import CellStates, ZERO_OID, ZERO_TID
from neo.tests import NeoUnitTestBase
from neo.lib.exception import DatabaseFailure
from neo.storage.database.mysqldb import MySQLDatabaseManager

MAX_TID = '\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE' # != INVALID_TID

class StorageDBTests(NeoUnitTestBase):

    def setUp(self):
        NeoUnitTestBase.setUp(self)

    @property
    def db(self):
        try:
            return self._db
        except AttributeError:
            self.setNumPartitions(1)
            return self._db

    def tearDown(self):
        try:
            self.__dict__.pop('_db', None).close()
        except AttributeError:
            pass
        NeoUnitTestBase.tearDown(self)

    def getDB(self):
        raise NotImplementedError

    def setNumPartitions(self, num_partitions, reset=0):
        try:
            db = self._db
        except AttributeError:
            self._db = db = self.getDB(reset)
        else:
            if reset:
                db.setup(reset)
            else:
                try:
                    n = db.getNumPartitions()
                except KeyError:
                    n = 0
                if num_partitions == n:
                    return
                if num_partitions < n:
                    db.dropPartitions(n, range(num_partitions, n))
        db.setNumPartitions(num_partitions)
        self.assertEqual(num_partitions, db.getNumPartitions())
        uuid = self.getNewUUID()
        db.setUUID(uuid)
        self.assertEqual(uuid, db.getUUID())
        db.setPartitionTable(1,
            [(i, uuid, CellStates.UP_TO_DATE) for i in xrange(num_partitions)])

    def checkConfigEntry(self, get_call, set_call, value):
        # generic test for all configuration entries accessors
        self.assertRaises(KeyError, get_call)
        set_call(value)
        self.assertEqual(get_call(), value)
        set_call(value * 2)
        self.assertEqual(get_call(), value * 2)

    def test_UUID(self):
        db = self.getDB()
        self.checkConfigEntry(db.getUUID, db.setUUID, 'TEST_VALUE')

    def test_Name(self):
        db = self.getDB()
        self.checkConfigEntry(db.getName, db.setName, 'TEST_NAME')

    def test_15_PTID(self):
        db = self.getDB()
        self.checkConfigEntry(db.getPTID, db.setPTID, self.getPTID(1))

    def test_getPartitionTable(self):
        db = self.getDB()
        ptid = self.getPTID(1)
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        cell1 = (0, uuid1, CellStates.OUT_OF_DATE)
        cell2 = (1, uuid1, CellStates.UP_TO_DATE)
        db.setPartitionTable(ptid, [cell1, cell2])
        result = db.getPartitionTable()
        self.assertEqual(set(result), set([cell1, cell2]))

    def test_getLastOID(self):
        db = self.getDB()
        oid1 = self.getOID(1)
        db.setLastOID(oid1)
        result1 = db.getLastOID()
        self.assertEqual(result1, oid1)

    def getOIDs(self, count):
        return map(self.getOID, xrange(count))

    def getTIDs(self, count):
        tid_list = [self.getNextTID()]
        while len(tid_list) != count:
            tid_list.append(self.getNextTID(tid_list[-1]))
        return tid_list

    def getTransaction(self, oid_list):
        transaction = (oid_list, 'user', 'desc', 'ext', False)
        H = "0" * 20
        for _ in oid_list:
            self.db.storeData(H, '', 1)
        object_list = [(oid, H, None) for oid in oid_list]
        return (transaction, object_list)

    def checkSet(self, list1, list2):
        self.assertEqual(set(list1), set(list2))

    def test_getLastTID(self):
        tid1, tid2, tid3, tid4 = self.getTIDs(4)
        oid1, oid2 = self.getOIDs(2)
        txn, objs = self.getTransaction([oid1, oid2])
        # max TID is in obj table
        self.db.storeTransaction(tid1, objs, txn, False)
        self.db.storeTransaction(tid2, objs, txn, False)
        self.assertEqual(self.db.getLastTID(), tid2)
        # max tid is in ttrans table
        self.db.storeTransaction(tid3, objs, txn)
        result = self.db.getLastTID()
        self.assertEqual(self.db.getLastTID(), tid3)
        # max tid is in tobj (serial)
        self.db.storeTransaction(tid4, objs, None)
        self.assertEqual(self.db.getLastTID(), tid4)

    def test_getUnfinishedTIDList(self):
        tid1, tid2, tid3, tid4 = self.getTIDs(4)
        oid1, oid2 = self.getOIDs(2)
        txn, objs = self.getTransaction([oid1, oid2])
        # nothing pending
        self.db.storeTransaction(tid1, objs, txn, False)
        self.checkSet(self.db.getUnfinishedTIDList(), [])
        # one unfinished txn
        self.db.storeTransaction(tid2, objs, txn)
        self.checkSet(self.db.getUnfinishedTIDList(), [tid2])
        # no changes
        self.db.storeTransaction(tid3, objs, None, False)
        self.checkSet(self.db.getUnfinishedTIDList(), [tid2])
        # a second txn known by objs only
        self.db.storeTransaction(tid4, objs, None)
        self.checkSet(self.db.getUnfinishedTIDList(), [tid2, tid4])

    def test_objectPresent(self):
        tid = self.getNextTID()
        oid = self.getOID(1)
        txn, objs = self.getTransaction([oid])
        # not present
        self.assertFalse(self.db.objectPresent(oid, tid, all=True))
        self.assertFalse(self.db.objectPresent(oid, tid, all=False))
        # available in temp table
        self.db.storeTransaction(tid, objs, txn)
        self.assertTrue(self.db.objectPresent(oid, tid, all=True))
        self.assertFalse(self.db.objectPresent(oid, tid, all=False))
        # available in both tables
        self.db.finishTransaction(tid)
        self.assertTrue(self.db.objectPresent(oid, tid, all=True))
        self.assertTrue(self.db.objectPresent(oid, tid, all=False))

    def test_getObject(self):
        oid1, = self.getOIDs(1)
        tid1, tid2 = self.getTIDs(2)
        FOUND_BUT_NOT_VISIBLE = False
        OBJECT_T1_NO_NEXT = (tid1, None, 1, "0"*20, '', None)
        OBJECT_T1_NEXT = (tid1, tid2, 1, "0"*20, '', None)
        OBJECT_T2 = (tid2, None, 1, "0"*20, '', None)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid1])
        # non-present
        self.assertEqual(self.db.getObject(oid1), None)
        self.assertEqual(self.db.getObject(oid1, tid1), None)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1), None)
        # one non-commited version
        self.db.storeTransaction(tid1, objs1, txn1)
        self.assertEqual(self.db.getObject(oid1), None)
        self.assertEqual(self.db.getObject(oid1, tid1), None)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1), None)
        # one commited version
        self.db.finishTransaction(tid1)
        self.assertEqual(self.db.getObject(oid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
            FOUND_BUT_NOT_VISIBLE)
        # two version available, one non-commited
        self.db.storeTransaction(tid2, objs2, txn2)
        self.assertEqual(self.db.getObject(oid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
            FOUND_BUT_NOT_VISIBLE)
        self.assertEqual(self.db.getObject(oid1, tid2), FOUND_BUT_NOT_VISIBLE)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid2),
            OBJECT_T1_NO_NEXT)
        # two commited versions
        self.db.finishTransaction(tid2)
        self.assertEqual(self.db.getObject(oid1), OBJECT_T2)
        self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NEXT)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
            FOUND_BUT_NOT_VISIBLE)
        self.assertEqual(self.db.getObject(oid1, tid2), OBJECT_T2)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid2),
            OBJECT_T1_NEXT)

    def test_setPartitionTable(self):
        db = self.getDB()
        ptid = self.getPTID(1)
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        cell1 = (0, uuid1, CellStates.OUT_OF_DATE)
        cell2 = (1, uuid1, CellStates.UP_TO_DATE)
        cell3 = (1, uuid1, CellStates.DISCARDED)
        # no partition table
        self.assertEqual(db.getPartitionTable(), [])
        # set one
        db.setPartitionTable(ptid, [cell1])
        result = db.getPartitionTable()
        self.assertEqual(result, [cell1])
        # then another
        db.setPartitionTable(ptid, [cell2])
        result = db.getPartitionTable()
        self.assertEqual(result, [cell2])
        # drop discarded cells
        db.setPartitionTable(ptid, [cell2, cell3])
        result = db.getPartitionTable()
        self.assertEqual(result, [])

    def test_changePartitionTable(self):
        db = self.getDB()
        ptid = self.getPTID(1)
        uuid1, uuid2 = self.getNewUUID(), self.getNewUUID()
        cell1 = (0, uuid1, CellStates.OUT_OF_DATE)
        cell2 = (1, uuid1, CellStates.UP_TO_DATE)
        cell3 = (1, uuid1, CellStates.DISCARDED)
        # no partition table
        self.assertEqual(db.getPartitionTable(), [])
        # set one
        db.changePartitionTable(ptid, [cell1])
        result = db.getPartitionTable()
        self.assertEqual(result, [cell1])
        # add more entries
        db.changePartitionTable(ptid, [cell2])
        result = db.getPartitionTable()
        self.assertEqual(set(result), set([cell1, cell2]))
        # drop discarded cells
        db.changePartitionTable(ptid, [cell2, cell3])
        result = db.getPartitionTable()
        self.assertEqual(result, [cell1])

    def test_dropUnfinishedData(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid1])
        # nothing
        self.assertEqual(self.db.getObject(oid1), None)
        self.assertEqual(self.db.getObject(oid2), None)
        self.assertEqual(self.db.getUnfinishedTIDList(), [])
        # one is still pending
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.finishTransaction(tid1)
        result = self.db.getObject(oid1)
        self.assertEqual(result, (tid1, None, 1, "0"*20, '', None))
        self.assertEqual(self.db.getObject(oid2), None)
        self.assertEqual(self.db.getUnfinishedTIDList(), [tid2])
        # drop it
        self.db.dropUnfinishedData()
        self.assertEqual(self.db.getUnfinishedTIDList(), [])
        result = self.db.getObject(oid1)
        self.assertEqual(result, (tid1, None, 1, "0"*20, '', None))
        self.assertEqual(self.db.getObject(oid2), None)

    def test_storeTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        # nothing in database
        self.assertEqual(self.db.getLastTID(), None)
        self.assertEqual(self.db.getUnfinishedTIDList(), [])
        self.assertEqual(self.db.getObject(oid1), None)
        self.assertEqual(self.db.getObject(oid2), None)
        self.assertEqual(self.db.getTransaction(tid1, True), None)
        self.assertEqual(self.db.getTransaction(tid2, True), None)
        self.assertEqual(self.db.getTransaction(tid1, False), None)
        self.assertEqual(self.db.getTransaction(tid2, False), None)
        # store in temporary tables
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))
        self.assertEqual(self.db.getTransaction(tid1, False), None)
        self.assertEqual(self.db.getTransaction(tid2, False), None)
        # commit pending transaction
        self.db.finishTransaction(tid1)
        self.db.finishTransaction(tid2)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid1, False)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, False)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))

    def test_askFinishTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        # stored but not finished
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))
        self.assertEqual(self.db.getTransaction(tid1, False), None)
        self.assertEqual(self.db.getTransaction(tid2, False), None)
        # stored and finished
        self.db.finishTransaction(tid1)
        self.db.finishTransaction(tid2)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid1, False)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, False)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))

    def test_deleteTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.finishTransaction(tid1)
        self.db.deleteTransaction(tid1, [oid1])
        self.db.deleteTransaction(tid2, [oid2])
        self.assertEqual(self.db.getTransaction(tid1, True), None)
        self.assertEqual(self.db.getTransaction(tid2, True), None)

    def test_deleteTransactionsAbove(self):
        self.setNumPartitions(2)
        tid1 = self.getOID(0)
        tid2 = self.getOID(1)
        tid3 = self.getOID(2)
        oid1 = self.getOID(1)
        for tid in (tid1, tid2, tid3):
            txn, objs = self.getTransaction([oid1])
            self.db.storeTransaction(tid, objs, txn)
            self.db.finishTransaction(tid)
        self.db.deleteTransactionsAbove(2, 0, tid2, tid3)
        # Right partition, below cutoff
        self.assertNotEqual(self.db.getTransaction(tid1, True), None)
        # Wrong partition, above cutoff
        self.assertNotEqual(self.db.getTransaction(tid2, True), None)
        # Right partition, above cutoff
        self.assertEqual(self.db.getTransaction(tid3, True), None)

    def test_deleteObject(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1, oid2])
        txn2, objs2 = self.getTransaction([oid1, oid2])
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.finishTransaction(tid1)
        self.db.finishTransaction(tid2)
        self.db.deleteObject(oid1)
        self.assertEqual(self.db.getObject(oid1, tid=tid1), None)
        self.assertEqual(self.db.getObject(oid1, tid=tid2), None)
        self.db.deleteObject(oid2, serial=tid1)
        self.assertFalse(self.db.getObject(oid2, tid=tid1))
        self.assertEqual(self.db.getObject(oid2, tid=tid2),
            (tid2, None, 1, "0" * 20, '', None))

    def test_deleteObjectsAbove(self):
        self.setNumPartitions(2)
        tid1 = self.getOID(1)
        tid2 = self.getOID(2)
        tid3 = self.getOID(3)
        oid1 = self.getOID(0)
        oid2 = self.getOID(1)
        oid3 = self.getOID(2)
        for tid in (tid1, tid2, tid3):
            txn, objs = self.getTransaction([oid1, oid2, oid3])
            self.db.storeTransaction(tid, objs, txn)
            self.db.finishTransaction(tid)
        self.db.deleteObjectsAbove(2, 0, oid1, tid2, tid3)
        # Check getObjectHistoryFrom because MySQL adapter use two tables
        # that must be synchronized
        self.assertEqual(self.db.getObjectHistoryFrom(ZERO_OID, ZERO_TID,
            MAX_TID, 10, 2, 0), {oid1: [tid1]})
        # Right partition, below cutoff
        self.assertNotEqual(self.db.getObject(oid1, tid=tid1), None)
        # Right partition, above tid cutoff
        self.assertFalse(self.db.getObject(oid1, tid=tid2))
        self.assertFalse(self.db.getObject(oid1, tid=tid3))
        # Wrong partition, above cutoff
        self.assertNotEqual(self.db.getObject(oid2, tid=tid1), None)
        self.assertNotEqual(self.db.getObject(oid2, tid=tid2), None)
        self.assertNotEqual(self.db.getObject(oid2, tid=tid3), None)
        # Right partition, above cutoff
        self.assertEqual(self.db.getObject(oid3), None)

    def test_getTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        # get from temporary table or not
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.finishTransaction(tid1)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False))
        # get from non-temporary only
        result = self.db.getTransaction(tid1, False)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False))
        self.assertEqual(self.db.getTransaction(tid2, False), None)

    def test_getObjectHistory(self):
        oid = self.getOID(1)
        tid1, tid2, tid3 = self.getTIDs(3)
        txn1, objs1 = self.getTransaction([oid])
        txn2, objs2 = self.getTransaction([oid])
        txn3, objs3 = self.getTransaction([oid])
        # one revision
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.finishTransaction(tid1)
        result = self.db.getObjectHistory(oid, 0, 3)
        self.assertEqual(result, [(tid1, 0)])
        result = self.db.getObjectHistory(oid, 1, 1)
        self.assertEqual(result, None)
        # two revisions
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.finishTransaction(tid2)
        result = self.db.getObjectHistory(oid, 0, 3)
        self.assertEqual(result, [(tid2, 0), (tid1, 0)])
        result = self.db.getObjectHistory(oid, 1, 3)
        self.assertEqual(result, [(tid1, 0)])
        result = self.db.getObjectHistory(oid, 2, 3)
        self.assertEqual(result, None)

    def test_getObjectHistoryFrom(self):
        self.setNumPartitions(2)
        oid1 = self.getOID(0)
        oid2 = self.getOID(2)
        oid3 = self.getOID(1)
        tid1, tid2, tid3, tid4, tid5 = self.getTIDs(5)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        txn3, objs3 = self.getTransaction([oid1])
        txn4, objs4 = self.getTransaction([oid2])
        txn5, objs5 = self.getTransaction([oid3])
        self.db.storeTransaction(tid1, objs1, txn1)
        self.db.storeTransaction(tid2, objs2, txn2)
        self.db.storeTransaction(tid3, objs3, txn3)
        self.db.storeTransaction(tid4, objs4, txn4)
        self.db.storeTransaction(tid5, objs5, txn5)
        self.db.finishTransaction(tid1)
        self.db.finishTransaction(tid2)
        self.db.finishTransaction(tid3)
        self.db.finishTransaction(tid4)
        self.db.finishTransaction(tid5)
        # Check full result
        result = self.db.getObjectHistoryFrom(ZERO_OID, ZERO_TID, MAX_TID, 10,
            2, 0)
        self.assertEqual(result, {
            oid1: [tid1, tid3],
            oid2: [tid2, tid4],
        })
        # Lower bound is inclusive
        result = self.db.getObjectHistoryFrom(oid1, tid1, MAX_TID, 10, 2, 0)
        self.assertEqual(result, {
            oid1: [tid1, tid3],
            oid2: [tid2, tid4],
        })
        # Upper bound is inclusive
        result = self.db.getObjectHistoryFrom(ZERO_OID, ZERO_TID, tid3, 10,
            2, 0)
        self.assertEqual(result, {
            oid1: [tid1, tid3],
            oid2: [tid2],
        })
        # Length is total number of serials
        result = self.db.getObjectHistoryFrom(ZERO_OID, ZERO_TID, MAX_TID, 3,
            2, 0)
        self.assertEqual(result, {
            oid1: [tid1, tid3],
            oid2: [tid2],
        })
        # Partition constraints are honored
        result = self.db.getObjectHistoryFrom(ZERO_OID, ZERO_TID, MAX_TID, 10,
            2, 1)
        self.assertEqual(result, {
            oid3: [tid5],
        })

    def _storeTransactions(self, count):
        # use OID generator to know result of tid % N
        tid_list = self.getOIDs(count)
        oid = self.getOID(1)
        for tid in tid_list:
            txn, objs = self.getTransaction([oid])
            self.db.storeTransaction(tid, objs, txn)
            self.db.finishTransaction(tid)
        return tid_list

    def test_getTIDList(self):
        self.setNumPartitions(2, True)
        tid1, tid2, tid3, tid4 = self._storeTransactions(4)
        # get tids
        # - all partitions
        result = self.db.getTIDList(0, 4, 2, [0, 1])
        self.checkSet(result, [tid1, tid2, tid3, tid4])
        # - one partition
        result = self.db.getTIDList(0, 4, 2, [0])
        self.checkSet(result, [tid1, tid3])
        result = self.db.getTIDList(0, 4, 2, [1])
        self.checkSet(result, [tid2, tid4])
        # get a subset of tids
        result = self.db.getTIDList(0, 1, 2, [0])
        self.checkSet(result, [tid3]) # desc order
        result = self.db.getTIDList(1, 1, 2, [1])
        self.checkSet(result, [tid2])
        result = self.db.getTIDList(2, 2, 2, [0])
        self.checkSet(result, [])

    def test_getReplicationTIDList(self):
        self.setNumPartitions(2, True)
        tid1, tid2, tid3, tid4 = self._storeTransactions(4)
        # get tids
        # - all
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 10, 2, 0)
        self.checkSet(result, [tid1, tid3])
        # - one partition
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 10, 2, 0)
        self.checkSet(result, [tid1, tid3])
        # - another partition
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 10, 2, 1)
        self.checkSet(result, [tid2, tid4])
        # - min_tid is inclusive
        result = self.db.getReplicationTIDList(tid3, MAX_TID, 10, 2, 0)
        self.checkSet(result, [tid3])
        # - max tid is inclusive
        result = self.db.getReplicationTIDList(ZERO_TID, tid2, 10, 2, 0)
        self.checkSet(result, [tid1])
        # - limit
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 1, 2, 0)
        self.checkSet(result, [tid1])

    def test_findUndoTID(self):
        self.setNumPartitions(4, True)
        db = self.db
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid5 = self.getNextTID()
        oid1 = self.getOID(1)
        foo = "3" * 20
        bar = "4" * 20
        db.storeData(foo, 'foo', 0)
        db.storeData(bar, 'bar', 0)
        db.unlockData((foo, bar))
        db.storeTransaction(
            tid1, (
                (oid1, foo, None),
            ), None, temporary=False)

        # Undoing oid1 tid1, OK: tid1 is latest
        # Result: current tid is tid1, data_tid is None (undoing object
        # creation)
        self.assertEqual(
            db.findUndoTID(oid1, tid5, tid4, tid1, None),
            (tid1, None, True))

        # Store a new transaction
        db.storeTransaction(
            tid2, (
                (oid1, bar, None),
            ), None, temporary=False)

        # Undoing oid1 tid2, OK: tid2 is latest
        # Result: current tid is tid2, data_tid is tid1
        self.assertEqual(
            db.findUndoTID(oid1, tid5, tid4, tid2, None),
            (tid2, tid1, True))

        # Undoing oid1 tid1, Error: tid2 is latest
        # Result: current tid is tid2, data_tid is -1
        self.assertEqual(
            db.findUndoTID(oid1, tid5, tid4, tid1, None),
            (tid2, None, False))

        # Undoing oid1 tid1 with tid2 being undone in same transaction,
        # OK: tid1 is latest
        # Result: current tid is tid1, data_tid is None (undoing object
        # creation)
        # Explanation of transaction_object: oid1, no data but a data serial
        # to tid1
        self.assertEqual(
            db.findUndoTID(oid1, tid5, tid4, tid1,
                (u64(oid1), None, tid1)),
            (tid1, None, True))

        # Store a new transaction
        db.storeTransaction(
            tid3, (
                (oid1, None, tid1),
            ), None, temporary=False)

        # Undoing oid1 tid1, OK: tid3 is latest with tid1 data
        # Result: current tid is tid2, data_tid is None (undoing object
        # creation)
        self.assertEqual(
            db.findUndoTID(oid1, tid5, tid4, tid1, None),
            (tid3, None, True))

if __name__ == "__main__":
    unittest.main()
