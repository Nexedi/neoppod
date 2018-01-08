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

from binascii import a2b_hex
from contextlib import contextmanager
import unittest
from neo.lib.util import add64, p64, u64
from neo.lib.protocol import CellStates, ZERO_HASH, ZERO_OID, ZERO_TID, MAX_TID
from .. import NeoUnitTestBase


class StorageDBTests(NeoUnitTestBase):

    _last_ttid = ZERO_TID

    def setUp(self):
        NeoUnitTestBase.setUp(self)

    @property
    def db(self):
        try:
            return self._db
        except AttributeError:
            self.setNumPartitions(1)
            return self._db

    def _tearDown(self, success):
        try:
            self.__dict__.pop('_db', None).close()
        except AttributeError:
            pass
        NeoUnitTestBase._tearDown(self, success)

    def getDB(self, reset=0):
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
                    db.dropPartitions(n)
        db.setNumPartitions(num_partitions)
        self.assertEqual(num_partitions, db.getNumPartitions())
        uuid = self.getStorageUUID()
        db.setUUID(uuid)
        self.assertEqual(uuid, db.getUUID())
        db.changePartitionTable(1,
            [(i, uuid, CellStates.UP_TO_DATE) for i in xrange(num_partitions)],
            reset=True)
        db.commit()

    def checkConfigEntry(self, get_call, set_call, value):
        # generic test for all configuration entries accessors
        self.assertEqual(get_call(), None)
        set_call(value)
        self.assertEqual(get_call(), value)
        set_call(value * 2)
        self.assertEqual(get_call(), value * 2)

    @contextmanager
    def commitTransaction(self, tid, objs, txn, commit=True):
        ttid = txn[-1]
        self.db.storeTransaction(ttid, objs, txn)
        self.db.lockTransaction(tid, ttid)
        yield
        if commit:
            self.db.unlockTransaction(tid, ttid)
            self.db.commit()
        elif commit is not None:
            self.db.abortTransaction(ttid)

    def test_UUID(self):
        db = self.getDB()
        self.checkConfigEntry(db.getUUID, db.setUUID, 123)

    def test_Name(self):
        db = self.getDB()
        self.checkConfigEntry(db.getName, db.setName, 'TEST_NAME')

    def test_getPartitionTable(self):
        db = self.getDB()
        db.setNumPartitions(3)
        uuid1, uuid2 = self.getStorageUUID(), self.getStorageUUID()
        cell1 = (0, uuid1, CellStates.OUT_OF_DATE)
        cell2 = (1, uuid1, CellStates.UP_TO_DATE)
        db.changePartitionTable(1, [cell1, cell2], 1)
        result = db.getPartitionTable()
        self.assertEqual(set(result), {cell1, cell2})

    def getOIDs(self, count):
        return map(p64, xrange(count))

    def getTIDs(self, count):
        tid_list = [self.getNextTID()]
        while len(tid_list) != count:
            tid_list.append(self.getNextTID(tid_list[-1]))
        return tid_list

    def getTransaction(self, oid_list):
        self._last_ttid = ttid = add64(self._last_ttid, 1)
        transaction = oid_list, 'user', 'desc', 'ext', False, ttid
        H = "0" * 20
        object_list = [(oid, self.db.holdData(H, oid, '', 1), None)
                       for oid in oid_list]
        return (transaction, object_list)

    def checkSet(self, list1, list2):
        self.assertEqual(set(list1), set(list2))

    def _test_lockDatabase_open(self):
        raise NotImplementedError

    def test_lockDatabase(self):
        db = self._test_lockDatabase_open()
        self.assertRaises(SystemExit, self._test_lockDatabase_open)
        db.close()
        self._test_lockDatabase_open().close()

    def test_getUnfinishedTIDDict(self):
        tid1, tid2, tid3, tid4 = self.getTIDs(4)
        oid1, oid2 = self.getOIDs(2)
        txn, objs = self.getTransaction([oid1, oid2])
        # one unfinished txn
        with self.commitTransaction(tid2, objs, txn):
            expected = {txn[-1]: tid2}
            self.assertEqual(self.db.getUnfinishedTIDDict(), expected)
            # no changes
            self.db.storeTransaction(tid3, objs, None, False)
            self.assertEqual(self.db.getUnfinishedTIDDict(), expected)
            # a second txn known by objs only
            expected[tid4] = None
            self.db.storeTransaction(tid4, objs, None)
            self.assertEqual(self.db.getUnfinishedTIDDict(), expected)
            self.db.abortTransaction(tid4)
        # nothing pending
        self.assertEqual(self.db.getUnfinishedTIDDict(), {})

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
        # one non-committed version
        with self.commitTransaction(tid1, objs1, txn1):
            self.assertEqual(self.db.getObject(oid1), None)
            self.assertEqual(self.db.getObject(oid1, tid1), None)
            self.assertEqual(self.db.getObject(oid1, before_tid=tid1), None)
        # one committed version
        self.assertEqual(self.db.getObject(oid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NO_NEXT)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
            FOUND_BUT_NOT_VISIBLE)
        # two version available, one non-committed
        with self.commitTransaction(tid2, objs2, txn2):
            self.assertEqual(self.db.getObject(oid1), OBJECT_T1_NO_NEXT)
            self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NO_NEXT)
            self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
                FOUND_BUT_NOT_VISIBLE)
            self.assertEqual(self.db.getObject(oid1, tid2),
                FOUND_BUT_NOT_VISIBLE)
            self.assertEqual(self.db.getObject(oid1, before_tid=tid2),
                OBJECT_T1_NO_NEXT)
        # two committed versions
        self.assertEqual(self.db.getObject(oid1), OBJECT_T2)
        self.assertEqual(self.db.getObject(oid1, tid1), OBJECT_T1_NEXT)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid1),
            FOUND_BUT_NOT_VISIBLE)
        self.assertEqual(self.db.getObject(oid1, tid2), OBJECT_T2)
        self.assertEqual(self.db.getObject(oid1, before_tid=tid2),
            OBJECT_T1_NEXT)

    def test_setPartitionTable(self):
        db = self.getDB()
        db.setNumPartitions(3)
        ptid = 1
        uuid = self.getStorageUUID()
        cell1 = 0, uuid, CellStates.OUT_OF_DATE
        cell2 = 1, uuid, CellStates.UP_TO_DATE
        cell3 = 1, uuid, CellStates.DISCARDED
        # no partition table
        self.assertEqual(list(db.getPartitionTable()), [])
        # set one
        db.changePartitionTable(ptid, [cell1], 1)
        result = db.getPartitionTable()
        self.assertEqual(list(result), [cell1])
        # then another
        db.changePartitionTable(ptid, [cell2], 1)
        result = db.getPartitionTable()
        self.assertEqual(list(result), [cell2])
        # drop discarded cells
        db.changePartitionTable(ptid, [cell2, cell3], 1)
        result = db.getPartitionTable()
        self.assertEqual(list(result), [])

    def test_changePartitionTable(self):
        db = self.getDB()
        ptid = 1
        uuid = self.getStorageUUID()
        cell1 = 0, uuid, CellStates.OUT_OF_DATE
        cell2 = 1, uuid, CellStates.UP_TO_DATE
        cell3 = 1, uuid, CellStates.DISCARDED
        # no partition table
        self.assertEqual(list(db.getPartitionTable()), [])
        # set one
        db.changePartitionTable(ptid, [cell1])
        result = db.getPartitionTable()
        self.assertEqual(list(result), [cell1])
        # add more entries
        db.changePartitionTable(ptid, [cell2])
        result = db.getPartitionTable()
        self.assertEqual(set(result), {cell1, cell2})
        # drop discarded cells
        db.changePartitionTable(ptid, [cell2, cell3])
        result = db.getPartitionTable()
        self.assertEqual(list(result), [cell1])

    def test_commitTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        # nothing in database
        self.assertEqual(self.db.getLastIDs(), (None, {}, {}, None))
        self.assertEqual(self.db.getUnfinishedTIDDict(), {})
        self.assertEqual(self.db.getObject(oid1), None)
        self.assertEqual(self.db.getObject(oid2), None)
        self.assertEqual(self.db.getTransaction(tid1, True), None)
        self.assertEqual(self.db.getTransaction(tid2, True), None)
        self.assertEqual(self.db.getTransaction(tid1, False), None)
        self.assertEqual(self.db.getTransaction(tid2, False), None)
        with self.commitTransaction(tid1, objs1, txn1), \
             self.commitTransaction(tid2, objs2, txn2):
            self.assertEqual(self.db.getTransaction(tid1, True),
                             ([oid1], 'user', 'desc', 'ext', False, p64(1)))
            self.assertEqual(self.db.getTransaction(tid2, True),
                             ([oid2], 'user', 'desc', 'ext', False, p64(2)))
            self.assertEqual(self.db.getTransaction(tid1, False), None)
            self.assertEqual(self.db.getTransaction(tid2, False), None)
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False, p64(1)))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False, p64(2)))
        result = self.db.getTransaction(tid1, False)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False, p64(1)))
        result = self.db.getTransaction(tid2, False)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False, p64(2)))

    def test_deleteTransaction(self):
        txn, objs = self.getTransaction([])
        tid = txn[-1]
        self.db.storeTransaction(tid, objs, txn, False)
        self.assertEqual(self.db.getTransaction(tid), txn)
        self.db.deleteTransaction(tid)
        self.assertEqual(self.db.getTransaction(tid), None)

    def test_deleteObject(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1, oid2])
        txn2, objs2 = self.getTransaction([oid1, oid2])
        tid1 = txn1[-1]
        tid2 = txn2[-1]
        self.db.storeTransaction(tid1, objs1, txn1, False)
        self.db.storeTransaction(tid2, objs2, txn2, False)
        self.assertEqual(self.db.getObject(oid1, tid=tid1),
            (tid1, tid2, 1, "0" * 20, '', None))
        self.db.deleteObject(oid1)
        self.assertIs(self.db.getObject(oid1, tid=tid1), None)
        self.assertIs(self.db.getObject(oid1, tid=tid2), None)
        self.db.deleteObject(oid2, serial=tid1)
        self.assertIs(self.db.getObject(oid2, tid=tid1), False)
        self.assertEqual(self.db.getObject(oid2, tid=tid2),
            (tid2, None, 1, "0" * 20, '', None))

    def test_deleteRange(self):
        np = 4
        self.setNumPartitions(np)
        t1, t2, t3 = map(p64, (1, 2, 3))
        oid_list = self.getOIDs(np * 2)
        for tid in t1, t2, t3:
            txn, objs = self.getTransaction(oid_list)
            self.db.storeTransaction(tid, objs, txn, False)
        def check(offset, tid_list, *tids):
            self.assertEqual(self.db.getReplicationTIDList(ZERO_TID,
                MAX_TID, len(tid_list) + 1, offset), tid_list)
            expected = [(t, oid_list[offset+i]) for t in tids for i in (0, np)]
            self.assertEqual(self.db.getReplicationObjectList(ZERO_TID,
                MAX_TID, len(expected) + 1, offset, ZERO_OID), expected)
        self.db._deleteRange(0, MAX_TID)
        self.db._deleteRange(0, max_tid=ZERO_TID)
        check(0, [], t1, t2, t3)
        self.db._deleteRange(0);             check(0, [])
        self.db._deleteRange(1, t2);         check(1, [t1], t1, t2)
        self.db._deleteRange(2, max_tid=t2); check(2, [], t3)
        self.db._deleteRange(3, t1, t2);     check(3, [t3], t1, t3)

    def test_getTransaction(self):
        oid1, oid2 = self.getOIDs(2)
        tid1, tid2 = self.getTIDs(2)
        txn1, objs1 = self.getTransaction([oid1])
        txn2, objs2 = self.getTransaction([oid2])
        # get from temporary table or not
        with self.commitTransaction(tid1, objs1, txn1), \
             self.commitTransaction(tid2, objs2, txn2, None):
            pass
        result = self.db.getTransaction(tid1, True)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False, p64(1)))
        result = self.db.getTransaction(tid2, True)
        self.assertEqual(result, ([oid2], 'user', 'desc', 'ext', False, p64(2)))
        # get from non-temporary only
        result = self.db.getTransaction(tid1, False)
        self.assertEqual(result, ([oid1], 'user', 'desc', 'ext', False, p64(1)))
        self.assertEqual(self.db.getTransaction(tid2, False), None)

    def test_getObjectHistory(self):
        oid = p64(1)
        tid1, tid2, tid3 = self.getTIDs(3)
        txn1, objs1 = self.getTransaction([oid])
        txn2, objs2 = self.getTransaction([oid])
        txn3, objs3 = self.getTransaction([oid])
        # one revision
        self.db.storeTransaction(tid1, objs1, txn1, False)
        result = self.db.getObjectHistory(oid, 0, 3)
        self.assertEqual(result, [(tid1, 0)])
        result = self.db.getObjectHistory(oid, 1, 1)
        self.assertEqual(result, None)
        # two revisions
        self.db.storeTransaction(tid2, objs2, txn2, False)
        result = self.db.getObjectHistory(oid, 0, 3)
        self.assertEqual(result, [(tid2, 0), (tid1, 0)])
        result = self.db.getObjectHistory(oid, 1, 3)
        self.assertEqual(result, [(tid1, 0)])
        result = self.db.getObjectHistory(oid, 2, 3)
        self.assertEqual(result, None)

    def _storeTransactions(self, count):
        # use OID generator to know result of tid % N
        tid_list = self.getOIDs(count)
        oid = p64(1)
        for tid in tid_list:
            txn, objs = self.getTransaction([oid])
            self.db.storeTransaction(tid, objs, txn, False)
        return tid_list

    def test_getTIDList(self):
        self.setNumPartitions(2, True)
        tid1, tid2, tid3, tid4 = self._storeTransactions(4)
        # get tids
        # - all partitions
        result = self.db.getTIDList(0, 4, [0, 1])
        self.checkSet(result, [tid1, tid2, tid3, tid4])
        # - one partition
        result = self.db.getTIDList(0, 4, [0])
        self.checkSet(result, [tid1, tid3])
        result = self.db.getTIDList(0, 4, [1])
        self.checkSet(result, [tid2, tid4])
        # get a subset of tids
        result = self.db.getTIDList(0, 1, [0])
        self.checkSet(result, [tid3]) # desc order
        result = self.db.getTIDList(1, 1, [1])
        self.checkSet(result, [tid2])
        result = self.db.getTIDList(2, 2, [0])
        self.checkSet(result, [])

    def test_getReplicationTIDList(self):
        self.setNumPartitions(2, True)
        tid1, tid2, tid3, tid4 = self._storeTransactions(4)
        # - one partition
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 10, 0)
        self.checkSet(result, [tid1, tid3])
        # - another partition
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 10, 1)
        self.checkSet(result, [tid2, tid4])
        # - min_tid is inclusive
        result = self.db.getReplicationTIDList(tid3, MAX_TID, 10, 0)
        self.checkSet(result, [tid3])
        # - max tid is inclusive
        result = self.db.getReplicationTIDList(ZERO_TID, tid2, 10, 0)
        self.checkSet(result, [tid1])
        # - limit
        result = self.db.getReplicationTIDList(ZERO_TID, MAX_TID, 1, 0)
        self.checkSet(result, [tid1])

    def test_checkRange(self):
        def check(trans, obj, *args):
            self.assertEqual(trans, self.db.checkTIDRange(*args))
            self.assertEqual(obj, self.db.checkSerialRange(*(args+(ZERO_OID,))))
        self.setNumPartitions(2, True)
        tid1, tid2, tid3, tid4 = self._storeTransactions(4)
        z = 0, ZERO_HASH, ZERO_TID, ZERO_HASH, ZERO_OID
        # - one partition
        check((2, a2b_hex('84320eb8dbbe583f67055c15155ab6794f11654d'), tid3),
            z,
            0, 10, ZERO_TID, MAX_TID)
        # - another partition
        check((2, a2b_hex('1f02f98cf775a9e0ce9252ff5972dce728c4ddb0'), tid4),
            (4, a2b_hex('e5b47bddeae2096220298df686737d939a27d736'), tid4,
                a2b_hex('1e9093698424b5370e19acd2d5fc20dcd56a32cd'), p64(1)),
            1, 10, ZERO_TID, MAX_TID)
        self.assertEqual(
            (3, a2b_hex('b85e2d4914e22b5ad3b82b312b3dc405dc17dcb8'), tid4,
                a2b_hex('1b6d73ecdc064595fe915a5c26da06b195caccaa'), p64(1)),
            self.db.checkSerialRange(1, 10, ZERO_TID, MAX_TID, p64(2)))
        # - min_tid is inclusive
        check((1, a2b_hex('da4b9237bacccdf19c0760cab7aec4a8359010b0'), tid3),
            z,
            0, 10, tid3, MAX_TID)
        # - max tid is inclusive
        x = 1, a2b_hex('b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'), tid1
        check(x, z, 0, 10, ZERO_TID, tid2)
        # - limit
        y = 1, a2b_hex('356a192b7913b04c54574d18c28d46e6395428ab'), tid2
        check(y, x + y[1:], 1, 1, ZERO_TID, MAX_TID)

    def test_findUndoTID(self):
        self.setNumPartitions(4, True)
        db = self.db
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid5 = self.getNextTID()
        oid1 = p64(1)
        foo = db.holdData("3" * 20, oid1, 'foo', 0)
        bar = db.holdData("4" * 20, oid1, 'bar', 0)
        db.releaseData((foo, bar))
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
