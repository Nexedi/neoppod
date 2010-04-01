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
import MySQLdb
from mock import Mock
from neo.util import dump, p64, u64
from neo.protocol import CellStates, INVALID_PTID
from neo.tests import NeoTestBase
from neo.exception import DatabaseFailure
from neo.storage.database.mysqldb import MySQLDatabaseManager

NEO_SQL_DATABASE = 'test_mysqldb0'
NEO_SQL_USER = 'test'

class StorageMySQSLdbTests(NeoTestBase):

    def setUp(self):
        self.prepareDatabase(number=1, prefix=NEO_SQL_DATABASE[:-1])
        # db manager
        database = '%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE)
        self.db = MySQLDatabaseManager(database)

    def tearDown(self):
        self.db.close()

    def checkCalledQuery(self, query=None, call=0):
        self.assertTrue(len(self.db.conn.mockGetNamedCalls('query')) > call)
        call = self.db.conn.mockGetNamedCalls('query')[call]
        call.checkArgs('BEGIN')

    def test_01_p64(self):
        self.assertEquals(p64(0), '\0' * 8)
        self.assertEquals(p64(1), '\0' * 7 +'\1')

    def test_02_u64(self):
        self.assertEquals(u64('\0' * 8), 0)
        self.assertEquals(u64('\0' * 7 + '\n'), 10)

    def test_03_MySQLDatabaseManagerInit(self):
        db = MySQLDatabaseManager('%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE))
        # init
        self.assertEquals(db.db, NEO_SQL_DATABASE)
        self.assertEquals(db.user, NEO_SQL_USER)
        # & connect
        self.assertTrue(isinstance(db.conn, MySQLdb.connection))
        self.assertEquals(db.isUnderTransaction(), False)

    def test_05_begin(self):
        # no current transaction
        self.db.conn = Mock({ })
        self.assertEquals(self.db.isUnderTransaction(), False)
        self.db.begin()
        self.checkCalledQuery(query='COMMIT')
        self.assertEquals(self.db.isUnderTransaction(), True)

    def test_06_commit(self):
        self.db.conn = Mock()
        self.db.begin()
        self.db.commit()
        self.assertEquals(len(self.db.conn.mockGetNamedCalls('commit')), 1)
        self.assertEquals(self.db.isUnderTransaction(), False)

    def test_06_rollback(self):
        # rollback called and no current transaction
        self.db.conn = Mock({ })
        self.db.under_transaction = True
        self.db.rollback()
        self.assertEquals(len(self.db.conn.mockGetNamedCalls('rollback')), 1)
        self.assertEquals(self.db.isUnderTransaction(), False)

    def test_07_query1(self):
        # fake result object
        from array import array
        result_object = Mock({
            "num_rows": 1,
            "fetch_row": ((1, 2, array('b', (1, 2, ))), ),
        })
        # expected formatted result
        expected_result = (
            (1, 2, '\x01\x02', ),
        )
        self.db.conn = Mock({ 'store_result': result_object })
        result = self.db.query('QUERY')
        self.assertEquals(result, expected_result)
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEquals(len(calls), 1)
        calls[0].checkArgs('QUERY')

    def test_07_query2(self):
        # test the OperationalError exception
        # fake object, raise exception during the first call
        from MySQLdb import OperationalError
        from MySQLdb.constants.CR import SERVER_GONE_ERROR
        class FakeConn(object):
            def query(*args):
                raise OperationalError(SERVER_GONE_ERROR, 'this is a test')
        self.db.conn = FakeConn()
        self.connect_called = False
        def connect_hook():
            # mock object, break raise/connect loop
            self.db.conn = Mock({'num_rows': 0})
            self.connect_called = True
        self.db._connect = connect_hook
        # make a query, exception will be raised then connect() will be
        # called and the second query will use the mock object
        self.db.query('QUERY')
        self.assertTrue(self.connect_called)

    def test_07_query3(self):
        # OperationalError > raise DatabaseFailure exception
        from MySQLdb import OperationalError
        class FakeConn(object):
            def close(self):
                pass
            def query(*args):
                raise OperationalError(-1, 'this is a test')
        self.db.conn = FakeConn()
        self.assertRaises(DatabaseFailure, self.db.query, 'QUERY')

    def test_08_escape(self):
        self.assertEquals(self.db.escape('a"b'), 'a\\"b')
        self.assertEquals(self.db.escape("a'b"), "a\\'b")

    def test_09_setup(self):
        # create all tables
        self.db.conn = Mock()
        self.db.setup()
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEquals(len(calls), 6)
        # create all tables but drop them first
        self.db.conn = Mock()
        self.db.setup(reset=True)
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEquals(len(calls), 7)

    def test_10_getConfiguration(self):
        # check if a configuration entry is well read
        self.db.setup()
        # doesn't exists, raise
        self.assertRaises(KeyError, self.db.getConfiguration, 'a')
        self.db.query("insert into config values ('a', 'b');")
        result = self.db.getConfiguration('a')
        # exists, check result
        self.assertEquals(result, 'b')

    def test_11_setConfiguration(self):
        # check if a configuration entry is well written
        self.db.setup()
        self.db.setConfiguration('a', 'c')
        result = self.db.getConfiguration('a')
        self.assertEquals(result, 'c')

    def checkConfigEntry(self, get_call, set_call, value):
        # generic test for all configuration entries accessors
        self.db.setup()
        self.assertRaises(KeyError, get_call)
        set_call(value)
        self.assertEquals(get_call(), value)
        set_call(value * 2)
        self.assertEquals(get_call(), value * 2)

    def test_12_UUID(self):
        self.checkConfigEntry(
            get_call=self.db.getUUID,
            set_call=self.db.setUUID,
            value='TEST_VALUE')

    def test_13_NumPartitions(self):
        self.checkConfigEntry(
            get_call=self.db.getNumPartitions,
            set_call=self.db.setNumPartitions,
            value=10)

    def test_14_Name(self):
        self.checkConfigEntry(
            get_call=self.db.getName,
            set_call=self.db.setName,
            value='TEST_NAME')

    def test_15_PTID(self):
        self.checkConfigEntry(
            get_call=self.db.getPTID,
            set_call=self.db.setPTID,
            value=self.getPTID(1))

    def test_16_getPartitionTable(self):
        # insert an entry and check it
        self.db.setup()
        rid, uuid, state = '\x00' * 8, '\x00' * 16, 0
        self.db.query("insert into pt (rid, uuid, state) values ('%s', '%s', %d)" %
                (dump(rid), dump(uuid), state))
        pt = self.db.getPartitionTable()
        self.assertEquals(pt, [(0L, uuid, state)])

    def test_17_getLastOID(self):
        self.db.setup()
        an_oid = '\x01' * 8
        self.db.setLastOID(an_oid)
        result = self.db.getLastOID()
        self.assertEquals(result, an_oid)

    def test_18_getLastTID(self):
        # max TID is in obj table
        self.db.setup()
        self.db.query("""insert into trans (tid, oids, user,
                description, ext) values (1, '', '', '', '')""")
        self.db.query("""insert into trans (tid, oids, user,
                description, ext) values (2, '', '', '', '')""")
        result = self.db.getLastTID()
        self.assertEquals(result, '\x00' * 7 + '\x02')
        # max tid is in ttrans table
        self.db.query("""insert into ttrans (tid, oids, user,
                description, ext) values (3, '', '', '', '')""")
        result = self.db.getLastTID()
        self.assertEquals(result, '\x00' * 7 + '\x03')
        # max tid is in tobj (serial)
        self.db.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        result = self.db.getLastTID()
        self.assertEquals(result, '\x00' * 7 + '\x04')

    def test_19_getUnfinishedTIDList(self):
        self.db.setup()
        self.db.query("""insert into ttrans (tid, oids, user,
                description, ext) values (3, '', '', '', '')""")
        self.db.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        result = self.db.getUnfinishedTIDList()
        expected = ['\x00' * 7 + '\x03', '\x00' * 7 + '\x04']
        self.assertEquals(result, expected)

    def test_20_objectPresent(self):
        self.db.setup()
        oid1, tid1 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x01'
        oid2, tid2 = '\x00' * 7 + '\x02', '\x00' * 7 + '\x02'
        # object not present
        self.assertFalse(self.db.objectPresent(oid1, tid1, all=False))
        self.assertFalse(self.db.objectPresent(oid1, tid1, all=True))
        self.assertFalse(self.db.objectPresent(oid2, tid2, all=False))
        self.assertFalse(self.db.objectPresent(oid2, tid2, all=True))
        # object present in temp table
        self.db.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (%d, %d, 0, 0, '')""" %
                (u64(oid1), u64(tid1)))
        self.assertFalse(self.db.objectPresent(oid1, tid1, all=False))
        self.assertTrue(self.db.objectPresent(oid1,  tid1, all=True))
        self.assertFalse(self.db.objectPresent(oid2, tid2, all=False))
        self.assertFalse(self.db.objectPresent(oid2, tid2, all=True))
        # object present in both table
        self.db.query("""insert into obj (oid, serial, compression,
                checksum, value) values ("%s", "%s", 0, 0, '')""" %
                (u64(oid2), u64(tid2)))
        self.assertFalse(self.db.objectPresent(oid1, tid1, all=False))
        self.assertTrue(self.db.objectPresent(oid1,  tid1, all=True))
        self.assertTrue(self.db.objectPresent(oid2,  tid2, all=False))
        self.assertTrue(self.db.objectPresent(oid2,  tid2, all=True))

    def test_21_getObject(self):
        self.db.setup()
        oid1, tid1 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x01'
        oid2, tid2 = '\x00' * 7 + '\x02', '\x00' * 7 + '\x02'
        # tid specified and object not present
        result = self.db.getObject(oid1, tid1)
        self.assertEquals(result, None)
        # tid specified and object present
        self.db.query("""insert into obj (oid, serial, compression,
                checksum, value) values (%d, %d, 0, 0, '')""" %
                (u64(oid1), u64(tid1)))
        result = self.db.getObject(oid1, tid1)
        self.assertEquals(result, (tid1, None, 0, 0, '', None))
        # before_tid specified, object not present
        result = self.db.getObject(oid2, before_tid=tid2)
        self.assertEquals(result, None)
        # before_tid specified, object present, no next serial
        result = self.db.getObject(oid1, before_tid=tid2)
        self.assertEquals(result, (tid1, None, 0, 0, '', None))
        # before_tid specified, object present, next serial exists
        self.db.query("""insert into obj (oid, serial, compression,
                checksum, value) values (%d, %d, 0, 0, '')""" %
                (u64(oid1), u64(tid2)))
        result = self.db.getObject(oid1, before_tid=tid2)
        self.assertEquals(result, (tid1, tid2, 0, 0, '', None))
        # no tid specified, retreive last object transaction, object unknown
        result = self.db.getObject(oid2)
        self.assertEquals(result, None)
        # same but object found
        result = self.db.getObject(oid1)
        self.assertEquals(result, (tid2, None, 0, 0, '', None))

    def test_23_changePartitionTable(self):
        # two sn, two partitions
        self.db.setup()
        ptid = '1'
        uuid1, uuid2 = '\x00' * 15 + '\x01', '\x00' * 15 + '\x02'
        cells = (
            (0, uuid1, CellStates.DISCARDED),
            (1, uuid2, CellStates.UP_TO_DATE),
        )
        self.db.setPTID(INVALID_PTID)
        # empty table -> insert for second cell
        self.db.changePartitionTable(ptid, cells)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1, dump(uuid2), 1))
        self.assertEquals(self.db.getPTID(), ptid)
        # delete previous entries for a CellStates.DISCARDEDnode
        self.db.query("delete from pt")
        args = (0, dump(uuid1), CellStates.DISCARDED)
        self.db.query('insert into pt (rid, uuid, state) values (%d, "%s", %d)' % args)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (0, dump(uuid1), 4))
        self.assertEquals(self.db.getPTID(), ptid)
        self.db.changePartitionTable(ptid, cells)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1, dump(uuid2), 1))
        self.assertEquals(self.db.getPTID(), ptid)
        # raise exception (config not set), check rollback
        self.db.query("drop table config") # will generate the exception
        args = (0, uuid1, CellStates.DISCARDED)
        self.db.query('insert into pt (rid, uuid, state) values (%d, "%s", %d)' % args)
        self.assertRaises(MySQLdb.ProgrammingError,
                self.db.changePartitionTable, ptid, cells)
        result = self.db.query('select count(*) from pt where rid=0')
        self.assertEquals(len(result), 1)

    def test_22_setGetPartitionTable(self):
        # two sn, two partitions
        self.db.setup()
        ptid = '1'
        uuid1, uuid2 = '\x00' * 15 + '\x01', '\x00' * 15 + '\x02'
        cells = (
            (0, uuid1, CellStates.DISCARDED),
            (1, uuid2, CellStates.UP_TO_DATE),
        )
        self.db.setPTID(INVALID_PTID)
        # not empty table, reset -> clean table first
        args = (0, uuid2, CellStates.UP_TO_DATE)
        self.db.query('insert into pt (rid, uuid, state) values (%d, "%s", %d)' % args)
        self.db.setPartitionTable(ptid, cells)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1, dump(uuid2), 1))
        self.assertEquals(self.db.getPTID(), ptid)
        # delete previous entries for a CellStates.DISCARDEDnode
        self.db.query("delete from pt")
        args = (0, uuid1, CellStates.DISCARDED)
        self.db.query('insert into pt (rid, uuid, state) values (%d, "%s", %d)' % args)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (0, uuid1, 4))
        self.assertEquals(self.db.getPTID(), ptid)
        self.db.setPartitionTable(ptid, cells)
        result = self.db.query('select rid, uuid, state from pt')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1, dump(uuid2), 1))
        self.assertEquals(self.db.getPTID(), ptid)
        # raise exception (config not set), check rollback
        self.db.query("drop table config") # will generate the exception
        args = (0, uuid1, CellStates.DISCARDED)
        self.db.query('insert into pt (rid, uuid, state) values (%d, "%s", %d)' % args)
        self.assertRaises(MySQLdb.ProgrammingError,
                self.db.setPartitionTable, ptid, cells)
        result = self.db.query('select count(*) from pt where rid=0')
        self.assertEquals(len(result), 1)

    def test_23_dropUnfinishedData(self):
        # delete entries from tobj and ttrans
        self.db.setup()
        self.db.query("""insert into tobj (oid, serial, compression,
                checksum, value) values (0, 4, 0, 0, '')""")
        self.db.query("""insert into ttrans (tid, oids, user,
                description, ext) values (3, '', '', '', '')""")
        self.db.dropUnfinishedData()
        result = self.db.query('select * from tobj')
        self.assertEquals(result, ())
        result = self.db.query('select * from ttrans')
        self.assertEquals(result, ())

    def test_24_storeTransaction1(self):
        # data set
        tid = '\x00' * 7 + '\x01'
        oid1, oid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        object_list = ( (oid1, 0, 0, '', None), (oid2, 0, 0, '', None),)
        transaction = ((oid1, oid2), 'user', 'desc', 'ext', False)
        # store objects in temporary table
        self.db.setup()
        self.db.storeTransaction(tid, object_list, transaction=None, temporary=True)
        result = self.db.query('select oid, serial, compression, checksum, value from tobj')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 1, 0, 0, ''))
        self.assertEquals(result[1], (2L, 1, 0, 0, ''))
        result = self.db.query('select * from obj')
        self.assertEquals(len(result), 0)
        # and in obj table
        self.db.setup(reset=True)
        self.db.storeTransaction(tid, object_list, transaction=None, temporary=False)
        result = self.db.query('select oid, serial, compression, checksum, value from obj')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 1, 0, 0, ''))
        self.assertEquals(result[1], (2L, 1, 0, 0, ''))
        result = self.db.query('select * from tobj')
        self.assertEquals(len(result), 0)
        # store transaction in temporary table
        self.db.setup(reset=True)
        self.db.storeTransaction(tid, (), transaction=transaction, temporary=True)
        result = self.db.query('select tid, oids, user, description, ext from ttrans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1L, oid1 + oid2, 'user', 'desc', 'ext',) )
        result = self.db.query('select * from trans')
        self.assertEquals(len(result), 0)
        # and in trans table
        self.db.setup(reset=True)
        self.db.storeTransaction(tid, (), transaction=transaction, temporary=False)
        result = self.db.query('select tid, oids, user, description, ext from trans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1L, oid1 + oid2, 'user', 'desc', 'ext',))
        result = self.db.query('select * from ttrans')
        self.assertEquals(len(result), 0)

    def test_25_askFinishTransaction(self):
        # data set
        tid1, tid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        oid1, oid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        object_list = ( (oid1, 0, 0, '', None), (oid2, 0, 0, '', None),)
        transaction = ((oid1, oid2), 'u', 'd', 'e', False)
        self.db.setup(reset=True)
        # store two temporary transactions
        self.db.storeTransaction(tid1, object_list, transaction, temporary=True)
        self.db.storeTransaction(tid2, object_list, transaction, temporary=True)
        result = self.db.query('select count(*) from tobj')
        self.assertEquals(result[0][0], 4)
        result = self.db.query('select count(*) from ttrans')
        self.assertEquals(result[0][0], 2)
        # finish t1
        self.db.finishTransaction(tid1)
        # t1 should be finished
        result = self.db.query('select * from obj order by oid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 1L, 0, 0, '', None))
        self.assertEquals(result[1], (2L, 1L, 0, 0, '', None))
        result = self.db.query('select * from trans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (1L, 0, oid1 + oid2, 'u', 'd', 'e',))
        # t2 should stay in temporary tables
        result = self.db.query('select * from tobj order by oid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 2L, 0, 0, '', None))
        self.assertEquals(result[1], (2L, 2L, 0, 0, '', None))
        result = self.db.query('select * from ttrans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (2L, 0, oid1 + oid2, 'u', 'd', 'e',))

    def test_26_deleteTransaction(self):
        # data set
        tid1, tid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        oid1, oid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        object_list = ( (oid1, 0, 0, '', None), (oid2, 0, 0, '', None),)
        transaction = ((oid1, oid2), 'u', 'd', 'e', False)
        self.db.setup(reset=True)
        # store two transactions in both state
        self.db.storeTransaction(tid1, object_list, transaction, temporary=True)
        self.db.storeTransaction(tid2, object_list, transaction, temporary=True)
        self.db.storeTransaction(tid1, object_list, transaction, temporary=False)
        self.db.storeTransaction(tid2, object_list, transaction, temporary=False)
        result = self.db.query('select count(*) from tobj')
        self.assertEquals(result[0][0], 4)
        result = self.db.query('select count(*) from ttrans')
        self.assertEquals(result[0][0], 2)
        result = self.db.query('select count(*) from obj')
        self.assertEquals(result[0][0], 4)
        result = self.db.query('select count(*) from trans')
        self.assertEquals(result[0][0], 2)
        # delete t1 (all)
        self.db.deleteTransaction(tid1, all=True)
        # t2 not altered
        result = self.db.query('select * from tobj order by oid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 2L, 0, 0, '', None))
        self.assertEquals(result[1], (2L, 2L, 0, 0, '', None))
        result = self.db.query('select * from ttrans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (2L, 0, oid1 + oid2, 'u', 'd', 'e',))
        result = self.db.query('select * from obj order by oid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 2L, 0, 0, '', None))
        self.assertEquals(result[1], (2L, 2L, 0, 0, '', None))
        result = self.db.query('select * from trans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (2L, 0, oid1 + oid2, 'u', 'd', 'e',))
        # store t1 again
        self.db.storeTransaction(tid1, object_list, transaction, temporary=True)
        self.db.storeTransaction(tid1, object_list, transaction, temporary=False)
        # and remove it but only from temporary tables
        self.db.deleteTransaction(tid1, all=False)
        # t2 not altered and t1 stay in obj/trans tables
        result = self.db.query('select * from tobj order by oid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 2L, 0, 0, '', None))
        self.assertEquals(result[1], (2L, 2L, 0, 0, '', None))
        result = self.db.query('select * from ttrans')
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0], (2L, 0, oid1 + oid2, 'u', 'd', 'e',))
        result = self.db.query('select * from obj order by oid, serial asc')
        self.assertEquals(len(result), 4)
        self.assertEquals(result[0], (1L, 1L, 0, 0, '', None))
        self.assertEquals(result[1], (1L, 2L, 0, 0, '', None))
        self.assertEquals(result[2], (2L, 1L, 0, 0, '', None))
        self.assertEquals(result[3], (2L, 2L, 0, 0, '', None))
        result = self.db.query('select * from trans order by tid asc')
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], (1L, 0, oid1 + oid2, 'u', 'd', 'e',))
        self.assertEquals(result[1], (2L, 0, oid1 + oid2, 'u', 'd', 'e',))

    def test_27_getTransaction(self):
        # data set
        tid1, tid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        oid1, oid2 = '\x00' * 7 + '\x01', '\x00' * 7 + '\x02'
        oids = [oid1, oid2]
        transaction = ((oid1, oid2), 'u', 'd', 'e', False)
        self.db.setup(reset=True)
        # store t1 in temporary and t2 in persistent tables
        self.db.storeTransaction(tid1, (), transaction, temporary=True)
        self.db.storeTransaction(tid2, (), transaction, temporary=False)
        # get t1 from all -> OK
        t = self.db.getTransaction(tid1, all=True)
        self.assertEquals(t, (oids, 'u', 'd', 'e', False))
        # get t1 from no tmp only -> fail
        t = self.db.getTransaction(tid1, all=False)
        self.assertEquals(t, None)
        # get t2 from all or not -> always OK
        t = self.db.getTransaction(tid2, all=True)
        self.assertEquals(t, (oids, 'u', 'd', 'e', False))
        t = self.db.getTransaction(tid2, all=False)
        self.assertEquals(t, (oids, 'u', 'd', 'e', False))
        # store wrong oids -> DatabaseFailure
        self.db.setup(reset=True)
        self.db.query("""replace into trans (tid, oids, user, description, ext)
        values ('%s', '%s', 'u', 'd', 'e')""" % (u64(tid1), 'OIDs_'))
        self.assertRaises(DatabaseFailure, self.db.getTransaction, tid1)

    def test_28_getOIDList(self):
        # there are two partitions and two objects in each of them
        # o1 & o3 in p1, o2 & o4 in p2
        self.db.setup(reset=True)
        tid = '\x00' * 7 + '\x01'
        oid1, oid2, oid3, oid4 = ['\x00' * 7 + chr(i) for i in xrange(4)]
        for oid in (oid1, oid2, oid3, oid4):
            self.db.query("replace into obj values (%d, %d, 0, 0, '', NULL)" %
                (u64(oid), u64(tid)))
        # get all oids for all partitions
        result = self.db.getOIDList(0, 4, 2, (0, 1))
        self.assertEquals(result, [oid4, oid3, oid2, oid1])
        # get all oids but from the second with a limit a two
        result = self.db.getOIDList(1, 2, 2, (0, 1))
        self.assertEquals(result, [oid3, oid2])
        # get all oids for the first partition
        result = self.db.getOIDList(0, 2, 2, (0, ))
        self.assertEquals(result, [oid3, oid1])
        # get all oids for the second partition with a limit of one
        result = self.db.getOIDList(0, 1, 2, (1, ))
        self.assertEquals(result, [oid4])
        # get all oids for the second partition with an offset of 3 > nothing
        result = self.db.getOIDList(3, 2, 2, (1, ))
        self.assertEquals(result, [])
        # get oids for an inexsitant partition -> nothing
        result = self.db.getOIDList(0, 2, 2, (3, ))
        self.assertEquals(result, [])

    def test_29_getObjectHistory(self):
        # there is one object with 4 revisions
        self.db.setup(reset=True)
        tids = ['\x00' * 7 + chr(i) for i in xrange(4)]
        oid = '\x00' * 8
        for tid in tids:
            self.db.query("replace into obj values (%d, %d, 0, 0, '', NULL)" %
                    (u64(oid), u64(tid)))
        # unkwown object
        result = self.db.getObjectHistory(oid='\x01' * 8)
        self.assertEquals(result, None)
        # retreive all revisions
        result = self.db.getObjectHistory(oid=oid, offset=0, length=4)
        expected = [(tid, 0) for tid in reversed(tids)]
        self.assertEquals(result, expected)
        # get from the second, limit to 2 revisions
        result = self.db.getObjectHistory(oid=oid, offset=1, length=2)
        expected = [(tids[2], 0), (tids[1], 0)]
        self.assertEquals(result, expected)
        # get from the fifth -> nothing
        result = self.db.getObjectHistory(oid=oid, offset=4, length=2)
        self.assertEquals(result, None)

    def test_28_getTIDList(self):
        # same as for getOIDList, 2 partitions and 4 transactions
        self.db.setup(reset=True)
        tids = ['\x00' * 7 + chr(i) for i in xrange(4)]
        tid1, tid2, tid3, tid4 = tids
        for tid in tids:
            self.db.query("""replace into trans values (%d, '', 'u', 'd', 'e',
                False)""" % (u64(tid)))
        # get all tids for all partitions
        result = self.db.getTIDList(0, 4, 2, (0, 1))
        self.assertEquals(result, [tid4, tid3, tid2, tid1])
        # get all tids but from the second with a limit a two
        result = self.db.getTIDList(1, 2, 2, (0, 1))
        self.assertEquals(result, [tid3, tid2])
        # get all tids for the first partition
        result = self.db.getTIDList(0, 2, 2, (0, ))
        self.assertEquals(result, [tid3, tid1])
        # get all tids for the second partition with a limit of one
        result = self.db.getTIDList(0, 1, 2, (1, ))
        self.assertEquals(result, [tid4])
        # get all tids for the second partition with an offset of 3 > nothing
        result = self.db.getTIDList(3, 2, 2, (1, ))
        self.assertEquals(result, [])
        # get tids for an inexsitant partition -> nothing
        result = self.db.getTIDList(0, 2, 2, (3, ))
        self.assertEquals(result, [])

    def test_29_getTIDListPresent(self):
        # 4 sample transactions
        self.db.setup(reset=True)
        tid = '\x00' * 7 + '\x01'
        tid1, tid2, tid3, tid4 = ['\x00' * 7 + chr(i) for i in xrange(4)]
        for tid in (tid1, tid2, tid3, tid4):
            self.db.query("""replace into trans values (%d, '', 'u', 'd', 'e',
                    False)""" % (u64(tid)))
        # all match
        result = self.db.getTIDListPresent((tid1, tid2, tid3, tid4))
        expected = [tid1, tid2, tid3, tid4]
        self.assertEquals(sorted(result), expected)
        # none match
        result = self.db.getTIDListPresent(('\x01' * 8, '\x02' * 8))
        self.assertEquals(result, [])
        # some match
        result = self.db.getTIDListPresent((tid1, tid3))
        self.assertEquals(sorted(result), [tid1, tid3])

    def test_30_getSerialListPresent(self):
        # 4 sample objects
        self.db.setup(reset=True)
        tids = ['\x00' * 7 + chr(i) for i in xrange(4)]
        tid1, tid2, tid3, tid4 = tids
        oid = '\x00' * 8
        for tid in tids:
            self.db.query("replace into obj values (%d, %d, 0, 0, '', NULL)" \
                % (u64(oid), u64(tid)))
        # all match
        result = self.db.getSerialListPresent(oid, tids)
        expected = list(tids)
        self.assertEquals(sorted(result), expected)
        # none match
        result = self.db.getSerialListPresent(oid, ('\x01' * 8, '\x02' * 8))
        self.assertEquals(result, [])
        # some match
        result = self.db.getSerialListPresent(oid, (tid1, tid3))
        self.assertEquals(sorted(result), [tid1, tid3])

    def test__getObjectData(self):
        db = self.db
        db.setup(reset=True)
        tid0 = self.getNextTID()
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        assert tid0 < tid1 < tid2 < tid3
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        oid3 = self.getOID(3)
        db.storeTransaction(
            tid1, (
                (oid1, 0, 0, 'foo', None),
                (oid2, None, None, None, u64(tid0)),
                (oid3, None, None, None, u64(tid2)),
            ), None, temporary=False)
        db.storeTransaction(
            tid2, (
                (oid1, None, None, None, u64(tid1)),
                (oid2, None, None, None, u64(tid1)),
                (oid3, 0, 0, 'bar', None),
            ), None, temporary=False)

        original_getObjectData = db._getObjectData
        def _getObjectData(*args, **kw):
            call_counter.append(1)
            return original_getObjectData(*args, **kw)
        db._getObjectData = _getObjectData

        # NOTE: all tests are done as if values were fetched by _getObject, so
        # there is already one indirection level.

        # oid1 at tid1: data is immediately found
        call_counter = []
        self.assertEqual(
            db._getObjectData(u64(oid1), u64(tid1), u64(tid3)),
            (u64(tid1), 0, 0, 'foo'))
        self.assertEqual(sum(call_counter), 1)

        # oid2 at tid1: missing data in table, raise IndexError on next
        # recursive call
        call_counter = []
        self.assertRaises(IndexError, db._getObjectData, u64(oid2), u64(tid1),
            u64(tid3))
        self.assertEqual(sum(call_counter), 2)

        # oid3 at tid1: data_serial grater than row's tid, raise ValueError
        # on next recursive call - even if data does exist at that tid (see
        # "oid3 at tid2" case below)
        call_counter = []
        self.assertRaises(ValueError, db._getObjectData, u64(oid3), u64(tid1),
            u64(tid3))
        self.assertEqual(sum(call_counter), 2)
        # Same with wrong parameters (tid0 < tid1)
        call_counter = []
        self.assertRaises(ValueError, db._getObjectData, u64(oid3), u64(tid1),
            u64(tid0))
        self.assertEqual(sum(call_counter), 1)
        # Same with wrong parameters (tid1 == tid1)
        call_counter = []
        self.assertRaises(ValueError, db._getObjectData, u64(oid3), u64(tid1),
            u64(tid1))
        self.assertEqual(sum(call_counter), 1)

        # oid1 at tid2: data is found after ons recursive call
        call_counter = []
        self.assertEqual(
            db._getObjectData(u64(oid1), u64(tid2), u64(tid3)),
            (u64(tid1), 0, 0, 'foo'))
        self.assertEqual(sum(call_counter), 2)

        # oid2 at tid2: missing data in table, raise IndexError after two
        # recursive calls
        call_counter = []
        self.assertRaises(IndexError, db._getObjectData, u64(oid2), u64(tid2),
            u64(tid3))
        self.assertEqual(sum(call_counter), 3)

        # oid3 at tid2: data is immediately found
        call_counter = []
        self.assertEqual(
            db._getObjectData(u64(oid3), u64(tid2), u64(tid3)),
            (u64(tid2), 0, 0, 'bar'))
        self.assertEqual(sum(call_counter), 1)

    def test__getDataTIDFromData(self):
        db = self.db
        db.setup(reset=True)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        oid1 = self.getOID(1)
        db.storeTransaction(
            tid1, (
                (oid1, 0, 0, 'foo', None),
            ), None, temporary=False)
        db.storeTransaction(
            tid2, (
                (oid1, None, None, None, u64(tid1)),
            ), None, temporary=False)

        self.assertEqual(
            db._getDataTIDFromData(u64(oid1),
                db._getObject(u64(oid1), tid=u64(tid1))),
            (u64(tid1), u64(tid1)))
        self.assertEqual(
            db._getDataTIDFromData(u64(oid1),
                db._getObject(u64(oid1), tid=u64(tid2))),
            (u64(tid2), u64(tid1)))

    def test__getDataTID(self):
        db = self.db
        db.setup(reset=True)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        oid1 = self.getOID(1)
        db.storeTransaction(
            tid1, (
                (oid1, 0, 0, 'foo', None),
            ), None, temporary=False)
        db.storeTransaction(
            tid2, (
                (oid1, None, None, None, u64(tid1)),
            ), None, temporary=False)

        self.assertEqual(
            db._getDataTID(u64(oid1), tid=u64(tid1)),
            (u64(tid1), u64(tid1)))
        self.assertEqual(
            db._getDataTID(u64(oid1), tid=u64(tid2)),
            (u64(tid2), u64(tid1)))

    def test__findUndoTID(self):
        db = self.db
        db.setup(reset=True)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        oid1 = self.getOID(1)
        db.storeTransaction(
            tid1, (
                (oid1, 0, 0, 'foo', None),
            ), None, temporary=False)

        # Undoing oid1 tid1, OK: tid1 is latest
        # Result: current tid is tid1, data_tid is None (undoing object
        # creation)
        self.assertEqual(
            db._findUndoTID(u64(oid1), u64(tid4), u64(tid1), None),
            (tid1, None))

        # Store a new transaction
        db.storeTransaction(
            tid2, (
                (oid1, 0, 0, 'bar', None),
            ), None, temporary=False)

        # Undoing oid1 tid2, OK: tid2 is latest
        # Result: current tid is tid2, data_tid is tid1
        self.assertEqual(
            db._findUndoTID(u64(oid1), u64(tid4), u64(tid2), None),
            (tid2, u64(tid1)))

        # Undoing oid1 tid1, Error: tid2 is latest
        # Result: current tid is tid2, data_tid is -1
        self.assertEqual(
            db._findUndoTID(u64(oid1), u64(tid4), u64(tid1), None),
            (tid2, -1))

        # Undoing oid1 tid1 with tid2 being undone in same transaction,
        # OK: tid1 is latest
        # Result: current tid is tid2, data_tid is None (undoing object
        # creation)
        # Explanation of transaction_object: oid1, no data but a data serial
        # to tid1
        self.assertEqual(
            db._findUndoTID(u64(oid1), u64(tid4), u64(tid1),
                (u64(oid1), None, None, None, u64(tid1))),
            (tid4, None))

        # Store a new transaction
        db.storeTransaction(
            tid3, (
                (oid1, None, None, None, u64(tid1)),
            ), None, temporary=False)

        # Undoing oid1 tid1, OK: tid3 is latest with tid1 data
        # Result: current tid is tid2, data_tid is None (undoing object
        # creation)
        self.assertEqual(
            db._findUndoTID(u64(oid1), u64(tid4), u64(tid1), None),
            (tid3, None))

    def test_getTransactionUndoData(self):
        db = self.db
        db.setup(reset=True)
        tid1 = self.getNextTID()
        tid2 = self.getNextTID()
        tid3 = self.getNextTID()
        tid4 = self.getNextTID()
        tid5 = self.getNextTID()
        assert tid1 < tid2 < tid3 < tid4 < tid5
        oid1 = self.getOID(1)
        oid2 = self.getOID(2)
        oid3 = self.getOID(3)
        oid4 = self.getOID(4)
        oid5 = self.getOID(5)
        db.storeTransaction(
            tid1, (
                (oid1, 0, 0, 'foo1', None),
                (oid2, 0, 0, 'foo2', None),
                (oid3, 0, 0, 'foo3', None),
                (oid4, 0, 0, 'foo5', None),
            ), None, temporary=False)
        db.storeTransaction(
            tid2, (
                (oid1, 0, 0, 'bar1', None),
                (oid2, None, None, None, None),
                (oid3, 0, 0, 'bar3', None),
            ), None, temporary=False)
        db.storeTransaction(
            tid3, (
                (oid3, 0, 0, 'baz3', None),
                (oid5, 0, 0, 'foo6', None),
            ), None, temporary=False)

        def getObjectFromTransaction(tid, oid):
            return None

        self.assertEqual(
            db.getTransactionUndoData(tid4, tid2, getObjectFromTransaction),
            {
                oid1: (tid2, u64(tid1)), # can be undone
                oid2: (tid2, u64(tid1)), # can be undone (creation redo)
                oid3: (tid3, -1),        # cannot be undone
                # oid4 & oid5: not present because not ins undone transaction
            })

        # Cannot undo future transaction
        self.assertRaises(ValueError, db.getTransactionUndoData, tid4, tid5,
            getObjectFromTransaction)

if __name__ == "__main__":
    unittest.main()
