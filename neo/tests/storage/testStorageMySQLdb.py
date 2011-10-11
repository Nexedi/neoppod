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
from neo.lib.exception import DatabaseFailure
from neo.tests.storage.testStorageDBTests import StorageDBTests
from neo.storage.database.mysqldb import MySQLDatabaseManager

NEO_SQL_DATABASE = 'test_mysqldb0'
NEO_SQL_USER = 'test'

class StorageMySQSLdbTests(StorageDBTests):

    def getDB(self, reset=0):
        self.prepareDatabase(number=1, prefix=NEO_SQL_DATABASE[:-1])
        # db manager
        database = '%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE)
        db = MySQLDatabaseManager(database)
        db.setup(reset)
        return db

    def checkCalledQuery(self, query=None, call=0):
        self.assertTrue(len(self.db.conn.mockGetNamedCalls('query')) > call)
        call = self.db.conn.mockGetNamedCalls('query')[call]
        call.checkArgs('BEGIN')

    def test_MySQLDatabaseManagerInit(self):
        db = MySQLDatabaseManager('%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE))
        # init
        self.assertEqual(db.db, NEO_SQL_DATABASE)
        self.assertEqual(db.user, NEO_SQL_USER)
        # & connect
        self.assertTrue(isinstance(db.conn, MySQLdb.connection))
        self.assertFalse(db.isUnderTransaction())

    def test_begin(self):
        # no current transaction
        self.db.conn = Mock({ })
        self.assertFalse(self.db.isUnderTransaction())
        self.db.begin()
        self.checkCalledQuery(query='COMMIT')
        self.assertTrue(self.db.isUnderTransaction())

    def test_commit(self):
        self.db.conn = Mock()
        self.db.begin()
        self.db.commit()
        self.assertEqual(len(self.db.conn.mockGetNamedCalls('commit')), 1)
        self.assertFalse(self.db.isUnderTransaction())

    def test_rollback(self):
        # rollback called and no current transaction
        self.db.conn = Mock({ })
        self.db.under_transaction = True
        self.db.rollback()
        self.assertEqual(len(self.db.conn.mockGetNamedCalls('rollback')), 1)
        self.assertFalse(self.db.isUnderTransaction())

    def test_query1(self):
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
        self.assertEqual(result, expected_result)
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs('QUERY')

    def test_query2(self):
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

    def test_query3(self):
        # OperationalError > raise DatabaseFailure exception
        from MySQLdb import OperationalError
        class FakeConn(object):
            def close(self):
                pass
            def query(*args):
                raise OperationalError(-1, 'this is a test')
        self.db.conn = FakeConn()
        self.assertRaises(DatabaseFailure, self.db.query, 'QUERY')

    def test_escape(self):
        self.assertEqual(self.db.escape('a"b'), 'a\\"b')
        self.assertEqual(self.db.escape("a'b"), "a\\'b")

del StorageDBTests

if __name__ == "__main__":
    unittest.main()
