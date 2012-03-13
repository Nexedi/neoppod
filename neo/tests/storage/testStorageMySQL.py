#
# Copyright (C) 2009-2012  Nexedi SA
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
import MySQLdb
from mock import Mock
from neo.lib.exception import DatabaseFailure
from .testStorageDBTests import StorageDBTests
from neo.storage.database.mysqldb import MySQLDatabaseManager

NEO_SQL_DATABASE = 'test_mysqldb0'
NEO_SQL_USER = 'test'

class StorageMySQSLdbTests(StorageDBTests):

    def getDB(self, reset=0):
        self.prepareDatabase(number=1, prefix=NEO_SQL_DATABASE[:-1])
        # db manager
        database = '%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE)
        db = MySQLDatabaseManager(database, 0)
        db.setup(reset)
        return db

    def test_MySQLDatabaseManagerInit(self):
        db = MySQLDatabaseManager('%s@%s' % (NEO_SQL_USER, NEO_SQL_DATABASE),
            0)
        # init
        self.assertEqual(db.db, NEO_SQL_DATABASE)
        self.assertEqual(db.user, NEO_SQL_USER)
        # & connect
        self.assertTrue(isinstance(db.conn, MySQLdb.connection))

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
