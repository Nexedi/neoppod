#
# Copyright (C) 2009-2016  Nexedi SA
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
from MySQLdb import OperationalError
from mock import Mock
from neo.lib.exception import DatabaseFailure
from neo.lib.util import p64
from .. import DB_PREFIX, DB_SOCKET, DB_USER
from .testStorageDBTests import StorageDBTests
from neo.storage.database.mysqldb import MySQLDatabaseManager


class StorageMySQLdbTests(StorageDBTests):

    engine = None

    def getDB(self, reset=0):
        self.prepareDatabase(number=1, prefix=DB_PREFIX)
        # db manager
        database = '%s@%s0%s' % (DB_USER, DB_PREFIX, DB_SOCKET)
        db = MySQLDatabaseManager(database, self.engine)
        self.assertEqual(db.db, DB_PREFIX + '0')
        self.assertEqual(db.user, DB_USER)
        db.setup(reset)
        return db

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
        result = self.db.query('SELECT ')
        self.assertEqual(result, expected_result)
        calls = self.db.conn.mockGetNamedCalls('query')
        self.assertEqual(len(calls), 1)
        calls[0].checkArgs('SELECT ')

    def test_query2(self):
        # test the OperationalError exception
        # fake object, raise exception during the first call
        from MySQLdb.constants.CR import SERVER_GONE_ERROR
        class FakeConn(object):
            def query(*args):
                raise OperationalError(SERVER_GONE_ERROR, 'this is a test')
        self.db.conn = FakeConn()
        self.connect_called = False
        def connect_hook():
            # mock object, break raise/connect loop
            self.db.conn = Mock()
            self.connect_called = True
        self.db._connect = connect_hook
        # make a query, exception will be raised then connect() will be
        # called and the second query will use the mock object
        self.db.query('INSERT')
        self.assertTrue(self.connect_called)

    def test_query3(self):
        # OperationalError > raise DatabaseFailure exception
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

    def test_max_allowed_packet(self):
        EXTRA = 2
        # Check EXTRA
        x = "SELECT '%s'" % ('x' * (self.db._max_allowed_packet - 11))
        assert len(x) + EXTRA == self.db._max_allowed_packet
        self.assertRaises(DatabaseFailure, self.db.query, x + ' ')
        self.db.query(x)
        # Check MySQLDatabaseManager._max_allowed_packet
        query_list = []
        query = self.db.query
        self.db.query = lambda query: query_list.append(EXTRA + len(query))
        self.assertEqual(2, max(len(self.db.escape(chr(x)))
                                for x in xrange(256)))
        self.assertEqual(2, len(self.db.escape('\0')))
        self.db.storeData('\0' * 20, '\0' * (2**24-1), 0)
        size, = query_list
        max_allowed = self.db.__class__._max_allowed_packet
        self.assertTrue(max_allowed - 1024 < size <= max_allowed, size)
        # Check storeTransaction
        for count, max_allowed_packet in (7, 64), (6, 65), (1, 215):
            self.db._max_allowed_packet = max_allowed_packet
            del query_list[:]
            self.db.storeTransaction(p64(0),
                ((p64(1<<i),0,None) for i in xrange(10)), None)
            self.assertEqual(max(query_list), max_allowed_packet)
            self.assertEqual(len(query_list), count)


class StorageMySQLdbTokuDBTests(StorageMySQLdbTests):

    engine = "TokuDB"

del StorageDBTests

if __name__ == "__main__":
    unittest.main()
